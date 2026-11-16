"""Persistence layer for the FastAPI web platform."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

from ..avro_codec import DEFAULT_SCHEMA_V1, fingerprint
from ..db_common import get_conn

_DDL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    fp                  TEXT PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('writer', 'reader')),
    version             INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    UNIQUE(kind, version)
);
CREATE TABLE IF NOT EXISTS writer_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    effective_fp        TEXT NOT NULL,
    effective_version   INTEGER NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id                  TEXT PRIMARY KEY,
    writer_fp           TEXT NOT NULL,
    writer_version      INTEGER NOT NULL,
    payload_json        TEXT NOT NULL,
    blob_b64            TEXT NOT NULL,
    ingested_at         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS writer_switches (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    new_fp              TEXT NOT NULL,
    schema_json         TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    target              TEXT NOT NULL DEFAULT 'local',
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reader_switches (
    switch_id           TEXT PRIMARY KEY,
    analyzer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    writer_fp           TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decode_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL,
    analyzer_id         TEXT NOT NULL,
    reader_version      INTEGER,
    decoded_ok          INTEGER NOT NULL,
    datum_json          TEXT,
    error               TEXT,
    decoded_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sim_config (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    delay_ms            INTEGER NOT NULL DEFAULT 0,
    drop_writer_switch  INTEGER NOT NULL DEFAULT 0,
    drop_reader_switch  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS qc_limits (
    project             TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    lower_limit         REAL NOT NULL,
    upper_limit         REAL NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS qc_runs (
    run_id              TEXT PRIMARY KEY,
    project             TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('scanning', 'published', 'failed')),
    limits_version      INTEGER,
    lower_limit         REAL,
    upper_limit         REAL,
    event_count         INTEGER NOT NULL DEFAULT 0,
    event_set_hash      TEXT,
    reader_a_version    INTEGER,
    reader_b_version    INTEGER,
    usable_count        INTEGER NOT NULL DEFAULT 0,
    issue_count         INTEGER NOT NULL DEFAULT 0,
    error               TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS qc_run_events (
    run_id              TEXT NOT NULL,
    event_id            TEXT NOT NULL,
    event_seq           INTEGER NOT NULL,
    verdict             TEXT NOT NULL CHECK (verdict IN ('usable', 'issue')),
    reasons_json        TEXT NOT NULL,
    a_value             REAL,
    b_value             REAL,
    a_reader_version    INTEGER,
    b_reader_version    INTEGER,
    a_ok                INTEGER NOT NULL,
    b_ok                INTEGER NOT NULL,
    PRIMARY KEY (run_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_qc_runs_project ON qc_runs(project);
"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def init(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(_DDL)
        fp = fingerprint(DEFAULT_SCHEMA_V1)
        conn.execute(
            "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
            "VALUES (?, 'writer', 1, ?, ?)",
            (fp, json.dumps(DEFAULT_SCHEMA_V1), _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO writer_state(id, effective_fp, effective_version, updated_at) "
            "VALUES (1, ?, 1, ?)", (fp, _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO sim_config(id, delay_ms, drop_writer_switch, "
            "drop_reader_switch) VALUES (1, 0, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


# ----- schemas --------------------------------------------------------------

def register_schema(conn: sqlite3.Connection, kind: str, version: int,
                    schema: Dict[str, Any]) -> str:
    fp = fingerprint(schema)
    conn.execute(
        "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (fp, kind, version, json.dumps(schema), _now()),
    )
    return fp


def next_version(conn: sqlite3.Connection, kind: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS m FROM schema_versions WHERE kind = ?", (kind,)
    ).fetchone()
    # Both formats start at v1 (the default schema shipped in every service),
    # so the first *switch* must propose v2.
    return max(2, row["m"] + 1)


def get_schema_by_fp(conn: sqlite3.Connection, fp: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT schema_json FROM schema_versions WHERE fp = ?", (fp,)
    ).fetchone()
    return json.loads(row["schema_json"]) if row else None


def all_writer_schemas(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT fp, version, schema_json, created_at FROM schema_versions "
        "WHERE kind = 'writer' ORDER BY version"
    ).fetchall()
    return [dict(fp=r["fp"], version=r["version"],
                 schema=json.loads(r["schema_json"]), created_at=r["created_at"])
            for r in rows]


# ----- writer state ---------------------------------------------------------

def get_writer_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM writer_state WHERE id = 1").fetchone()


def set_writer_state(conn: sqlite3.Connection, fp: str, version: int) -> None:
    conn.execute(
        "UPDATE writer_state SET effective_fp = ?, effective_version = ?, updated_at = ? "
        "WHERE id = 1", (fp, version, _now()),
    )


# ----- events ---------------------------------------------------------------

def insert_event(conn: sqlite3.Connection, *, writer_fp: str, writer_version: int,
                 payload: Dict[str, Any], blob_b64: str) -> str:
    event_id = "evt_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO events(id, writer_fp, writer_version, payload_json, blob_b64, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (event_id, writer_fp, writer_version, json.dumps(payload, ensure_ascii=False),
         blob_b64, _now()),
    )
    return event_id


def list_events(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events ORDER BY ingested_at DESC, rowid DESC"
    ).fetchall()


def get_event(conn: sqlite3.Connection, event_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()


# ----- switches -------------------------------------------------------------

def insert_writer_switch(conn: sqlite3.Connection, *, version: int, base_version: int,
                         new_fp: str, schema: Dict[str, Any], target: str) -> str:
    switch_id = "wsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO writer_switches(switch_id, version, base_version, new_fp, schema_json, "
        "status, target, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', "
        "?, NULL, ?, ?)",
        (switch_id, version, base_version, new_fp, json.dumps(schema), target, _now(), _now()),
    )
    return switch_id


def update_writer_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE writer_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_writer_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_writer_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE status = 'pending' ORDER BY version"
    ).fetchall()


def insert_reader_switch(conn: sqlite3.Connection, *, analyzer_id: str, version: int,
                         base_version: int, schema: Dict[str, Any], writer_fp: str) -> str:
    switch_id = "rsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO reader_switches(switch_id, analyzer_id, version, base_version, schema_json, "
        "writer_fp, status, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, "
        "'pending', NULL, ?, ?)",
        (switch_id, analyzer_id, version, base_version, json.dumps(schema),
         writer_fp, _now(), _now()),
    )
    return switch_id


def update_reader_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE reader_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_reader_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_reader_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE status = 'pending' ORDER BY created_at"
    ).fetchall()


# ----- decode results -------------------------------------------------------

def insert_decode_result(conn: sqlite3.Connection, *, event_id: str, analyzer_id: str,
                         reader_version: Optional[int], decoded_ok: bool,
                         datum: Any, error: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO decode_results(event_id, analyzer_id, reader_version, decoded_ok, "
        "datum_json, error, decoded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, analyzer_id, reader_version, int(decoded_ok),
         json.dumps(datum, ensure_ascii=False) if datum is not None else None, error, _now()),
    )


def latest_decode_results(conn: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM decode_results ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()


# ----- simulation -----------------------------------------------------------

def get_sim(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM sim_config WHERE id = 1").fetchone()
    return {
        "delay_ms": row["delay_ms"],
        "drop_writer_switch": bool(row["drop_writer_switch"]),
        "drop_reader_switch": bool(row["drop_reader_switch"]),
    }


def set_sim(conn: sqlite3.Connection, delay_ms: int, drop_writer: bool,
            drop_reader: bool) -> None:
    conn.execute(
        "UPDATE sim_config SET delay_ms = ?, drop_writer_switch = ?, "
        "drop_reader_switch = ? WHERE id = 1",
        (max(0, int(delay_ms)), int(bool(drop_writer)), int(bool(drop_reader))),
    )


def consume_drop_flag(conn: sqlite3.Connection, flag: str) -> bool:
    """One-shot read-and-reset inside the same transaction."""
    assert flag in {"drop_writer_switch", "drop_reader_switch"}
    row = conn.execute(f"SELECT {flag} AS v FROM sim_config WHERE id = 1").fetchone()
    armed = bool(row["v"])
    if armed:
        conn.execute(f"UPDATE sim_config SET {flag} = 0 WHERE id = 1")
    return armed


# ----- project QC limits -----------------------------------------------------

def get_qc_limit(conn: sqlite3.Connection, project: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM qc_limits WHERE project = ?", (project,)).fetchone()


def list_qc_limits(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM qc_limits ORDER BY project").fetchall()


def upsert_qc_limit(conn: sqlite3.Connection, *, project: str,
                    lower_limit: float, upper_limit: float) -> sqlite3.Row:
    """Insert v1 or bump the version. Every change is a new limits version so
    previously published conclusions can be told they are stale."""
    row = get_qc_limit(conn, project)
    version = 1 if row is None else row["version"] + 1
    conn.execute(
        "INSERT INTO qc_limits(project, version, lower_limit, upper_limit, updated_at) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT(project) DO UPDATE SET "
        "version=excluded.version, lower_limit=excluded.lower_limit, "
        "upper_limit=excluded.upper_limit, updated_at=excluded.updated_at",
        (project, version, float(lower_limit), float(upper_limit), _now()),
    )
    return get_qc_limit(conn, project)


# ----- QC runs ---------------------------------------------------------------

def insert_qc_run(conn: sqlite3.Connection, *, run_id: str, project: str,
                  limits_version: int, lower_limit: float, upper_limit: float,
                  event_count: int, event_set_hash: str,
                  reader_a_version: int, reader_b_version: int) -> None:
    conn.execute(
        "INSERT INTO qc_runs(run_id, project, status, limits_version, lower_limit, "
        "upper_limit, event_count, event_set_hash, reader_a_version, reader_b_version, "
        "usable_count, issue_count, error, created_at, updated_at) "
        "VALUES (?, ?, 'scanning', ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, ?, ?)",
        (run_id, project, limits_version, float(lower_limit), float(upper_limit),
         event_count, event_set_hash, reader_a_version, reader_b_version,
         _now(), _now()),
    )


def update_qc_run(conn: sqlite3.Connection, run_id: str, *, status: str,
                  usable_count: Optional[int] = None,
                  issue_count: Optional[int] = None,
                  error: Optional[str] = None) -> None:
    sets = ["status = ?", "updated_at = ?"]
    params: List[Any] = [status, _now()]
    if usable_count is not None:
        sets.append("usable_count = ?")
        params.append(usable_count)
    if issue_count is not None:
        sets.append("issue_count = ?")
        params.append(issue_count)
    if error is not None:
        sets.append("error = ?")
        params.append(error)
    params.append(run_id)
    conn.execute(f"UPDATE qc_runs SET {', '.join(sets)} WHERE run_id = ?", params)


def get_qc_run(conn: sqlite3.Connection, run_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM qc_runs WHERE run_id = ?",
                        (run_id,)).fetchone()


def scanning_qc_runs(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM qc_runs WHERE status = 'scanning' ORDER BY rowid"
    ).fetchall()


def latest_qc_runs(conn: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM qc_runs ORDER BY rowid DESC LIMIT ?", (limit,)).fetchall()


def insert_qc_run_event(conn: sqlite3.Connection, *, run_id: str, event_id: str,
                        event_seq: int, verdict: str, reasons: List[str],
                        a_value: Optional[float], b_value: Optional[float],
                        a_reader_version: Optional[int],
                        b_reader_version: Optional[int],
                        a_ok: bool, b_ok: bool) -> None:
    conn.execute(
        "INSERT INTO qc_run_events(run_id, event_id, event_seq, verdict, reasons_json, "
        "a_value, b_value, a_reader_version, b_reader_version, a_ok, b_ok) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, event_id, event_seq, verdict,
         json.dumps(reasons, ensure_ascii=False), a_value, b_value,
         a_reader_version, b_reader_version, int(a_ok), int(b_ok)),
    )


def qc_run_events(conn: sqlite3.Connection, run_id: str) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM qc_run_events WHERE run_id = ? ORDER BY event_seq",
        (run_id,)).fetchall()


def project_event_ids(conn: sqlite3.Connection, project: str) -> List[str]:
    """Full retained event set for one project, identified by the ``project``
    field of the ingested Avro payload (authoritative record at ingest time)."""
    rows = conn.execute("SELECT id, payload_json FROM events").fetchall()
    ids = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("project") == project:
            ids.append(row["id"])
    return sorted(ids)
