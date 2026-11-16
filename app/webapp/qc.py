"""Project quality-control (QC) scans.

The warehouse engineer must deliver *trustworthy* measurements to the
analytics group. For one project the engineer configures non-negative lower /
upper limits, then starts a QC scan which:

  1. snapshots the complete retained event set of that project, the current
     limits *version* and both analyzers' effective reader versions;
  2. asks analyzer-a and analyzer-b to decode every event's **original Avro
     bytes** with their own currently-effective read formats;
  3. classifies every event by event id:
       - a side's measurement is missing / non-numeric,
       - the two sides disagree,
       - either finite numeric value violates the limits,
     every such event is an *issue* with per-event reasons; the rest are
     *usable*;
  4. publishes all event verdicts in one atomic transaction.

Hard guarantees
---------------
* If either analyzer cannot decode an event (transport failure, non-2xx
  response or ``decoded_ok == False``) the whole run is marked ``failed`` and
  **no partial result is ever published** (``qc_run_events`` rows are written
  only inside the publishing transaction).
* Ingestion, limit edits and reader-format switches stay available while a
  scan runs. A published run is bound to one full event set, one limits
  version and both effective reader versions. Any later change makes the old
  conclusion display as *stale (needs recomputation)* via a dynamic check at
  read time; a still-scanning run can never be displayed as current.
* On restart every leftover ``scanning`` run is marked ``failed`` at startup,
  so a half-finished scan can never surface as a conclusion.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

from . import db
from .analyzer_client import ANALYZER_ORDER, AnalyzerClient

# Decoded numeric values are compared exactly (Python numeric promotion keeps
# int/float values equal, e.g. 7 == 7.0). A tiny tolerance only absorbs
# representation noise from separately decoded JSON round-trips, never real
# instrument disagreement.
_VALUE_REL_TOL = 1e-12
_MEASUREMENT_KEY = "measurement"


class QcError(Exception):
    """A QC precondition rejected the request."""


class _BaselineDrift(Exception):
    """Internal control-flow signal: the run was already marked failed."""


# ---------------------------------------------------------------------------
# Pure classification logic (unit-tested directly)
# ---------------------------------------------------------------------------

def is_finite_number(value: Any) -> bool:
    """Booleans are numbers in Python but never valid measurements here;
    NaN / Infinity are rejected as non-numeric as well."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and math.isfinite(float(value))


def _fmt(value: Any) -> str:
    if isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return json.dumps(value, ensure_ascii=False, default=str)


def values_agree(a: float, b: float) -> bool:
    return math.isclose(
        float(a), float(b), rel_tol=_VALUE_REL_TOL,
        abs_tol=_VALUE_REL_TOL,
    )


def classify_measurement(measurement_a: Any, measurement_b: Any,
                         lower_limit: float, upper_limit: float
                         ) -> Tuple[str, List[str], Optional[float], Optional[float]]:
    """Classify one event from both sides' decoded measurements.

    Returns ``(verdict, reasons, numeric_a, numeric_b)`` where the numeric
    slots are ``None`` when that side's value is missing/non-finite.
    """
    reasons: List[str] = []
    numeric: Dict[str, Optional[float]] = {}

    for side, value in (("analyzer-a", measurement_a),
                        ("analyzer-b", measurement_b)):
        if value is None:
            reasons.append(f"[{side}] 测值缺失")
            numeric[side] = None
            continue
        if not is_finite_number(value):
            reasons.append(f"[{side}] 测值非数值: {_fmt(value)}")
            numeric[side] = None
            continue
        num = float(value)
        numeric[side] = num
        if num < lower_limit or num > upper_limit:
            reasons.append(
                f"[{side}] 测值 {_fmt(value)} 超出非负限值 "
                f"[{lower_limit:g}, {upper_limit:g}]")

    a_num, b_num = numeric["analyzer-a"], numeric["analyzer-b"]
    if a_num is not None and b_num is not None and not values_agree(a_num, b_num):
        reasons.append(
            f"两方测值不一致: analyzer-a={_fmt(a_num)} "
            f"analyzer-b={_fmt(b_num)}")

    return ("issue" if reasons else "usable", reasons, a_num, b_num)


def event_set_hash(event_ids: List[str]) -> str:
    # Order-insensitive: the same event set must hash identically regardless
    # of query ordering.
    digest = hashlib.sha256("\n".join(sorted(event_ids)).encode("utf-8")).hexdigest()
    return digest[:16]


class QCService:
    def __init__(self, db_path: str, clients: Dict[str, AnalyzerClient]):
        self.db_path = db_path
        self.clients = clients
        self._lock = threading.RLock()

    def _conn(self):
        from ..db_common import get_conn
        return get_conn(self.db_path)

    # ======================================================================
    # Startup recovery
    # ======================================================================

    def recover_interrupted(self) -> int:
        """A process restart means every still-scanning run died mid-flight.
        Mark them failed; they never had any qc_run_events rows, so no half
        result can ever be read."""
        recovered = 0
        with self._lock, self._conn() as conn:
            for row in db.scanning_qc_runs(conn):
                db.update_qc_run(
                    conn, row["run_id"], status="failed",
                    error="服务重启导致扫描中断，未发布任何部分结果，请重新核查")
                recovered += 1
            conn.commit()
        return recovered

    # ======================================================================
    # Limits
    # ======================================================================

    def set_limits(self, project: str, lower_limit: Any,
                   upper_limit: Any) -> Dict[str, Any]:
        project = (project or "").strip()
        if not project:
            raise QcError("项目名不能为空")
        try:
            lower = float(lower_limit)
            upper = float(upper_limit)
        except (TypeError, ValueError):
            raise QcError("限值必须是数值")
        if not (math.isfinite(lower) and math.isfinite(upper)):
            raise QcError("限值必须是有限数值（不接受 NaN / Infinity）")
        if lower < 0 or upper < 0:
            raise QcError("限值为非负测值而设，上下限都必须 >= 0")
        if lower > upper:
            raise QcError(f"下限 {lower:g} 不能大于上限 {upper:g}")
        with self._lock, self._conn() as conn:
            row = db.upsert_qc_limit(
                conn, project=project, lower_limit=lower, upper_limit=upper)
            conn.commit()
            return dict(row)

    def _require_limits(self, conn, project: str):
        row = db.get_qc_limit(conn, project)
        if row is None:
            raise QcError(
                f"项目 '{project}' 尚未设置质控限值，请先设置非负上下限再发起核查")
        return row

    # ======================================================================
    # Scan orchestration
    # ======================================================================

    def start_scan(self, project: str) -> Dict[str, Any]:
        project = (project or "").strip()
        if not project:
            raise QcError("项目名不能为空")
        with self._lock:
            with self._conn() as conn:
                limit = self._require_limits(conn, project)
                active = [r for r in db.scanning_qc_runs(conn)
                          if r["project"] == project]
                if active:
                    raise QcError(
                        f"项目 '{project}' 已有核查任务 {active[0]['run_id']} "
                        "正在扫描，请等待其完成")
                event_ids = db.project_event_ids(conn, project)
                # Snapshot both readers' effective versions *before* the first
                # decode so the published verdict is bound to exact versions.
                reader_versions: Dict[str, int] = {}
                for analyzer_id in ANALYZER_ORDER:
                    try:
                        state = self.clients[analyzer_id].reader_state()
                    except Exception as exc:
                        # RemoteError on non-2xx, plus raw transport errors
                        # (connection refused / timeout) from httpx.
                        raise QcError(
                            f"分析服务 {analyzer_id} 当前不可达，不能开始核查: {exc}")
                    reader_versions[analyzer_id] = state["effective_version"]
                run_id = "qcr_" + uuid.uuid4().hex[:12]
                db.insert_qc_run(
                    conn, run_id=run_id, project=project,
                    limits_version=limit["version"],
                    lower_limit=limit["lower_limit"],
                    upper_limit=limit["upper_limit"],
                    event_count=len(event_ids),
                    event_set_hash=event_set_hash(event_ids),
                    reader_a_version=reader_versions["analyzer-a"],
                    reader_b_version=reader_versions["analyzer-b"])
                conn.commit()

        thread = threading.Thread(
            target=self._scan_worker, args=(run_id,),
            name=f"qc-scan-{run_id}", daemon=True)
        thread.start()
        with self._conn() as conn:
            return self._run_view(db.get_qc_run(conn, run_id), conn)

    def _fail_run(self, run_id: str, message: str) -> None:
        with self._lock, self._conn() as conn:
            row = db.get_qc_run(conn, run_id)
            if row is not None and row["status"] == "scanning":
                db.update_qc_run(conn, run_id, status="failed", error=message)
                conn.commit()

    def _scan_worker(self, run_id: str) -> None:
        try:
            with self._conn() as conn:
                run = db.get_qc_run(conn, run_id)
                if run is None or run["status"] != "scanning":
                    return
                project = run["project"]
                snapshot = {
                    "limits_version": run["limits_version"],
                    "event_set_hash": run["event_set_hash"],
                    "reader_versions": {
                        "analyzer-a": run["reader_a_version"],
                        "analyzer-b": run["reader_b_version"],
                    },
                }
                limit = db.get_qc_limit(conn, project)
                event_ids = db.project_event_ids(conn, project)
                event_rows = {r["id"]: r for r in conn.execute(
                    "SELECT * FROM events").fetchall()}
                conn.commit()

            if limit is None or limit["version"] != snapshot["limits_version"]:
                self._fail_run(
                    run_id, "扫描期间限值版本发生变化，本次核查作废，请重新发起")
                return

            verdicts: List[Dict[str, Any]] = []
            for seq, event_id in enumerate(event_ids):
                ev = event_rows[event_id]
                writer_schema = None
                with self._conn() as conn:
                    writer_schema = db.get_schema_by_fp(conn, ev["writer_fp"])
                blob = base64.b64decode(ev["blob_b64"])
                side_results: Dict[str, Dict[str, Any]] = {}
                for analyzer_id in ANALYZER_ORDER:
                    try:
                        result = self.clients[analyzer_id].decode(
                            event_id=event_id, blob=blob,
                            writer_schema=writer_schema)
                    except Exception as exc:
                        self._fail_run(
                            run_id,
                            f"事件 {event_id} 经 {analyzer_id} 解码失败（调用未确认）: "
                            f"{exc}；按规则不发布部分结果")
                        return
                    if not result.get("decoded_ok"):
                        self._fail_run(
                            run_id,
                            f"事件 {event_id} 经 {analyzer_id} 解码失败: "
                            f"{result.get('error')}；按规则不发布部分结果")
                        return
                    observed_version = result.get("reader_version")
                    if observed_version != snapshot["reader_versions"][analyzer_id]:
                        self._fail_run(
                            run_id,
                            f"事件 {event_id} 解码时 {analyzer_id} 生效读取版本已从 "
                            f"v{snapshot['reader_versions'][analyzer_id]} 变为 "
                            f"v{observed_version}，扫描基线漂移，本次核查作废，请重新发起")
                        return
                    side_results[analyzer_id] = result
                    # Persist decode evidence in the shared decode_results table.
                    with self._conn() as conn:
                        db.insert_decode_result(
                            conn, event_id=event_id, analyzer_id=analyzer_id,
                            reader_version=observed_version, decoded_ok=True,
                            datum=result.get("datum"), error=None)
                        conn.commit()

                datum_a = side_results["analyzer-a"].get("datum")
                datum_b = side_results["analyzer-b"].get("datum")
                verdict, reasons, a_val, b_val = classify_measurement(
                    (datum_a or {}).get(_MEASUREMENT_KEY),
                    (datum_b or {}).get(_MEASUREMENT_KEY),
                    run["lower_limit"], run["upper_limit"])
                verdicts.append({
                    "event_id": event_id, "seq": seq, "verdict": verdict,
                    "reasons": reasons, "a_value": a_val, "b_value": b_val,
                    "a_reader_version": snapshot["reader_versions"]["analyzer-a"],
                    "b_reader_version": snapshot["reader_versions"]["analyzer-b"],
                })

            # Final baseline re-check before the atomic publish.
            self._verify_baseline_or_fail(run_id, project, snapshot)
            with self._conn() as conn:
                if db.get_qc_run(conn, run_id)["status"] != "scanning":
                    return
                usable = sum(1 for v in verdicts if v["verdict"] == "usable")
                issue = len(verdicts) - usable
                for v in verdicts:
                    db.insert_qc_run_event(
                        conn, run_id=run_id, event_id=v["event_id"],
                        event_seq=v["seq"], verdict=v["verdict"],
                        reasons=v["reasons"], a_value=v["a_value"],
                        b_value=v["b_value"],
                        a_reader_version=v["a_reader_version"],
                        b_reader_version=v["b_reader_version"],
                        a_ok=True, b_ok=True)
                db.update_qc_run(conn, run_id, status="published",
                                 usable_count=usable, issue_count=issue)
                conn.commit()
        except Exception as exc:  # never let the worker die silently
            self._fail_run(run_id, f"扫描发生未预期错误: {exc}")

    def _verify_baseline_or_fail(self, run_id: str, project: str,
                                 snapshot: Dict[str, Any]) -> None:
        with self._conn() as conn:
            limit = db.get_qc_limit(conn, project)
            current_ids = db.project_event_ids(conn, project)
        if limit is None or limit["version"] != snapshot["limits_version"]:
            self._fail_run(run_id,
                           "发布前复检发现限值版本已变化，本次核查作废，请重新发起")
            raise _BaselineDrift()
        if event_set_hash(current_ids) != snapshot["event_set_hash"]:
            self._fail_run(run_id,
                           "发布前复检发现项目事件集合已变化（有新事件入仓），"
                           "本次核查不对应完整事件集合，作废请重新发起")
            raise _BaselineDrift()
        for analyzer_id in ANALYZER_ORDER:
            try:
                state = self.clients[analyzer_id].reader_state()
            except Exception as exc:
                self._fail_run(
                    run_id, f"发布前复检 {analyzer_id} 不可达: {exc}，本次核查作废")
                raise _BaselineDrift()
            if state["effective_version"] != snapshot["reader_versions"][analyzer_id]:
                self._fail_run(
                    run_id,
                    f"发布前复检发现 {analyzer_id} 生效读取版本已变化，"
                    "扫描基线漂移，本次核查作废，请重新发起")
                raise _BaselineDrift()

    # ======================================================================
    # Views
    # ======================================================================

    def status(self) -> Dict[str, Any]:
        with self._conn() as conn:
            limits = [dict(r) for r in db.list_qc_limits(conn)]
            run_rows = db.latest_qc_runs(conn, limit=20)
            projects = sorted({r["project"] for r in run_rows})
            current_hashes = {
                p: event_set_hash(db.project_event_ids(conn, p))
                for p in projects}
            limit_versions = {}
            for p in projects:
                limit_row = db.get_qc_limit(conn, p)
                limit_versions[p] = limit_row["version"] if limit_row else None
            views = []
            for row in run_rows:
                view = self._run_view(row, conn)
                view["stale_reasons"] = []
                if row["status"] == "published":
                    if limit_versions.get(row["project"]) != row["limits_version"]:
                        view["stale_reasons"].append(
                            f"限值版本已变化（核查基于 v{row['limits_version']}，"
                            f"当前 v{limit_versions.get(row['project'])}），结论待重算")
                    if current_hashes.get(row["project"]) != row["event_set_hash"]:
                        view["stale_reasons"].append(
                            "项目留存事件集合已变化（有新事件入仓或事件变动），结论待重算")
                # Only a published run can ever be current; scanning/failed
                # runs are always non-current.
                view["is_current"] = (
                    row["status"] == "published" and not view["stale_reasons"])
                views.append(view)

        # Live reader versions decide staleness; an unreachable service can
        # never confirm a conclusion as current.
        live_versions = self._live_reader_versions()
        for view in views:
            if view["status"] != "published":
                continue
            for analyzer_id, col in (("analyzer-a", "reader_a_version"),
                                     ("analyzer-b", "reader_b_version")):
                live = live_versions.get(analyzer_id)
                if live is None:
                    view["is_current"] = False
                    view["stale_reasons"].append(
                        f"{analyzer_id} 当前不可达，无法确认其生效读取版本，结论待重算")
                elif live != view[col]:
                    view["is_current"] = False
                    view["stale_reasons"].append(
                        f"{analyzer_id} 读取格式已切换（核查基于 v{view[col]}，"
                        f"当前 v{live}），结论待重算")

        return {"limits": limits, "runs": views,
                "reader_versions": live_versions}

    def _live_reader_versions(self) -> Dict[str, Any]:
        live_versions: Dict[str, Any] = {}
        for analyzer_id in ANALYZER_ORDER:
            try:
                live_versions[analyzer_id] = \
                    self.clients[analyzer_id].reader_state()["effective_version"]
            except Exception:
                # RemoteError on non-2xx, plus raw httpx transport errors.
                live_versions[analyzer_id] = None
        return live_versions

    def get_run(self, run_id: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = db.get_qc_run(conn, run_id)
            if row is None:
                raise QcError(f"核查任务 '{run_id}' 不存在")
            view = self._run_view(row, conn)
        view["stale_reasons"] = []
        if row["status"] != "published":
            view["is_current"] = False
            return view
        with self._conn() as conn:
            limit = db.get_qc_limit(conn, row["project"])
            current_hash = event_set_hash(
                db.project_event_ids(conn, row["project"]))
        if limit is None or limit["version"] != row["limits_version"]:
            view["stale_reasons"].append(
                f"限值版本已变化（核查基于 v{row['limits_version']}，"
                f"当前 v{None if limit is None else limit['version']}），结论待重算")
        if current_hash != row["event_set_hash"]:
            view["stale_reasons"].append(
                "项目留存事件集合已变化（有新事件入仓或事件变动），结论待重算")
        live_versions = self._live_reader_versions()
        for analyzer_id, col in (("analyzer-a", "reader_a_version"),
                                 ("analyzer-b", "reader_b_version")):
            live = live_versions.get(analyzer_id)
            if live is None:
                view["stale_reasons"].append(
                    f"{analyzer_id} 当前不可达，无法确认其生效读取版本，结论待重算")
            elif live != view[col]:
                view["stale_reasons"].append(
                    f"{analyzer_id} 读取格式已切换（核查基于 v{view[col]}，"
                    f"当前 v{live}），结论待重算")
        view["is_current"] = not view["stale_reasons"]
        return view

    @staticmethod
    def _run_view(row, conn) -> Dict[str, Any]:
        view = {
            "run_id": row["run_id"],
            "project": row["project"],
            "status": row["status"],
            "limits_version": row["limits_version"],
            "lower_limit": row["lower_limit"],
            "upper_limit": row["upper_limit"],
            "event_count": row["event_count"],
            "event_set_hash": row["event_set_hash"],
            "reader_a_version": row["reader_a_version"],
            "reader_b_version": row["reader_b_version"],
            "usable_count": row["usable_count"],
            "issue_count": row["issue_count"],
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "events": [],
        }
        if row["status"] == "published":
            view["events"] = [
                {
                    "event_id": e["event_id"],
                    "seq": e["event_seq"],
                    "verdict": e["verdict"],
                    "reasons": json.loads(e["reasons_json"]),
                    "a_value": e["a_value"],
                    "b_value": e["b_value"],
                    "a_reader_version": e["a_reader_version"],
                    "b_reader_version": e["b_reader_version"],
                }
                for e in db.qc_run_events(conn, row["run_id"])
            ]
        return view
