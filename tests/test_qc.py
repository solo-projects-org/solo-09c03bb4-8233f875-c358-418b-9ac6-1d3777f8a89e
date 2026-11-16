"""Tests for the project quality-control (QC) feature.

* pure classification logic (missing / non-numeric / disagree / out-of-limits),
* end-to-end scans through the real FastAPI apps with in-process ASGI
  transports and isolated SQLite databases,
* failure atomicity (any analyzer decode failure -> no partial publication),
* staleness after limits edits, ingestion and reader-format switches,
* restart recovery (a leftover scanning run can never become a conclusion),
* baseline drift during a scan and concurrency guards.

Run: PYTHONPATH=/workspace:/workspace/.pylibs \
     python3 -m unittest tests.test_qc -v
"""
import copy
import time
import unittest
from unittest import mock

from app.analyzer import db as analyzer_db
from app.webapp import db as web_db
from app.webapp import main as web_main
from app.webapp.analyzer_client import RemoteError
from app.webapp.qc import classify_measurement, event_set_hash, is_finite_number

from tests.test_integration import PlatformHarness


# ---------------------------------------------------------------------------
# Pure classification unit tests
# ---------------------------------------------------------------------------

class TestClassification(unittest.TestCase):
    def test_usable_when_both_finite_in_range_and_equal(self):
        verdict, reasons, a, b = classify_measurement(1.5, 1.5, 0, 10)
        self.assertEqual(verdict, "usable")
        self.assertEqual(reasons, [])
        self.assertEqual((a, b), (1.5, 1.5))

    def test_missing_measurements_listed_per_side(self):
        verdict, reasons, a, b = classify_measurement(None, 2.0, 0, 10)
        self.assertEqual(verdict, "issue")
        self.assertTrue(any("analyzer-a" in r and "缺失" in r for r in reasons))
        self.assertIsNone(a)
        self.assertEqual(b, 2.0)

        verdict2, reasons2, _, _ = classify_measurement(None, None, 0, 10)
        self.assertEqual(verdict2, "issue")
        self.assertEqual(len(reasons2), 2)

    def test_non_numeric_values_rejected(self):
        verdict, reasons, a, b = classify_measurement("oops", 1.0, 0, 10)
        self.assertEqual(verdict, "issue")
        self.assertTrue(any("非数值" in r for r in reasons))
        self.assertIsNone(a)
        self.assertEqual(b, 1.0)

    def test_boolean_and_nan_are_not_finite_numbers(self):
        self.assertFalse(is_finite_number(True))
        self.assertFalse(is_finite_number(float("nan")))
        self.assertFalse(is_finite_number(float("inf")))
        verdict, reasons, _, _ = classify_measurement(
            True, float("nan"), 0, 10)
        self.assertEqual(verdict, "issue")
        self.assertEqual(len(reasons), 2)
        self.assertTrue(all("非数值" in r for r in reasons))

    def test_disagreement_flagged(self):
        verdict, reasons, a, b = classify_measurement(1.0, 1.2, 0, 10)
        self.assertEqual(verdict, "issue")
        self.assertTrue(any("不一致" in r for r in reasons))
        self.assertEqual((a, b), (1.0, 1.2))

    def test_promoted_numeric_values_stay_equal(self):
        # int 7 read as long/double becomes 7.0; JSON round-trips keep the
        # value numerically identical, so this must stay usable.
        verdict, reasons, _, _ = classify_measurement(7, 7.0, 0, 10)
        self.assertEqual(verdict, "usable")
        self.assertEqual(reasons, [])
        # tiny representation noise is tolerated
        verdict2, _, _, _ = classify_measurement(
            1.0, 1.0 + 1e-15, 0, 10)
        self.assertEqual(verdict2, "usable")

    def test_limits_must_be_non_negative_and_bounded(self):
        verdict, reasons, _, _ = classify_measurement(-0.5, -0.5, 0, 10)
        self.assertEqual(verdict, "issue")
        self.assertTrue(any("超出非负限值" in r for r in reasons))

        verdict2, reasons2, _, _ = classify_measurement(11.0, 11.0, 0, 10)
        self.assertEqual(verdict2, "issue")
        self.assertTrue(any("超出非负限值" in r for r in reasons2))

        # boundary values are inclusive and therefore usable
        verdict3, reasons3, _, _ = classify_measurement(0.0, 0.0, 0, 10)
        self.assertEqual(verdict3, "usable")
        verdict4, reasons4, _, _ = classify_measurement(10.0, 10.0, 0, 10)
        self.assertEqual(verdict4, "usable")

    def test_event_set_hash_order_insensitive(self):
        self.assertEqual(event_set_hash(["a", "b"]),
                         event_set_hash(["b", "a"]))
        self.assertNotEqual(event_set_hash(["a", "b"]),
                            event_set_hash(["a", "c"]))


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------

class TestQcIntegration(unittest.TestCase):
    PROJECT = "QC-项目"

    def setUp(self):
        self.h = PlatformHarness()
        self.web = self.h.web
        web_main.RECONCILE_INTERVAL = 3600
        web_main._bg_stop.clear()
        self._ctx = self.web
        self._ctx.__enter__()

    def tearDown(self):
        self._ctx.__exit__(None, None, None)
        web_main._bg_stop.set()

    # -- helpers -------------------------------------------------------------
    def set_limits(self, lower=0.0, upper=10.0, project=None):
        r = self.web.post("/api/qc/limits", json={
            "project": project or self.PROJECT,
            "lower_limit": lower, "upper_limit": upper})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["limits"]

    def ingest(self, measurement, project=None, sample=None):
        r = self.web.post("/api/events", json={
            "payload": {"sample_id": sample or f"S-{time.time_ns()}",
                        "project": project or self.PROJECT,
                        "measurement": measurement}})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["event_id"]

    def start_scan(self, project=None):
        r = self.web.post("/api/qc/scan",
                          json={"project": project or self.PROJECT})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["run"]["run_id"]

    def wait_run(self, run_id, timeout=15.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self.web.get(f"/api/qc/runs/{run_id}")
            run = r.json()["run"]
            if run["status"] != "scanning":
                return run
            time.sleep(0.05)
        self.fail(f"run {run_id} did not finish within {timeout}s")

    def latest_published_run(self, project=None):
        st = self.web.get("/api/status").json()["qc"]
        runs = [r for r in st["runs"]
                if r["project"] == (project or self.PROJECT)]
        published = [r for r in runs if r["status"] == "published"]
        self.assertTrue(published, "no published run found")
        return published[0]

    # -- tests ---------------------------------------------------------------
    def test_01_scan_requires_limits(self):
        r = self.web.post("/api/qc/scan", json={"project": "没有限值的项目"})
        self.assertEqual(r.status_code, 409)
        self.assertIn("限值", r.json()["error"])

    def test_02_limits_validation_non_negative_and_ordered(self):
        for body in (
            {"project": "X", "lower_limit": -1, "upper_limit": 10},
            {"project": "X", "lower_limit": 5, "upper_limit": 4},
        ):
            r = self.web.post("/api/qc/limits", json=body)
            self.assertEqual(r.status_code, 422, body)

    def test_02b_nan_limit_is_structured_422_not_500(self):
        # Python's json module emits NaN by default; the endpoint must never
        # answer a bare 500 and must reject the non-finite limit.
        r = self.web.post(
            "/api/qc/limits",
            content=b'{"project":"X","lower_limit":0,"upper_limit":NaN}',
            headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 422)
        self.assertFalse(r.json()["ok"])

    def test_02c_scan_with_unreachable_analyzer_is_clean_409(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        client = self.h.service.clients["analyzer-a"]
        with mock.patch.object(client, "reader_state",
                               side_effect=RemoteError("connection refused")):
            r = self.web.post("/api/qc/scan", json={"project": self.PROJECT})
        self.assertEqual(r.status_code, 409)
        self.assertIn("不可达", r.json()["error"])

    def test_03_full_scan_classifies_usable_and_issues_by_event_id(self):
        self.set_limits(0, 10)
        good = self.ingest(2.5)
        high = self.ingest(12.0)
        low = self.ingest(-3.0)
        run_id = self.start_scan()
        run = self.wait_run(run_id)

        self.assertEqual(run["status"], "published")
        self.assertEqual(run["event_count"], 3)
        self.assertEqual(run["usable_count"], 1)
        self.assertEqual(run["issue_count"], 2)
        self.assertTrue(run["is_current"])
        self.assertEqual(run["stale_reasons"], [])

        by_id = {e["event_id"]: e for e in run["events"]}
        self.assertEqual(by_id[good]["verdict"], "usable")
        self.assertEqual(by_id[good]["reasons"], [])
        self.assertEqual(by_id[high]["verdict"], "issue")
        self.assertTrue(any("超出非负限值" in x for x in by_id[high]["reasons"]))
        self.assertEqual(by_id[low]["verdict"], "issue")
        # every issue row carries the event id
        self.assertTrue(all(e["event_id"] for e in run["events"]))

    def test_04_decode_failure_fails_whole_run_and_publishes_nothing(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        self.ingest(2.0)
        # Arm a one-shot dropped decode response inside analyzer-a.
        analyzer_db.set_sim(self.h.a_db, 0, False, True)

        run_id = self.start_scan()
        run = self.wait_run(run_id)
        self.assertEqual(run["status"], "failed")
        self.assertIn("analyzer-a", run["error"])
        self.assertIn("不发布部分结果", run["error"])
        # Nothing was published: no qc_run_events rows exist for this run.
        with self.h.qc._conn() as conn:
            rows = conn.execute(
                "SELECT COUNT(*) AS n FROM qc_run_events WHERE run_id = ?",
                (run_id,)).fetchone()
            self.assertEqual(rows["n"], 0)
        # A failed run is never presented as a current conclusion.
        st = self.web.get("/api/status").json()["qc"]
        view = next(r for r in st["runs"] if r["run_id"] == run_id)
        self.assertFalse(view["is_current"])

    def test_05_stale_after_ingestion_limit_change_and_reader_switch(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        run = self.wait_run(self.start_scan())
        self.assertEqual(run["status"], "published")
        self.assertTrue(run["is_current"])

        # (a) new event for the project -> event set no longer matches
        self.ingest(3.0)
        st = self.web.get("/api/status").json()["qc"]
        view = next(r for r in st["runs"] if r["run_id"] == run["run_id"])
        self.assertFalse(view["is_current"])
        self.assertTrue(any("事件集合" in x for x in view["stale_reasons"]))

        # recompute -> current again
        run2 = self.wait_run(self.start_scan())
        self.assertEqual(run2["event_count"], 2)
        self.assertTrue(run2["is_current"])

        # (b) limits edit -> previous conclusion stale with versions named
        self.set_limits(0, 8)
        st = self.web.get("/api/status").json()["qc"]
        view = next(r for r in st["runs"] if r["run_id"] == run2["run_id"])
        self.assertFalse(view["is_current"])
        self.assertTrue(any("限值版本" in x for x in view["stale_reasons"]))

        # (c) reader-format switch on analyzer-a -> stale on reader version
        run3 = self.wait_run(self.start_scan())
        self.assertTrue(run3["is_current"])
        good_reader = copy.deepcopy(
            self.web.get("/api/default-schema").json())
        good_reader["fields"].append(
            {"name": "temperature", "type": "double", "default": 0.0})
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": good_reader})
        self.assertEqual(r.status_code, 200, r.text)
        st = self.web.get("/api/status").json()["qc"]
        view = next(r for r in st["runs"] if r["run_id"] == run3["run_id"])
        self.assertFalse(view["is_current"])
        self.assertTrue(any("analyzer-a" in x and "读取格式" in x
                            for x in view["stale_reasons"]))

    def test_06_restart_marks_scanning_run_failed_no_half_result(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        run_id = self.start_scan()
        # The in-process scan will normally finish fast; force a crashed
        # scanning row to simulate a process killed mid-scan.
        self.wait_run(run_id)  # ensure the real run completed first
        with self.h.qc._conn() as conn:
            web_db.insert_qc_run(
                conn, run_id="qcr_interrupted", project=self.PROJECT,
                limits_version=1, lower_limit=0, upper_limit=10,
                event_count=1, event_set_hash=event_set_hash(["x"]),
                reader_a_version=1, reader_b_version=1)
            conn.commit()

        recovered = self.h.qc.recover_interrupted()
        self.assertGreaterEqual(recovered, 1)
        r = self.web.get("/api/qc/runs/qcr_interrupted")
        run = r.json()["run"]
        self.assertEqual(run["status"], "failed")
        self.assertIn("重启", run["error"])
        self.assertEqual(run["events"], [])

    def test_07_baseline_drift_during_scan_fails_the_run(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        # Make analyzer decode calls slow so the scan is still in flight while
        # the baseline (event set) changes underneath it.
        analyzer_db.set_sim(self.h.a_db, 400, False, False)
        analyzer_db.set_sim(self.h.b_db, 400, False, False)
        run_id = self.start_scan()
        time.sleep(0.15)
        # Ingestion stays available during the scan.
        self.ingest(4.0)
        run = self.wait_run(run_id)
        self.assertEqual(run["status"], "failed")
        self.assertIn("事件集合", run["error"])
        with self.h.qc._conn() as conn:
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM qc_run_events WHERE run_id = ?",
                (run_id,)).fetchone()["n"]
            self.assertEqual(n, 0)

    def test_08_reader_version_switch_during_scan_fails_the_run(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        good_reader = copy.deepcopy(
            self.web.get("/api/default-schema").json())
        good_reader["fields"].append(
            {"name": "temperature", "type": "double", "default": 0.0})
        client = self.h.service.clients["analyzer-a"]
        original_state = client.reader_state
        switched = {"after": False}

        def flapping_state():
            state = original_state()
            if switched["after"]:
                state = dict(state)
                state["effective_version"] = 99
            return state

        # First reader_state (snapshot) returns the real v1; the publish-time
        # re-check observes a switched version.
        with mock.patch.object(client, "reader_state",
                               side_effect=flapping_state):
            run_id = self.start_scan()
            switched["after"] = True
            run = self.wait_run(run_id)
        self.assertEqual(run["status"], "failed")
        self.assertIn("读取版本", run["error"])

    def test_09_concurrent_scan_for_same_project_rejected(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        analyzer_db.set_sim(self.h.a_db, 400, False, False)
        analyzer_db.set_sim(self.h.b_db, 400, False, False)
        first = self.start_scan()
        r = self.web.post("/api/qc/scan", json={"project": self.PROJECT})
        self.assertEqual(r.status_code, 409)
        self.assertIn("正在扫描", r.json()["error"])
        run = self.wait_run(first)
        self.assertEqual(run["status"], "published")

    def test_10_empty_project_publishes_empty_current_conclusion(self):
        empty_project = "空项目-不存在事件"
        # limits can be configured even before any event exists
        self.set_limits(0, 10, project=empty_project)
        run = self.wait_run(self.start_scan(project=empty_project))
        self.assertEqual(run["status"], "published")
        self.assertEqual(run["event_count"], 0)
        self.assertEqual(run["usable_count"], 0)
        self.assertEqual(run["issue_count"], 0)
        self.assertTrue(run["is_current"])

    def test_11_limit_change_mid_scan_fails_the_run(self):
        self.set_limits(0, 10)
        self.ingest(1.0)
        analyzer_db.set_sim(self.h.a_db, 400, False, False)
        analyzer_db.set_sim(self.h.b_db, 400, False, False)
        run_id = self.start_scan()
        time.sleep(0.15)
        # Limits edits stay available during the scan.
        self.set_limits(0, 9)
        run = self.wait_run(run_id)
        self.assertEqual(run["status"], "failed")
        self.assertIn("限值版本", run["error"])

    def test_12_end_to_end_decode_succeeds_but_measurement_not_available(self):
        """After a compatible schema evolution analyzer-b's effective reader
        decodes the original Avro bytes fine (decoded_ok=True) but no longer
        exposes a numeric top-level ``measurement`` (it projects the field
        away), while analyzer-a keeps it and the value also violates the
        limits. QC must flag one event with both kinds of reasons instead of
        ever treating it as usable."""
        v2 = {
            "type": "record", "name": "InstrumentEvent",
            "namespace": "lab.warehouse",
            "fields": [
                {"name": "sample_id", "type": "string"},
                {"name": "project", "type": "string"},
                {"name": "measurement", "type": "double"},
                {"name": "instrument_id", "type": "string", "default": "X"},
            ],
        }
        # analyzer-b drops measurement (reader-side field removal is always
        # allowed) and keeps the new string field instead.
        reader_b = {
            "type": "record", "name": "InstrumentEvent",
            "namespace": "lab.warehouse",
            "fields": [
                {"name": "sample_id", "type": "string"},
                {"name": "project", "type": "string"},
                {"name": "instrument_id", "type": "string", "default": "X"},
            ],
        }
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-b", "schema": reader_b})
        self.assertEqual(r.status_code, 200, r.text)
        r = self.web.post("/api/switches/writer", json={"schema": v2})
        self.assertEqual(r.status_code, 200, r.text)

        self.set_limits(0, 10)
        event_id = self.ingest(42.0)  # analyzer-a: finite but above upper limit
        run = self.wait_run(self.start_scan())
        self.assertEqual(run["status"], "published")
        row = next(e for e in run["events"] if e["event_id"] == event_id)
        self.assertEqual(row["verdict"], "issue")
        joined = " | ".join(row["reasons"])
        self.assertIn("analyzer-b", joined)
        self.assertIn("缺失", joined)
        self.assertIn("超出非负限值", joined)
        self.assertEqual(row["a_value"], 42.0)
        self.assertIsNone(row["b_value"])

    def test_13_end_to_end_missing_measurement_on_one_side(self):
        """analyzer-b's compatible reader projects the measurement field away
        entirely, so its decoded datum has no measurement while analyzer-a
        retains it: the event must be listed as an issue with a missing reason."""
        v2 = {
            "type": "record", "name": "InstrumentEvent",
            "namespace": "lab.warehouse",
            "fields": [
                {"name": "sample_id", "type": "string"},
                {"name": "project", "type": "string"},
                {"name": "measurement", "type": "double"},
                {"name": "instrument_id", "type": "string", "default": "X"},
            ],
        }
        r = self.web.post("/api/switches/writer", json={"schema": v2})
        self.assertEqual(r.status_code, 200, r.text)
        reader_b = {
            "type": "record", "name": "InstrumentEvent",
            "namespace": "lab.warehouse",
            "fields": [
                {"name": "sample_id", "type": "string"},
                {"name": "project", "type": "string"},
            ],
        }
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-b", "schema": reader_b})
        self.assertEqual(r.status_code, 200, r.text)

        self.set_limits(0, 10)
        event_id = self.ingest(3.5)
        run = self.wait_run(self.start_scan())
        self.assertEqual(run["status"], "published")
        row = next(e for e in run["events"] if e["event_id"] == event_id)
        self.assertEqual(row["verdict"], "issue")
        self.assertTrue(
            any("analyzer-b" in x and "缺失" in x for x in row["reasons"]))
        self.assertEqual(row["a_value"], 3.5)
        self.assertIsNone(row["b_value"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
