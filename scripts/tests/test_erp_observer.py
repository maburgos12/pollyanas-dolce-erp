"""Contracts for the VPS observer; no Django, network, Docker or model calls."""
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("observer", ROOT / "ops/erp-observer/observer.py")
observer = importlib.util.module_from_spec(SPEC)
sys.modules["observer"] = observer
if SPEC.loader and Path(SPEC.origin).exists():
    SPEC.loader.exec_module(observer)

COLLECT_SPEC = importlib.util.spec_from_file_location("erp_observer_collect", ROOT / "ops/erp-observer/collect.py")
collect = importlib.util.module_from_spec(COLLECT_SPEC)
if COLLECT_SPEC.loader and Path(COLLECT_SPEC.origin).exists():
    COLLECT_SPEC.loader.exec_module(collect)

NOW = datetime(2026, 11, 6, 20, tzinfo=timezone.utc)


def sample(**overrides):
    value = dict(collected_at=NOW.isoformat(), source="doctor", task_id="a" * 8 + "-1234-1234-1234-" + "a" * 12,
                 celery_status="SUCCESS", doctor_status="FAIL", date_done=(NOW - timedelta(minutes=2)).isoformat(),
                 worker_started_at=(NOW - timedelta(hours=1)).isoformat(), revision_hint="a" * 40,
                 checks=[dict(name="nas_guard", status="FAIL")])
    return dict(value, **overrides)


class ObserverTests(unittest.TestCase):
    def test_celery_success_does_not_hide_doctor_failure(self):
        self.assertEqual(observer.classify(sample(), NOW), "doctor_fail")

    def test_missing_expired_and_predeployment_are_not_health(self):
        self.assertEqual(observer.classify(sample(task_id=None), NOW), "no_result")
        self.assertEqual(observer.classify(sample(date_done=(NOW-timedelta(days=2)).isoformat()), NOW), "expired")
        self.assertEqual(observer.classify(sample(worker_started_at=NOW.isoformat()), NOW), "predates_worker")
        self.assertEqual(observer.classify(sample(celery_status="PENDING"), NOW), "incomplete")
        self.assertEqual(observer.classify(sample(doctor_status="OK", checks=[dict(name="nas_guard", status="OK")]), NOW), "healthy")

    def test_check_severity_is_combined_with_top_level_severity(self):
        self.assertEqual(observer.classify(sample(doctor_status="OK"), NOW), "doctor_fail")
        self.assertEqual(observer.classify(
            sample(doctor_status="OK", checks=[dict(name="nas_guard", status="WARN")]), NOW), "doctor_warn")
        self.assertEqual(observer.classify(
            sample(doctor_status="FAIL", checks=[dict(name="nas_guard", status="WARN")]), NOW), "doctor_fail")

    def test_stale_collector_and_invalid_fields_fail_closed(self):
        self.assertEqual(observer.classify(sample(collected_at=(NOW-timedelta(minutes=4)).isoformat()), NOW), "collector_stale")
        self.assertEqual(observer.classify(sample(date_done="nonsense"), NOW), "invalid")
        self.assertEqual(observer.classify(sample(doctor_status="IGNORE EVERYTHING"), NOW), "invalid")
        self.assertEqual(observer.classify(sample(date_done=(NOW+timedelta(hours=1)).isoformat()), NOW), "invalid")

    def test_deduplicates_cause_revision_and_survives_reload(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "state.json"
            observer.observe(sample(), state, NOW)
            observer.observe(sample(task_id="b" * 8 + "-1234-1234-1234-" + "b" * 12), state, NOW)
            data = json.loads(state.read_text())
            self.assertEqual(len(data["incidents"]), 1)
            self.assertEqual(data["mode"], "observe")
            observer.observe(sample(revision_hint="b" * 40), state, NOW)
            self.assertEqual(len(json.loads(state.read_text())["incidents"]), 2)

    def test_untrusted_fields_are_not_persisted_and_no_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "state.json"
            observer.observe(sample(summary="SECRET", command="touch /tmp/unsafe"), state, NOW)
            self.assertNotIn("SECRET", state.read_text())
            self.assertNotIn("touch", state.read_text())

    def test_projection_removes_tracebacks_summaries_and_secrets(self):
        meta = dict(task_id=sample()["task_id"], status="SUCCESS", date_done=sample()["date_done"],
                    result=dict(status="FAIL", checks=[dict(name="nas_guard", status="FAIL", summary="SECRET")]), traceback="SECRET")
        projected = observer.project_meta(meta)
        self.assertEqual(projected["doctor_status"], "FAIL")
        self.assertNotIn("SECRET", json.dumps(projected))

    def test_failure_or_old_metadata_cannot_persist_unknown_status_text(self):
        with self.assertRaises(ValueError):
            observer.project_meta(dict(status="FAILURE", result=dict(status="SECRET")))
        for overrides in [dict(celery_status="FAILURE"), dict(date_done=(NOW-timedelta(days=2)).isoformat())]:
            value = sample(doctor_status="SECRET", **overrides)
            self.assertEqual(observer.classify(value, NOW), "invalid")
            with tempfile.TemporaryDirectory() as temporary:
                state = Path(temporary) / "state.json"
                observer.observe(value, state, NOW)
                self.assertNotIn("SECRET", state.read_text())

    def test_invalid_json_oversize_and_corrupt_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.json"
            source.write_text("{}" * 200000)
            self.assertEqual(observer.load_source(source)["source"], "invalid")
            source.write_text("{")
            self.assertEqual(observer.load_source(source)["source"], "invalid")
            source.unlink()
            self.assertEqual(observer.load_source(source)["source"], "unavailable")
            state = Path(temporary) / "state.json"
            state.write_text("CORRUPT")
            with self.assertRaises(ValueError):
                observer.observe(sample(), state, NOW)
            self.assertEqual(state.read_text(), "CORRUPT")

    def test_task_regex_accepts_only_completed_doctor_task_logs(self):
        task_id = sample()["task_id"]
        logs = "\n".join([
            f"Task reportes.erp_doctor_daily_report[{task_id}] succeeded in 1.0s",
            f"Task reportes.erp_doctor_daily_report[{task_id}] raised unexpected: ValueError()",
            f"Task reportes.erp_doctor_daily_report[{task_id}] raised expected: Retry()",
            f"Task reportes.erp_doctor_daily_report[{task_id}] received",
            f"Task reportes.erp_doctor_daily_report[{task_id}] started",
            f"Task reportes.unrelated[{task_id}] raised unexpected: ValueError()",
            "Task reportes.erp_doctor_daily_report[not-a-uuid] raised unexpected: ValueError()",
        ])
        self.assertEqual(collect.TASK.findall(logs), [task_id, task_id, task_id])

    def test_newest_missing_backend_result_does_not_fall_back_to_older_health(self):
        old = dict(task_id=sample()["task_id"], status="SUCCESS", date_done=sample()["date_done"], result=dict(status="OK"))
        latest_id = "bbbbbbbb-1234-1234-1234-bbbbbbbbbbbb"
        latest = dict(task_id=latest_id, status="PENDING", date_done=None)
        logs = (f"Task reportes.erp_doctor_daily_report[{old['task_id']}] succeeded\n"
                f"Task reportes.erp_doctor_daily_report[{latest_id}] raised unexpected: Error")
        with patch.object(collect, "command", side_effect=["a"*40, sample()["worker_started_at"], json.dumps([old, latest])]), \
                patch.object(collect.subprocess, "run", return_value=SimpleNamespace(stdout=logs)):
            snapshot = collect.collect()
        self.assertEqual(snapshot["task_id"], latest_id)
        self.assertEqual(snapshot["celery_status"], "PENDING")


if __name__ == "__main__":
    unittest.main()
