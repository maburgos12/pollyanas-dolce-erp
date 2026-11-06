"""Contracts for the VPS observer; no Django, network, Docker or model calls."""
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("observer", ROOT / "ops/erp-observer/observer.py")
observer = importlib.util.module_from_spec(SPEC)
if SPEC.loader and Path(SPEC.origin).exists():
    SPEC.loader.exec_module(observer)

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
        self.assertEqual(observer.classify(sample(doctor_status="OK"), NOW), "healthy")

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


if __name__ == "__main__":
    unittest.main()
