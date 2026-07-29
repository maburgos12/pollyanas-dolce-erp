from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import health


UTC = timezone.utc


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class _RecorderHandler(BaseHTTPRequestHandler):
    requests: list[tuple[str, dict, dict]] = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).requests.append(
            (self.path, {key.lower(): value for key, value in self.headers.items()}, body)
        )
        response = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_args):
        return


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "state.db"
        self.now = datetime(2026, 7, 28, 18, 0, tzinfo=UTC)
        with sqlite3.connect(self.db_path) as con:
            con.executescript(
                """
                CREATE TABLE sync_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE event_outbox (
                    event_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    employee_no TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    last_error TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    acked_at TEXT
                );
                """
            )

    def tearDown(self):
        self.tmp.cleanup()

    def _state(self, **values):
        with sqlite3.connect(self.db_path) as con:
            con.executemany(
                "INSERT OR REPLACE INTO sync_state(key, value) VALUES (?, ?)",
                [(key, str(value)) for key, value in values.items()],
            )

    def _pending(self, *, event_id="guid-1", age_minutes=2, attempts=1, error="timeout ERP"):
        created = self.now - timedelta(minutes=age_minutes)
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                """
                INSERT INTO event_outbox VALUES (?, 'hikconnect_cloud', '328', ?, 'check_in',
                    '{}', 'pending', ?, ?, ?, ?, NULL)
                """,
                (
                    event_id,
                    _iso(created),
                    attempts,
                    error,
                    _iso(created),
                    _iso(created),
                ),
            )

    def test_healthy_when_last_success_is_fresh_and_outbox_is_clear(self):
        self._state(
            last_cycle_at=_iso(self.now - timedelta(minutes=1)),
            last_success_at=_iso(self.now - timedelta(minutes=1)),
            last_cloud_record_at=_iso(self.now - timedelta(minutes=3)),
            failure_count=0,
        )

        report = health.inspect_health(self.db_path, now=self.now)

        self.assertEqual(report["status"], "healthy")
        self.assertEqual(report["outbox_pending"], 0)
        self.assertEqual(report["incident_key"], "")

    def test_temporary_pending_event_is_recovering_without_human_incident(self):
        self._state(
            last_cycle_at=_iso(self.now - timedelta(minutes=1)),
            last_success_at=_iso(self.now - timedelta(minutes=6)),
            failure_count=2,
            last_error="timeout ERP",
        )
        self._pending(age_minutes=4, attempts=2)

        report = health.inspect_health(self.db_path, now=self.now)

        self.assertEqual(report["status"], "recovering")
        self.assertEqual(report["outbox_pending"], 1)
        self.assertEqual(report["incident_key"], "")

    def test_recovery_exhausted_after_ten_minutes_requires_action(self):
        self._state(
            last_cycle_at=_iso(self.now - timedelta(minutes=1)),
            last_success_at=_iso(self.now - timedelta(minutes=14)),
            failure_count=5,
            failure_category="hik_auth",
            last_error="credenciales revocadas",
        )
        self._pending(age_minutes=14, attempts=5, error="identity_unresolved")

        report = health.inspect_health(self.db_path, now=self.now)

        self.assertEqual(report["status"], "action_required")
        self.assertEqual(report["identity_deferred"], 1)
        self.assertEqual(report["incident_key"], "hik_auth")

    def test_stuck_identity_requires_action_even_when_cloud_cycles_succeed(self):
        self._state(
            last_cycle_at=_iso(self.now - timedelta(minutes=1)),
            last_success_at=_iso(self.now - timedelta(minutes=1)),
            failure_count=0,
        )
        self._pending(age_minutes=14, attempts=5, error="identity_unresolved")

        report = health.inspect_health(self.db_path, now=self.now)

        self.assertEqual(report["status"], "action_required")
        self.assertEqual(report["incident_key"], "identity_unresolved")

    def test_action_alert_is_deduplicated_and_recovery_closes_it_silently(self):
        report = {
            "status": "action_required",
            "incident_key": "hik_auth",
            "last_error": "credenciales revocadas",
            "outbox_pending": 3,
        }
        sent = []

        first = health.notify_if_required(
            self.db_path,
            report,
            sender=lambda message, incident_key: sent.append((incident_key, message)),
            now=self.now,
        )
        second = health.notify_if_required(
            self.db_path,
            report,
            sender=lambda message, incident_key: sent.append((incident_key, message)),
            now=self.now + timedelta(minutes=5),
        )
        recovered = health.notify_if_required(
            self.db_path,
            {"status": "healthy", "incident_key": ""},
            sender=lambda message, incident_key: sent.append((incident_key, message)),
            now=self.now + timedelta(minutes=10),
        )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertFalse(recovered)
        self.assertEqual(len(sent), 1)
        with sqlite3.connect(self.db_path) as con:
            closed = con.execute(
                "SELECT resolved_at FROM health_incidents WHERE incident_key='hik_auth'"
            ).fetchone()[0]
        self.assertIsNotNone(closed)

    def test_posts_health_and_maya_only_when_configured(self):
        _RecorderHandler.requests = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), _RecorderHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        report = {
            "status": "action_required",
            "last_cycle_at": _iso(self.now),
            "last_success_at": _iso(self.now - timedelta(minutes=20)),
            "last_cloud_record_at": None,
            "outbox_pending": 2,
            "identity_deferred": 0,
            "failure_count": 5,
            "incident_key": "erp_unreachable",
            "last_error": "timeout ERP",
        }
        try:
            health.post_erp_health(
                report,
                base_url=base,
                api_key="secret",
                timeout=2,
            )
            with patch.dict(
                health.os.environ,
                {
                    "HIK_MAYA_WEBHOOK_URL": f"{base}/maya",
                    "HIK_ALERT_WHATSAPP_TO": "5215555555555",
                },
                clear=True,
            ):
                health.send_configured_alerts("mensaje", "erp_unreachable")
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

        self.assertEqual(_RecorderHandler.requests[0][0], "/rrhh/api/asistencia-hik/v2/health/")
        self.assertEqual(_RecorderHandler.requests[0][1]["x-api-key"], "secret")
        self.assertEqual(_RecorderHandler.requests[0][2]["status"], "action_required")
        self.assertEqual(_RecorderHandler.requests[1][0], "/maya")
        self.assertEqual(_RecorderHandler.requests[1][2]["incident_key"], "erp_unreachable")


if __name__ == "__main__":
    unittest.main()
