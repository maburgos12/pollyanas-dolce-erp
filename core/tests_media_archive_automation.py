import fcntl
import hashlib
from io import BytesIO, StringIO
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.conf import settings
from django.contrib.sessions.models import Session
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.management.commands.sync_bitacora_media_archive import Command, NoRedirect


class AutomaticMediaArchiveTests(TestCase):
    """Synthetic local/NAS/HTTP fixtures; these do not prove production receipt."""

    def setUp(self):
        from core.tests_media_archive import HistoricalBitacoraArchiveTests

        HistoricalBitacoraArchiveTests.setUp(self)
        self.plan_dir = self.media.parent / "plans"
        self.plan = self.plan_dir / "2026-05.plan.json"
        self.state_path = self.plan_dir / "2026-05.state.json"
        self.stage = self.media.parent / "stage"
        mount = patch("core.management.commands.sync_bitacora_media_archive.verify_archive_mount")
        mount.start()
        self.addCleanup(mount.stop)
        self.http_keys = []
        self.opener = MagicMock()
        self.opener.open.side_effect = self.synthetic_http
        http = patch("core.management.commands.sync_bitacora_media_archive.build_opener", return_value=self.opener)
        http.start()
        self.addCleanup(http.stop)

    def synthetic_http(self, request, *, timeout):
        self.assertEqual(timeout, 10)
        self.assertTrue(request.full_url.startswith("http://127.0.0.1:8011/media/bitacora/"))
        self.assertTrue(request.full_url.endswith("?archive=1"))
        self.assertEqual(request.get_header("Host"), "erp.pollyanasdolce.com")
        self.assertEqual(request.get_header("X-forwarded-proto"), "https")
        cookie = request.get_header("Cookie")
        key = cookie.removeprefix(settings.SESSION_COOKIE_NAME + "=")
        self.http_keys.append(key)
        session = Session.objects.get(session_key=key)
        self.assertEqual(session.get_decoded()["_auth_user_id"], str(self.reviewer.pk))
        stream = BytesIO(self.payload)
        response = MagicMock()
        response.status = 200
        response.headers = {"Cache-Control": "private, no-store"}
        response.read.side_effect = stream.read
        response.__enter__.return_value = response
        return response

    def run_sync(self, **overrides):
        output = StringIO()
        options = {
            "month": "2026-05", "plan_dir": str(self.plan_dir), "stage_root": str(self.stage),
            "verification_user": self.reviewer.username,
            "verification_origin": "http://127.0.0.1:8011",
            "verification_host": "erp.pollyanasdolce.com",
        }
        call_command("sync_bitacora_media_archive", stdout=output, **{**options, **overrides})
        return json.loads(output.getvalue())

    def test_missing_nas_waits_after_stage_and_never_retires_original(self):
        result = self.run_sync()
        self.assertEqual(result["status"], "WAIT_NAS")
        self.assertEqual((self.stage / self.name).read_bytes(), self.payload)
        self.assertEqual(self.source.read_bytes(), self.payload)
        self.assertFalse(json.loads(self.state_path.read_text())["completed"])
        self.opener.open.assert_not_called()
        self.assertFalse(Session.objects.exists())

    def test_http_denial_corruption_or_public_cache_never_retires(self):
        self.remote.write_bytes(self.payload)
        for status, payload, cache in (
            (403, self.payload, "private, no-store"),
            (200, b"corrupt", "private, no-store"),
            (200, self.payload, "public, max-age=60"),
        ):
            with self.subTest(status=status, cache=cache):
                response = MagicMock()
                response.__enter__.return_value = response
                response.status = status
                response.headers = {"Cache-Control": cache}
                response.read.return_value = payload
                self.opener.open.side_effect = None
                self.opener.open.return_value = response
                with self.assertRaises(CommandError):
                    self.run_sync()
                self.assertEqual(self.source.read_bytes(), self.payload)
                self.assertFalse(Session.objects.exists())
                self.assertEqual(json.loads(self.state_path.read_text())["status"], "ERROR")

    def test_success_repeat_preserves_records_deletes_session_and_only_own_staging(self):
        self.remote.write_bytes(self.payload)
        self.stage.mkdir(mode=0o700)
        unrelated = self.stage / "unrelated.jpg"
        unrelated.write_bytes(b"do not touch")
        last_login = self.reviewer.last_login
        result = self.run_sync()
        self.assertEqual(result["status"], "COMPLETE")
        self.assertFalse(self.source.exists())
        self.assertFalse((self.stage / self.name).exists())
        self.assertEqual(unrelated.read_bytes(), b"do not touch")
        self.assertEqual(self.remote.read_bytes(), self.payload)
        self.assertTrue(json.loads(self.state_path.read_text())["completed"])
        self.assertEqual(len(self.http_keys), 1)
        self.assertFalse(Session.objects.filter(session_key__in=self.http_keys).exists())
        self.reviewer.refresh_from_db()
        self.assertEqual(self.reviewer.last_login, last_login)
        self.record.refresh_from_db()
        self.assertEqual(self.record.foto_tablero_salida.name, self.name)
        with patch.object(Command, "archive_command") as archive_command:
            self.assertEqual(self.run_sync()["status"], "COMPLETE")
            archive_command.assert_not_called()
        self.assertEqual(self.opener.open.call_count, 1)

    def test_concurrent_lock_returns_wait_without_plan_or_session(self):
        self.plan_dir.mkdir(mode=0o700)
        with (self.plan_dir / "sync.lock").open("w") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.run_sync()["status"], "WAIT_LOCK")
        self.assertFalse(self.plan.exists())
        self.assertFalse(Session.objects.exists())
        self.opener.open.assert_not_called()

    def test_completed_batch_missing_or_corrupt_index_cannot_report_complete(self):
        self.remote.write_bytes(self.payload)
        self.assertEqual(self.run_sync()["status"], "COMPLETE")
        original_state = self.state_path.read_bytes()
        index_path = self.index / (hashlib.sha256(self.name.encode()).hexdigest() + ".json")
        index_path.unlink()
        for content in (None, b"{corrupt"):
            with self.subTest(content=content):
                if content is not None:
                    index_path.write_bytes(content)
                with self.assertRaises(CommandError):
                    self.run_sync()
                self.assertEqual(self.state_path.read_bytes(), original_state)
                self.assertFalse(self.source.exists())
                self.assertFalse((self.stage / self.name).exists())
        self.assertEqual(self.opener.open.call_count, 1)

    def test_external_origins_credentials_redirects_and_unknown_host_are_rejected(self):
        for origin in (
            "https://127.0.0.1:8011", "http://erp.pollyanasdolce.com:8011",
            "http://127.0.0.1", "http://localhost:8011/path", "http://localhost:8011?x=1",
            "http://127.0.0.1:0",
            "http://user:secret@127.0.0.1:8011", "http://127.0.0.1:8011#fragment",
        ):
            with self.subTest(origin=origin), self.assertRaises(CommandError):
                self.run_sync(verification_origin=origin)
        with self.assertRaises(CommandError):
            self.run_sync(verification_host="external.invalid")
        self.assertIsNone(NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://external.invalid"))
        self.opener.open.assert_not_called()
        self.assertFalse(Session.objects.exists())

    def test_http_timeout_deletes_only_owned_session_and_retains_sources(self):
        unrelated_session = self.client.session
        unrelated_session["preserve"] = True
        unrelated_session.save()
        self.remote.write_bytes(self.payload)
        self.opener.open.side_effect = TimeoutError
        with self.assertRaises(CommandError):
            self.run_sync()
        self.assertTrue(self.source.exists())
        self.assertEqual(list(Session.objects.values_list("session_key", flat=True)), [unrelated_session.session_key])

    def test_partial_retirement_reuses_plan_and_stages_remaining_only(self):
        second_name = "bitacora/second.jpg"
        second_source = self.media / second_name
        second_source.write_bytes(self.payload)
        (self.archive / second_name).write_bytes(self.payload)
        type(self.record).objects.filter(pk=self.record.pk).update(foto_tablero_llegada=second_name)
        self.remote.write_bytes(self.payload)
        original_archive_command = Command.archive_command

        def stop_after_retire(command, options, mode, plan, **kwargs):
            if mode == "retire":
                self.source.unlink()  # Synthetic interruption after one owned original.
                raise OSError("synthetic interruption")
            original_archive_command(command, options, mode, plan, **kwargs)

        with patch.object(Command, "archive_command", stop_after_retire), self.assertRaises(CommandError):
            self.run_sync()
        self.assertFalse(self.source.exists())
        self.assertTrue(second_source.exists())
        original_plan = self.plan.read_bytes()
        self.assertFalse(json.loads(self.state_path.read_text())["completed"])
        self.assertEqual(self.run_sync()["status"], "COMPLETE")
        self.assertEqual(self.plan.read_bytes(), original_plan)
        self.assertEqual([entry["name"] for entry in json.loads((self.plan_dir / "2026-05.stage.json").read_text())["entries"]], [second_name])
        self.assertFalse(second_source.exists())
        self.assertFalse((self.stage / self.name).exists())
        self.assertEqual(self.opener.open.call_count, 4)

    def test_all_http_photos_must_pass_before_any_original_is_retired(self):
        second_name = "bitacora/second.jpg"
        second_source = self.media / second_name
        second_source.write_bytes(self.payload)
        (self.archive / second_name).write_bytes(self.payload)
        type(self.record).objects.filter(pk=self.record.pk).update(foto_tablero_llegada=second_name)
        self.remote.write_bytes(self.payload)
        first = True

        def deny_second(request, *, timeout):
            nonlocal first
            response = self.synthetic_http(request, timeout=timeout)
            if not first:
                response.status = 403
            first = False
            return response

        self.opener.open.side_effect = deny_second
        with self.assertRaises(CommandError):
            self.run_sync()
        self.assertTrue(self.source.exists())
        self.assertTrue(second_source.exists())
        self.assertFalse(Session.objects.exists())

    def test_completed_batch_retains_modified_stage_and_records_complete_for_retry(self):
        self.remote.write_bytes(self.payload)
        original_cleanup = Command.cleanup_staging

        def changed_staging(command, entries, stage, plan_sha):
            (stage / self.name).write_bytes(b"different new contents")
            original_cleanup(command, entries, stage, plan_sha)

        with patch.object(Command, "cleanup_staging", changed_staging), self.assertRaises(CommandError):
            self.run_sync()
        self.assertFalse(self.source.exists())
        self.assertTrue(json.loads(self.state_path.read_text())["completed"])
        self.assertEqual((self.stage / self.name).read_bytes(), b"different new contents")

    def test_plan_identity_change_and_inactive_or_unprivileged_user_keep_original(self):
        self.assertEqual(self.run_sync()["status"], "WAIT_NAS")
        state = self.state_path.read_bytes()
        plan = json.loads(self.plan.read_text())
        plan["created_at"] = "changed"
        self.plan.write_text(json.dumps(plan))
        with self.assertRaises(CommandError):
            self.run_sync()
        self.assertEqual(self.state_path.read_bytes(), state)
        self.assertTrue(self.source.exists())

    def test_inactive_or_unprivileged_existing_user_never_creates_session(self):
        self.remote.write_bytes(self.payload)
        with self.assertRaises(CommandError):
            self.run_sync(verification_user=self.other.username)
        self.reviewer.is_active = False
        self.reviewer.save(update_fields=["is_active"])
        with self.assertRaises(CommandError):
            self.run_sync()
        self.assertTrue(self.source.exists())
        self.assertFalse(Session.objects.exists())
