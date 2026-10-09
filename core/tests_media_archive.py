import hashlib
import json
from datetime import date
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase, override_settings

from core.models import Sucursal, UserModuleAccess
from logistica.models import BitacoraSalidaLlegada, Repartidor, Unidad


@override_settings(MEDIA_ARCHIVE_ROOT="/app/storage/nas/media_archive", MEDIA_ARCHIVE_MOUNT_SOURCE="//10.77.216.2/ERP_BACKUPS")
class ArchiveMountIdentityTests(SimpleTestCase):
    def test_only_expected_read_only_bounded_nas_mount_is_accepted(self):
        from core.management.commands.archive_bitacora_media import verify_archive_mount

        cases = [
            ("8 7 0:40 / /app/storage/nas ro,nosuid,nodev,noexec - cifs //10.77.216.2/ERP_BACKUPS ro,soft,echo_interval=5", True),
            ("8 7 8:1 / / rw - ext4 /dev/vda1 rw", False),
            ("8 7 0:40 / /app/storage/nas rw,nosuid,nodev,noexec - cifs //10.77.216.2/ERP_BACKUPS rw,soft", False),
            ("8 7 0:40 / /app/storage/nas ro,nosuid,nodev,noexec - cifs //other/ERP_BACKUPS ro,soft", False),
            ("8 7 0:40 / /app/storage/nas ro,nosuid,nodev,noexec - cifs //10.77.216.2/ERP_BACKUPS ro,hard", False),
        ]
        for mountinfo, accepted in cases:
            with self.subTest(mountinfo=mountinfo), patch.object(Path, "read_text", return_value=mountinfo):
                if accepted:
                    verify_archive_mount()
                else:
                    with self.assertRaises(CommandError):
                        verify_archive_mount()
        with override_settings(MEDIA_ARCHIVE_ROOT="/app/storage/nas/../staging"), patch.object(Path, "read_text", return_value=cases[0][0]):
            with self.assertRaises(CommandError):
                verify_archive_mount()


class HistoricalBitacoraArchiveTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name).resolve()
        self.media, self.archive, self.index = (base / name for name in ("media", "nas", "index"))
        self.plan = base / "may.json"
        self.stage = base / "stage"
        self.name = "bitacora/mayo.jpg"
        self.payload = b"\xff\xd8\xffsynthetic-evidence"
        self.override = override_settings(
            MEDIA_ROOT=str(self.media), MEDIA_ARCHIVE_ROOT=str(self.archive),
            MEDIA_ARCHIVE_INDEX_ROOT=str(self.index),
            STORAGES={
                "default": {"BACKEND": "core.media_archive.ArchivedMediaStorage"},
                "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
            },
        )
        self.override.enable()
        self.addCleanup(self.override.disable)
        # This is a local NAS fixture, explicitly not actual NAS receipt.
        mount = patch("core.management.commands.archive_bitacora_media.verify_archive_mount")
        mount.start()
        self.addCleanup(mount.stop)
        self.source = self.media / self.name
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(self.payload)
        self.remote = self.archive / self.name
        self.remote.parent.mkdir(parents=True)
        self.branch = Sucursal.objects.create(codigo="ARCHIVE", nombre="Archive synthetic")
        users = get_user_model()
        self.driver = users.objects.create_user("archive-driver")
        self.other = users.objects.create_user("archive-other")
        self.reviewer = users.objects.create_user("archive-review")
        UserModuleAccess.objects.create(user=self.reviewer, module="logistica.bitacoras", access=UserModuleAccess.ACCESS_VIEW)
        repartidor = Repartidor.objects.create(user=self.driver, sucursal=self.branch)
        unit = Unidad.objects.create(codigo="ARCHIVE", descripcion="Archive synthetic", sucursal=self.branch)
        self.record = BitacoraSalidaLlegada.objects.create(
            repartidor=repartidor, unidad=unit, km_salida=1, nivel_gas_salida="lleno",
            foto_tablero_salida=self.name, cerrada=True,
        )
        BitacoraSalidaLlegada.objects.filter(pk=self.record.pk).update(fecha=date(2026, 5, 15))

    def command(self, mode, **kwargs):
        output = StringIO()
        call_command("archive_bitacora_media", month="2026-05", mode=mode, plan=str(self.plan), actor="synthetic-admin", stdout=output, **kwargs)
        return json.loads(output.getvalue())

    def verified(self):
        self.command("plan")
        self.remote.write_bytes(self.payload)
        self.command("verify")

    def test_copy_receipt_retirement_repeat_and_restore_preserve_record_and_bytes(self):
        result = self.command("plan")
        self.assertEqual(result["files"], 1)
        self.command("stage", stage_root=str(self.stage))
        self.assertEqual((self.stage / self.name).read_bytes(), self.payload)
        self.assertTrue(self.source.exists())
        self.remote.write_bytes(self.payload)
        self.command("verify")
        self.assertTrue(self.source.exists())
        self.client.force_login(self.reviewer)
        preview = self.client.get(f"/media/{self.name}?archive=1")
        self.assertEqual(preview.status_code, 200)
        self.assertEqual(b"".join(preview.streaming_content), self.payload)
        self.command("retire", confirm_plan=result["plan_sha256"])
        self.assertFalse(self.source.exists())
        self.command("retire", confirm_plan=result["plan_sha256"])
        self.record.refresh_from_db()
        self.assertEqual(self.record.foto_tablero_salida.name, self.name)
        with self.record.foto_tablero_salida.open("rb") as image:
            self.assertEqual(image.read(), self.payload)
        response = self.client.get(f"/media/{self.name}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.streaming_content), self.payload)
        self.assertEqual(response["Cache-Control"], "private, no-store")
        self.command("restore")
        self.assertEqual(self.source.read_bytes(), self.payload)

    def test_archive_requires_current_resource_authorization(self):
        self.verified()
        for original_present in (True, False):
            if not original_present:
                self.source.unlink()
            for query in ("", "?archive=1"):
                for user, expected in ((None, 404), (self.other, 404), (self.driver, 200), (self.reviewer, 200)):
                    with self.subTest(original_present=original_present, query=query, user=user):
                        self.client.logout()
                        if user:
                            self.client.force_login(user)
                        response = self.client.get(f"/media/{self.name}{query}")
                        self.assertEqual(response.status_code, expected)
                        self.assertEqual(response["Cache-Control"], "private, no-store")
                        self.assertEqual(response["X-Content-Type-Options"], "nosniff")
                        if getattr(response, "streaming", False):
                            self.assertEqual(b"".join(response.streaming_content), self.payload)
                        else:
                            self.assertNotIn(self.payload, response.content)
        self.driver.is_active = False
        self.driver.save(update_fields=["is_active"])
        self.client.force_login(self.driver)
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 404)
        self.source.write_bytes(self.payload)
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 404)

    def test_indexed_original_rechecks_current_record_module_and_staff_permissions(self):
        self.verified()
        self.client.force_login(self.reviewer)
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 200)
        UserModuleAccess.objects.filter(user=self.reviewer).delete()
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 404)
        self.other.is_staff = True
        self.other.save(update_fields=["is_staff"])
        self.client.force_login(self.other)
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 404)
        self.other.user_permissions.add(Permission.objects.get(
            content_type__app_label="logistica", codename="view_bitacorasalidallegada",
        ))
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 200)
        BitacoraSalidaLlegada.objects.filter(pk=self.record.pk).update(foto_tablero_salida="")
        self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 404)

    def test_indexed_original_canonical_aliases_require_the_same_permission(self):
        self.verified()
        for alias in ("bitacora/./mayo.jpg", "public/../bitacora/mayo.jpg", "bitacora%252Fmayo.jpg", "bitacora%5Cmayo.jpg"):
            for user, expected in ((None, 404), (self.other, 404), (self.reviewer, 200)):
                with self.subTest(alias=alias, user=user):
                    self.client.logout()
                    if user:
                        self.client.force_login(user)
                    response = self.client.get(f"/media/{alias}")
                    self.assertEqual(response.status_code, expected)
                    self.assertEqual(response["Cache-Control"], "private, no-store")
                    if getattr(response, "streaming", False):
                        self.assertEqual(b"".join(response.streaming_content), self.payload)

    def test_corrupt_index_fails_closed_even_while_original_is_present(self):
        self.verified()
        index_file = next(self.index.glob("*.json"))
        index_file.write_text("{corrupt")
        for user in (None, self.other, self.reviewer):
            with self.subTest(user=user):
                self.client.logout()
                if user:
                    self.client.force_login(user)
                response = self.client.get(f"/media/{self.name}")
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response["Retry-After"], "30")
                self.assertEqual(response["Cache-Control"], "private, no-store")
                self.assertEqual(response["X-Content-Type-Options"], "nosniff")
                self.assertNotIn(self.payload, response.content)
        self.assertEqual(self.source.read_bytes(), self.payload)

    def test_indexed_local_original_is_available_without_opening_offline_nas(self):
        self.verified()
        self.remote.unlink()
        self.client.force_login(self.reviewer)
        with patch("core.media_archive.open_archive", side_effect=AssertionError("NAS must not be opened")):
            response = self.client.get(f"/media/{self.name}")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(b"".join(response.streaming_content), self.payload)
            self.assertEqual(response["Cache-Control"], "private, no-store")
        response = self.client.get(f"/media/{self.name}?archive=1")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response["Retry-After"], "30")

    def test_unindexed_and_disabled_archive_original_keep_existing_public_behavior(self):
        response = self.client.get(f"/media/{self.name}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.streaming_content), self.payload)
        with override_settings(MEDIA_ARCHIVE_ROOT=""):
            response = self.client.get(f"/media/{self.name}")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(b"".join(response.streaming_content), self.payload)
        public = self.media / "public.jpg"
        public.write_bytes(self.payload)
        response = self.client.get("/media/public.jpg")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.streaming_content), self.payload)
        response = self.client.get("/media/missing-public.jpg")
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("private", response.get("Cache-Control", ""))

    def test_disabling_nas_reads_does_not_make_an_indexed_original_public(self):
        from core.media_archive import load_archive_entry

        self.verified()
        with override_settings(MEDIA_ARCHIVE_ROOT=""):
            # Default storage/command callers retain their disabled behavior.
            self.assertIsNone(load_archive_entry(self.name))
            for user, expected in ((None, 404), (self.other, 404), (self.reviewer, 200)):
                with self.subTest(user=user):
                    self.client.logout()
                    if user:
                        self.client.force_login(user)
                    response = self.client.get(f"/media/{self.name}")
                    self.assertEqual(response.status_code, expected)
                    self.assertEqual(response["Cache-Control"], "private, no-store")
                    if getattr(response, "streaming", False):
                        self.assertEqual(b"".join(response.streaming_content), self.payload)
            next(self.index.glob("*.json")).write_text("{corrupt")
            response = self.client.get(f"/media/{self.name}")
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response["Retry-After"], "30")
            self.assertEqual(response["Cache-Control"], "private, no-store")
            self.assertNotIn(self.payload, response.content)

    def test_disabled_nas_privacy_lookup_still_rejects_overlap_and_index_symlinks(self):
        from core.media_archive import ArchiveUnavailable, load_archive_entry

        self.verified()
        with override_settings(MEDIA_ARCHIVE_ROOT="", MEDIA_ARCHIVE_INDEX_ROOT=str(self.media)):
            with self.assertRaises(ArchiveUnavailable):
                load_archive_entry(self.name, include_disabled=True)
        index_file = next(self.index.glob("*.json"))
        target = self.index.parent / "index-copy.json"
        target.write_bytes(index_file.read_bytes())
        index_file.unlink()
        index_file.symlink_to(target)
        with override_settings(MEDIA_ARCHIVE_ROOT=""):
            with self.assertRaises(ArchiveUnavailable):
                load_archive_entry(self.name, include_disabled=True)

    def test_offline_and_corrupt_archive_return_retryable_error_without_leaking_bytes(self):
        self.verified()
        self.source.unlink()
        self.client.force_login(self.reviewer)
        for payload in (None, b"wrong"):
            if payload is None:
                self.remote.unlink()
            else:
                self.remote.write_bytes(payload)
            response = self.client.get(f"/media/{self.name}")
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response["Retry-After"], "30")
            self.assertNotIn(self.payload, response.content)

    def test_driver_exception_only_allows_own_archived_evidence(self):
        self.verified()
        self.source.unlink()
        Repartidor.objects.create(user=self.other, sucursal=self.branch)
        UserModuleAccess.objects.create(user=self.other, module="logistica.bitacoras", access=UserModuleAccess.ACCESS_VIEW)
        self.client.force_login(self.other)
        response = self.client.get(f"/media/{self.name}")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, "/logistica/app/")
        self.client.force_login(self.driver)
        with override_settings(MEDIA_ARCHIVE_ROOT=""):
            self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 302)
        self.assertEqual(self.client.get("/rrhh/").status_code, 302)

    def test_retirement_blocked_without_receipt_confirmation_or_with_changed_original(self):
        result = self.command("plan")
        with self.assertRaises(CommandError):
            self.command("retire", confirm_plan=result["plan_sha256"])
        self.assertTrue(self.source.exists())
        self.remote.write_bytes(self.payload)
        self.command("verify")
        with self.assertRaises(CommandError):
            self.command("retire", confirm_plan="wrong")
        self.source.write_bytes(b"changed")
        with self.assertRaises(CommandError):
            self.command("retire", confirm_plan=result["plan_sha256"])
        self.assertEqual(self.source.read_bytes(), b"changed")

    def test_open_or_newer_records_and_shared_names_are_not_eligible(self):
        BitacoraSalidaLlegada.objects.filter(pk=self.record.pk).update(cerrada=False)
        self.assertEqual(self.command("plan")["files"], 0)
        self.plan.unlink()
        BitacoraSalidaLlegada.objects.filter(pk=self.record.pk).update(cerrada=True)
        # Same name referenced by an ineligible record must remain local.
        second = BitacoraSalidaLlegada.objects.create(
            repartidor=self.record.repartidor, unidad=self.record.unidad, km_salida=2,
            nivel_gas_salida="lleno", foto_tablero_salida=self.name,
        )
        self.assertEqual(self.command("plan")["files"], 0)
        self.assertTrue(self.source.exists())

    def test_changed_reference_or_reopened_record_blocks_retirement(self):
        self.verified()
        sha = hashlib.sha256(self.plan.read_bytes()).hexdigest()
        BitacoraSalidaLlegada.objects.filter(pk=self.record.pk).update(cerrada=False)
        with self.assertRaises(CommandError):
            self.command("retire", confirm_plan=sha)
        self.assertTrue(self.source.exists())

    def test_disabled_archive_and_missing_original_keep_not_found_behavior(self):
        self.source.unlink()
        self.client.force_login(self.reviewer)
        with override_settings(MEDIA_ARCHIVE_ROOT=""):
            self.assertEqual(self.client.get(f"/media/{self.name}").status_code, 404)

    def test_month_plan_path_and_symlink_guards(self):
        with self.assertRaises(CommandError):
            call_command("archive_bitacora_media", month="2026-10", plan=str(self.plan))
        with self.assertRaises(CommandError):
            call_command("archive_bitacora_media", month="2026-05", plan=str(self.media / "private.json"))
        self.source.unlink()
        self.source.symlink_to(self.remote)
        with self.assertRaises(CommandError):
            self.command("plan")

    def test_unreferenced_edited_plan_cannot_be_retired(self):
        self.command("plan")
        value = json.loads(self.plan.read_text())
        value["entries"][0]["source_refs"] = []
        self.plan.write_text(json.dumps(value))
        with self.assertRaises(CommandError):
            self.command("retire", confirm_plan=hashlib.sha256(self.plan.read_bytes()).hexdigest())
        self.assertTrue(self.source.exists())

    def test_interrupted_restore_does_not_publish_partial_original(self):
        self.verified()
        self.source.unlink()

        def partial_copy(source, destination):
            destination.write(source.read(3))
            raise OSError("synthetic disk full")

        with patch("core.management.commands.archive_bitacora_media.shutil.copyfileobj", side_effect=partial_copy):
            with self.assertRaises(CommandError):
                self.command("restore")
        self.assertFalse(self.source.exists())
        with self.record.foto_tablero_salida.open("rb") as image:
            self.assertEqual(image.read(), self.payload)
        self.command("restore")
        self.assertEqual(self.source.read_bytes(), self.payload)

    def test_retirement_requires_durable_audit_before_removing_original(self):
        self.verified()
        sha = hashlib.sha256(self.plan.read_bytes()).hexdigest()
        with patch("core.management.commands.archive_bitacora_media.audit_event", side_effect=OSError("synthetic audit disk full")):
            with self.assertRaises(CommandError):
                self.command("retire", confirm_plan=sha)
        self.assertTrue(self.source.exists())
