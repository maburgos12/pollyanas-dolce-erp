import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.files.base import ContentFile
from django.test import SimpleTestCase, override_settings

from core.media_archive import (
    ArchiveUnavailable,
    ArchivedMediaStorage,
    MAX_ARCHIVE_BYTES,
    archive_path,
    load_archive_entry,
    open_archive,
    write_archive_entry,
)


class ArchivedMediaStorageTests(SimpleTestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.local = self.root / "media"
        self.archive = self.root / "archive"
        self.index = self.root / "private-index"
        self.local.mkdir()
        self.archive.mkdir()
        self.configuration = override_settings(
            MEDIA_ROOT=str(self.local),
            MEDIA_ARCHIVE_ROOT=str(self.archive),
            MEDIA_ARCHIVE_INDEX_ROOT=str(self.index),
        )
        self.configuration.enable()
        self.addCleanup(self.configuration.disable)
        self.storage = ArchivedMediaStorage()
        self.name = "bitacora/tickets/2025-09/evidencia.jpg"
        self.content = b"ticket real\x00\xff"

    def register(self, *, content=None, name=None):
        content = self.content if content is None else content
        name = self.name if name is None else name
        path = self.archive / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        entry = {
            "name": name,
            "sha256": hashlib.sha256(content).hexdigest(),
            "bytes": len(content),
            "status": "verified",
            "source_refs": [{"model": "logistica.BitacoraSalidaLlegada", "id": 4}],
        }
        write_archive_entry(entry)
        return entry

    def index_path(self):
        return self.index / (hashlib.sha256(self.name.encode("utf-8")).hexdigest() + ".json")

    def test_disabled_and_unknown_do_not_read_archive(self):
        self.register()
        with override_settings(MEDIA_ARCHIVE_ROOT=""), patch("core.media_archive._open_read") as reader:
            self.assertIsNone(load_archive_entry(self.name))
            self.assertFalse(self.storage.exists(self.name))
            with self.assertRaises(FileNotFoundError):
                self.storage.open(self.name)
            reader.assert_not_called()
        with self.assertRaises(FileNotFoundError):
            open_archive("bitacora/unregistered.jpg")
        (self.archive / "bitacora/unregistered.jpg").write_bytes(b"unlisted")
        self.assertFalse(self.storage.exists("bitacora/unregistered.jpg"))

    def test_local_content_takes_precedence_even_with_unavailable_archive(self):
        self.register()
        self.storage.save(self.name, ContentFile(b"local"))
        # exists() sees registered names, so a normal save chooses a fresh name.
        local_path = self.local / self.name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"local original")
        with patch("core.media_archive.load_archive_entry", side_effect=ArchiveUnavailable):
            with self.storage.open(self.name) as source:
                self.assertEqual(source.read(), b"local original")
            self.assertTrue(self.storage.exists(self.name))
            self.assertEqual(self.storage.size(self.name), 14)

    def test_archive_fallback_verifies_identical_bytes_without_local_cache(self):
        self.register()
        with self.storage.open(self.name) as source:
            self.assertEqual(source.name, self.name)
            self.assertEqual(source.read(), self.content)
        self.assertFalse((self.local / self.name).exists())
        self.assertEqual(archive_path(self.name), self.archive / self.name)

    @override_settings(STORAGES={
        "default": {"BACKEND": "core.media_archive.ArchivedMediaStorage"},
        "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
    })
    def test_actual_bitacora_field_open_reads_archived_ticket(self):
        from logistica.models import BitacoraSalidaLlegada

        self.register()
        bitacora = BitacoraSalidaLlegada(foto_ticket_combustible=self.name)
        with bitacora.foto_ticket_combustible.open("rb") as source:
            self.assertEqual(source.read(), self.content)
        self.assertEqual(bitacora.foto_ticket_combustible.size, len(self.content))
        with bitacora.foto_ticket_combustible.open("rb") as source:
            self.assertEqual(source.read(), self.content)
        (self.archive / self.name).write_bytes(b"corrupt")
        with self.assertRaises(ArchiveUnavailable):
            bitacora.foto_ticket_combustible.open("rb")

    def test_exists_and_size_use_only_index_even_when_archive_is_offline(self):
        self.register()
        (self.archive / self.name).unlink()
        from core.media_archive import _open_read

        def local_index_only(path):
            self.assertTrue(path.is_relative_to(self.index))
            return _open_read(path)

        with patch("core.media_archive._open_read", side_effect=local_index_only):
            self.assertTrue(self.storage.exists(self.name))
            self.assertEqual(self.storage.size(self.name), len(self.content))

    def test_corrupt_truncated_oversized_or_missing_archive_fails_closed(self):
        self.register()
        path = self.archive / self.name
        for content in (b"x" * len(self.content), self.content[:-1], self.content + b"extra"):
            with self.subTest(content=content):
                path.write_bytes(content)
                with self.assertRaises(ArchiveUnavailable) as caught:
                    self.storage.open(self.name)
                self.assertNotIn(str(self.archive), str(caught.exception))
        path.unlink()
        with self.assertRaises(ArchiveUnavailable):
            self.storage.open(self.name)

    def test_index_filename_and_stored_name_are_exact(self):
        entry = self.register()
        self.assertEqual(list(self.index.iterdir()), [self.index_path()])
        self.assertEqual(load_archive_entry(self.name), entry)
        entry["name"] = "bitacora/another.jpg"
        self.index_path().write_text(json.dumps(entry))
        with self.assertRaises(ArchiveUnavailable):
            load_archive_entry(self.name)

    def test_invalid_index_values_fail_closed(self):
        original = self.register()
        for field, value in (
            ("status", "copied"), ("sha256", "A" * 64), ("sha256", "invalid"),
            ("bytes", 0), ("bytes", True), ("bytes", MAX_ARCHIVE_BYTES + 1),
        ):
            with self.subTest(field=field, value=value):
                entry = dict(original, **{field: value})
                self.index_path().write_text(json.dumps(entry))
                with self.assertRaises(ArchiveUnavailable):
                    load_archive_entry(self.name)
                with self.assertRaises(ValueError):
                    write_archive_entry(entry)
        self.index_path().write_text("{")
        with self.assertRaises(ArchiveUnavailable):
            load_archive_entry(self.name)

    def test_noncanonical_and_traversing_names_cannot_be_registered_or_opened(self):
        entry = self.register()
        for name in ("/bitacora/file", "bitacora/../secret", "bitacora/./file", "bitacora//file", "bitacora/file/", "bitacora\\file", "other/file", "bitacora", "bitacora/\x00file"):
            with self.subTest(name=name):
                self.assertIsNone(load_archive_entry(name))
                with self.assertRaises(FileNotFoundError):
                    open_archive(name)
                with self.assertRaises(ValueError):
                    write_archive_entry(dict(entry, name=name))

    def test_conflicting_roots_are_rejected_without_archive_io(self):
        for setting, root in (
            ("MEDIA_ARCHIVE_ROOT", self.local),
            ("MEDIA_ARCHIVE_ROOT", self.local / "nested"),
            ("MEDIA_ARCHIVE_ROOT", self.root),
            ("MEDIA_ARCHIVE_INDEX_ROOT", self.local),
            ("MEDIA_ARCHIVE_INDEX_ROOT", self.local / "nested"),
            ("MEDIA_ARCHIVE_INDEX_ROOT", self.archive),
        ):
            with self.subTest(setting=setting, root=root), override_settings(**{setting: str(root)}):
                with patch("core.media_archive._open_read") as reader:
                    with self.assertRaises(ArchiveUnavailable):
                        load_archive_entry(self.name)
                    reader.assert_not_called()

    def test_conflicting_archive_config_does_not_affect_unrelated_files(self):
        with override_settings(MEDIA_ARCHIVE_ROOT=str(self.local)), patch("core.media_archive._open_read") as reader:
            name = "other/new-capture.jpg"
            self.assertFalse(self.storage.exists(name))
            self.assertIsNone(load_archive_entry(name))
            with self.assertRaises(FileNotFoundError):
                self.storage.open(name)
            self.assertEqual(self.storage.save(name, ContentFile(b"unrelated")), name)
            with self.storage.open(name) as source:
                self.assertEqual(source.read(), b"unrelated")
            reader.assert_not_called()

    def test_archive_file_parent_and_root_symlinks_are_rejected(self):
        self.register()
        path = self.archive / self.name
        outside = self.root / "outside.jpg"
        outside.write_bytes(self.content)
        path.unlink()
        path.symlink_to(outside)
        with self.assertRaises(ArchiveUnavailable):
            open_archive(self.name)
        with self.assertRaises(ArchiveUnavailable):
            archive_path(self.name)
        path.unlink()
        path.parent.rmdir()
        path.parent.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ArchiveUnavailable):
            open_archive(self.name)
        link = self.root / "archive-link"
        link.symlink_to(self.archive, target_is_directory=True)
        with override_settings(MEDIA_ARCHIVE_ROOT=str(link)):
            with self.assertRaises(ArchiveUnavailable):
                open_archive(self.name)

    def test_index_symlinks_are_rejected(self):
        self.register()
        real_index = self.root / "real-index.json"
        self.index_path().rename(real_index)
        self.index_path().symlink_to(real_index)
        with self.assertRaises(ArchiveUnavailable):
            load_archive_entry(self.name)
        index_link = self.root / "index-link"
        index_link.symlink_to(self.index, target_is_directory=True)
        with override_settings(MEDIA_ARCHIVE_INDEX_ROOT=str(index_link)):
            with self.assertRaises(ArchiveUnavailable):
                load_archive_entry(self.name)

    def test_archive_writes_are_rejected_and_local_save_delete_work(self):
        self.register()
        for mode in ("r", "w", "wb", "ab", "r+b"):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.storage.open(self.name, mode)
        self.assertFalse((self.local / self.name).exists())
        saved = self.storage.save(self.name, ContentFile(b"new capture"))
        self.assertNotEqual(saved, self.name)
        with self.storage.open(saved, "wb") as source:
            source.write(b"edited")
        with self.storage.open(saved) as source:
            self.assertEqual(source.read(), b"edited")
        self.storage.delete(saved)
        self.assertFalse(self.storage.exists(saved))
        self.storage.delete(self.name)
        self.assertEqual((self.archive / self.name).read_bytes(), self.content)

    def test_large_archive_spools_to_temporary_disk_and_is_removed_on_close(self):
        content = b"x" * (1024 * 1024 + 1)
        self.register(content=content)
        with open_archive(self.name) as source:
            self.assertTrue(source.file._rolled)
            self.assertEqual(source.read(), content)
        self.assertTrue(source.closed)
