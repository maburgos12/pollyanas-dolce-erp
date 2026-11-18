import hashlib
import json
import os
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from pos_bridge.services import archive_access

from pos_bridge.services.archive_access import ArchiveStore, ArchivePending, ArchiveUnavailable, entry_id


class ArchiveAccessTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sources = Path(self.tmp.name).resolve() / 'raw_exports'
        self.sources.mkdir()
        self.store = ArchiveStore(root=Path(self.tmp.name).resolve() / 'archive', sources_root=self.sources, wait_seconds=0)
        self.path = self.sources / 'report.xls'
        self.path.write_bytes(b'workbook')
        digest = hashlib.sha256(b'workbook').hexdigest()
        self.entry = dict(schema=1, id=entry_id(self.path.name, digest), relative_path=self.path.name,
                          sha256=digest, bytes=8, nas_key=f'point-archive/objects/{digest}.xls',
                          verified_at=1700000000, consumers=['category_report'])

    def test_local_never_needs_catalog_or_queue(self):
        with self.store.open_source(self.path) as source:
            self.assertEqual(source.local_path.read_bytes(), b'workbook')
            self.assertEqual(source.logical_path, self.path)
        self.assertFalse((self.store.root / 'requests').exists())

    def test_missing_registered_requests_only_id_and_pending(self):
        self.store.register(self.entry)
        self.path.unlink()
        with self.assertRaises(ArchivePending):
            with self.store.open_source(self.path):
                self.fail('missing file yielded')
        request = json.loads((self.store.root / 'requests' / (self.entry['id'] + '.json')).read_text())
        self.assertEqual(request, {'id': self.entry['id']})

    def test_unknown_missing_remains_file_not_found(self):
        self.path.unlink()
        with self.assertRaises(FileNotFoundError):
            with self.store.open_source(self.path):
                pass

    def test_restore_verified_and_preserves_existing_different_original(self):
        self.store.register(self.entry)
        stage = Path(self.tmp.name).resolve() / 'stage.xls'
        stage.write_bytes(b'workbook')
        self.path.unlink()
        self.store.publish_restored(self.entry, stage)
        with self.store.open_source(self.path) as source:
            self.assertEqual(source.local_path.read_bytes(), b'workbook')
        self.path.write_bytes(b'different')
        with self.assertRaises(ArchiveUnavailable):
            self.store.publish_restored(self.entry, stage)
        self.assertEqual(self.path.read_bytes(), b'different')

    def test_registry_rejects_traversal_tamper_and_unverified(self):
        for changes in ({'relative_path': '../report.xls'}, {'id': 'a' * 64}, {'nas_key': '/etc/passwd'},
                        {'verified_at': ''}, {'bytes': 2**40}, {'consumers': ['unsupported']}, {'schema': True}, {'unexpected': 'field'}):
            with self.subTest(changes=changes), self.assertRaises(ArchiveUnavailable):
                self.store.register({**self.entry, **changes})

    def test_symlink_and_hardlink_rejected(self):
        other = self.sources / 'other.xls'
        other.symlink_to(self.path)
        with self.assertRaises(ArchiveUnavailable):
            self.store.snapshot(other)
        other.unlink()
        other.hardlink_to(self.path)
        with self.assertRaises(ArchiveUnavailable):
            self.store.snapshot(self.path)

    def test_readers_hold_shared_lease_and_exclude_removal(self):
        with self.store.open_source(self.path), self.store.open_source(self.path):
            with self.assertRaises(ArchivePending):
                with self.store.path_lock(self.path.name, exclusive=True, timeout=0):
                    self.fail('exclusive lease crossed active readers')
        with self.store.path_lock(self.path.name, exclusive=True, timeout=0):
            self.assertTrue(self.path.exists())

    def test_registered_local_tamper_and_symlink_catalog_rejected(self):
        self.store.register(self.entry)
        self.path.write_bytes(b'tampered')
        with self.assertRaises(ArchiveUnavailable):
            with self.store.open_source(self.path):
                pass
        index = self.store.root / 'catalog' / 'by-path' / (self.store.path_key(self.path.name) + '.json')
        index.unlink()
        index.symlink_to(self.store.root / 'catalog' / 'objects' / (self.entry['id'] + '.json'))
        with self.assertRaises(ArchiveUnavailable):
            self.store.catalog_entry(relative_path=self.path.name)

    def test_consumer_error_is_not_swallowed_as_missing_source(self):
        with self.assertRaisesRegex(FileNotFoundError, 'consumer failure'):
            with self.store.open_source(self.path):
                raise FileNotFoundError('consumer failure')

    def test_queue_symlink_cannot_write_outside_queue(self):
        self.store.register(self.entry)
        outside = Path(self.tmp.name).resolve() / 'outside'
        outside.mkdir()
        (self.store.root / 'requests').symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ArchiveUnavailable):
            self.store.enqueue(self.entry)
        self.assertEqual(list(outside.iterdir()), [])


    def test_process_crash_after_publication_leaves_single_link_readable_original(self):
        self.store.register(self.entry)
        stage = Path(self.tmp.name).resolve() / 'stage.xls'
        stage.write_bytes(b'workbook')
        self.path.unlink()
        rename = archive_access._rename_noreplace
        child = os.fork()
        if child == 0:
            def crash_after_rename(source, target):
                rename(source, target)
                os._exit(17)
            archive_access._rename_noreplace = crash_after_rename
            try:
                self.store.publish_restored(self.entry, stage)
            finally:
                os._exit(18)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 17)
        self.assertEqual(self.path.stat().st_nlink, 1)
        self.assertEqual(list(self.sources.glob('.restore-*')), [])
        with self.store.open_source(self.path) as source:
            self.assertEqual(source.local_path.read_bytes(), b'workbook')
        self.assertEqual(self.store.publish_restored(self.entry, stage), self.path)

    def test_atomic_publication_preserves_racing_destination(self):
        stage = Path(self.tmp.name).resolve() / 'stage.xls'
        stage.write_bytes(b'workbook')
        self.path.unlink()
        rename = archive_access._rename_noreplace
        def race_before_rename(source, target):
            target.write_bytes(b'new original')
            rename(source, target)
        with patch.object(archive_access, '_rename_noreplace', race_before_rename):
            with self.assertRaises(FileExistsError):
                self.store.publish_restored(self.entry, stage)
        self.assertEqual(self.path.read_bytes(), b'new original')
        self.assertEqual(list(self.sources.glob('.restore-*')), [])

    def test_storage_root_environment_and_explicit_sources(self):
        project_root = Path(archive_access.__file__).absolute().parents[2]
        for configured, expected in [('other/point', project_root / 'other/point'),
                                     (str(self.sources.parent), self.sources.parent)]:
            with self.subTest(configured=configured), patch.dict(os.environ, {'POINT_BRIDGE_STORAGE_ROOT': configured}):
                store = ArchiveStore()
                self.assertEqual(store.sources_root, expected / 'raw_exports')
                self.assertEqual(store.root, expected / 'archive')
                self.assertEqual(ArchiveStore(sources_root=self.sources).sources_root, self.sources)
