"""Standalone backup contract tests; no Docker daemon or database is used."""
import gzip
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'backup_db.sh'
STAMP = '20990101_010101'


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.backups = self.root / 'backups'
        self.backups.mkdir()
        # flock uses a persistent inode; never unlink it while waiters may exist.
        (self.backups / '.backup.flock').touch()
        self.evidence = self.root / 'storage' / 'conteos_evidencias'
        self.evidence.mkdir(parents=True)
        self.log = self.root / 'backup.log'
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.script = self.root / 'scripts' / 'backup_db.sh'
        self.script.parent.mkdir()
        # Keep legacy pre-change execution isolated even before it supports overrides.
        script = SOURCE.read_text().replace('/opt/backups/erp', str(self.backups)).replace('/var/log/erp_backup.log', str(self.log))
        self.script.write_text(script)
        self.env = {**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
                    'BACKUP_DIR': str(self.backups), 'LOG_FILE': str(self.log),
                    'CONTEOS_EVIDENCE_DIR': str(self.evidence), 'CONTAINER': 'isolated-test-db'}
        self.stub('docker', 'printf "CREATE TABLE evidence (id int);\\n"\nexit "${FAIL_DUMP:-0}"\n')
        self.stub('tar', 'if [ "${FAIL_TAR:-0}" = 1 ]; then exit 19; fi\nexec ' + shutil.which('tar') + ' "$@"\n')
        self.stub('date', 'if [ "$1" = "+%Y%m%d_%H%M%S" ]; then echo ' + STAMP + '; else exec ' + shutil.which('date') + ' "$@"; fi\n')

    def stub(self, name, body):
        path = self.bin / name
        path.write_text('#!/bin/bash\n' + body)
        path.chmod(0o755)

    def run_backup(self, **overrides):
        return subprocess.run(['bash', str(self.script)], env={**self.env, **overrides}, text=True, capture_output=True)

    def seed(self, number=8):
        for day in range(1, number + 1):
            prefix = f'backup_202001{day:02d}_010101'
            self.seed_pair(prefix)
        legacy = self.backups / 'backup_20100101_010101.sql.gz'
        legacy.write_bytes(gzip.compress(b'legacy SQL'))
        return set(p.name for p in self.backups.iterdir())

    def seed_pair(self, prefix):
        entries = []
        for suffix in ('.sql.gz', '.conteos.tar.gz'):
            name = prefix + suffix
            data = gzip.compress(b'historical fixture')
            (self.backups / name).write_bytes(data)
            entries.append(hashlib.sha256(data).hexdigest() + '  ' + name)
        (self.backups / (prefix + '.manifest')).write_text('\n'.join(entries) + '\n')

    def test_restorable_pair_and_checksums(self):
        (self.evidence / 'proof.pdf').write_bytes(b'%PDF-1.4 evidence')
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        manifest = self.backups / f'backup_{STAMP}.manifest'
        self.assertTrue(manifest.exists(), 'SQL alone is not a complete backup')
        entries = manifest.read_text().splitlines()
        self.assertEqual(len(entries), 2)
        for entry in entries:
            digest, name = entry.split('  ', 1)
            self.assertEqual(digest, hashlib.sha256((self.backups / name).read_bytes()).hexdigest())
        self.assertIn('CREATE TABLE', gzip.decompress((self.backups / f'backup_{STAMP}.sql.gz').read_bytes()).decode())
        with tarfile.open(self.backups / f'backup_{STAMP}.conteos.tar.gz') as archive:
            proof = next(m for m in archive.getmembers() if Path(m.name).name == 'proof.pdf')
            self.assertEqual(archive.extractfile(proof).read(), b'%PDF-1.4 evidence')
        self.assertFalse(any('partial' in p.name for p in self.backups.iterdir()))

    def test_failure_of_dump_does_not_publish_or_rotate(self):
        original = self.seed()
        result = self.run_backup(FAIL_DUMP='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(set(p.name for p in self.backups.iterdir()), original)
        self.assertNotIn('Backup completado', result.stdout)

    def test_failure_of_archive_does_not_publish_or_rotate(self):
        original = self.seed()
        result = self.run_backup(FAIL_TAR='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(set(p.name for p in self.backups.iterdir()), original)
        self.assertNotIn('Backup completado', result.stdout)

    def test_retains_seven_total_and_rotates_legacy(self):
        self.seed()
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(list(self.backups.glob('*.manifest'))), 7)
        self.assertEqual(len(list(self.backups.glob('*.conteos.tar.gz'))), 7)
        self.assertEqual(len(list(self.backups.glob('*.sql.gz'))), 7)
        self.assertFalse((self.backups / 'backup_20100101_010101.sql.gz').exists())
        self.assertFalse((self.backups / 'backup_20200101_010101.sql.gz').exists())

    def test_transition_keeps_only_seven_legacy_plus_new_total(self):
        for day in range(1, 8):
            (self.backups / f'backup_202001{day:02d}_010101.sql.gz').write_bytes(gzip.compress(b'legacy SQL'))
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(list(self.backups.glob('*.sql.gz'))), 7)
        self.assertEqual(len(list(self.backups.glob('*.manifest'))), 1)
        self.assertFalse((self.backups / 'backup_20200101_010101.sql.gz').exists())

    def test_incomplete_and_invalid_backups_never_count_or_rotate(self):
        self.seed(number=6)
        # Both marked SQL-only and an interrupted pair are not legacy backups.
        partials = {}
        for day, suffixes in [(1, ['.sql.gz', '.incomplete']), (2, ['.sql.gz', '.conteos.tar.gz']),
                              (3, ['.sql.gz', '.conteos.tar.gz', '.manifest']), (4, ['.sql.gz'])]:
            for suffix in suffixes:
                path = self.backups / f'backup_200001{day:02d}_010101{suffix}'
                path.write_bytes(gzip.compress(b'partial') if suffix == '.sql.gz' and day != 4 else b'invalid')
                partials[path.name] = path.read_bytes()
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name, content in partials.items():
            self.assertTrue((self.backups / name).exists(), name)
            self.assertEqual((self.backups / name).read_bytes(), content)
        self.assertFalse((self.backups / 'backup_20100101_010101.sql.gz').exists())
        self.assertTrue((self.backups / 'backup_20200101_010101.sql.gz').exists())

    def test_complete_manifest_overrides_stale_incomplete_marker(self):
        self.seed(number=7)
        prefix = 'backup_20000101_010101'
        self.seed_pair(prefix)
        (self.backups / (prefix + '.incomplete')).touch()
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(list(self.backups.glob(prefix + '.*')), [])
        self.assertEqual(len(list(self.backups.glob('*.sql.gz'))), 7)

    def test_incomplete_marker_exists_before_first_publication(self):
        real_mv = shutil.which('mv')
        self.stub('mv', 'test -f "$BACKUP_DIR/backup_' + STAMP + '.incomplete" || exit 29\nexec ' + real_mv + ' "$@"\n')
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.backups / f'backup_{STAMP}.incomplete').exists())

    def test_missing_source_archives_empty_without_creating_source(self):
        self.evidence.rmdir()
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.evidence.exists())
        self.assertTrue((self.backups / f'backup_{STAMP}.conteos.tar.gz').exists())
        with tarfile.open(self.backups / f'backup_{STAMP}.conteos.tar.gz') as archive:
            self.assertEqual(archive.getnames(), [])

    def test_publication_failure_leaves_old_backups_intact(self):
        original = self.seed()
        real_mv = shutil.which('mv')
        self.stub('mv', 'case "$*" in *.manifest*) exit 20;; esac\nexec ' + real_mv + ' "$@"\n')
        result = self.run_backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(set(p.name for p in self.backups.iterdir()), original)
        self.assertNotIn('Backup completado', result.stdout)

    def test_flock_uses_inherited_descriptor_and_leaves_no_directory_lock(self):
        marker = self.root / 'flock-called'
        self.stub('flock', ': >&9\nprintf "%s" "$*" > "$FLOCK_MARKER"\nexit 0\n')
        result = self.run_backup(FLOCK_MARKER=str(marker))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(marker.exists(), 'Use auto-released flock when available')
        self.assertEqual(marker.read_text(), '-n 9')
        self.assertFalse((self.backups / '.backup.lock').exists())

    def test_busy_flock_does_not_start_or_rotate_backup(self):
        original = self.seed()
        self.stub('flock', 'exit 1\n')
        result = self.run_backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(set(p.name for p in self.backups.iterdir()), original)

    def test_sigterm_cleans_staging_and_portable_lock(self):
        original = self.seed()
        self.stub('docker', 'kill -TERM "$PPID"\nprintf "interrupted dump"\n')
        result = self.run_backup()
        self.assertEqual(result.returncode, 143, result.stderr)
        self.assertEqual(set(p.name for p in self.backups.iterdir()), original)
        self.assertFalse((self.backups / '.backup.lock').exists())

    def test_existing_timestamp_is_never_overwritten(self):
        self.assertEqual(self.run_backup().returncode, 0)
        before = {p.name: p.read_bytes() for p in self.backups.iterdir()}
        result = self.run_backup()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual({p.name: p.read_bytes() for p in self.backups.iterdir()}, before)


if __name__ == '__main__':
    unittest.main()
