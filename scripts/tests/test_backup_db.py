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
            for suffix in ('.sql.gz', '.conteos.tar.gz', '.manifest'):
                (self.backups / (prefix + suffix)).write_text('historical')
        legacy = self.backups / 'backup_20100101_010101.sql.gz'
        legacy.write_text('legacy')
        return set(p.name for p in self.backups.iterdir())

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

    def test_retains_seven_complete_pairs_and_legacy(self):
        self.seed()
        result = self.run_backup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(list(self.backups.glob('*.manifest'))), 7)
        self.assertEqual(len(list(self.backups.glob('*.conteos.tar.gz'))), 7)
        self.assertEqual(len(list(self.backups.glob('*.sql.gz'))), 8)
        self.assertTrue((self.backups / 'backup_20100101_010101.sql.gz').exists())
        self.assertFalse((self.backups / 'backup_20200101_010101.sql.gz').exists())

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
