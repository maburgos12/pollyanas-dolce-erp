"""Opt-in native NAS test: real PostgreSQL, synthetic data, no production backups."""
import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

RUNNER = Path('/share/homes/DolcePasteleria/.erp-backup-verifier/nas_restore_daily.py')


@unittest.skipUnless(RUNNER.is_file(), 'Requires provisioned NAS runtime')
class NativeRecoveryTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('native_recovery', RUNNER)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.folder = Path(tempfile.mkdtemp(prefix='.erp-test-', dir='/share/homes/DolcePasteleria'))
        self.module.HOME = self.folder
        (self.folder / 'runtime').symlink_to(RUNNER.parent / 'runtime', target_is_directory=True)
        self.module.NAS = self.folder / 'nas'
        self.module.NAS.mkdir()
        self.point = 'backup_20000101_000001'

    def tearDown(self):
        self.assertFalse(list(self.folder.glob('restore-*')), 'Native scratch was not cleaned')
        shutil.rmtree(self.folder)

    def fixture(self, sql):
        nas = self.module.NAS
        media = nas / 'ERP_ACTIVE_MEDIA_VERSIONS/ERP medios activos - 7 versiones/latest/ERP_BACKUPS/ERP_ACTIVE_MEDIA_STAGING'
        media.mkdir(parents=True)
        (media / 'sample.txt').write_bytes(b'recovered media\n')
        with gzip.open(nas / (self.point + '.sql.gz'), 'wb') as stream:
            stream.write(sql.encode())
        inventory = dict(version=1, files=[dict(path='sample.txt', bytes=16, sha256=self.module.sha(media / 'sample.txt'))])
        (nas / (self.point + '.media.json')).write_text(json.dumps(inventory))
        for suffix in ('conteos.tar.gz', 'inventory-audit.tar.gz'):
            with tarfile.open(nas / (self.point + '.' + suffix), 'w:gz') as archive:
                member = tarfile.TarInfo('evidence.txt')
                member.size = 3
                archive.addfile(member, io.BytesIO(b'ok\n'))
        lines = []
        for suffix in self.module.SUFFIXES:
            file = nas / (self.point + '.' + suffix)
            lines.append(self.module.sha(file) + '  ' + file.name)
        manifest = nas / (self.point + '.manifest')
        manifest.write_text('\n'.join(lines) + '\n')
        return self.module.sha(manifest)

    def test_real_sql_media_and_receipt(self):
        checksum = self.fixture("CREATE TABLE django_migrations(id integer PRIMARY KEY); INSERT INTO django_migrations VALUES(1); CREATE TABLE logistica_documentounidad(id integer PRIMARY KEY, archivo text NOT NULL); INSERT INTO logistica_documentounidad VALUES(1,'sample.txt');")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.module.restore(self.point, checksum)
        result = json.loads(output.getvalue())
        self.assertEqual(result['receipt']['verified_media'], 1)
        self.assertEqual(result['receipt']['verified_references'], 1)
        self.assertEqual(result['receipt']['sql_restore_exit'], 0)
        self.assertEqual(hashlib.sha256(result['evidence'].encode()).hexdigest(), result['receipt']['evidence_sha256'])
        self.assertTrue((self.module.NAS / (self.point + '.sql.gz')).exists())

    def test_sql_failure_has_no_receipt_and_keeps_source(self):
        checksum = self.fixture('SELECT missing_column;')
        with self.assertRaisesRegex(ValueError, 'SQL restore failed'):
            self.module.restore(self.point, checksum)
        self.assertFalse(list((self.folder / 'evidence').glob('*/receipt.json')))
        self.assertTrue((self.module.NAS / (self.point + '.sql.gz')).exists())


if __name__ == '__main__':
    unittest.main()
