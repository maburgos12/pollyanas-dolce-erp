import hashlib
import importlib.util
import io
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location('direct', Path(__file__).parents[1] / 'nas_smb_direct.py')
direct = importlib.util.module_from_spec(spec)
spec.loader.exec_module(direct)


class DirectSMBTests(unittest.TestCase):
    def test_session_requires_encryption_and_exact_dialect(self):
        client = Mock()
        with patch.dict(sys.modules, {'smbclient': client}), patch.object(direct, '_config', return_value=('u', 'p')):
            for dialect, encrypted in [(0x302, True), (0x311, False)]:
                client.register_session.return_value = SimpleNamespace(
                    connection=SimpleNamespace(dialect=dialect), encrypt_data=encrypted)
                with self.assertRaisesRegex(ValueError, 'SMB 3.1.1 cifrada'):
                    direct.DirectSMBReader()
            client.register_session.return_value = SimpleNamespace(
                connection=SimpleNamespace(dialect=0x311), encrypt_data=True)
            self.assertIs(direct.DirectSMBReader().client, client)
            client.register_session.assert_called_with(direct.HOST, username='u', password='p',
                                                        encrypt=True, connection_timeout=8)

    def test_reader_opens_only_exact_regular_paths_without_following_reparse_points(self):
        reader = direct.DirectSMBReader.__new__(direct.DirectSMBReader)
        reader.client = Mock()
        filename = 'backup_20261121_020001.manifest'
        flags = SimpleNamespace(CreateOptions=SimpleNamespace(FILE_OPEN_REPARSE_POINT=0x200000),
                                FileAttributes=SimpleNamespace(FILE_ATTRIBUTE_REPARSE_POINT=0x400,
                                                               FILE_ATTRIBUTE_DIRECTORY=0x10))
        with patch.dict(sys.modules, {'smbprotocol.open': flags}):
            with self.assertRaisesRegex(ValueError, 'ruta SMB'):
                reader.digest_manifest('../' + filename)
            reader.client.open_file.assert_not_called()
            stream = io.BytesIO(b'manifest')
            stream.fd = SimpleNamespace(file_attributes=0)
            reader.client.open_file.return_value = stream
            self.assertEqual(reader.digest_manifest(filename), hashlib.sha256(b'manifest').hexdigest())
            reader.client.open_file.assert_called_once_with(
                "\\\\10.77.216.2\\ERP_BACKUPS\\" + filename,
                mode="rb", buffering=0, create_options=0x200000)
            for attr in (0x400, 0x10):
                stream = io.BytesIO(b'not regular')
                stream.fd = SimpleNamespace(file_attributes=attr)
                reader.client.open_file.return_value = stream
                with self.assertRaisesRegex(ValueError, 'inaccesible'):
                    reader.readable(filename)
