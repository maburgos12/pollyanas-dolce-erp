import hashlib
import importlib.util
from pathlib import Path
import re
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


health = module('vps_backup_health')
nas = module('nas_restore_daily')


class RestoreGatesTests(unittest.TestCase):
    def test_unsafe_paths_rejected(self):
        for value in ('../secret', '/etc/passwd', 'ok/../../secret', ''):
            with self.subTest(value=value), self.assertRaises(ValueError):
                nas.relative(value)

    def test_manifest_requires_all_payloads_and_exact_bytes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(nas, 'NAS', Path(tmp)):
            point = 'backup_20261109_020001'
            lines = []
            for suffix in nas.SUFFIXES:
                path = Path(tmp) / (point + '.' + suffix)
                path.write_bytes(b'test')
                lines.append(hashlib.sha256(b'test').hexdigest() + '  ' + path.name)
            manifest = Path(tmp) / (point + '.manifest')
            manifest.write_text('\n'.join(lines) + '\n')
            expected = nas.sha(manifest)
            nas.manifest(point, expected)
            path.write_bytes(b'corrupted')
            with self.assertRaisesRegex(ValueError, 'payload mismatch'):
                nas.manifest(point, expected)

    def test_failed_recovery_never_calls_rotation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / '.backup.flock').touch()
            (root / 'backup_20261109_020001.manifest').touch()
            rotator = types.SimpleNamespace(BACKUPS=root, NAS=root, POINT=re.compile(r'backup_\d{8}_\d{6}'),
                                            require=nas.require, actual_nas_device=Mock(return_value=1))
            with patch.object(health, 'run', return_value='') as run, patch.object(health, 'verify_point', side_effect=ValueError('restore failed')):
                with self.assertRaisesRegex(ValueError, 'restore failed'):
                    health.maintain(rotator)
                self.assertFalse(any(call.args[0][0] == health.ROTATOR for call in run.call_args_list))
            (root / 'backup_20261110_020001.incomplete').touch()
            with patch.object(health, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'Incomplete'):
                    health.maintain(rotator)
                run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
