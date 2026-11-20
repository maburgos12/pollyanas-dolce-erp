import fcntl
import hashlib
import importlib.util
import json
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

    def test_receipt_publication_has_one_fresh_probe_and_checks_published_proof(self):
        for outcome in ('new', 'existing', 'timeout', 'invalid-published', 'changed-published'):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                receipts = root / 'receipts'
                receipts.mkdir()
                stem = 'backup_20261119_020001'
                (root / (stem + '.media.json')).write_text('{"files": []}')
                evidence = 'SQL and media restore passed'
                proof = dict(point=stem, manifest_sha256='a' * 64, verified_media=3,
                             evidence_sha256=hashlib.sha256(evidence.encode()).hexdigest(), media_aliases={})
                payload = json.dumps(dict(receipt=proof, evidence=evidence))
                published = dict(proof)
                if outcome == 'changed-published':
                    published['manifest_sha256'] = 'b' * 64
                reads = [proof] if outcome == 'existing' else [FileNotFoundError(),
                         ValueError('invalid published proof') if outcome == 'invalid-published' else published]
                events = []
                def probe(*args):
                    events.append('probe')
                    if outcome == 'timeout':
                        raise ValueError('NAS timeout')
                rotator = types.SimpleNamespace(BACKUPS=root, RECEIPTS=receipts, NAS=Path('/unused-nas'),
                    require=nas.require, local_set=Mock(return_value=(stem + '.manifest', 'a' * 64, {})),
                    read_receipt=Mock(side_effect=reads), bounded_check_proof=Mock(side_effect=probe))
                with patch.object(health, 'run', return_value=payload), patch.object(health, 'atomic', side_effect=lambda *args: events.append('publish')):
                    if outcome in ('new', 'existing'):
                        self.assertEqual(health.verify_point(rotator, stem)['media'], 3)
                    else:
                        with self.assertRaises(ValueError):
                            health.verify_point(rotator, stem)
                rotator.bounded_check_proof.assert_called_once()
                self.assertEqual(events, ['probe'] if outcome in ('timeout', 'existing') else ['probe', 'publish', 'publish'])
                self.assertEqual(rotator.read_receipt.call_count, 1 if outcome in ('timeout', 'existing') else 2)

    def test_verify_only_publishes_private_receipt_without_authorizing_rotation(self):
        rotator = module('rotate_verified_backups')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            canonical = root / 'canonical'
            canonical.mkdir(mode=0o700)
            state = root / 'state'
            state.mkdir(mode=0o700)
            stem = 'backup_20261119_020001'
            (root / '.backup.flock').touch()
            (root / (stem + '.manifest')).touch()
            (root / (stem + '.media.json')).write_text('{"files": []}')
            evidence = 'SQL and media passed'
            proof = dict(point=stem, manifest_sha256='a' * 64, verified_media=3,
                         evidence_sha256=hashlib.sha256(evidence.encode()).hexdigest(),
                         postgresql_major=16, sql_restore_exit=0, media_restore_exit=0,
                         verified_references=1, media_generations=['latest'], media_aliases={})
            payload = json.dumps(dict(receipt=proof, evidence=evidence))
            with patch.object(health, 'STATE', state), patch.object(rotator, 'BACKUPS', root), patch.object(rotator, 'RECEIPTS', canonical), patch.object(rotator, 'local_set', return_value=(stem + '.manifest', 'a' * 64, {})), patch.object(rotator, 'bounded_check_proof') as probe, patch.object(health, 'run', side_effect=lambda args: payload if health.NAS_RUNNER in args else '') as run, patch.object(health, 'atomic', wraps=health.atomic) as publish:
                health.maintain(rotator, verify_only=True)
                private = state / 'verification-receipts'
                self.assertEqual(len(publish.call_args_list), 2)
                self.assertTrue(all(call.args[0].parent == private for call in publish.call_args_list))
                self.assertEqual(rotator.read_receipt(private / (stem + '.json'), stem), proof)
                with self.assertRaises(FileNotFoundError):
                    rotator.read_receipt(canonical / (stem + '.json'), stem)
                self.assertEqual(list(canonical.iterdir()), [])
                run.reset_mock()
                publish.reset_mock()
                health.maintain(rotator, verify_only=True)
                self.assertEqual(len(run.call_args_list), 1)
                self.assertEqual(run.call_args.args[0][-1], 'true')
                publish.assert_not_called()
                self.assertEqual(probe.call_count, 2)

    def test_private_invalid_receipt_can_read_valid_canonical_without_writing_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            canonical = root / 'canonical'
            canonical.mkdir(mode=0o700)
            private = root / 'verification-receipts'
            private.mkdir(mode=0o700)
            stem = 'backup_20261119_020001'
            proof = dict(point=stem, manifest_sha256='a' * 64, verified_media=3)
            rotator = types.SimpleNamespace(BACKUPS=root, RECEIPTS=canonical, NAS=Path('/unused-nas'),
                require=nas.require, local_set=Mock(return_value=('manifest', 'a' * 64, {})),
                read_receipt=Mock(side_effect=[ValueError('invalid private receipt'), proof]),
                bounded_check_proof=Mock())
            with patch.object(health, 'STATE', root), patch.object(health, 'run') as restore, patch.object(health, 'atomic') as publish:
                self.assertEqual(health.verify_point(rotator, stem, receipts=private)['media'], 3)
                self.assertEqual([call.args[0].parent for call in rotator.read_receipt.call_args_list], [private, canonical])
                rotator.bounded_check_proof.assert_called_once()
                restore.assert_not_called()
                publish.assert_not_called()

    def test_new_private_receipt_reuses_valid_aliases_from_both_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            canonical, private = root / 'canonical', root / 'verification-receipts'
            for directory in (canonical, private):
                directory.mkdir(mode=0o700)
                (directory / 'backup_20261118_020001.json').touch()
            (private / 'backup_20261117_020001.json').touch()
            stem = 'backup_20261119_020001'
            (root / (stem + '.media.json')).write_text('{"files": [{"path":"a"}, {"path":"b"}]}')
            evidence = 'restore passed'
            proof = dict(point=stem, manifest_sha256='a' * 64, verified_media=2,
                         evidence_sha256=hashlib.sha256(evidence.encode()).hexdigest())
            def receipt(path, point):
                if point != stem:
                    if point == 'backup_20261117_020001':
                        return {'media_aliases': {'a': 'outdated_alias'}}
                    return {'media_aliases': {'a': 'a_alias'} if path.parent == canonical else {'b': 'b_alias'}}
                return json.loads(path.read_text())
            rotator = types.SimpleNamespace(BACKUPS=root, RECEIPTS=canonical, NAS=Path('/unused-nas'),
                require=nas.require, local_set=Mock(return_value=('manifest', 'a' * 64, {})),
                read_receipt=Mock(side_effect=receipt), bounded_check_proof=Mock())
            with patch.object(health, 'run', return_value=json.dumps(dict(receipt=proof, evidence=evidence))):
                health.verify_point(rotator, stem, receipts=private)
            checked = rotator.bounded_check_proof.call_args.args[3]
            self.assertEqual(checked['media_aliases'], {'a': 'a_alias', 'b': 'b_alias'})
            self.assertFalse((canonical / (stem + '.json')).exists())

    def test_public_readonly_state_parent_allows_private_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / 'state'
            state.mkdir(mode=0o755)
            state.chmod(0o755)
            (root / '.backup.flock').touch()
            (root / 'backup_20261119_020001.manifest').touch()
            rotator = types.SimpleNamespace(BACKUPS=root, POINT=re.compile(r'backup_\d{8}_\d{6}'), require=nas.require)
            with patch.object(health, 'STATE', state), patch.object(health, 'run', return_value=''), patch.object(health, 'verify_point', return_value={}):
                health.maintain(rotator, verify_only=True)
            self.assertEqual(state.stat().st_mode & 0o777, 0o755)
            self.assertEqual((state / 'verification-receipts').stat().st_mode & 0o777, 0o700)

    def test_unsafe_state_parent_is_rejected_without_touching_destination(self):
        for kind in ('permissions', 'symlink', 'owner'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state = root / 'state'
                actual = root / 'actual' if kind == 'symlink' else state
                actual.mkdir(mode=0o700)
                private = actual / 'verification-receipts'
                private.mkdir(mode=0o700)
                marker = private / 'retained.json'
                marker.write_text('preserved')
                inode = private.stat().st_ino
                if kind == 'permissions':
                    actual.chmod(0o777)
                elif kind == 'symlink':
                    state.symlink_to(actual)
                rotator = types.SimpleNamespace(require=nas.require)
                with patch.object(health, 'STATE', state), patch.object(health, 'verify_point') as verify:
                    if kind == 'owner':
                        with patch.object(health.os, 'geteuid', return_value=-1), self.assertRaises(ValueError):
                            health.maintain(rotator, verify_only=True)
                    else:
                        with self.assertRaises(ValueError):
                            health.maintain(rotator, verify_only=True)
                    verify.assert_not_called()
                self.assertEqual(marker.read_text(), 'preserved')
                self.assertEqual(private.stat().st_ino, inode)
                self.assertEqual(private.stat().st_mode & 0o777, 0o700)

    def test_verify_only_rejects_insecure_receipt_directory(self):
        for kind in ('permissions', 'symlink', 'owner'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                private = root / 'verification-receipts'
                if kind == 'symlink':
                    other = root / 'other'
                    other.mkdir(mode=0o700)
                    private.symlink_to(other)
                else:
                    private.mkdir(mode=0o755 if kind == 'permissions' else 0o700)
                rotator = types.SimpleNamespace(require=nas.require)
                with patch.object(health, 'STATE', root), patch.object(health, 'verify_point') as verify:
                    if kind == 'owner':
                        with patch.object(health.os, 'geteuid', return_value=-1), self.assertRaisesRegex(ValueError, 'private'):
                            health.maintain(rotator, verify_only=True)
                    else:
                        with self.assertRaisesRegex(ValueError, 'private'):
                            health.maintain(rotator, verify_only=True)
                    verify.assert_not_called()

    def test_verify_only_never_rotates_or_calls_nas_in_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / '.backup.flock').touch()
            (root / 'backup_20261119_020001.manifest').touch()
            rotator = types.SimpleNamespace(BACKUPS=root, POINT=re.compile(r'backup_\d{8}_\d{6}'),
                                            require=nas.require, actual_nas_device=Mock())
            with patch.object(health, 'STATE', root), patch.object(health, 'run', return_value='') as run, patch.object(health, 'verify_point', return_value={'verified': True}):
                result = health.maintain(rotator, verify_only=True)
                self.assertIn('verify-only', result['rotation'])
                rotator.actual_nas_device.assert_not_called()
                self.assertEqual(len(run.call_args_list), 1)
                self.assertEqual(run.call_args.args[0][0], 'ssh')

    def test_timeout_releases_backup_lock_without_publishing_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lock_path = root / '.backup.flock'
            lock_path.touch()
            (root / 'backup_20261119_020001.manifest').touch()
            rotator = types.SimpleNamespace(
                BACKUPS=root, RECEIPTS=root, NAS=Path('/unused-nas'),
                POINT=re.compile(r'backup_\d{8}_\d{6}'), require=nas.require,
                local_set=Mock(return_value=('manifest', 'a' * 64, {})),
                read_receipt=Mock(return_value={'manifest_sha256': 'a' * 64}),
                bounded_check_proof=Mock(side_effect=ValueError('NAS verification timed out')))
            with patch.object(health, 'run', return_value=''), patch.object(health, 'atomic') as publish:
                with self.assertRaisesRegex(ValueError, 'timed out'):
                    health.maintain(rotator)
                publish.assert_not_called()
            with lock_path.open() as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_backup_completed_between_verification_and_rotation_is_retained(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / '.backup.flock').touch()
            original = root / 'backup_20261119_020001.manifest'
            original.touch()
            newest = root / 'backup_20261119_090001.manifest'
            rotator = types.SimpleNamespace(BACKUPS=root, POINT=re.compile(r'backup_\d{8}_\d{6}'),
                                            require=nas.require, local_set=Mock())
            def run(args):
                if health.ROTATOR in args:
                    original.unlink()
                    newest.touch()
                    return 'rotated'
                if args[0] == 'df':
                    return 'Filesystem Blocks Used Available Capacity Mounted\nroot 100 50 50 50% /'
                return ''
            with patch.object(health, 'run', side_effect=run), patch.object(health, 'verify_point', return_value={}):
                result = health.maintain(rotator)
            self.assertEqual(result['retained'], newest.stem)
            rotator.local_set.assert_called_once_with(root, newest.stem)

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
                self.assertFalse(any(health.ROTATOR in call.args[0] for call in run.call_args_list))
            (root / 'backup_20261110_020001.incomplete').touch()
            with patch.object(health, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'Incomplete'):
                    health.maintain(rotator)
                run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
