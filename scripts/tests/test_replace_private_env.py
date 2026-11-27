import fcntl
import subprocess
import sys
import hashlib
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("replace_env", Path(__file__).parents[1] / "replace_private_env.py")
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class PrivateEnvTests(unittest.TestCase):
    def setUp(self):
        if not hasattr(os, "listxattr"):
            patch = mock.patch.object(module.os, "listxattr", return_value=[], create=True)
            patch.start()
            self.addCleanup(patch.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / ".env"
        self.path.write_bytes(b"KEY=old\n")
        self.path.chmod(0o600)
        # Linux integration runs as root to reproduce root writer/appuser reader.
        self.uid = 1000 if os.geteuid() == 0 else os.geteuid()
        self.gid = 1000 if os.geteuid() == 0 else os.getegid()
        os.chown(self.path, self.uid, self.gid)
        self.digest = hashlib.sha256(self.path.read_bytes()).hexdigest()

    def replace(self):
        module.replace_private_env(self.path, b"KEY=new\n", self.digest, self.uid, self.gid)

    def test_owner_and_mode_correct_before_atomic_replace(self):
        real_replace = os.replace
        def inspected_replace(source, target):
            info = os.stat(source)
            self.assertEqual((info.st_uid, info.st_gid, info.st_mode & 0o777), (self.uid, self.gid, 0o600))
            self.assertEqual(self.path.read_bytes(), b"KEY=old\n")
            real_replace(source, target)
        with mock.patch.object(module.os, "replace", side_effect=inspected_replace):
            self.replace()
        self.assertEqual(self.path.read_bytes(), b"KEY=new\n")
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_wrong_revision_retains_original(self):
        self.digest = "0" * 64
        with self.assertRaises(ValueError):
            self.replace()
        self.assertEqual(self.path.read_bytes(), b"KEY=old\n")

    def test_wrong_owner_rejected(self):
        self.uid += 1
        with self.assertRaises(ValueError):
            self.replace()

    def test_broad_permissions_not_silently_repaired(self):
        self.path.chmod(0o644)
        with self.assertRaises(ValueError):
            self.replace()
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o644)

    def test_chown_failure_retains_original_and_removes_staging(self):
        with mock.patch.object(module.os, "fchown", side_effect=PermissionError):
            with self.assertRaises(PermissionError):
                self.replace()
        self.assertEqual(self.path.read_bytes(), b"KEY=old\n")
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_concurrent_update_not_overwritten(self):
        real_fsync = os.fsync
        def race(fd):
            self.path.write_bytes(b"KEY=concurrent\n")
            real_fsync(fd)
        with mock.patch.object(module.os, "fsync", side_effect=race):
            with self.assertRaises(ValueError):
                self.replace()
        self.assertEqual(self.path.read_bytes(), b"KEY=concurrent\n")

    def test_symlink_rejected(self):
        target = self.path.with_name("real")
        self.path.rename(target)
        self.path.symlink_to(target)
        with self.assertRaises(ValueError):
            self.replace()
        self.assertEqual(target.read_bytes(), b"KEY=old\n")

    def test_hardlink_rejected(self):
        os.link(self.path, self.path.with_name("alias"))
        with self.assertRaises(ValueError):
            self.replace()

    @unittest.skipUnless(os.geteuid() == 0 and hasattr(os, 'setuid'), 'Linux root integration')
    def test_real_app_user_reads_but_other_user_cannot(self):
        self.path.parent.chmod(0o755)
        self.replace()
        for uid, expected in ((1000, 0), (1001, 1)):
            child = os.fork()
            if child == 0:
                os.setgroups([])
                os.setgid(uid)
                os.setuid(uid)
                try:
                    self.path.read_bytes()
                    os._exit(0)
                except PermissionError:
                    os._exit(1)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), expected)

    def test_cli_refuses_deployment_lock_before_reading_stdin(self):
        lock_path = self.path.with_name("deploy.lock")
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            process = subprocess.Popen([
                sys.executable, str(Path(module.__file__)), "--file", str(self.path),
                "--expected-sha256", self.digest, "--app-uid", str(self.uid),
                "--app-gid", str(self.gid), "--lock", str(lock_path),
            ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(process.wait(timeout=5), 1)
                self.assertEqual(self.path.read_bytes(), b"KEY=old\n")
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate()
