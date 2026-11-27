#!/usr/bin/env python3
"""Replace a private env file without changing ownership or exposing its contents.

Input is a complete replacement on stdin, never command-line values. The caller
must authorize the configuration change and retain a verified private backup.
"""
import argparse
import fcntl
import hashlib
import os
from pathlib import Path
import stat
import sys
import tempfile


def fingerprint(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("env must be a regular file with one link")
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError("env must already have mode 600; no permissions are repaired")
    if not hasattr(os, "listxattr"):
        raise ValueError("metadata verification requires Linux")
    if os.listxattr(path):
        raise ValueError("extended metadata requires a separately reviewed update")
    return info, hashlib.sha256(path.read_bytes()).hexdigest()


def replace_private_env(path, content, expected_sha, expected_uid, expected_gid):
    path = Path(path)
    original, digest = fingerprint(path)
    if digest != expected_sha:
        raise ValueError("env changed since preparation")
    if (original.st_uid, original.st_gid) != (expected_uid, expected_gid):
        raise ValueError("env ownership differs from the verified application identity")
    if expected_uid == 0:
        raise ValueError("application identity must not be root")
    if not content or b"\x00" in content:
        raise ValueError("replacement must be nonempty text without NUL")
    content.decode("utf-8")
    fd, name = tempfile.mkstemp(prefix=".env.private-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            # chown can clear mode bits: set ownership FIRST, then the exact mode.
            os.fchown(stream.fileno(), original.st_uid, original.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(original.st_mode))
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            staged = os.fstat(stream.fileno())
            if (staged.st_uid, staged.st_gid, stat.S_IMODE(staged.st_mode)) != (
                original.st_uid, original.st_gid, 0o600
            ):
                raise ValueError("staged metadata does not match")
        current, current_digest = fingerprint(path)
        fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(current, key) != getattr(original, key) for key in fields) or current_digest != digest:
            raise ValueError("env changed during preparation; original retained")
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--app-uid", type=int, required=True)
    parser.add_argument("--app-gid", type=int, required=True)
    parser.add_argument("--lock", type=Path, default=Path("/run/lock/erp-deploy.lock"))
    args = parser.parse_args()
    # Same lock as deployment; all env writers must use this helper.
    with args.lock.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        replace_private_env(args.file, sys.stdin.buffer.read(), args.expected_sha256,
                            args.app_uid, args.app_gid)
    print("env replaced; owner/group/mode preserved; services unchanged")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError):
        # Do not echo values, paths supplied as secrets, or source fragments.
        print("env update not confirmed; verify metadata and hash before retrying", file=sys.stderr)
        sys.exit(1)
