"""Local Point sources with bounded, credential-free requests for archived originals."""
from __future__ import annotations

import ctypes
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import time
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from pathlib import Path

MAX_SOURCE_BYTES = 1024 ** 3
CONSUMERS = {'category_report', 'product_history', 'sales_reconciliation'}


class ArchiveUnavailable(RuntimeError):
    """An archived source cannot be safely used."""


class ArchivePending(ArchiveUnavailable):
    """Recovery was requested; retry later without substituting empty data."""


def _rename_noreplace(source, target):
    """Publish in one filesystem operation, without a temporary second hard link."""
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'linux' and hasattr(libc, 'renameat2'):
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        args = (-100, os.fsencode(source), -100, os.fsencode(target), 1)  # AT_FDCWD, RENAME_NOREPLACE
    elif sys.platform == 'darwin' and hasattr(libc, 'renamex_np'):
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        args = (os.fsencode(source), os.fsencode(target), 4)  # RENAME_EXCL
    else:
        raise ArchiveUnavailable('Atomic no-overwrite rename is unavailable on this platform')
    rename.restype = ctypes.c_int
    if rename(*args) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def entry_id(relative_path: str, sha256: str) -> str:
    return hashlib.sha256((relative_path + '\0' + sha256).encode()).hexdigest()


def _relative(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_. -]{0,239}\.xls', value) or '..' in value:
        raise ArchiveUnavailable('Invalid archive relative path')
    return value


def validate_entry(entry: dict) -> dict:
    try:
        if not isinstance(entry, dict) or set(entry) != {'schema', 'id', 'relative_path', 'sha256', 'bytes', 'nas_key', 'verified_at', 'consumers'}:
            raise ValueError('Unexpected catalog fields')
        relative = _relative(entry['relative_path'])
        digest = entry['sha256']
        valid = type(entry['schema']) is int and entry['schema'] == 1 and re.fullmatch('[0-9a-f]{64}', digest)
        valid = valid and entry['id'] == entry_id(relative, digest)
        valid = valid and type(entry['bytes']) is int and 0 < entry['bytes'] <= MAX_SOURCE_BYTES
        valid = valid and entry['nas_key'] == f'point-archive/objects/{digest}.xls'
        valid = valid and type(entry['verified_at']) is int and 0 < entry['verified_at'] <= int(time.time())
        valid = valid and isinstance(entry['consumers'], list) and entry['consumers'] and set(entry['consumers']) <= CONSUMERS
        if not valid:
            raise ValueError('Invalid entry')
    except (KeyError, TypeError, ValueError) as exc:
        raise ArchiveUnavailable('Invalid or unverified archive catalog entry') from exc
    return dict(entry)


def _no_symlink_parents(path):
    for parent in path.parents:
        if parent.is_symlink():
            raise ArchiveUnavailable(f'Symlink parent forbidden: {parent}')


@contextmanager
def _regular(path, flags=os.O_RDONLY, mode=0o600):
    path = Path(path)
    _no_symlink_parents(path)
    try:
        fd = os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK, mode)
    except OSError as exc:
        if isinstance(exc, FileNotFoundError):
            raise
        raise ArchiveUnavailable(f'Cannot safely open {path}') from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ArchiveUnavailable(f'Not an exclusive regular file: {path}')
        yield fd
    finally:
        os.close(fd)


@dataclass(frozen=True)
class Source:
    local_path: Path
    logical_path: Path


class ArchiveStore:
    def __init__(self, root=None, sources_root=None, wait_seconds=2):
        project_root = Path(__file__).absolute().parents[2]
        storage = Path(os.getenv('POINT_BRIDGE_STORAGE_ROOT', 'storage/pos_bridge')).expanduser()
        if not storage.is_absolute():
            storage = project_root / storage
        self.sources_root = Path(os.path.abspath(sources_root or storage / 'raw_exports'))
        self.root = Path(os.path.abspath(root or self.sources_root.parent / 'archive'))
        self.wait_seconds = max(0, min(float(wait_seconds), 2))

    @staticmethod
    def path_key(relative_path):
        return hashlib.sha256(_relative(relative_path).encode()).hexdigest()

    @staticmethod
    def snapshot(path):
        digest = hashlib.sha256()
        size = 0
        with _regular(path) as fd:
            before = os.fstat(fd)
            if before.st_size > MAX_SOURCE_BYTES:
                raise ArchiveUnavailable('Source exceeds bounded size')
            while chunk := os.read(fd, 1024 * 1024):
                size += len(chunk)
                if size > MAX_SOURCE_BYTES:
                    raise ArchiveUnavailable('Source exceeds bounded size')
                digest.update(chunk)
            after = os.fstat(fd)
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_ino) or os.stat(path, follow_symlinks=False).st_ino != before.st_ino:
                raise ArchiveUnavailable('Source changed while reading')
        return {'sha256': digest.hexdigest(), 'bytes': size}

    @staticmethod
    def _read_json(path):
        with _regular(path) as fd:
            raw = os.read(fd, 16385)
        if len(raw) > 16384:
            raise ArchiveUnavailable('Archive metadata too large')
        try:
            return json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise ArchiveUnavailable('Malformed archive metadata') from exc

    @staticmethod
    def _atomic_json(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        _no_symlink_parents(path)
        fd, temporary = tempfile.mkstemp(prefix='.write-', dir=path.parent)
        try:
            with os.fdopen(fd, 'w') as handle:
                json.dump(value, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o644)
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def catalog_entry(self, *, relative_path=None, entry_id=None):
        if (relative_path is None) == (entry_id is None):
            raise ArchiveUnavailable('Supply exactly one catalog identity')
        if relative_path is not None:
            path = self.root / 'catalog' / 'by-path' / (self.path_key(relative_path) + '.json')
        else:
            if not isinstance(entry_id, str) or not re.fullmatch('[0-9a-f]{64}', entry_id):
                raise ArchiveUnavailable('Invalid archive id')
            path = self.root / 'catalog' / 'objects' / (entry_id + '.json')
        entry = validate_entry(self._read_json(path))
        if (relative_path is not None and entry['relative_path'] != relative_path) or (entry_id is not None and entry['id'] != entry_id):
            raise ArchiveUnavailable('Catalog identity mismatch')
        return entry

    def register(self, entry):
        entry = validate_entry(entry)
        self._atomic_json(self.root / 'catalog' / 'objects' / (entry['id'] + '.json'), entry)
        self._atomic_json(self.root / 'catalog' / 'by-path' / (self.path_key(entry['relative_path']) + '.json'), entry)

    @contextmanager
    def path_lock(self, relative_path, exclusive=False, timeout=2):
        path = self.root / 'locks' / (self.path_key(relative_path) + '.lock')
        path.parent.mkdir(parents=True, exist_ok=True)
        with ExitStack() as stack:
            try:
                fd = stack.enter_context(_regular(path, os.O_RDWR))
            except FileNotFoundError:
                try:
                    fd = stack.enter_context(_regular(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o666))
                except ArchiveUnavailable:
                    fd = stack.enter_context(_regular(path, os.O_RDWR))
            deadline = time.monotonic() + max(0, timeout)
            while True:
                try:
                    fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise ArchivePending("Source lease busy; retry later")
                    time.sleep(.02)
            try:
                if os.stat(path, follow_symlinks=False).st_ino != os.fstat(fd).st_ino:
                    raise ArchiveUnavailable('Lock inode replaced')
                yield fd
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)

    def enqueue(self, entry):
        entry = validate_entry(entry)
        if self.catalog_entry(entry_id=entry['id']) != entry:
            raise ArchiveUnavailable('Request differs from catalog')
        self._atomic_json(self.root / 'requests' / (entry['id'] + '.json'), {'id': entry['id']})

    @contextmanager
    def open_source(self, logical_path):
        logical = Path(logical_path)
        physical = Path(os.path.abspath(logical))
        try:
            relative = physical.relative_to(self.sources_root).as_posix()
        except ValueError:
            # Existing caller-selected files outside the archive retain their original behavior.
            yield Source(physical, logical)
            return
        relative = _relative(relative)
        deadline = time.monotonic() + self.wait_seconds
        queued = False
        while True:
            with self.path_lock(relative, timeout=max(0, deadline - time.monotonic())):
                try:
                    with _regular(physical) as fd:
                        if os.fstat(fd).st_size > MAX_SOURCE_BYTES:
                            raise ArchiveUnavailable('Source exceeds bounded size')
                    available = True
                except FileNotFoundError:
                    available = False
                if available:
                    try:
                        registered = self.catalog_entry(relative_path=relative)
                    except FileNotFoundError:
                        registered = None
                    if registered and self.snapshot(physical) != {key: registered[key] for key in ('sha256', 'bytes')}:
                        raise ArchiveUnavailable('Local archived source differs from verified catalog')
                    yield Source(physical, logical)
                    return
            entry = self.catalog_entry(relative_path=relative)
            if not queued:
                self.enqueue(entry)
                queued = True
            if time.monotonic() >= deadline:
                raise ArchivePending(f"Archived source recovery pending: {entry['id']}")
            time.sleep(min(.05, max(0, deadline - time.monotonic())))

    def publish_restored(self, entry, staged_path):
        entry = validate_entry(entry)
        expected = {key: entry[key] for key in ('sha256', 'bytes')}
        if self.snapshot(staged_path) != expected:
            raise ArchiveUnavailable('Recovered content checksum mismatch')
        target = self.sources_root / entry['relative_path']
        self.sources_root.mkdir(parents=True, exist_ok=True)
        with self.path_lock(entry['relative_path'], exclusive=True):
            if target.exists() or target.is_symlink():
                if self.snapshot(target) != expected:
                    raise ArchiveUnavailable('Refusing to overwrite a different original')
                return target
            fd, temporary = tempfile.mkstemp(prefix='.restore-', dir=self.sources_root)
            try:
                with os.fdopen(fd, 'wb') as output, _regular(staged_path) as source:
                    copied = 0
                    while chunk := os.read(source, 1024 * 1024):
                        copied += len(chunk)
                        if copied > entry['bytes']:
                            raise ArchiveUnavailable('Recovered content grew')
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                if self.snapshot(temporary) != expected:
                    raise ArchiveUnavailable('Recovered content changed')
                os.chmod(temporary, 0o644)
                _rename_noreplace(temporary, target)
            finally:
                Path(temporary).unlink(missing_ok=True)
                directory_fd = os.open(self.sources_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        return target


def open_source(logical_path):
    return ArchiveStore().open_source(logical_path)
