"""Read verified historical evidence without writing to the archive filesystem."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from tempfile import SpooledTemporaryFile
from uuid import uuid4

from django.conf import settings
from django.core.files import File
from django.core.files.storage import FileSystemStorage


MAX_ARCHIVE_BYTES = 20 * 1024 * 1024
MAX_INDEX_BYTES = 16 * 1024


class ArchiveUnavailable(OSError):
    """A registered archive cannot currently be read safely."""


class _ArchivedFile(File):
    def open(self, mode=None, *args, **kwargs):
        if mode not in (None, "rb"):
            raise ValueError("El archivo histórico solo admite lectura binaria.")
        if self.closed:
            self.file = open_archive(self.name).file
        self.seek(0)
        return self


def _canonical_name(name):
    if not isinstance(name, str) or not name or "\\" in name or "\x00" in name:
        return False
    path = PurePosixPath(name)
    return (
        not path.is_absolute()
        and len(path.parts) >= 2
        and path.parts[0] == "bitacora"
        and ".." not in path.parts
        and path.as_posix() == name
    )


def _overlap(first, second):
    return first == second or first in second.parents or second in first.parents


def _roots():
    archive = getattr(settings, "MEDIA_ARCHIVE_ROOT", "")
    if not archive:
        return None
    index = getattr(settings, "MEDIA_ARCHIVE_INDEX_ROOT", "")
    if not index:
        raise ArchiveUnavailable("Archivo histórico no disponible.")
    archive, index, media = [Path(os.path.abspath(value)) for value in (archive, index, settings.MEDIA_ROOT)]
    if _overlap(archive, media) or _overlap(index, media) or _overlap(archive, index):
        raise ArchiveUnavailable("Archivo histórico no disponible.")
    return archive, index


def _directory_fd(path, *, create=False):
    """Walk through directory descriptors so symlinks cannot redirect a read."""
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_read(path):
    directory = _directory_fd(path.parent)
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    finally:
        os.close(directory)
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise OSError("Archivo inválido.")
    return os.fdopen(descriptor, "rb")


def _index_filename(name):
    return hashlib.sha256(name.encode("utf-8")).hexdigest() + ".json"


def _valid_entry(entry, name):
    return (
        isinstance(entry, dict)
        and _canonical_name(name)
        and entry.get("name") == name
        and entry.get("status") == "verified"
        and isinstance(entry.get("sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is not None
        and type(entry.get("bytes")) is int
        and 0 < entry["bytes"] <= MAX_ARCHIVE_BYTES
    )


def load_archive_entry(name, *, include_disabled=False):
    """Read the local index; privacy checks may opt in when NAS reads are disabled."""
    if not _canonical_name(name):
        return None
    roots = _roots()
    if roots is None:
        index = getattr(settings, "MEDIA_ARCHIVE_INDEX_ROOT", "")
        if not include_disabled or not index:
            return None
        index, media = [Path(os.path.abspath(value)) for value in (index, settings.MEDIA_ROOT)]
        if _overlap(index, media):
            raise ArchiveUnavailable("Archivo histórico no disponible.")
    else:
        index = roots[1]
    try:
        with _open_read(index / _index_filename(name)) as source:
            content = source.read(MAX_INDEX_BYTES + 1)
        if len(content) > MAX_INDEX_BYTES:
            raise ValueError
        entry = json.loads(content)
        if not _valid_entry(entry, name):
            raise ValueError
        return entry
    except FileNotFoundError:
        return None
    except (OSError, ValueError, UnicodeError, RecursionError):
        raise ArchiveUnavailable("Archivo histórico no disponible.") from None


def archive_path(name):
    entry = load_archive_entry(name)
    if entry is None:
        raise FileNotFoundError("Archivo histórico no registrado.")
    path = _roots()[0] / name
    try:
        with _open_read(path):
            pass
    except OSError:
        raise ArchiveUnavailable("Archivo histórico no disponible.") from None
    return path


def open_archive(name):
    entry = load_archive_entry(name)
    if entry is None:
        raise FileNotFoundError("Archivo histórico no registrado.")
    spool = SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b")
    try:
        digest = hashlib.sha256()
        count = 0
        with _open_read(_roots()[0] / name) as source:
            while count <= entry["bytes"]:
                chunk = source.read(min(64 * 1024, entry["bytes"] + 1 - count))
                if not chunk:
                    break
                count += len(chunk)
                digest.update(chunk)
                spool.write(chunk)
        if count != entry["bytes"] or digest.hexdigest() != entry["sha256"]:
            raise ValueError
        spool.seek(0)
        return _ArchivedFile(spool, name=name)
    except (OSError, ValueError):
        spool.close()
        raise ArchiveUnavailable("Archivo histórico no disponible.") from None


def write_archive_entry(entry):
    """Publish a locally verified index atomically; callers verify the NAS first."""
    name = entry.get("name") if isinstance(entry, dict) else None
    if not _valid_entry(entry, name):
        raise ValueError("Registro histórico inválido.")
    roots = _roots()
    if roots is None:
        raise ValueError("Archivo histórico no configurado.")
    content = json.dumps(entry, ensure_ascii=False, sort_keys=True).encode("utf-8")
    if len(content) > MAX_INDEX_BYTES:
        raise ValueError("Registro histórico inválido.")
    directory = None
    temporary = "." + uuid4().hex + ".tmp"
    try:
        directory = _directory_fd(roots[1], create=True)
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory)
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, _index_filename(name), src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    except OSError:
        raise ArchiveUnavailable("Índice histórico no disponible.") from None
    finally:
        if directory is not None:
            try:
                os.unlink(temporary, dir_fd=directory)
            except OSError:
                pass
            finally:
                os.close(directory)


class ArchivedMediaStorage(FileSystemStorage):
    def _open(self, name, mode="rb"):
        if mode != "rb" and not super().exists(name) and load_archive_entry(name) is not None:
            raise ValueError("El archivo histórico solo admite lectura binaria.")
        try:
            return super()._open(name, mode)
        except FileNotFoundError:
            if mode != "rb" and load_archive_entry(name) is not None:
                raise ValueError("El archivo histórico solo admite lectura binaria.") from None
            return open_archive(name)

    def exists(self, name):
        return super().exists(name) or load_archive_entry(name) is not None

    def size(self, name):
        try:
            return super().size(name)
        except FileNotFoundError:
            entry = load_archive_entry(name)
            if entry is None:
                raise
            return entry["bytes"]
