#!/usr/bin/env python3
"""Record or verify the active media tree without copying its file contents."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


def files_under(root: Path) -> list[Path]:
    if not root.is_dir() or root.is_symlink():
        raise ValueError(f"media root is not a directory: {root}")
    found = []
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
                raise ValueError(f"unsafe media path: {name}")
            if name in files and not path.is_file():
                raise ValueError(f"non-file in media tree: {name}")
        found.extend(Path(directory) / name for name in files)
    return sorted(found)


def digest(path: Path) -> tuple[int, str]:
    before = path.stat()
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (
        after.st_size, after.st_mtime_ns, after.st_ino
    ):
        raise ValueError(f"media changed while hashing: {path}")
    return after.st_size, h.hexdigest()


def create(root: Path, output: Path) -> None:
    paths = files_under(root)
    rows = []
    for path in paths:
        size, sha = digest(path)
        rows.append({"path": path.relative_to(root).as_posix(), "bytes": size, "sha256": sha})
    if files_under(root) != paths:
        raise ValueError("media file list changed while hashing")
    payload = {"version": 1, "files": rows}
    output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def verify(root: Path, manifest: Path) -> None:
    payload = json.loads(manifest.read_text())
    if payload.get("version") != 1 or not isinstance(payload.get("files"), list):
        raise ValueError("invalid media manifest")
    seen = set()
    for row in payload["files"]:
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() in seen:
            raise ValueError("unsafe or duplicate media path")
        seen.add(relative.as_posix())
        path = root / relative
        if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError(f"missing media: {relative}")
        size, sha = digest(path)
        if size != row["bytes"] or sha != row["sha256"]:
            raise ValueError(f"media checksum mismatch: {relative}")
    print(f"verified {len(seen)} media files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("create", "verify"))
    parser.add_argument("root", type=Path)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    try:
        (create if args.action == "create" else verify)(args.root, args.manifest)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"media manifest: {exc}", file=sys.stderr)
        sys.exit(1)
