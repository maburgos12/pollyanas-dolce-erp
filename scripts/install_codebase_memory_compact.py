#!/usr/bin/env python3
"""Preview/apply a backed-up Codex-only graph adapter installation (Python 3.11+)."""
import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import tomllib
from pathlib import Path


def digest(data):
    return hashlib.sha256(data).hexdigest()


def plan_install(source, config, install_root, python, upstream_override=None):
    source, config, install_root = Path(source), Path(config), Path(install_root)
    before = config.read_bytes()
    parsed = tomllib.loads(before.decode())
    server = parsed["mcp_servers"]["codebase-memory-mcp"]
    upstream, upstream_args = server["command"], list(server.get("args", []))
    if upstream_args and Path(upstream_args[0]).name == "codebase_memory_compact.py":
        if Path(upstream_args[0]).parent.parent != install_root or upstream_args[1:2] != ["--upstream"]:
            raise RuntimeError("Existing adapter is not owned by this installation")
        upstream, upstream_args = upstream_args[2], upstream_args[3:]
        if upstream_args[:1] == ["--"]:
            upstream_args = upstream_args[1:]
    if upstream_override is not None:
        native = Path(upstream_override).expanduser().resolve()
        if not native.is_file() or not os.access(native, os.X_OK):
            raise RuntimeError("Native override must be an executable file")
        upstream = str(native)
    runtime = source.read_bytes()
    destination = install_root / digest(runtime)[:16] / "codebase_memory_compact.py"
    text = before.decode()
    match = re.search(r"(?m)^\[mcp_servers\.codebase-memory-mcp\][ \t]*\r?\n", text)
    if not match:
        raise RuntimeError("Expected exact MCP table is missing; configuration was not changed")
    end = re.search(r"(?m)^\[", text[match.end():])
    stop = match.end()+end.start() if end else len(text)
    stanza = text[match.end():stop]
    arguments = [str(destination), "--upstream", upstream, "--", *upstream_args]
    stanza, count = re.subn(r"(?m)^command[ \t]*=.*$", lambda _: "command = " + json.dumps(str(python)), stanza)
    if count != 1:
        raise RuntimeError("Expected one command in MCP table")
    args_line = "args = " + json.dumps(arguments)
    if "args" in server:
        # Multiline arrays are valid TOML but deliberately require a reviewed
        # edit rather than risking a broad regex replacement.
        stanza, count = re.subn(r"(?m)^args[ \t]*=[ \t]*\[[^\n]*\][ \t]*(?:#[^\n]*)?$", lambda _: args_line, stanza)
        if count != 1:
            raise RuntimeError("Upstream args are multiline; use a reviewed table edit")
    else:
        stanza = args_line + "\n" + stanza
    after = text[:match.end()] + stanza + text[stop:]
    updated = tomllib.loads(after)
    expected = dict(server, command=str(python), args=arguments)
    if updated["mcp_servers"]["codebase-memory-mcp"] != expected:
        raise RuntimeError("Unexpected change to the MCP table")
    return {"config_path": str(config), "config_before_sha256": digest(before),
            "config_after": after, "config_after_sha256": digest(after.encode()),
            "source_path": str(source), "runtime_sha256": digest(runtime),
            "runtime_path": str(destination), "install_root": str(install_root),
            "upstream_command": upstream, "upstream_args": upstream_args,
            "changed": before != after.encode()}


def atomic_write(path, data, mode):
    fd, temporary = tempfile.mkstemp(prefix=".graph-compact-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            os.fchmod(handle.fileno(), mode)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def apply_install(plan):
    config = Path(plan["config_path"])
    before = config.read_bytes()
    if digest(before) != plan["config_before_sha256"]:
        raise RuntimeError("Configuration changed after preview; generate a fresh plan")
    runtime = Path(plan["source_path"]).read_bytes()
    if digest(runtime) != plan["runtime_sha256"]:
        raise RuntimeError("Runtime changed after preview; generate a fresh plan")
    destination = Path(plan["runtime_path"])
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.exists():
        if destination.read_bytes() != runtime:
            raise RuntimeError("Existing immutable runtime differs; refusing to overwrite")
    else:
        atomic_write(destination, runtime, 0o600)
    receipt = {key: value for key,value in plan.items() if key != "config_after"}
    if plan["changed"]:
        backups = Path(plan["install_root"]) / "backups"
        backups.mkdir(exist_ok=True, mode=0o700)
        fd, backup = tempfile.mkstemp(prefix="config-", suffix=".toml", dir=backups)
        with os.fdopen(fd, "wb") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(before)
        receipt["config_backup"] = backup
        # Avoid replacing edits made while preparing the runtime/backup.
        if config.read_bytes() != before:
            raise RuntimeError("Configuration changed during installation; nothing was replaced")
        atomic_write(config, plan["config_after"].encode(), config.stat().st_mode & 0o777)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path.home()/".codex/config.toml")
    parser.add_argument("--install-root", type=Path, default=Path.home()/".codex/tools/codebase-memory-compact")
    parser.add_argument("--apply", action="store_true", help="Default is a read-only preview")
    parser.add_argument("--upstream-command", type=Path, help="Explicit native binary override; keeps the compact adapter and existing upstream arguments")
    args = parser.parse_args()
    plan = plan_install(Path(__file__).with_name("codebase_memory_compact.py"), args.config, args.install_root, sys.executable,
                        upstream_override=args.upstream_command)
    receipt = apply_install(plan) if args.apply else {key:value for key,value in plan.items() if key != "config_after"}
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
