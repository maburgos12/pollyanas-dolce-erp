#!/usr/bin/env python3
"""Lossless-for-source stdio adapter for codebase-memory-mcp (Python 3.9+).

Only search_graph/get_code_snippet symbol matching fingerprints are omitted.
Queries, source, complexity, pagination, diagnostics and unknown fields survive.
No graph writes, indexing, source execution or third-party dependencies.
"""
import argparse
import copy
import json
import signal
import subprocess
import sys
import threading
from pathlib import Path

COMPACT_TOOLS = frozenset({"search_graph", "get_code_snippet"})
FINGERPRINTS = frozenset({"fp", "sp", "bt"})


def compact_payload(payload):
    """Remove known machine matching data from symbol objects only."""
    if not isinstance(payload, dict) or "_response_detail" in payload or "error" in payload:
        return payload
    removed = set()

    def visit(value):
        if isinstance(value, list):
            return [visit(item) for item in value]
        if not isinstance(value, dict):
            return value
        is_symbol = "qualified_name" in value and "file_path" in value
        result = {}
        for key, item in value.items():
            if is_symbol and key in FINGERPRINTS:
                removed.add(key)
            else:
                result[key] = visit(item)
        return result

    result = visit(payload)
    if not removed:
        return payload
    result["_response_detail"] = {
        "mode": "compact", "omitted_fields": sorted(removed),
        "full_arguments": {"response_detail": "full"},
    }
    return result


class GraphAdapter:
    """Correlate requests without modifying upstream tool arguments/contracts."""
    def __init__(self):
        self.pending = {}
        self.lock = threading.Lock()

    def request(self, message):
        if not isinstance(message, dict) or "id" not in message or "method" not in message:
            return message
        method = message.get("method")
        tool = None
        mode = "full"
        if method == "tools/call":
            params = message.get("params", {})
            tool = params.get("name")
            if tool in COMPACT_TOOLS:
                arguments = params.get("arguments", {})
                mode = arguments.get("response_detail", "compact")
                if mode not in {"compact", "full"}:
                    # Let the upstream validate unexpected input; do not guess.
                    mode = "full"
                elif "response_detail" in arguments:
                    message = copy.deepcopy(message)
                    del message["params"]["arguments"]["response_detail"]
        with self.lock:
            self.pending[message["id"]] = (method, tool, mode)
        return message

    def response(self, message):
        if not isinstance(message, dict) or "method" in message or "id" not in message:
            return message
        with self.lock:
            request = self.pending.pop(message["id"], None)
        if request is None or "error" in message or not isinstance(message.get("result"), dict):
            return message
        method, tool, mode = request
        if method == "tools/list":
            output = copy.deepcopy(message)
            for item in output["result"].get("tools", []):
                if item.get("name") in COMPACT_TOOLS:
                    item["inputSchema"] = copy.deepcopy(item.get("inputSchema", {}))
                    properties = item["inputSchema"].setdefault("properties", {})
                    if "response_detail" not in properties:
                        properties["response_detail"] = {
                            "type": "string", "enum": ["compact", "full"], "default": "compact",
                            "description": "compact omits only machine matching fingerprints fp/sp/bt; full returns every upstream field. Source, locations, metrics and truncation are preserved.",
                        }
            return output
        if tool not in COMPACT_TOOLS or mode != "compact" or message["result"].get("isError"):
            return message
        output = copy.deepcopy(message)
        for block in output["result"].get("content", []):
            if block.get("type") != "text" or not isinstance(block.get("text"), str):
                continue
            try:
                payload = json.loads(block["text"])
            except (ValueError, TypeError):
                continue
            compact = compact_payload(payload)
            if compact is not payload:
                block["text"] = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
        if isinstance(output["result"].get("structuredContent"), dict):
            output["result"]["structuredContent"] = compact_payload(output["result"]["structuredContent"])
        return output


def relay_line(line, transform):
    """Fail open for unknown frames; keep logs off the MCP stdout channel."""
    try:
        message = json.loads(line)
        output = transform(message)
        return (json.dumps(output, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        return line


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", default=str(Path.home() / ".local/bin/codebase-memory-mcp"))
    parser.add_argument("upstream_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    upstream_args = args.upstream_args[1:] if args.upstream_args[:1] == ["--"] else args.upstream_args
    adapter = GraphAdapter()
    child = subprocess.Popen([args.upstream, *upstream_args], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def input_worker():
        try:
            for line in sys.stdin.buffer:
                child.stdin.write(relay_line(line, adapter.request))
                child.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                child.stdin.close()
            except OSError:
                pass

    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    threading.Thread(target=input_worker, daemon=True).start()
    try:
        for line in child.stdout:
            sys.stdout.buffer.write(relay_line(line, adapter.response))
            sys.stdout.buffer.flush()
        return child.wait(timeout=5)
    except (KeyboardInterrupt, BrokenPipeError):
        return 130
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    raise SystemExit(main())
