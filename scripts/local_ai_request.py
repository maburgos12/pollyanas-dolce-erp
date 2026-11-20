#!/usr/bin/env python3
"""Private client for the local queue. Existing management SSH access is required."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from uuid import NAMESPACE_URL, uuid5


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent", default="local_ai_router")
    parser.add_argument("--kind", choices=["summary", "extract", "verify"])
    parser.add_argument("--request-id", help="UUID; default is stable for identical agent/kind/evidence")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    if not args.status and not args.kind:
        parser.error("--kind is required for a request")
    payload = ""
    if not args.status:
        raw = sys.stdin.read(12001)
        if len(raw) > 12000:
            parser.error("Input exceeds 12000 characters")
        data = json.loads(raw)
        body = {"agent": args.agent, "kind": args.kind, "data": data}
        digest = hashlib.sha256(json.dumps(body, sort_keys=True, allow_nan=False).encode()).hexdigest()
        payload = json.dumps({**body, "request_id": args.request_id or str(uuid5(NAMESPACE_URL, "pastelerias-local:" + digest))})
        if len(payload) > 16384:
            parser.error("Encoded request too large")
    action = "status" if args.status else "submit"
    proc = subprocess.run(["ssh", "-T", "-oBatchMode=yes", "-oStrictHostKeyChecking=yes", "-oConnectTimeout=8",
        "-i", str(Path.home() / ".ssh/agente_dg_ops"), "root@68.183.165.47",
        "cd /opt/pastelerias-erp && docker compose exec -T web python manage.py local_ai_route " + action],
        input=payload, text=True, capture_output=True, timeout=70)
    if proc.returncode:
        raise SystemExit("Local router transport/request failed; no paid fallback was called")
    value = json.loads(proc.stdout)
    print(json.dumps(value, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
