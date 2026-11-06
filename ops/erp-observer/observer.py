#!/usr/bin/env python3
"""Observe sanitized Doctor results. This process never starts commands or agents."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import time

MAX_BYTES = 262144


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return parsed


def project_meta(meta):
    result = meta.get("result")
    result = result if isinstance(result, dict) else {}
    checks = result.get("checks", [])
    if not isinstance(checks, list) or len(checks) > 64:
        raise ValueError("invalid checks")
    safe_checks = []
    for check in checks:
        name, status = check.get("name"), check.get("status")
        if not isinstance(name, str) or not re.fullmatch(r"[\w .:/()-]{1,120}", name):
            raise ValueError("invalid check name")
        if status not in {"OK", "WARN", "FAIL", "SKIPPED"}:
            raise ValueError("invalid check status")
        safe_checks.append(dict(name=name, status=status))
    return dict(task_id=meta.get("task_id"), celery_status=meta.get("status"),
                date_done=meta.get("date_done"), doctor_status=result.get("status"), checks=safe_checks)


def classify(snapshot, now):
    try:
        age = (now - timestamp(snapshot["collected_at"])).total_seconds()
        if age < -60:
            return "invalid"
        if age > 180:
            return "collector_stale"
        if snapshot["source"] != "doctor":
            return "source_unavailable"
        if not snapshot.get("task_id"):
            return "no_result"
        if not re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", snapshot["task_id"]):
            return "invalid"
        if not re.fullmatch(r"[0-9a-f]{40}", snapshot["revision_hint"]):
            return "invalid"
        status = snapshot["celery_status"]
        if status in {"PENDING", "STARTED", "RETRY", "RECEIVED"}:
            return "incomplete"
        done = timestamp(snapshot["date_done"])
        if (done - now).total_seconds() > 60:
            return "invalid"
        if (now - done).total_seconds() > 86400:
            return "expired"
        if done < timestamp(snapshot["worker_started_at"]):
            return "predates_worker"
        if status in {"FAILURE", "REVOKED"}:
            return "execution_failed"
        if status != "SUCCESS" or snapshot["doctor_status"] not in {"OK", "WARN", "FAIL"}:
            return "invalid"
        if not isinstance(snapshot.get("checks"), list):
            return "invalid"
        # Reuse the projection validator at this trust boundary too.
        project_meta(dict(result=dict(status=snapshot["doctor_status"], checks=snapshot["checks"])))
        return {"OK": "healthy", "WARN": "doctor_warn", "FAIL": "doctor_fail"}[snapshot["doctor_status"]]
    except (KeyError, TypeError, ValueError, AttributeError):
        return "invalid"


def atomic_json(path, value, mode=0o600):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        os.chmod(temporary, mode)
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_source(path):
    try:
        with path.open("rb") as stream:
            content = stream.read(MAX_BYTES + 1)
        if len(content) > MAX_BYTES:
            raise ValueError("snapshot too large")
        value = json.loads(content)
        if not isinstance(value, dict):
            raise ValueError("object required")
        return value
    except FileNotFoundError:
        return dict(source="unavailable", collected_at=datetime.now(timezone.utc).isoformat())
    except (ValueError, UnicodeError):
        return dict(source="invalid", collected_at=datetime.now(timezone.utc).isoformat())


def observe(snapshot, path, now):
    state = json.loads(path.read_text()) if path.exists() else dict(schema=1, mode="observe", incidents={})
    if state.get("schema") != 1 or state.get("mode") != "observe" or not isinstance(state.get("incidents"), dict):
        raise ValueError("invalid state; preserve and investigate")
    kind = classify(snapshot, now)
    safe = dict(kind=kind)
    if kind in {"healthy", "doctor_warn", "doctor_fail", "execution_failed", "predates_worker", "expired", "incomplete"}:
        safe.update({key: snapshot.get(key) for key in ("task_id", "date_done", "revision_hint", "celery_status", "doctor_status")})
    if kind in {"doctor_warn", "doctor_fail"}:
        safe["checks"] = sorted(project_meta(dict(result=dict(checks=snapshot["checks"])))["checks"],
                                key=lambda check: (check["name"], check["status"]))
    fingerprint = hashlib.sha256(json.dumps({k: v for k, v in safe.items() if k not in {"task_id", "date_done"}}, sort_keys=True).encode()).hexdigest()
    changed = state.get("fingerprint") != fingerprint
    if kind != "healthy":
        if fingerprint not in state["incidents"] and len(state["incidents"]) >= 256:
            raise ValueError("incident capacity reached; preserve evidence and review")
        incident = state["incidents"].setdefault(fingerprint, dict(first_seen=now.isoformat(), observation=safe))
        incident["last_seen"] = now.isoformat()
    state.update(heartbeat=now.isoformat(), fingerprint=fingerprint, last_observation=safe)
    atomic_json(path, state)
    if changed:
        print(json.dumps(dict(event="observation", mode="observe", kind=kind, fingerprint=fingerprint)), flush=True)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/var/lib/erp-observer-source/doctor.json"))
    parser.add_argument("--state", type=Path, default=Path("/var/lib/erp-observer/state.json"))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    with args.state.with_suffix(".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            observe(load_source(args.source), args.state, datetime.now(timezone.utc))
            if args.once:
                return
            time.sleep(30)


if __name__ == "__main__":
    main()
