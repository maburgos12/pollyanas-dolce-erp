#!/usr/bin/env python3
"""Fixed privileged reader, separate from the unprivileged observer and Codex."""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess

from observer import atomic_json, project_meta, timestamp

WORKER = "pastelerias-erp-worker-1"
REPO = "/opt/pastelerias-erp"
OUTPUT = Path("/var/lib/erp-observer-source/doctor.json")
TASK = re.compile(r"Task reportes\.erp_doctor_daily_report\[([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\] (?:succeeded|failed)")
QUERY = '''
import json, sys, signal
signal.alarm(12)
from config.celery import app
app.backend.accept = {"application/json"}
rows = []
for task_id in json.load(sys.stdin):
    meta = app.backend.get_task_meta(task_id)
    result = meta.get("result")
    result = result if isinstance(result, dict) else {}
    checks = result.get("checks", [])
    rows.append(dict(task_id=task_id, status=meta.get("status"), date_done=meta.get("date_done"),
        result=dict(status=result.get("status"), checks=[dict(name=c.get("name"), status=c.get("status"))
            for c in checks[:64]] if isinstance(checks, list) else None)))
print(json.dumps(rows, default=str))
'''


def command(arguments, **kwargs):
    return subprocess.run(arguments, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, timeout=20, **kwargs).stdout


def collect():
    snapshot = dict(source="doctor", collected_at=datetime.now(timezone.utc).isoformat(), task_id=None)
    snapshot["revision_hint"] = command(["git", "-C", REPO, "rev-parse", "HEAD"]).strip()
    snapshot["worker_started_at"] = command(["docker", "inspect", "--format", "{{.State.StartedAt}}", WORKER]).strip()
    logs = subprocess.run(["docker", "logs", "--since", "48h", "--tail", "12000", WORKER],
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=20, check=True).stdout
    candidates = list(dict.fromkeys(TASK.findall(logs)))[-8:]
    if candidates:
        rows = json.loads(command(["docker", "exec", "-i", WORKER, "/opt/venv/bin/python", "-c", QUERY], input=json.dumps(candidates)))
        completed = [row for row in rows if row.get("date_done")]
        if completed:
            snapshot.update(project_meta(max(completed, key=lambda row: timestamp(row["date_done"]))))
    return snapshot


def main():
    try:
        snapshot = collect()
    except (subprocess.SubprocessError, OSError, ValueError, TypeError, KeyError, AttributeError):
        # Never persist raw Docker logs, exception messages or backend results.
        snapshot = dict(source="unavailable", collected_at=datetime.now(timezone.utc).isoformat(), task_id=None)
    atomic_json(OUTPUT, snapshot, mode=0o640)
    print(json.dumps(dict(event="collection", source=snapshot["source"], has_result=bool(snapshot.get("task_id")))))


if __name__ == "__main__":
    main()
