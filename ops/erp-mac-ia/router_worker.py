#!/usr/bin/env python3
"""VPS router consumer; shares the existing pilot lock and runs one job per node."""
import concurrent.futures
import fcntl
import json
import os
import subprocess
import time

from worker import command as pilot_command

ROOT = "/opt/pastelerias-erp"
CONFIG = "/etc/pastelerias-local-router/ssh_config"


def command(action, payload=None):
    proc = subprocess.run(["docker", "compose", "exec", "-T", "web", "python", "manage.py", "local_ai_route", action],
                          input=json.dumps(payload) if payload is not None else "", capture_output=True,
                          text=True, timeout=60, cwd=ROOT, check=True)
    return json.loads(proc.stdout)


def transport(node, payload):
    proc = subprocess.run(["ssh", "-T", "-F", CONFIG, node], input=json.dumps(payload), capture_output=True,
                          text=True, timeout=480, check=True)
    if len(proc.stdout.encode()) > 16384:
        raise ValueError("invalid_output")
    value = json.loads(proc.stdout)
    if not isinstance(value, dict):
        raise ValueError("invalid_output")
    return value


def work(node):
    job = None
    try:
        probe = transport(node, {"probe": True})
        state = "idle" if probe == {"state": "idle"} else "busy" if probe == {"error": "busy"} else "offline"
        status = command("heartbeat", {"node": node, "state": state})
        if not status["enabled"] or state != "idle":
            return {"node": node, "state": state}
        job = command("claim", {"node": node})
        if not job:
            return {"node": node, "state": "idle"}
        command("heartbeat", {"node": node, "state": "running"})
        output = transport(node, {"model": job["model"], "kind": job["kind"], "data": job["data"]})
        if "error" in output:
            output = {"error": output["error"] if output["error"] in {"busy", "timeout", "invalid_output", "offline"} else "worker_error"}
        elif set(output) != {"result", "usage", "duration"}:
            output = {"error": "invalid_output"}
        finished = command("finish", {"task_id": job["id"], "lease": job["lease"], **output})
        command("heartbeat", {"node": node, "state": "busy" if output.get("error") == "busy" else "idle"})
        return {"node": node, "id": job["id"], "model": job["model"], "status": finished["status"]}
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        # Neither output nor transport stderr is logged: either may contain evidence.
        if job:
            try:
                command("finish", {"task_id": job["id"], "lease": job["lease"], "error": "worker_error"})
            except subprocess.SubprocessError:
                pass
        command("heartbeat", {"node": node, "state": "offline"})
        return {"node": node, "state": "offline"}


def drain(node):
    started, results = time.monotonic(), []
    for _ in range(4):
        if time.monotonic() - started >= 120:
            break
        value = work(node)
        results.append(value)
        if value.get("status") not in {"resolved", "cancelled"}:
            break
    return results


def main():
    os.umask(0o077)
    with open("/run/pastelerias-mac-ia/worker.lock", "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        status = command("status")
        if not status["enabled"] or not status["ready_by_node"]:
            return
        # Drain previously accepted pilot jobs before competing for this Mac.
        if any(j["status"] in {"pending", "running"} for j in pilot_command("status")["jobs"]):
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for results in pool.map(drain, [node for node in ("mac_ia", "mac_ia_02") if status["ready_by_node"].get(node)]):
                for value in results:
                    print(json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
