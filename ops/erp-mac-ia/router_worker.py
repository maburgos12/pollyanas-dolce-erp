#!/usr/bin/env python3
"""VPS router consumer; shares the existing pilot lock and runs one job per node."""
import concurrent.futures
import fcntl
import json
import os
import subprocess
import time
from functools import partial
from threading import Event

from worker import command as pilot_command

ROOT = "/opt/pastelerias-erp"
CONFIG = "/etc/pastelerias-local-router/ssh_config"
PHASES = {"probe", "claim", "execute", "finish", "heartbeat", "input", "prepare", "guard", "startup", "health", "generate", "parse"}
CATEGORIES = {"timeout", "process_exit", "io", "json", "contract", "runtime", "interrupted", "unknown"}
REASONS = {"invalid_output", "invalid_usage", "expired_lease", "invalid_error", "unknown",
           "http_status", "response_size", "model_mismatch", "token_limit", "finish_reason"}


def diagnostic(exc, phase):
    category = next((label for kind, label in (
        (subprocess.TimeoutExpired, "timeout"), (TimeoutError, "timeout"),
        (subprocess.CalledProcessError, "process_exit"), (OSError, "io"),
        (json.JSONDecodeError, "json"), (ValueError, "contract"), (KeyError, "contract"))
        if isinstance(exc, kind)), "unknown")
    reason = "unknown"
    if isinstance(exc, subprocess.CalledProcessError) and isinstance(exc.stderr, str) and len(exc.stderr.encode()) <= 256:
        try:
            value = json.loads(exc.stderr.removeprefix("CommandError: "))
            if isinstance(value, dict) and set(value) == {"router_error"} and isinstance(value["router_error"], str) and value["router_error"] in REASONS:
                reason = value["router_error"]
        except ValueError:
            pass
    return {"phase": phase, "category": category, "reason": reason}


def node_diagnostic(value):
    if (isinstance(value, dict) and set(value) == {"phase", "category", "reason"}
            and all(isinstance(v, str) for v in value.values())
            and value["phase"] in PHASES and value["category"] in CATEGORIES and value["reason"] in REASONS):
        return value
    return None


def command(action, payload=None):
    proc = subprocess.run(["docker", "compose", "exec", "-T", "web", "python", "manage.py", "local_ai_route", action],
                          input=json.dumps(payload) if payload is not None else "", capture_output=True,
                          text=True, timeout=60, cwd=ROOT, check=True)
    return json.loads(proc.stdout)


def transport(node, payload):
    timeout = 480
    if payload.get("kind") == "hours_draft_edit":
        timeout = 255 if payload["data"].get("mode") == "initial" else 155
    proc = subprocess.run(["ssh", "-T", "-F", CONFIG, node], input=json.dumps(payload), capture_output=True,
                          text=True, timeout=timeout, check=True)
    if len(proc.stdout.encode()) > 16384:
        raise ValueError("invalid_output")
    value = json.loads(proc.stdout)
    if not isinstance(value, dict):
        raise ValueError("invalid_output")
    return value


def work(node, *, first_claim=None):
    job = None
    phase, detail = "probe", None
    try:
        probe = transport(node, {"probe": True})
        state = "idle" if probe == {"state": "idle"} else "busy" if probe.get("error") == "busy" else "offline"
        detail = node_diagnostic(probe.get("diagnostic"))
        phase = "heartbeat"
        status = command("heartbeat", {"node": node, "state": state})
        if not status["enabled"] or state != "idle":
            return {"node": node, "state": state, **({"diagnostic": detail} if detail else {})}
        phase = "claim"
        if first_claim is not None and node == "mac_ia_02":
            first_claim.wait(timeout=12)
        job = command("claim", {"node": node})
        if first_claim is not None and node == "mac_ia":
            first_claim.set()
        if not job:
            return {"node": node, "state": "idle"}
        phase = "heartbeat"
        command("heartbeat", {"node": node, "state": "running"})
        phase = "execute"
        output = transport(node, {"model": job["model"], "kind": job["kind"], "data": job["data"]})
        if "error" in output:
            detail = node_diagnostic(output.get("diagnostic"))
            output = {"error": output["error"] if isinstance(output["error"], str) and output["error"] in {"busy", "timeout", "invalid_output", "offline"} else "worker_error"}
        elif set(output) != {"result", "usage", "duration"}:
            output = {"error": "invalid_output"}
        phase = "finish"
        finished = command("finish", {"task_id": job["id"], "lease": job["lease"], **output})
        phase = "heartbeat"
        command("heartbeat", {"node": node, "state": "busy" if output.get("error") == "busy" else "idle"})
        return {"node": node, "id": job["id"], "model": job["model"], "status": finished["status"], **({"diagnostic": detail} if detail else {})}
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        # Neither output nor transport stderr is logged: either may contain evidence.
        detail = diagnostic(exc, phase)
        if job:
            try:
                command("finish", {"task_id": job["id"], "lease": job["lease"], "error": "worker_error"})
            except (OSError, ValueError, KeyError, subprocess.SubprocessError):
                pass
        try:
            command("heartbeat", {"node": node, "state": "offline"})
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            pass
        return {"node": node, "state": "offline", **({"id": job["id"], "model": job["model"]} if job else {}), "diagnostic": detail}
    finally:
        if first_claim is not None and node == "mac_ia":
            first_claim.set()


def drain(node, *, first_claim=None):
    started, results = time.monotonic(), []
    for _ in range(4):
        if time.monotonic() - started >= 120:
            break
        value = work(node, first_claim=first_claim) if first_claim is not None and not results else work(node)
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
        nodes = [node for node in ("mac_ia", "mac_ia_02") if status["ready_by_node"].get(node)]
        first_claim = Event() if len(nodes) == 2 and status.get("hours_ready") else None
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for results in pool.map(partial(drain, first_claim=first_claim), nodes):
                for value in results:
                    print(json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
