#!/usr/bin/env python3
"""One fixed local-model job per invocation. No paid provider or model-selected commands."""
import fcntl
import http.client
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = "/opt/pastelerias-erp"
KEY = Path("/etc/pastelerias-mac-ia/api.key")
MODEL = "pastelerias-qwen3-8b"


def command(action, payload=None):
    value = json.dumps(payload) if isinstance(payload, dict) else payload or ""
    proc = subprocess.run(
        ["docker", "compose", "exec", "-T", "web", "python", "manage.py", "mac_ia_jobs", action],
        input=value, capture_output=True, text=True, timeout=60, cwd=ROOT, check=True,
    )
    return json.loads(proc.stdout)


def request(path, key, payload=None, *, port=18125):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=300 if payload else 5)
    try:
        conn.request("POST" if payload else "GET", path,
            body=json.dumps(payload) if payload else None,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        response = conn.getresponse()
        raw = response.read(32769)
        if response.status != 200 or len(raw) > 32768:
            raise ValueError("http_error")
        return json.loads(raw)
    finally:
        conn.close()


def infer(job, key):
    cases = job["cases"]
    keys = [case["key"] for case in cases]
    schema = {"type": "object", "properties": {
        "summary": {"type": "string"},
        "case_keys": {"type": "array", "items": {"type": "string"}},
    }, "required": ["summary", "case_keys"], "additionalProperties": False}
    prompt = (
        "Resume en español, en máximo 60 palabras, las tareas agrupadas del ERP. "
        "Son evidencias registradas, no estados operativos verificados. Describe prioridad y revisión humana. "
        "No autorices, no inventes datos ni sugieras comandos. ajuste=inventario; demand=demanda; "
        "production=producción; purchase=compras; director=dirección. "
        "Devuelve JSON con summary y case_keys exactamente en el mismo orden: "
        + json.dumps(keys, ensure_ascii=False) + ". Datos: " + json.dumps(cases, ensure_ascii=False)
    )
    if len(prompt.encode()) > 4096 or not 1 <= len(cases) <= 4:
        raise ValueError("invalid_output")
    start = time.monotonic()
    response = request("/v1/chat/completions", key, {
        "model": MODEL, "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 192, "temperature": 0,
        "response_format": {"type": "json_schema", "json_schema": {"name": "local_summary", "strict": True, "schema": schema}},
    })
    try:
        choice = response["choices"][0]
        if choice["finish_reason"] != "stop" or response.get("model") != MODEL:
            raise ValueError("invalid_output")
        result = json.loads(choice["message"]["content"])
        if (set(result) != {"summary", "case_keys"} or result["case_keys"] != keys
                or not isinstance(result["summary"], str) or not 1 <= len(result["summary"].strip()) <= 800):
            raise ValueError("invalid_output")
        usage = response["usage"]
        if any(type(usage.get(k)) is not int or not 0 <= usage[k] <= 100000
               for k in ("prompt_tokens", "completion_tokens")):
            raise ValueError("invalid_output")
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid_output") from exc
    return {"result": result, "usage": usage, "duration": time.monotonic() - start}


def main():
    os.umask(0o077)
    with open("/run/pastelerias-mac-ia/worker.lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if KEY.stat().st_mode & 0o077:
            raise RuntimeError("Model credential must be private")
        key = KEY.read_text().strip()
        if not key:
            raise RuntimeError("Missing model credential")
        try:
            health = request("/health", key)
            healthy = isinstance(health, dict) and health.get("status") == "ok"
        except (OSError, ValueError, http.client.HTTPException):
            healthy = False
        enabled = command("heartbeat", "idle" if healthy else "offline")["enabled"]
        if not enabled or not healthy:
            if not enabled:
                command("heartbeat", "disabled")
            return
        job = command("claim")
        if not job:
            return
        command("heartbeat", "running")
        try:
            completed = infer(job, key)
        except TimeoutError:
            completed = {"error": "timeout"}
        except (OSError, http.client.HTTPException):
            completed = {"error": "offline"}
        except ValueError as exc:
            completed = {"error": str(exc) if str(exc) in {"invalid_output", "http_error"} else "invalid_output"}
        command("finish", {"task_id": job["id"], "lease": job["lease"], **completed})
        command("heartbeat", "idle" if "error" not in completed else "offline")
        print(json.dumps({"task_id": job["id"], "completed": "error" not in completed,
                          "error": completed.get("error"), "duration": completed.get("duration")}))


if __name__ == "__main__":
    main()
