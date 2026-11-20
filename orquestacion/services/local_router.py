"""Private, bounded work routing. Inputs/results are data, never executable instructions."""
import hashlib
import json
import math
import secrets
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from uuid import UUID

from django.db import transaction
from django.db.models import Q, Count
from django.utils import timezone

from orquestacion.models import AgentDefinition, AgentTask, OrchestrationRule, OrchestrationRun
from . import local_ai

RULE = "local_ai_router"
TASK = "local_ai_request"
NODES = {"mac_ia", "mac_ia_02"}
MODELS = {"qwen4b": "mac_ia", "python": "mac_ia", "qwen8b": "mac_ia_02", "nemotron": "mac_ia_02"}
ERRORS = {"busy", "offline", "timeout", "invalid_output", "worker_error"}


def configure(*, enabled):
    agent, _ = AgentDefinition.objects.get_or_create(code=RULE, defaults={
        "name": "Asignación de IA local", "domain": "seguimiento_tecnico", "status": "active",
        "description": "Canaliza evidencia acotada; no ejecuta operaciones de negocio.",
    })
    rule, _ = OrchestrationRule.objects.get_or_create(code=RULE, defaults={
        "name": "Asignación de solicitudes locales", "primary_agent": agent,
        "trigger_type": "manual", "is_active": False,
    })
    rule.is_active = enabled
    rule.save(update_fields=["is_active", "updated_at"])
    return {"enabled": enabled}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _text(data):
    text = data.get("text")
    if not isinstance(text, str) or not 1 <= len(text.strip()) <= 2500 or len(text.encode()) > 2500:
        raise ValueError("invalid_text")


def verification(data):
    rows = data.get("rows")
    if not isinstance(rows, list) or not 1 <= len(rows) <= 100:
        raise ValueError("invalid_rows")
    mismatches = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != {"expected", "actual"}:
            raise ValueError("invalid_row")
        values = []
        for value in row.values():
            if not isinstance(value, str) or not 1 <= len(value) <= 40:
                raise ValueError("invalid_number")
            try:
                number = Decimal(value)
            except InvalidOperation as exc:
                raise ValueError("invalid_number") from exc
            if not number.is_finite() or number.copy_abs() > Decimal("1e20"):
                raise ValueError("invalid_number")
            values.append(number)
        if values[0] != values[1]:
            mismatches.append(index)
    return {"checked": len(rows), "mismatch_indices": mismatches}


def route(kind, data):
    if not isinstance(data, dict) or len(json.dumps(data, allow_nan=False).encode()) > 12000:
        raise ValueError("invalid_data")
    if kind in {"summary", "extract"}:
        expected = {"text", "fields"} if kind == "extract" else {"text"}
        if set(data) != expected:
            raise ValueError("invalid_fields")
        _text(data)
        if kind == "extract":
            fields = data["fields"]
            if (not isinstance(fields, list) or not 1 <= len(fields) <= 6
                    or any(not isinstance(f, str) or not f.isidentifier() or len(f) > 40 for f in fields)
                    or len(set(fields)) != len(fields)):
                raise ValueError("invalid_fields")
            return "nemotron", "Extracción breve con valores literales verificables"
        return ("qwen4b", "Resumen breve de hasta 1000 bytes UTF-8") if len(data["text"].encode()) <= 1000 else (
            "qwen8b", "Resumen de evidencia de hasta 2500 bytes UTF-8")
    if kind == "verify":
        if set(data) != {"rows"}:
            raise ValueError("invalid_fields")
        verification(data)
        return "python", "Comparación exacta con Decimal; no necesita inferencia"
    if kind == "summary_cases":
        if set(data) != {"cases", "references"} or not isinstance(data["cases"], list) or not 1 <= len(data["cases"]) <= 4:
            raise ValueError("invalid_cases")
        # Only snapshots built by the existing source reader may enter this route.
        cases, references, stats = local_ai._sources()
        if stats["truncated"] or any(cases.get(c.get("key")) != c for c in data["cases"] if isinstance(c, dict)):
            raise ValueError("invalid_cases")
        if (any(not isinstance(c, dict) for c in data["cases"])
                or data["references"] != [references.get(c["key"]) for c in data["cases"]]):
            raise ValueError("invalid_references")
        return ("qwen4b", "Hasta dos casos sanitizados del agente") if len(data["cases"]) <= 2 else (
            "qwen8b", "De tres a cuatro casos sanitizados del agente")
    if set(data) != {"text"}:
        raise ValueError("invalid_fields")
    _text(data)
    return None, "Solicitud fuera de los contratos locales: revisión de Codex o humana"


@transaction.atomic
def submit(*, agent, kind, data, request_id):
    if not isinstance(request_id, str) or not isinstance(agent, str) or len(agent) > 80:
        raise ValueError("invalid_identity")
    request_id = str(UUID(request_id))
    caller = AgentDefinition.objects.filter(code=agent, status="active").first()
    if not caller or not isinstance(kind, str) or not 1 <= len(kind) <= 40:
        raise ValueError("invalid_agent_or_kind")
    rule = OrchestrationRule.objects.select_for_update().filter(code=RULE, is_active=True).first()
    if not rule:
        raise ValueError("router_disabled")
    digest = _digest({"agent": agent, "kind": kind, "data": data})
    previous = OrchestrationRun.objects.filter(run_key=f"{TASK}:{request_id}").first()
    if previous:
        if previous.context_json.get("digest") != digest:
            raise ValueError("idempotency_conflict")
        return {**receipt(previous.tasks.get(task_type=TASK)), "created": False}
    model, reason = route(kind, data)
    if AgentTask.objects.filter(task_type=TASK, status__in=["pending", "running"]).count() >= 16:
        raise ValueError("queue_full")
    node = MODELS.get(model)
    status = "pending" if model else "blocked"
    run = OrchestrationRun.objects.create(run_key=f"{TASK}:{request_id}", rule=rule,
        trigger_source="local_ai_request", status="pending" if model else "failed",
        finished_at=None if model else timezone.now(), context_json={"digest": digest, "node": node})
    task = AgentTask.objects.create(run=run, agent=caller, task_type=TASK,
        title=f"IA local · {agent} · {kind}", status=status,
        input_payload={"kind": kind, "data": data, "node": node, "model": model, "reason": reason},
        output_payload={"attempts": 0, "error": "escalated" if not model else ""})
    return {**receipt(task), "created": True}


def receipt(task):
    request = task.input_payload
    result = task.output_payload
    value = result.get("result")
    display = ""
    if value:
        if request["kind"] == "verify":
            display = f"{value['checked']} pares comprobados; {len(value['mismatch_indices'])} diferencias."
        elif request["kind"] == "extract":
            display = "\n".join(f"{field}: {item if item is not None else 'No aportado'}" for field, item in value.items())
        else:
            display = value["summary"]
    return {"id": task.id, "agent": task.agent.code, "agent_name": task.agent.name,
            "kind_label": {"summary": "Resumen", "extract": "Extracción de campos", "verify": "Cotejo numérico", "summary_cases": "Resumen de expedientes"}.get(request["kind"], "Revisión requerida"),
            "display_result": display, "kind": request["kind"], "node": request["node"],
            "model": request["model"], "reason": request["reason"], "status": task.status, "label": task.get_status_display(),
            "attempts": result.get("attempts", 0), "error": result.get("error", ""),
            "result": result.get("result"), "duration": result.get("duration"),
            "usage": result.get("usage", {}), "updated_at": timezone.localtime(task.updated_at).strftime("%d/%m/%Y %H:%M:%S %Z")}


def _fresh(task):
    if task.agent.status != "active":
        return False
    if task.input_payload["kind"] != "summary_cases":
        return True
    current, references, stats = local_ai._sources()
    data = task.input_payload["data"]
    return (not stats["truncated"] and all(current.get(c["key"]) == c for c in data["cases"])
            and data["references"] == [references.get(c["key"]) for c in data["cases"]])


@transaction.atomic
def claim(node):
    if node not in NODES:
        raise ValueError("invalid_node")
    rule = OrchestrationRule.objects.select_for_update().filter(code=RULE, is_active=True).first()
    if not rule:
        return None
    now = timezone.now()
    jobs = AgentTask.objects.filter(task_type=TASK, input_payload__node=node)
    running = jobs.select_related("run").filter(status="running").first()
    if running:
        if running.due_at and running.due_at > now:
            return None
        local_ai._save(running, "blocked" if running.output_payload["attempts"] >= 3 else "pending",
                       {"attempts": running.output_payload["attempts"], "error": "lease_expired"})
    for task in jobs.select_related("run", "agent").filter(status="pending").order_by("id")[:16]:
        if task.due_at and task.due_at > now:
            continue
        if not _fresh(task):
            local_ai._save(task, "cancelled", {"error": "source_changed"})
            continue
        lease = secrets.token_hex(24)
        task.due_at = now + timedelta(minutes=12)
        local_ai._save(task, "running", {"lease": lease, "attempts": task.output_payload.get("attempts", 0) + 1,
                                        "started_at": now.isoformat()})
        return {"id": task.id, "lease": lease, **task.input_payload}
    return None


def validate_result(task, result):
    request = task.input_payload
    kind, data = request["kind"], request["data"]
    if kind == "verify":
        if (not isinstance(result, dict) or set(result) != {"checked", "mismatch_indices"}
                or type(result["checked"]) is not int or not isinstance(result["mismatch_indices"], list)
                or any(type(i) is not int for i in result["mismatch_indices"])
                or result != verification(data)):
            raise ValueError("invalid_output")
    elif kind == "extract":
        if (not isinstance(result, dict) or set(result) != set(data["fields"])
                or any(value is not None and (not isinstance(value, str) or not value or value not in data["text"])
                       for value in result.values())):
            raise ValueError("invalid_output")
    elif kind == "summary_cases":
        local_ai.validate_result(result, data["cases"])
    elif (not isinstance(result, dict) or set(result) != {"summary"}
          or not isinstance(result["summary"], str) or not 1 <= len(result["summary"].strip()) <= 800):
        raise ValueError("invalid_output")


@transaction.atomic
def finish(task_id, lease, *, result=None, usage=None, duration=0, error=None):
    rule = OrchestrationRule.objects.select_for_update().get(code=RULE)
    task = AgentTask.objects.select_for_update().select_related("run", "agent").get(pk=task_id, task_type=TASK)
    previous = task.output_payload
    if (task.status != "running" or not isinstance(lease, str) or
            not secrets.compare_digest(previous.get("lease", ""), lease) or not task.due_at or task.due_at <= timezone.now()):
        raise ValueError("expired_lease")
    if not rule.is_active or not _fresh(task):
        local_ai._save(task, "cancelled", {"error": "source_changed"})
        return receipt(task)
    payload = {"attempts": previous["attempts"]}
    if error is not None:
        if error not in ERRORS:
            raise ValueError("invalid_error")
        payload["error"] = error
        task.due_at = timezone.now() + timedelta(minutes=5 * previous["attempts"])
        state = "blocked" if previous["attempts"] >= 3 else "pending"
    else:
        validate_result(task, result)
        usage = usage or {}
        if (not isinstance(usage, dict) or set(usage) - {"prompt_tokens", "completion_tokens"}
                or any(type(v) is not int or not 0 <= v <= 100000 for v in usage.values())
                or type(duration) not in (int, float) or not math.isfinite(duration) or not 0 <= duration <= 600):
            raise ValueError("invalid_usage")
        payload.update(result=result, usage=usage, duration=round(duration, 2), finished_at=timezone.now().isoformat(),
                       validation="exact" if task.input_payload["kind"] == "verify" else "structure_and_source_spans" if task.input_payload["kind"] == "extract" else "structure_only_human_review")
        state = "resolved"
    local_ai._save(task, state, payload)
    return receipt(task)


@transaction.atomic
def heartbeat(node, state):
    if node not in NODES or state not in {"idle", "running", "offline", "busy"}:
        raise ValueError("invalid_heartbeat")
    rule = OrchestrationRule.objects.select_for_update().filter(code=RULE).first()
    if rule:
        rule.condition_json = {**rule.condition_json, node: {"state": state, "at": timezone.now().isoformat()}}
        rule.save(update_fields=["condition_json", "updated_at"])
    return {"enabled": bool(rule and rule.is_active)}


def panel():
    rule = OrchestrationRule.objects.filter(code=RULE).first()
    eligible = AgentTask.objects.filter(task_type=TASK, status__in=["pending", "running"]).filter(Q(due_at__isnull=True) | Q(due_at__lte=timezone.now()))
    pending = {row["input_payload__node"]: row["count"] for row in eligible.values("input_payload__node").annotate(count=Count("id"))}
    return {"enabled": bool(rule and rule.is_active), "nodes": rule.condition_json if rule else {}, "ready_by_node": pending,
            "jobs": [receipt(t) for t in AgentTask.objects.select_related("agent").filter(task_type=TASK).order_by("-id")[:12]]}
