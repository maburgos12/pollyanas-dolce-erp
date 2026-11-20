"""Bounded, read-only summaries of existing evidence; only technical jobs are written."""
import hashlib
import json
import secrets
from uuid import NAMESPACE_URL, uuid5
from datetime import timedelta

from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from orquestacion.models import AgentDefinition, AgentTask, OrchestrationRule, OrchestrationRun

NODE = "mac_ia_02"
RULE = "mac_ia_02_local_summary"
TASK = "mac_ia_02_summary"
SCHEDULE = "orquestacion: resumen local Mac-IA-02"
SOURCES = {
    "inventory_adjustment_authorization_guard": "ajuste_id",
    "plan_demand_chain_review": "plan_id",
    "plan_production_chain_review": "plan_id",
    "plan_purchase_chain_review": "plan_id",
    "plan_director_chain_review": "plan_id",
}
COUNTS = {"historico_days", "forecast_count", "sales_request_count", "recipes_in_plan",
          "incomplete_recipe_count", "master_blocker_count", "shortage_count", "critical_shortage_count"}
ERRORS = {"offline", "timeout", "invalid_output", "http_error", "worker_error", "lease_expired"}
MAX_ATTEMPTS = 3


def configure(*, enabled):
    agent, _ = AgentDefinition.objects.get_or_create(code=NODE, defaults={
        "name": "Mac-IA-02 · resúmenes locales", "domain": "seguimiento_tecnico",
        "status": "active", "description": "Agrupa evidencia; no autoriza ni ejecuta operaciones.",
    })
    rule, _ = OrchestrationRule.objects.get_or_create(code=RULE, defaults={
        "name": "Resúmenes locales Mac-IA-02", "primary_agent": agent, "is_active": False,
        "trigger_type": "schedule",
    })
    rule.is_active = enabled
    rule.save(update_fields=["is_active", "updated_at"])
    interval, _ = IntervalSchedule.objects.get_or_create(every=1, period=IntervalSchedule.HOURS)
    PeriodicTask.objects.update_or_create(name=SCHEDULE, defaults={
        "task": "orquestacion.local_ai_enqueue", "interval": interval, "crontab": None,
        "kwargs": "{}", "enabled": enabled,
    })
    return {"enabled": enabled}


def _signature(case):
    return hashlib.sha256(json.dumps(case, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _sources():
    rows = list(AgentTask.objects.filter(
        task_type__in=SOURCES, created_at__gte=timezone.now() - timedelta(days=7),
    ).order_by("-created_at", "-id")[:2001])
    cases, references = {}, {}
    for task in rows[:2000]:
        entity = (task.input_payload or {}).get(SOURCES[task.task_type])
        if type(entity) is not int or entity <= 0:
            continue
        role = {"ajuste_id": "ajuste", "plan_id": task.task_type.split("_")[1]}[SOURCES[task.task_type]]
        key = f"{role}:{entity}"
        if key in cases:
            continue
        case = {"key": key, "status": task.status, "priority": task.priority}
        counts = {name: value for name, value in (task.output_payload or {}).items()
                  if name in COUNTS and type(value) is int and 0 <= value <= 1000000}
        recorded_status = (task.input_payload or {}).get("estatus")
        if task.task_type.startswith("inventory") and recorded_status in {"PENDIENTE", "APLICADO", "RECHAZADO"}:
            case["recorded_inventory_status"] = recorded_status
        if counts:
            case["counts"] = counts
        cases[key] = case
        references[key] = {"task_id": task.id, "run_id": task.run_id, "entity_id": entity,
                           "task_type": task.task_type}
    return cases, references, {"source_count": min(len(rows), 2000), "case_count": len(cases),
                               "truncated": len(rows) > 2000}


@transaction.atomic
def enqueue():
    rule = OrchestrationRule.objects.select_for_update().filter(code=RULE).first()
    if not rule or not rule.is_active:
        return {"created": 0}
    cases, references, stats = _sources()
    from . import local_router
    routed = OrchestrationRule.objects.filter(code=local_router.RULE, is_active=True).exists()
    jobs = AgentTask.objects.filter(task_type__in=[TASK, local_router.TASK])
    # Content-addressed evidence is reusable even when routine IDs/timestamps change.
    seen = set()
    # shortcut: 2000 recent jobs cover this pilot; review retention before expanding sources.
    for payload in jobs.exclude(status="cancelled").order_by("-id").values_list("input_payload", flat=True)[:2000]:
        seen.update(_signature(case) for case in payload.get("cases", payload.get("data", {}).get("cases", [])))
    changed = [cases[key] for key in sorted(cases) if _signature(cases[key]) not in seen]
    capacity = max(0, 8 - jobs.filter(status__in=["pending", "running"]).count())
    created = 0
    if not stats["truncated"]:
        for offset in range(0, min(len(changed), capacity * 4), 4):
            batch = changed[offset:offset + 4]
            digest = _signature(batch)
            if routed:
                data = {"cases": batch, "references": [references[c["key"]] for c in batch]}
                cancelled = jobs.filter(task_type=local_router.TASK, status="cancelled",
                    input_payload__kind="summary_cases", input_payload__data=data).order_by("-id").values_list("id", flat=True).first()
                receipt = local_router.submit(agent=rule.primary_agent.code, kind="summary_cases", data=data,
                    request_id=str(uuid5(NAMESPACE_URL, f"{TASK}:{_signature(data)}:after:{cancelled or 0}")))
                created += int(receipt["created"])
                continue
            run, new = OrchestrationRun.objects.get_or_create(run_key=f"{TASK}:{digest}", defaults={
                "trigger_source": "local_ai_hourly", "rule": rule, "status": "pending",
                "context_json": {"node": NODE},
            })
            if not new:
                previous = run.tasks.filter(task_type=TASK, status="cancelled").first()
                if previous:
                    previous.due_at = None
                    previous.input_payload = {"cases": batch, "references": [references[c["key"]] for c in batch]}
                    previous.save(update_fields=["input_payload"])
                    _save(previous, "pending", {"attempts": 0})
                    created += 1
                continue
            AgentTask.objects.create(run=run, agent=rule.primary_agent, task_type=TASK,
                title=f"Mac-IA-02: resumir {len(batch)} casos agrupados",
                input_payload={"cases": batch, "references": [references[c["key"]] for c in batch]},
                output_payload={"attempts": 0})
            created += 1
    stats.update(created=created, unchanged=len(cases) - len(changed), queued_cases=min(len(changed), created * 4))
    rule.condition_json = {**rule.condition_json, "last_scan": timezone.now().isoformat(), "scan": stats}
    rule.save(update_fields=["condition_json", "updated_at"])
    return stats


def _current(task):
    cases, _, stats = _sources()
    return not stats["truncated"] and all(cases.get(case["key"]) == case for case in task.input_payload["cases"])


def _save(task, status, payload):
    if task.output_payload.get("started_at"):
        payload.setdefault("started_at", task.output_payload["started_at"])
    task.status = status
    task.output_payload = payload
    task.save(update_fields=["status", "output_payload", "due_at", "updated_at"])
    task.run.status = {"running": "running", "resolved": "success", "cancelled": "cancelled",
                       "blocked": "failed", "pending": "pending"}[status]
    task.run.finished_at = timezone.now() if status in {"resolved", "cancelled", "blocked"} else None
    task.run.save(update_fields=["status", "finished_at"])


@transaction.atomic
def claim():
    rule = OrchestrationRule.objects.select_for_update().filter(code=RULE, is_active=True).first()
    if not rule:
        return None
    now = timezone.now()
    running = AgentTask.objects.filter(task_type=TASK, status="running").first()
    if running:
        if running.due_at and running.due_at > now:
            return None
        payload = {**running.output_payload, "error": "lease_expired"}
        payload.pop("lease", None)
        _save(running, "blocked" if payload.get("attempts", 0) >= MAX_ATTEMPTS else "pending", payload)
    for task in AgentTask.objects.select_related("run").filter(task_type=TASK, status="pending").order_by("id")[:8]:
        if task.due_at and task.due_at > now:
            continue
        if not _current(task):
            _save(task, "cancelled", {**task.output_payload, "error": "source_changed"})
            continue
        lease = secrets.token_hex(24)
        task.due_at = now + timedelta(minutes=12)
        _save(task, "running", {"lease": lease, "attempts": task.output_payload.get("attempts", 0) + 1,
                                "started_at": now.isoformat()})
        return {"id": task.id, "lease": lease, "cases": task.input_payload["cases"]}
    return None


def validate_result(result, cases):
    if (not isinstance(result, dict) or set(result) != {"summary", "case_keys"}
            or not isinstance(result["summary"], str) or not 1 <= len(result["summary"].strip()) <= 800
            or not isinstance(result["case_keys"], list)
            or result["case_keys"] != [case["key"] for case in cases]):
        raise ValueError("invalid_output")


@transaction.atomic
def finish(task_id, lease, *, result=None, usage=None, duration=0, error=None):
    OrchestrationRule.objects.select_for_update().get(code=RULE)
    task = AgentTask.objects.select_for_update().select_related("run").get(pk=task_id, task_type=TASK)
    payload = task.output_payload
    if (task.status != "running" or not secrets.compare_digest(str(payload.get("lease", "")), str(lease))
            or not task.due_at or task.due_at <= timezone.now()):
        raise ValueError("expired_lease")
    if not _current(task):
        _save(task, "cancelled", {"attempts": payload["attempts"], "error": "source_changed"})
        return {"status": "cancelled"}
    if error:
        if error not in ERRORS:
            raise ValueError("invalid_error")
        payload = {"attempts": payload["attempts"], "error": error}
        task.due_at = timezone.now() + timedelta(minutes=5 * payload["attempts"])
        status = "blocked" if payload["attempts"] >= MAX_ATTEMPTS else "pending"
    else:
        validate_result(result, task.input_payload["cases"])
        usage = usage or {}
        if (not isinstance(usage, dict) or any(type(usage.get(k, 0)) is not int or not 0 <= usage.get(k, 0) <= 100000
                for k in ("prompt_tokens", "completion_tokens"))
                or type(duration) not in (float, int) or not 0 <= duration <= 600):
            raise ValueError("invalid_usage")
        payload = {"attempts": payload["attempts"], "result": result,
                   "usage": {k: usage.get(k, 0) for k in ("prompt_tokens", "completion_tokens")},
                   "duration": round(duration, 2), "finished_at": timezone.now().isoformat()}
        status = "resolved"
    _save(task, status, payload)
    return {"status": status}


@transaction.atomic
def heartbeat(state):
    if state not in {"idle", "running", "offline", "disabled"}:
        raise ValueError("invalid_state")
    rule = OrchestrationRule.objects.select_for_update().filter(code=RULE).first()
    if not rule:
        return {"enabled": False}
    now = timezone.now().isoformat()
    rule.condition_json = {**rule.condition_json, "worker_state": state, "heartbeat_at": now}
    if state in {"idle", "running"}:
        rule.condition_json["last_connected_at"] = now
    rule.save(update_fields=["condition_json", "updated_at"])
    return {"enabled": rule.is_active}


def panel():
    from . import local_router
    rule = OrchestrationRule.objects.filter(code=RULE).first()
    config = rule.condition_json if rule else {}
    jobs = []
    for task in AgentTask.objects.filter(task_type=TASK).order_by("-created_at", "-id")[:12]:
        payload = task.output_payload
        usage = payload.get("usage", {})
        jobs.append({"id": task.id, "status": task.status, "label": task.get_status_display(),
            "cases": task.input_payload.get("cases", []), "references": task.input_payload.get("references", []),
            "summary": payload.get("result", {}).get("summary", ""), "error": payload.get("error", ""),
            "attempts": payload.get("attempts", 0), "duration": payload.get("duration"),
            "tokens": sum(usage.values()), "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0), "updated_at": task.updated_at,
            "started_at": parse_datetime(payload["started_at"]) if payload.get("started_at") else None, "lease_until": task.due_at if task.status == "running" else None})
    return {"enabled": bool(rule and rule.is_active), "model": "Qwen3-8B Q4_K_M · CPU local", "jobs": jobs,
            "state": config.get("worker_state", "unknown"), "heartbeat_at": parse_datetime(config["heartbeat_at"]) if config.get("heartbeat_at") else None,
            "last_connected_at": parse_datetime(config["last_connected_at"]) if config.get("last_connected_at") else None,
            "last_scan": parse_datetime(config["last_scan"]) if config.get("last_scan") else None,
            "scan": config.get("scan", {}), "router": local_router.panel()}
