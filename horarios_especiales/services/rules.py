"""Versioned, explicit ERP schedule rules; publication is a separate outcome."""

import hashlib
import json
import re
from datetime import date


PRECEDENCE = {"weekly": 1, "range": 2, "date": 3}


def rule_revision(payload):
    content = {key: payload[key] for key in ("schema_version", "locations", "rules", "reason", "source_channel")}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def validate_rules(rules):
    if not isinstance(rules, list) or not 1 <= len(rules) <= 31:
        raise ValueError("Se requieren entre 1 y 31 reglas.")
    for rule in rules:
        if not isinstance(rule, dict) or set(rule) - {"kind", "start_date", "end_date", "weekdays", "closed_all_day", "time_windows"}:
            raise ValueError("Regla inválida o campos desconocidos.")
        kind = rule.get("kind")
        if not isinstance(kind, str) or kind not in PRECEDENCE:
            raise ValueError("Tipo de regla inválido.")
        for key in ("start_date", "end_date"):
            value = rule.get(key)
            if key == "end_date" and value is None and kind in {"date", "weekly"}:
                continue
            if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise ValueError("La vigencia requiere fechas ISO explícitas.")
            date.fromisoformat(value)
        start, end = rule["start_date"], rule.get("end_date")
        if end and (end < start or (kind == "date" and end != start)):
            raise ValueError("Vigencia inválida.")
        weekdays = rule.get("weekdays")
        if kind == "weekly":
            if (not isinstance(weekdays, list) or not 1 <= len(weekdays) <= 7
                    or any(type(day) is not int or not 0 <= day <= 6 for day in weekdays)
                    or len(set(weekdays)) != len(weekdays)):
                raise ValueError("La recurrencia requiere días únicos de lunes=0 a domingo=6.")
        elif weekdays is not None:
            raise ValueError("Sólo la recurrencia semanal admite días de semana.")
        # Share the strict interval validator used by the existing effective-hours contract.
        from horarios_especiales.services.effective_hours import _windows
        if _windows(rule.get("time_windows"), rule.get("closed_all_day")) is None:
            raise ValueError("Rangos horarios inválidos o superpuestos.")
    return rules


def validate_rule_payload(payload):
    try:
        validate_rules(payload.get("rules"))
        locations = payload.get("locations")
        if (not isinstance(locations, list) or not 1 <= len(locations) <= 10
                or any(not isinstance(row, dict) or type(row.get("branch_id")) is not int for row in locations)
                or len({row["branch_id"] for row in locations}) != len(locations)):
            raise ValueError("Se requieren sucursales únicas identificadas por su ID ERP.")
        if payload.get("revision") != rule_revision(payload):
            raise ValueError("La revisión no coincide con el contenido de la vista previa.")
    except (ValueError, TypeError, KeyError) as exc:
        return [str(exc)]
    return []


def rule_applies(rule, target_date):
    target = target_date.isoformat()
    if target < rule["start_date"] or (rule.get("end_date") and target > rule["end_date"]):
        return False
    if rule["kind"] == "date":
        return target == rule["start_date"]
    return rule["kind"] == "range" or target_date.weekday() in rule["weekdays"]


def resolve_approved_rule(branch, target_date):
    from horarios_especiales.models import SolicitudHorarioEspecial

    candidates = []
    requests = SolicitudHorarioEspecial.objects.filter(
        canonical_payload__schema_version=2,
        canonical_payload__locations__contains=[{"branch_id": branch.id}],
        status__in=["APROBADO", "EJECUTADO", "FALLIDO"],
        approved_at__isnull=False, cancelled_at__isnull=True,
    )
    for request in requests.iterator():
        payload = request.canonical_payload
        if validate_rule_payload(payload) or not request.audit_entries.filter(
            action="APPROVED", payload_json__revision=payload.get("revision"),
        ).exists():
            return {"status": "UNKNOWN", "reason": "SPECIAL_HOURS_CONFLICT_OR_INVALID"}
        for index, rule in enumerate(payload["rules"]):
            if rule_applies(rule, target_date):
                candidates.append(((PRECEDENCE[rule["kind"]], request.approved_at, request.id, index), request, rule))
    if not candidates:
        return None
    _, request, rule = max(candidates, key=lambda candidate: candidate[0])
    return {"status": "VERIFIED", "source": "ERP_APPROVED_SPECIAL_HOURS", "reason": None,
            "closed_all_day": rule["closed_all_day"], "windows": sorted(rule["time_windows"], key=lambda w: w["open"]),
            "revision": request.canonical_payload["revision"], "request_id": request.id,
            "rule_kind": rule["kind"], "source_channel": request.source_channel,
            "approved_at": request.approved_at.isoformat()}
