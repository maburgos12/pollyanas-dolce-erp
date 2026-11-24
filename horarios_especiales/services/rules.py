"""Versioned, explicit ERP schedule rules; publication is a separate outcome."""

import hashlib
import json
import re
from datetime import date


PRECEDENCE = {"weekly": 1, "range": 2, "date": 3}


def rule_revision(payload):
    content = {key: payload[key] for key in ("schema_version", "locations", "rules", "reason", "source_channel")}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def validate_rules(rules, *, branch_ids=None):
    if not isinstance(rules, list) or not 1 <= len(rules) <= 31:
        raise ValueError("Se requieren entre 1 y 31 reglas.")
    scoped = any(isinstance(rule, dict) and 'branch_ids' in rule for rule in rules)
    covered = set()
    common_period = None
    for rule in rules:
        if not isinstance(rule, dict) or set(rule) - {"kind", "start_date", "end_date", "weekdays", "closed_all_day", "time_windows", "branch_ids", "excluded_dates"}:
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
        if scoped:
            ids = rule.get('branch_ids')
            if (not isinstance(ids, list) or not ids
                    or any(type(pk) is not int or pk <= 0 for pk in ids)
                    or len(set(ids)) != len(ids) or covered.intersection(ids)):
                raise ValueError('Los grupos requieren sucursales únicas, sin cruces.')
            covered.update(ids)
            if kind != 'range' or (date.fromisoformat(end) - date.fromisoformat(start)).days >= 31:
                raise ValueError('Cada grupo requiere un periodo de hasta 31 días.')
            if common_period is None:
                common_period = (start, end)
            elif common_period != (start, end):
                raise ValueError('Los grupos deben compartir el mismo periodo.')
        if 'excluded_dates' in rule:
            excluded = rule['excluded_dates']
            duration = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1 if scoped else 0
            if (not scoped or not isinstance(excluded, list) or len(excluded) >= duration
                    or any(not isinstance(day, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day)
                           or not start <= day <= end for day in excluded)
                    or len(set(excluded)) != len(excluded)):
                raise ValueError('Las fechas conservadas deben estar dentro del periodo y dejar días aplicables.')
            for day in excluded:
                date.fromisoformat(day)
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
    if scoped and branch_ids is not None and covered != set(branch_ids):
        raise ValueError('Los grupos deben cubrir exactamente las sucursales de la solicitud.')
    return rules


def validate_rule_payload(payload):
    try:
        locations = payload.get("locations")
        if (not isinstance(locations, list) or not 1 <= len(locations) <= 10
                or any(not isinstance(row, dict) or type(row.get("branch_id")) is not int for row in locations)
                or len({row["branch_id"] for row in locations}) != len(locations)):
            raise ValueError("Se requieren sucursales únicas identificadas por su ID ERP.")
        validate_rules(payload.get("rules"), branch_ids=[row["branch_id"] for row in locations])
        if payload.get("revision") != rule_revision(payload):
            raise ValueError("La revisión no coincide con el contenido de la vista previa.")
    except (ValueError, TypeError, KeyError) as exc:
        return [str(exc)]
    return []


def indexed_branch_rules(payload, branch_id):
    return [(index, rule) for index, rule in enumerate(payload["rules"])
            if "branch_ids" not in rule or branch_id in rule["branch_ids"]]


def rule_applies(rule, target_date):
    target = target_date.isoformat()
    if target < rule["start_date"] or (rule.get("end_date") and target > rule["end_date"]) or target in rule.get("excluded_dates", ()):
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
    relevant = 0
    # Bound scanning separately from applicable requests so expired history cannot fill the active budget.
    for scanned, request in enumerate(requests.iterator(chunk_size=100), start=1):
        if scanned > 10000:
            return {"status": "UNKNOWN", "reason": "SPECIAL_HOURS_CONFLICT_OR_INVALID"}
        payload = request.canonical_payload
        if validate_rule_payload(payload) or not request.audit_entries.filter(
            action="APPROVED", payload_json__revision=payload.get("revision"),
        ).exists():
            return {"status": "UNKNOWN", "reason": "SPECIAL_HOURS_CONFLICT_OR_INVALID"}
        applicable = [(index, rule) for index, rule in indexed_branch_rules(payload, branch.id) if rule_applies(rule, target_date)]
        relevant += bool(applicable)
        if relevant > 100:
            return {"status": "UNKNOWN", "reason": "SPECIAL_HOURS_CONFLICT_OR_INVALID"}
        for index, rule in applicable:
            candidates.append(((PRECEDENCE[rule["kind"]], request.approved_at, request.id, index), request, rule))
    if not candidates:
        return None
    _, request, rule = max(candidates, key=lambda candidate: candidate[0])
    return {"status": "VERIFIED", "source": "ERP_APPROVED_SPECIAL_HOURS", "reason": None,
            "closed_all_day": rule["closed_all_day"], "windows": sorted(rule["time_windows"], key=lambda w: w["open"]),
            "revision": request.canonical_payload["revision"], "request_id": request.id,
            "rule_kind": rule["kind"], "source_channel": request.source_channel,
            "approved_at": request.approved_at.isoformat()}
