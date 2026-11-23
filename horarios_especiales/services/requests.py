from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import date

from django.db import connection, transaction
from django.utils import timezone

from horarios_especiales.models import HorarioEspecialDetalle, HorarioEspecialIntentoPublicacion, SolicitudHorarioEspecial
from horarios_especiales.services.audit import log_special_hours_event
from horarios_especiales.services.command_parser import build_preview_from_command
from horarios_especiales.services.validation import validate_canonical_payload


def _build_idempotency_key(payload: dict) -> str:
    serialized = str(
        {
            "effective_date": payload.get("effective_date"),
            "closed_all_day": payload.get("closed_all_day"),
            "time_windows": payload.get("time_windows"),
            "location_ids": sorted(item.get("branch_id") for item in payload.get("locations", []) if item.get("branch_id")),
        }
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@transaction.atomic
def create_request_from_text(*, raw_text: str, actor=None, reason: str = "", source_channel: str = SolicitudHorarioEspecial.SOURCE_WEB):
    preview = build_preview_from_command(raw_text)
    payload = dict(preview.canonical_payload)
    payload["source_channel"] = source_channel
    payload["reason"] = reason.strip()
    payload["validation_errors"] = validate_canonical_payload(payload)

    request_obj = SolicitudHorarioEspecial.objects.create(
        raw_command=raw_text.strip(),
        source_channel=source_channel,
        reason=reason.strip(),
        canonical_payload=payload,
        status=SolicitudHorarioEspecial.STATUS_BORRADOR,
        idempotency_key=_build_idempotency_key(payload),
        requested_by=actor if getattr(actor, "is_authenticated", False) else None,
    )

    for location in payload.get("locations", []):
        HorarioEspecialDetalle.objects.create(
            request=request_obj,
            sucursal_id=location["branch_id"],
            target_date=date.fromisoformat(payload["effective_date"]),
            closed_all_day=bool(payload.get("closed_all_day")),
            time_windows_json=list(payload.get("time_windows") or []),
            validation_errors_json=list(payload.get("validation_errors") or []),
            platform_payload_json={},
        )

    log_special_hours_event(
        request_obj=request_obj,
        action="CREATED",
        actor=actor,
        payload={"source_channel": source_channel, "reason": reason.strip()},
    )
    return request_obj, payload


@transaction.atomic
def validate_request(*, request_obj: SolicitudHorarioEspecial, actor=None):
    SolicitudHorarioEspecial.objects.select_for_update().get(pk=request_obj.pk)
    request_obj.refresh_from_db()
    if request_obj.canonical_payload.get("schema_version") == 2 and request_obj.status not in {"BORRADOR", "VALIDADO"}:
        raise ValueError("La revisión ya fue aprobada o cancelada; crea una nueva solicitud.")
    payload = dict(request_obj.canonical_payload or {})
    errors = validate_canonical_payload(payload)
    payload["validation_errors"] = errors
    request_obj.canonical_payload = payload
    request_obj.status = SolicitudHorarioEspecial.STATUS_VALIDADO if not errors else SolicitudHorarioEspecial.STATUS_BORRADOR
    request_obj.save(update_fields=["canonical_payload", "status", "updated_at"])
    request_obj.details.update(validation_errors_json=errors)
    log_special_hours_event(
        request_obj=request_obj,
        action="VALIDATED",
        actor=actor,
        payload={"errors": errors, "status": request_obj.status},
    )
    return errors


def _lock_request_branches(request_obj):
    branch_ids = {row["branch_id"] for row in request_obj.canonical_payload.get("locations", [])}
    branch_ids.update(request_obj.details.values_list("sucursal_id", flat=True))
    lock_branch_ids(branch_ids)


def lock_branch_ids(branch_ids):
    for branch_id in sorted(branch_ids):
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_xact_lock(%s, %s)", [72419, branch_id])
            if not cursor.fetchone()[0]:
                raise ValueError("Hay una publicación en curso; reintenta al terminar.")


@transaction.atomic
def approve_request(*, request_obj: SolicitudHorarioEspecial, actor=None, expected_revision=None):
    SolicitudHorarioEspecial.objects.select_for_update().get(pk=request_obj.pk)
    request_obj.refresh_from_db()
    _lock_request_branches(request_obj)
    if request_obj.canonical_payload.get("schema_version") == 2:
        if not expected_revision or expected_revision != request_obj.canonical_payload.get("revision"):
            raise ValueError("Debes aprobar la revisión exacta de la vista previa.")
        if request_obj.status not in {"BORRADOR", "VALIDADO"} or request_obj.cancelled_at:
            raise ValueError("La revisión ya fue aprobada o cancelada.")
    errors = validate_request(request_obj=request_obj, actor=actor)
    if errors:
        raise ValueError("La solicitud tiene errores de validación y no puede aprobarse.")
    request_obj.status = SolicitudHorarioEspecial.STATUS_APROBADO
    request_obj.approved_by = actor if getattr(actor, "is_authenticated", False) else None
    request_obj.approved_at = timezone.now()
    request_obj.save(update_fields=["status", "approved_by", "approved_at", "updated_at"])
    log_special_hours_event(request_obj=request_obj, action="APPROVED", actor=actor, payload={"revision": request_obj.canonical_payload.get("revision")})
    return request_obj


@transaction.atomic
def cancel_request(*, request_obj: SolicitudHorarioEspecial, actor=None, reason: str = ""):
    SolicitudHorarioEspecial.objects.select_for_update().get(pk=request_obj.pk)
    request_obj.refresh_from_db()
    _lock_request_branches(request_obj)
    covered_elsewhere = False
    if request_obj.canonical_payload.get("schema_version") == 2:
        covered_elsewhere = HorarioEspecialIntentoPublicacion.objects.filter(
            detail__sucursal_id__in=[row["branch_id"] for row in request_obj.canonical_payload["locations"]],
            response_payload_json__mutation_started=True,
            error_payload_json__covered_revisions__contains=[{
                "request_id": request_obj.pk, "revision": request_obj.canonical_payload["revision"],
            }],
        ).exists()
    legacy_published = request_obj.canonical_payload.get("schema_version") != 2 and request_obj.details.filter(
        publication_attempts__response_payload_json__mutation_started=True,
    ).exists()
    if covered_elsewhere or legacy_published:
        raise ValueError("Ya se envió a Google; la reversión requiere una nueva solicitud aprobada.")
    request_obj.status = SolicitudHorarioEspecial.STATUS_CANCELADO
    request_obj.cancelled_at = timezone.now()
    request_obj.save(update_fields=["status", "cancelled_at", "updated_at"])
    request_obj.details.update(execution_status=HorarioEspecialDetalle.EXEC_STATUS_CANCELLED)
    log_special_hours_event(
        request_obj=request_obj,
        action="CANCELLED",
        actor=actor,
        payload={"reason": reason.strip()},
    )
    return request_obj


@transaction.atomic
def create_request_from_rules(*, raw_text, branch_ids, rules, actor=None, reason="", source_channel=SolicitudHorarioEspecial.SOURCE_API):
    from core.models import sucursales_operativas
    from horarios_especiales.services.rules import rule_revision, validate_rules

    if (not isinstance(branch_ids, list) or not 1 <= len(branch_ids) <= 10
            or any(type(value) is not int or value <= 0 for value in branch_ids)
            or len(set(branch_ids)) != len(branch_ids)):
        raise ValueError("Se requieren entre 1 y 10 IDs de sucursal únicos.")
    if (not isinstance(raw_text, str) or not raw_text.strip() or len(raw_text) > 10000
            or not isinstance(reason, str) or len(reason) > 255
            or source_channel not in dict(SolicitudHorarioEspecial.SOURCE_CHOICES)):
        raise ValueError("Texto, motivo o canal inválido.")
    rules = deepcopy(validate_rules(rules, branch_ids=branch_ids))
    for rule in rules:
        targets = rule.get("branch_ids", branch_ids)
        eligible = set(sucursales_operativas(reference_date=date.fromisoformat(rule["start_date"]))
                       .filter(pk__in=targets).values_list("pk", flat=True))
        if eligible != set(targets):
            raise ValueError("La solicitud contiene una sucursal no operativa o inexistente.")
    branches = sucursales_operativas(reference_date=date.fromisoformat(rules[0]["start_date"]))
    locations = [{"branch_id": b.pk, "branch_code": b.codigo, "branch_name": b.nombre}
                 for b in branches.filter(pk__in=branch_ids).order_by("pk")]
    payload = {"schema_version": 2, "locations": locations, "rules": rules,
               "source_channel": source_channel, "reason": reason.strip()}
    payload["revision"] = rule_revision(payload)
    payload["validation_errors"] = []
    obj = SolicitudHorarioEspecial.objects.create(
        raw_command=raw_text.strip(), source_channel=source_channel, reason=reason.strip(),
        canonical_payload=payload, idempotency_key=payload["revision"],
        requested_by=actor if getattr(actor, "is_authenticated", False) else None,
    )
    log_special_hours_event(request_obj=obj, action="CREATED", actor=actor,
                           payload={"revision": payload["revision"], "source_channel": source_channel})
    return obj, payload
