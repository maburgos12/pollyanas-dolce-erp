from __future__ import annotations

import hashlib
from datetime import date

from django.db import transaction
from django.utils import timezone

from horarios_especiales.models import HorarioEspecialDetalle, SolicitudHorarioEspecial
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


@transaction.atomic
def approve_request(*, request_obj: SolicitudHorarioEspecial, actor=None):
    errors = validate_request(request_obj=request_obj, actor=actor)
    if errors:
        raise ValueError("La solicitud tiene errores de validación y no puede aprobarse.")
    request_obj.status = SolicitudHorarioEspecial.STATUS_APROBADO
    request_obj.approved_by = actor if getattr(actor, "is_authenticated", False) else None
    request_obj.approved_at = timezone.now()
    request_obj.save(update_fields=["status", "approved_by", "approved_at", "updated_at"])
    log_special_hours_event(request_obj=request_obj, action="APPROVED", actor=actor, payload={})
    return request_obj


@transaction.atomic
def cancel_request(*, request_obj: SolicitudHorarioEspecial, actor=None, reason: str = ""):
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
