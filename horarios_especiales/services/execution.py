from __future__ import annotations

from django.utils import timezone

from horarios_especiales.models import (
    HorarioEspecialDetalle,
    HorarioEspecialIntentoPublicacion,
    SolicitudHorarioEspecial,
    SucursalPlataformaExterna,
)
from horarios_especiales.services.audit import log_special_hours_event
from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher


def _next_attempt_number(detail: HorarioEspecialDetalle, platform: str) -> int:
    latest = (
        detail.publication_attempts.filter(platform=platform)
        .order_by("-attempt_no")
        .values_list("attempt_no", flat=True)
        .first()
    )
    return int(latest or 0) + 1


def execute_request(*, request_obj: SolicitudHorarioEspecial, actor=None) -> dict:
    if request_obj.status != SolicitudHorarioEspecial.STATUS_APROBADO:
        raise ValueError("La solicitud debe estar aprobada antes de ejecutarse.")

    publisher = GoogleBusinessProfilePublisher()
    execution_summary = {"details": [], "success": 0, "failed": 0, "skipped": 0}

    log_special_hours_event(request_obj=request_obj, action="EXECUTION_REQUESTED", actor=actor, payload={})

    for detail in request_obj.details.select_related("sucursal").order_by("target_date", "sucursal__codigo"):
        config = SucursalPlataformaExterna.objects.filter(
            sucursal=detail.sucursal,
            platform=SucursalPlataformaExterna.PLATFORM_GOOGLE,
            is_active=True,
        ).first()
        if config is None:
            detail.execution_status = HorarioEspecialDetalle.EXEC_STATUS_FAILED
            detail.save(update_fields=["execution_status", "updated_at"])
            execution_summary["failed"] += 1
            execution_summary["details"].append(
                {
                    "detail_id": detail.id,
                    "branch_code": detail.sucursal.codigo,
                    "status": "FAILED",
                    "error": "No existe configuración activa de Google Business Profile para la sucursal.",
                }
            )
            log_special_hours_event(
                request_obj=request_obj,
                action="EXECUTION_FAILED",
                actor=actor,
                detail=detail,
                payload={"error": "missing_platform_config"},
            )
            continue

        attempt = HorarioEspecialIntentoPublicacion.objects.create(
            detail=detail,
            platform=config.platform,
            status=HorarioEspecialIntentoPublicacion.STATUS_RUNNING,
            attempt_no=_next_attempt_number(detail, config.platform),
            executed_by=actor if getattr(actor, "is_authenticated", False) else None,
        )
        try:
            result = publisher.publish_detail(detail=detail, config=config)
        except Exception as exc:
            attempt.status = HorarioEspecialIntentoPublicacion.STATUS_FAILED
            attempt.error_message = str(exc)
            attempt.error_payload_json = {"error": str(exc)}
            attempt.finished_at = timezone.now()
            attempt.save(update_fields=["status", "error_message", "error_payload_json", "finished_at"])
            detail.execution_status = HorarioEspecialDetalle.EXEC_STATUS_FAILED
            detail.save(update_fields=["execution_status", "updated_at"])
            execution_summary["failed"] += 1
            execution_summary["details"].append(
                {
                    "detail_id": detail.id,
                    "branch_code": detail.sucursal.codigo,
                    "status": "FAILED",
                    "error": str(exc),
                }
            )
            log_special_hours_event(
                request_obj=request_obj,
                action="EXECUTION_FAILED",
                actor=actor,
                detail=detail,
                payload={"error": str(exc)},
            )
            continue

        attempt.status = (
            HorarioEspecialIntentoPublicacion.STATUS_SKIPPED
            if result.get("noop")
            else HorarioEspecialIntentoPublicacion.STATUS_SUCCESS
        )
        attempt.request_payload_json = result.get("request_payload") or {}
        attempt.response_payload_json = result.get("response_payload") or {}
        attempt.external_operation_id = str(result.get("operation_id") or "")
        attempt.finished_at = timezone.now()
        attempt.save(
            update_fields=[
                "status",
                "request_payload_json",
                "response_payload_json",
                "external_operation_id",
                "finished_at",
            ]
        )

        detail.execution_status = HorarioEspecialDetalle.EXEC_STATUS_SUCCESS
        detail.platform_payload_json = result.get("request_payload") or {}
        detail.published_snapshot_json = result.get("response_payload") or {}
        detail.save(
            update_fields=[
                "execution_status",
                "platform_payload_json",
                "published_snapshot_json",
                "updated_at",
            ]
        )

        config.last_published_at = timezone.now()
        config.save(update_fields=["last_published_at", "updated_at"])

        if result.get("noop"):
            execution_summary["skipped"] += 1
        else:
            execution_summary["success"] += 1
        execution_summary["details"].append(
            {
                "detail_id": detail.id,
                "branch_code": detail.sucursal.codigo,
                "status": "SKIPPED" if result.get("noop") else "SUCCESS",
            }
        )
        log_special_hours_event(
            request_obj=request_obj,
            action="EXECUTION_SUCCESS",
            actor=actor,
            detail=detail,
            payload={"noop": bool(result.get("noop"))},
        )

    request_obj.execution_summary_json = execution_summary
    request_obj.executed_by = actor if getattr(actor, "is_authenticated", False) else None
    request_obj.executed_at = timezone.now()
    request_obj.status = (
        SolicitudHorarioEspecial.STATUS_FALLIDO
        if execution_summary["failed"]
        else SolicitudHorarioEspecial.STATUS_EJECUTADO
    )
    request_obj.save(
        update_fields=["execution_summary_json", "executed_by", "executed_at", "status", "updated_at"]
    )
    return execution_summary
