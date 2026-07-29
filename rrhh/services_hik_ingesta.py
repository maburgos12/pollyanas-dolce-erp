from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from django.db import IntegrityError, transaction
from django.db.models import F, Q
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .models import AsistenciaEmpleado, Empleado, EmpleadoIdentidadPendiente, EventoHikCloud
from .services import generar_horas_extra_automatico
from .services_asistencia_reglas import evaluar_dia_empleado
from .services_bonos_checador import programar_sincronizacion_bonos_desde_checador
from .services_hikvision import MarcaHik, _aplicar_marcajes, _detectar_turno, _marcas_existentes, _resolver_sucursal
from .services_identidad import buscar_empleado_por_codigo, registrar_identidad_pendiente


CONTRACT_VERSION = 2
SOURCE_HIKCONNECT_CLOUD = "hikconnect_cloud"
ALLOWED_SOURCES = {SOURCE_HIKCONNECT_CLOUD}
ALLOWED_KINDS = {"check_in", "check_out"}
MAX_BATCH_SIZE = 100


def _canonical_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _result(
    event_id: str,
    outcome: str,
    *,
    retryable: bool,
    reason_code: str = "",
    receipt_id: int | None = None,
    projection: str = "none",
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "outcome": outcome,
        "reason_code": reason_code,
        "retryable": retryable,
        "receipt_id": receipt_id,
        "projection": projection,
    }


def _validate_event(event: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not isinstance(event, dict):
        return None, _result("", "rejected", retryable=False, reason_code="invalid_event")

    event_id = str(event.get("event_id") or "").strip()
    if not event_id:
        return None, _result("", "rejected", retryable=False, reason_code="missing_event_id")
    if len(event_id) > 128:
        return None, _result(event_id[:128], "rejected", retryable=False, reason_code="event_id_too_long")

    source = str(event.get("source") or "").strip()
    if source not in ALLOWED_SOURCES:
        return None, _result(
            event_id,
            "rejected",
            retryable=False,
            reason_code="invalid_source",
        )

    employee_external_id = str(event.get("employee_external_id") or "").strip()
    if not employee_external_id:
        return None, _result(
            event_id,
            "rejected",
            retryable=False,
            reason_code="missing_employee_external_id",
        )
    if len(employee_external_id) > 80:
        return None, _result(
            event_id,
            "rejected",
            retryable=False,
            reason_code="employee_external_id_too_long",
        )

    occurred_at_raw = str(event.get("occurred_at") or "").strip()
    occurred_at = parse_datetime(occurred_at_raw)
    if occurred_at is None or timezone.is_naive(occurred_at):
        return None, _result(
            event_id,
            "rejected",
            retryable=False,
            reason_code="invalid_occurred_at",
        )
    now = timezone.now()
    if occurred_at < now - timedelta(days=400) or occurred_at > now + timedelta(days=1):
        return None, _result(
            event_id,
            "rejected",
            retryable=False,
            reason_code="occurred_at_out_of_range",
        )

    kind = str(event.get("kind") or "").strip()
    if kind not in ALLOWED_KINDS:
        return None, _result(
            event_id,
            "rejected",
            retryable=False,
            reason_code="invalid_kind",
        )

    device_id = str(event.get("device_id") or "").strip()
    if len(device_id) > 128:
        return None, _result(event_id, "rejected", retryable=False, reason_code="device_id_too_long")

    normalized = {
        "event_id": event_id,
        "source": source,
        "employee_external_id": employee_external_id,
        "occurred_at": occurred_at.isoformat(),
        "kind": kind,
        "device_id": device_id,
    }
    return normalized, None


def _duplicate_result(receipt: EventoHikCloud, incoming_hash: str) -> dict[str, Any]:
    if receipt.payload_hash != incoming_hash:
        EventoHikCloud.objects.filter(pk=receipt.pk).update(
            intentos=F("intentos") + 1,
            conflict_count=F("conflict_count") + 1,
            last_conflict_hash=incoming_hash,
            ultimo_error="event_id_payload_mismatch",
        )
        return _result(
            receipt.event_id,
            "payload_conflict",
            retryable=False,
            reason_code="event_id_payload_mismatch",
            receipt_id=receipt.id,
        )
    if receipt.estado == EventoHikCloud.ESTADO_DIFERIDO:
        outcome = "deferred"
        retryable = True
    elif receipt.projection_status != "applied":
        outcome = "deferred"
        retryable = True
    else:
        outcome = "duplicate"
        retryable = False
    return _result(
        receipt.event_id,
        outcome,
        retryable=retryable,
        reason_code=receipt.reason_code or ("projection_incomplete" if retryable else ""),
        receipt_id=receipt.id,
        projection=receipt.projection_status,
    )


def _run_post_projection_effects(receipt_id: int) -> None:
    try:
        with transaction.atomic():
            receipt = (
                EventoHikCloud.objects.select_for_update()
                .get(pk=receipt_id)
            )
            if (
                not receipt.empleado_id
                or receipt.effects_version >= receipt.projection_version
            ):
                return
            local_dt = timezone.localtime(receipt.ocurrido_en)
            asistencia = AsistenciaEmpleado.objects.filter(
                empleado_id=receipt.empleado_id,
                fecha=local_dt.date(),
            ).first()
            if asistencia and asistencia.salida and asistencia.turno_id:
                generar_horas_extra_automatico(asistencia)
            evaluar_dia_empleado(receipt.empleado, local_dt.date())
            programar_sincronizacion_bonos_desde_checador(
                receipt.empleado_id,
                local_dt.date(),
            )
            receipt.effects_status = "completed"
            receipt.effects_version = receipt.projection_version
            receipt.ultimo_error = ""
            receipt.save(
                update_fields=[
                    "effects_status",
                    "effects_version",
                    "ultimo_error",
                    "actualizado_en",
                ]
            )
    except Exception as exc:
        EventoHikCloud.objects.filter(pk=receipt_id).update(
            effects_status="failed",
            ultimo_error=str(exc)[:2000],
        )


def project_receipt(receipt_id: int, *, empleado_id: int | None = None) -> EventoHikCloud:
    with transaction.atomic():
        receipt = EventoHikCloud.objects.select_for_update().get(pk=receipt_id)
        if receipt.projection_status == "applied":
            return receipt

        if empleado_id:
            receipt.empleado_id = empleado_id
        if not receipt.empleado_id:
            empleado = buscar_empleado_por_codigo(receipt.codigo_externo)
            if not empleado:
                return receipt
            receipt.empleado = empleado

        # El lock de la persona serializa incluso cuando todavía no existe
        # AsistenciaEmpleado para ese día.
        empleado = Empleado.objects.select_for_update().get(pk=receipt.empleado_id)
        local_dt = timezone.localtime(receipt.ocurrido_en)
        fecha = local_dt.date()
        asistencia, created = AsistenciaEmpleado.objects.get_or_create(
            empleado=empleado,
            fecha=fecha,
            defaults={
                "fuente": AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
                "sucursal": _resolver_sucursal(empleado),
            },
        )
        fuente_anterior = asistencia.fuente
        inicio_dia = timezone.make_aware(datetime.combine(fecha, datetime.min.time()))
        fin_dia = inicio_dia + timedelta(days=1)
        receipts = list(
            EventoHikCloud.objects.filter(
                Q(empleado=empleado) | Q(pk=receipt.pk),
                ocurrido_en__gte=inicio_dia,
                ocurrido_en__lt=fin_dia,
                tipo_evento__in=ALLOWED_KINDS,
            )
            .exclude(estado=EventoHikCloud.ESTADO_RECHAZADO)
            .order_by("ocurrido_en", "event_id")
        )
        ledger_times = {item.ocurrido_en for item in receipts}
        marcas_previas = [
            marca for marca in _marcas_existentes(asistencia)
            if marca.dt not in ledger_times
        ]
        marcas_ledger = [
            MarcaHik(
                dt=item.ocurrido_en,
                status="checkOut" if item.tipo_evento == "check_out" else "checkIn",
                serial_no=item.event_id,
            )
            for item in receipts
        ]
        asistencia.entrada = None
        asistencia.salida_comida = None
        asistencia.regreso_comida = None
        asistencia.salida = None
        _aplicar_marcajes(
            asistencia,
            [*marcas_previas, *marcas_ledger],
            filtrar_cercanas=False,
        )
        todas_las_marcas = [*marcas_previas, *marcas_ledger]
        if len(todas_las_marcas) == 1 and todas_las_marcas[0].status == "checkOut":
            asistencia.entrada = None
            asistencia.salida = todas_las_marcas[0].dt
            asistencia.minutos_trabajados = 0
        if not created and fuente_anterior != AsistenciaEmpleado.FUENTE_HIKCONNECT_API:
            asistencia.fuente = fuente_anterior
        if not asistencia.turno_id and asistencia.entrada:
            asistencia.turno = _detectar_turno(timezone.localtime(asistencia.entrada).time())
        asistencia.save()

        receipt.empleado = empleado
        receipt.estado = EventoHikCloud.ESTADO_ACEPTADO
        receipt.reason_code = ""
        receipt.retryable = False
        receipt.projection_status = "applied"
        receipt.effects_status = "pending"
        receipt.procesado_en = timezone.now()
        receipt.save(
            update_fields=[
                "empleado",
                "estado",
                "reason_code",
                "retryable",
                "projection_status",
                "effects_status",
                "procesado_en",
                "actualizado_en",
            ]
        )
        return receipt


def replay_deferred_identity(codigo_externo: str, empleado_id: int) -> int:
    receipt_ids = list(
        EventoHikCloud.objects.filter(
            codigo_externo__iexact=codigo_externo,
            estado=EventoHikCloud.ESTADO_DIFERIDO,
        ).values_list("id", flat=True)
    )
    for receipt_id in receipt_ids:
        try:
            receipt = project_receipt(receipt_id, empleado_id=empleado_id)
            if receipt.projection_status == "applied":
                _run_post_projection_effects(receipt_id)
        except Exception as exc:
            EventoHikCloud.objects.filter(pk=receipt_id).update(
                ultimo_error=str(exc)[:2000],
                intentos=F("intentos") + 1,
            )
    return len(receipt_ids)


def ingest_event(event: Any) -> dict[str, Any]:
    normalized, validation_error = _validate_event(event)
    if validation_error:
        return validation_error
    assert normalized is not None

    raw_payload = dict(event)
    payload_hash = _canonical_hash(raw_payload)
    source = normalized["source"]
    event_id = normalized["event_id"]

    with transaction.atomic():
        receipt = (
            EventoHikCloud.objects.select_for_update()
            .filter(fuente=source, event_id=event_id)
            .first()
        )
        if receipt:
            if receipt.payload_hash != payload_hash:
                return _duplicate_result(receipt, payload_hash)
            EventoHikCloud.objects.filter(pk=receipt.pk).update(intentos=F("intentos") + 1)
            if receipt.projection_status == "applied":
                return _duplicate_result(receipt, payload_hash)

        if not receipt:
            try:
                with transaction.atomic():
                    receipt = EventoHikCloud.objects.create(
                        fuente=source,
                        event_id=event_id,
                        payload_hash=payload_hash,
                        payload=raw_payload,
                        codigo_externo=normalized["employee_external_id"],
                        ocurrido_en=parse_datetime(normalized["occurred_at"]),
                        tipo_evento=normalized["kind"],
                        device_id=normalized["device_id"],
                    )
            except IntegrityError:
                receipt = EventoHikCloud.objects.get(fuente=source, event_id=event_id)
                return _duplicate_result(receipt, payload_hash)

        empleado = buscar_empleado_por_codigo(normalized["employee_external_id"])
        if not empleado:
            registrar_identidad_pendiente(
                fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
                codigo_externo=normalized["employee_external_id"],
                nombre_externo="",
                notas=f"Evento Hik pendiente: {event_id}",
            )
            if receipt.estado != EventoHikCloud.ESTADO_DIFERIDO:
                receipt.estado = EventoHikCloud.ESTADO_DIFERIDO
            receipt.reason_code = "identity_unresolved"
            receipt.retryable = True
            receipt.projection_status = "deferred"
            receipt.effects_status = "deferred"
            receipt.procesado_en = timezone.now()
            receipt.save(
                update_fields=[
                    "estado",
                    "reason_code",
                    "retryable",
                    "projection_status",
                    "effects_status",
                    "procesado_en",
                    "actualizado_en",
                ]
            )
            return _result(
                event_id,
                "deferred",
                retryable=True,
                reason_code="identity_unresolved",
                receipt_id=receipt.id,
                projection="deferred",
            )

        receipt_id = receipt.id

    projected = project_receipt(receipt_id, empleado_id=empleado.id)
    _run_post_projection_effects(receipt_id)
    return _result(
        event_id,
        "accepted",
        retryable=False,
        receipt_id=receipt_id,
        projection=projected.projection_status,
    )


def ingest_batch(events: list[Any]) -> list[dict[str, Any]]:
    return [ingest_event(event) for event in events]
