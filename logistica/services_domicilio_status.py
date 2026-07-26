from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db import transaction
from django.http import Http404
from django.utils import timezone

from core.audit import log_event
from logistica.models import (
    SolicitudDomicilio,
    SolicitudDomicilioStatusOperation,
)
from logistica.services_domicilio_assignment import (
    repartidores_disponibles_queryset,
)


@dataclass(frozen=True)
class DomicilioStatusError(Exception):
    detail: str
    status_code: int = 409


def _same_request(operation, *, api_client, repartidor_id, requested_status, actor):
    return (
        operation.api_client_id == api_client.id
        and operation.repartidor_id == repartidor_id
        and operation.requested_status == requested_status
        and operation.actor_id == actor["id"]
        and operation.actor_nombre == actor["nombre"]
    )


def update_domicilio_status(
    *,
    solicitud_id: int,
    api_client,
    repartidor_id: int,
    requested_status: str,
    operation_id,
    actor: dict[str, str],
) -> dict[str, Any]:
    with transaction.atomic():
        solicitud = (
            SolicitudDomicilio.objects.select_for_update()
            .filter(
                pk=solicitud_id,
                pedido_cliente__public_api_client=api_client,
            )
            .first()
        )
        if solicitud is None:
            raise Http404

        operation = SolicitudDomicilioStatusOperation.objects.filter(
            solicitud=solicitud,
            operation_id=operation_id,
        ).first()
        if operation is not None:
            if not _same_request(
                operation,
                api_client=api_client,
                repartidor_id=repartidor_id,
                requested_status=requested_status,
                actor=actor,
            ):
                raise DomicilioStatusError(
                    "operation_id ya fue usado con otro payload."
                )
            return {**operation.result_snapshot, "idempotent": True}

        if solicitud.repartidor_id != repartidor_id:
            raise DomicilioStatusError(
                "La solicitud ya no está asignada a este repartidor."
            )
        allowed = api_client.repartidores_logistica_autorizados.filter(
            pk=repartidor_id
        ).exists()
        available = repartidores_disponibles_queryset().filter(
            pk=repartidor_id
        ).exists()
        if not allowed or not available:
            raise DomicilioStatusError("Repartidor no disponible.")

        expected = {
            SolicitudDomicilio.ESTATUS_EN_RUTA:
                SolicitudDomicilio.ESTATUS_ASIGNADO,
            SolicitudDomicilio.ESTATUS_ENTREGADO:
                SolicitudDomicilio.ESTATUS_EN_RUTA,
        }[requested_status]
        if solicitud.estatus != expected:
            raise DomicilioStatusError(
                f"Transición inválida desde {solicitud.estatus}."
            )

        previous_status = solicitud.estatus
        solicitud.estatus = requested_status
        solicitud.revision += 1
        update_fields = ["estatus", "revision"]
        if requested_status == SolicitudDomicilio.ESTATUS_ENTREGADO:
            solicitud.entregado_en = timezone.now()
            update_fields.append("entregado_en")
        solicitud.save(update_fields=update_fields)
        snapshot = {
            "id": solicitud.id,
            "repartidor_id": repartidor_id,
            "estatus": solicitud.estatus,
            "revision": solicitud.revision,
        }
        SolicitudDomicilioStatusOperation.objects.create(
            solicitud=solicitud,
            operation_id=operation_id,
            api_client=api_client,
            repartidor_id=repartidor_id,
            requested_status=requested_status,
            final_status=solicitud.estatus,
            actor_id=actor["id"],
            actor_nombre=actor["nombre"],
            result_snapshot=snapshot,
        )
        log_event(
            None,
            "STATUS_CHANGE",
            "logistica.SolicitudDomicilio",
            solicitud.id,
            {
                "estatus_anterior": previous_status,
                "estatus_nuevo": solicitud.estatus,
                "operation_id": str(operation_id),
                "repartidor_id": repartidor_id,
                "api_client": {
                    "id": api_client.id,
                    "nombre": api_client.nombre,
                },
                "actor_externo": actor,
            },
        )
        return {**snapshot, "idempotent": False}
