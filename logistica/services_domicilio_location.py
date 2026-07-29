from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any

from django.db import transaction
from django.http import Http404
from django.utils import timezone

from core.audit import log_event
from logistica.models import (
    SolicitudDomicilio,
    SolicitudDomicilioLocationOperation,
)
from logistica.services_domicilio_assignment import (
    repartidores_disponibles_queryset,
)


@dataclass(frozen=True)
class DomicilioLocationError(Exception):
    detail: str
    status_code: int = 409


def _same_request(
    operation,
    *,
    api_client,
    repartidor_id: int,
    latitud: Decimal,
    longitud: Decimal,
    accuracy_m: Decimal,
    captured_at,
    actor: dict[str, str],
) -> bool:
    return (
        operation.api_client_id == api_client.id
        and operation.repartidor_id == repartidor_id
        and operation.latitud == latitud
        and operation.longitud == longitud
        and operation.accuracy_m == accuracy_m
        and operation.captured_at == captured_at
        and operation.actor_id == actor["id"]
        and operation.actor_nombre == actor["nombre"]
    )


def record_domicilio_location(
    *,
    solicitud_id: int,
    api_client,
    repartidor_id: int,
    operation_id,
    latitud: Decimal,
    longitud: Decimal,
    accuracy_m: Decimal,
    captured_at,
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

        operation = SolicitudDomicilioLocationOperation.objects.filter(
            solicitud=solicitud,
            operation_id=operation_id,
        ).first()
        if operation is not None:
            if not _same_request(
                operation,
                api_client=api_client,
                repartidor_id=repartidor_id,
                latitud=latitud,
                longitud=longitud,
                accuracy_m=accuracy_m,
                captured_at=captured_at,
                actor=actor,
            ):
                raise DomicilioLocationError(
                    "operation_id ya fue usado con otro payload."
                )
            return dict(operation.result_snapshot)

        now = timezone.now()
        if captured_at < now - timedelta(minutes=10):
            raise DomicilioLocationError(
                "La ubicación es demasiado antigua. Actualiza el GPS.", 400
            )
        if captured_at > now + timedelta(minutes=2):
            raise DomicilioLocationError(
                "La hora de la ubicación no es válida.", 400
            )

        if solicitud.repartidor_id != repartidor_id:
            raise DomicilioLocationError(
                "La solicitud ya no está asignada a este repartidor."
            )
        if solicitud.estatus not in {
            SolicitudDomicilio.ESTATUS_LISTO,
            SolicitudDomicilio.ESTATUS_EN_RUTA,
        }:
            raise DomicilioLocationError("La tarea ya no está disponible.")
        allowed = api_client.repartidores_logistica_autorizados.filter(
            pk=repartidor_id
        ).exists()
        available = repartidores_disponibles_queryset().filter(
            pk=repartidor_id
        ).exists()
        if not allowed or not available:
            raise DomicilioLocationError("Repartidor no disponible.")

        snapshot = {
            "id": solicitud.id,
            "repartidor_id": repartidor_id,
            "latitud": float(latitud),
            "longitud": float(longitud),
            "accuracy_m": float(accuracy_m),
            "captured_at": captured_at.isoformat(),
            "idempotent": False,
        }
        SolicitudDomicilioLocationOperation.objects.create(
            solicitud=solicitud,
            operation_id=operation_id,
            api_client=api_client,
            repartidor_id=repartidor_id,
            latitud=latitud,
            longitud=longitud,
            accuracy_m=accuracy_m,
            captured_at=captured_at,
            actor_id=actor["id"],
            actor_nombre=actor["nombre"],
            result_snapshot=snapshot,
        )
        log_event(
            None,
            "DRIVER_LOCATION",
            "logistica.SolicitudDomicilio",
            solicitud.id,
            {
                "operation_id": str(operation_id),
                "repartidor_id": repartidor_id,
                "captured_at": captured_at.isoformat(),
                "source": "driver_app",
            },
        )
        return snapshot
