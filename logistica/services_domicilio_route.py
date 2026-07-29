from __future__ import annotations

from django.core.exceptions import ValidationError
from django.db import transaction

from core.audit import log_event
from logistica.models import ParadaRuta, RutaEntrega, SolicitudDomicilio
from logistica.services_domicilio_status import (
    DomicilioStatusError,
    apply_domicilio_status_transition,
)


class DomicilioRouteSyncError(ValidationError):
    pass


@transaction.atomic
def sync_linked_domicilios_on_route_start(*, ruta: RutaEntrega, actor) -> int:
    solicitudes = list(
        SolicitudDomicilio.objects.select_for_update()
        .filter(parada_ruta__ruta_id=ruta.id)
        .order_by("parada_ruta__orden", "id")
    )
    invalid = [
        solicitud
        for solicitud in solicitudes
        if solicitud.estatus
        not in {
            SolicitudDomicilio.ESTATUS_LISTO,
            SolicitudDomicilio.ESTATUS_EN_RUTA,
        }
    ]
    if invalid:
        folios = ", ".join(
            solicitud.pedido_cliente.point_note_folio
            or str(solicitud.id)
            for solicitud in invalid
        )
        raise DomicilioRouteSyncError(
            f"No se puede iniciar la ruta: los domicilios {folios} todavía no están listos."
        )

    changed = 0
    for solicitud in solicitudes:
        if solicitud.estatus == SolicitudDomicilio.ESTATUS_EN_RUTA:
            continue
        try:
            apply_domicilio_status_transition(
                solicitud=solicitud,
                requested_status=SolicitudDomicilio.ESTATUS_EN_RUTA,
            )
        except DomicilioStatusError as exc:
            raise DomicilioRouteSyncError(str(exc)) from exc
        changed += 1
        log_event(
            actor,
            "STATUS_CHANGE",
            "logistica.SolicitudDomicilio",
            solicitud.id,
            {
                "estatus_anterior": SolicitudDomicilio.ESTATUS_LISTO,
                "estatus_nuevo": SolicitudDomicilio.ESTATUS_EN_RUTA,
                "ruta_id": ruta.id,
                "ruta_folio": ruta.folio,
                "origen": "PWA_LOGISTICA",
            },
        )
    return changed


def sync_linked_domicilio_from_stop_delivery(
    *,
    parada: ParadaRuta,
    entrega_estado: str,
    motivo: str,
    actor,
) -> bool:
    solicitud = (
        SolicitudDomicilio.objects.select_for_update()
        .filter(parada_ruta_id=parada.id)
        .first()
    )
    if solicitud is None:
        return False

    if entrega_estado == ParadaRuta.ENTREGA_ENTREGADA:
        requested_status = SolicitudDomicilio.ESTATUS_ENTREGADO
        transition_kwargs = {}
    else:
        requested_status = SolicitudDomicilio.ESTATUS_INCIDENCIA
        transition_kwargs = {
            "incidencia_motivo": motivo
            or "Incidencia registrada por el repartidor.",
        }

    if solicitud.estatus == requested_status:
        return False
    if solicitud.estatus != SolicitudDomicilio.ESTATUS_EN_RUTA:
        raise DomicilioRouteSyncError(
            "El domicilio ligado a esta parada no está en ruta."
        )
    previous_status = solicitud.estatus
    try:
        apply_domicilio_status_transition(
            solicitud=solicitud,
            requested_status=requested_status,
            **transition_kwargs,
        )
    except DomicilioStatusError as exc:
        raise DomicilioRouteSyncError(str(exc)) from exc
    log_event(
        actor,
        "STATUS_CHANGE",
        "logistica.SolicitudDomicilio",
        solicitud.id,
        {
            "estatus_anterior": previous_status,
            "estatus_nuevo": requested_status,
            "ruta_id": parada.ruta_id,
            "parada_id": parada.id,
            "entrega_estado": entrega_estado,
            "origen": "PWA_LOGISTICA",
        },
    )
    return True
