from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

from django.db import transaction
from django.db.models import Max, Q
from django.shortcuts import get_object_or_404
from django.utils import timezone

from core.audit import log_event
from logistica.models import (
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaEntrega,
    SolicitudDomicilio,
    Unidad,
)
from logistica.services_google_routes import recalcular_ruta_programada
from rrhh.services_identidad import nombre_operativo_usuario


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DomicilioAssignmentError(Exception):
    detail: str
    status_code: int


def repartidores_disponibles_queryset():
    """Perfiles logísticos canónicos habilitados para recibir domicilios."""
    return (
        Repartidor.objects.select_related(
            "user",
            "user__empleado_rrhh",
            "sucursal",
            "unidad_asignada",
        )
        .filter(user__is_active=True)
        .filter(unidad_asignada__isnull=False, unidad_asignada__activa=True)
        .filter(Q(user__empleado_rrhh__isnull=True) | Q(user__empleado_rrhh__activo=True))
        .exclude(tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA)
        .order_by("user__first_name", "user__last_name", "user__username", "id")
    )


def unidades_disponibles_queryset():
    return Unidad.objects.filter(activa=True).select_related("sucursal").order_by("codigo")


def serialize_repartidor_disponible(repartidor: Repartidor) -> dict[str, Any]:
    return {
        "id": repartidor.id,
        "nombre": nombre_operativo_usuario(repartidor.user),
        "telefono": repartidor.telefono,
        "sucursal_id": repartidor.sucursal_id,
        "sucursal": repartidor.sucursal.nombre,
        "unidad_id": repartidor.unidad_asignada_id,
        "unidad": (
            repartidor.unidad_asignada.codigo
            if repartidor.unidad_asignada_id
            else None
        ),
    }


def list_repartidores_disponibles() -> list[dict[str, Any]]:
    return [
        serialize_repartidor_disponible(repartidor)
        for repartidor in repartidores_disponibles_queryset()
    ]


def list_repartidores_disponibles_minimal(*, api_client) -> list[dict[str, Any]]:
    """Catálogo M2M mínimo.

    Solicitud.unidad, PedidoCliente.sucursal_ref y Repartidor.unidad_asignada son
    opcionales y el dominio actual no exige que coincidan. No se infiere una
    compatibilidad nueva que pudiera ocultar repartidores válidos.
    """
    return [
        {
            "id": repartidor.id,
            "nombre": nombre_operativo_usuario(repartidor.user),
            "unidad_id": repartidor.unidad_asignada_id,
            "unidad_codigo": repartidor.unidad_asignada.codigo,
            "unidad_nombre": repartidor.unidad_asignada.descripcion,
        }
        for repartidor in repartidores_disponibles_queryset().filter(
            api_clients_logistica_autorizados=api_client,
        )
    ]


def get_owned_domicilio_or_404(*, solicitud_id: int, api_client):
    return get_object_or_404(
        SolicitudDomicilio.objects.select_related("pedido_cliente"),
        pk=solicitud_id,
        pedido_cliente__public_api_client=api_client,
    )


def _route_payload(solicitud: SolicitudDomicilio) -> dict[str, Any]:
    if not solicitud.parada_ruta_id:
        return {
            "route_linked": False,
            "ruta_id": None,
            "ruta_folio": "",
            "parada_id": None,
        }
    parada = solicitud.parada_ruta
    return {
        "route_linked": True,
        "ruta_id": parada.ruta_id,
        "ruta_folio": parada.ruta.folio,
        "parada_id": parada.id,
    }


def _can_link_route(solicitud: SolicitudDomicilio) -> bool:
    direccion = solicitud.direccion_cliente
    pedido = solicitud.pedido_cliente
    return bool(
        direccion
        and direccion.latitud is not None
        and direccion.longitud is not None
        and pedido
        and pedido.point_note_id
    )


def _route_for_assignment(
    *,
    repartidor: Repartidor,
    unidad: Unidad,
    created_by,
) -> RutaEntrega:
    today = timezone.localdate()
    routes = RutaEntrega.objects.select_for_update().filter(
        repartidor=repartidor,
        unidad_operativa=unidad,
        fecha_ruta=today,
    )
    active = routes.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).first()
    if active:
        return active

    other_active = (
        RutaEntrega.objects.select_for_update()
        .filter(
            repartidor=repartidor,
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        .exclude(unidad_operativa=unidad)
        .first()
    )
    if other_active:
        raise DomicilioAssignmentError(
            "El repartidor ya opera una ruta activa con otra unidad.",
            409,
        )

    planned = routes.filter(estatus=RutaEntrega.ESTATUS_PLANEADA).first()
    if planned:
        return planned
    return RutaEntrega.objects.create(
        nombre=f"Domicilios {today:%d/%m/%Y} · {nombre_operativo_usuario(repartidor.user)}",
        fecha_ruta=today,
        chofer=nombre_operativo_usuario(repartidor.user),
        unidad=unidad.codigo,
        repartidor=repartidor,
        unidad_operativa=unidad,
        created_by=created_by,
        notas="Ruta canónica generada desde Centro Operativo Domicilios.",
    )


def _refresh_programmed_route(route_id: int) -> None:
    try:
        ruta = RutaEntrega.objects.filter(pk=route_id).first()
        if ruta and ruta.estatus in {
            RutaEntrega.ESTATUS_PLANEADA,
            RutaEntrega.ESTATUS_EN_RUTA,
        }:
            recalcular_ruta_programada(ruta)
    except Exception:
        logger.exception("No se pudo recalcular la ruta de domicilios %s.", route_id)


def _link_solicitud_to_route(
    *,
    solicitud: SolicitudDomicilio,
    repartidor: Repartidor,
    unidad: Unidad,
    created_by,
) -> tuple[int | None, int | None]:
    if not _can_link_route(solicitud):
        return None, None

    previous_route_id = (
        solicitud.parada_ruta.ruta_id
        if solicitud.parada_ruta_id
        else None
    )
    if (
        solicitud.parada_ruta_id
        and solicitud.parada_ruta.ruta.repartidor_id == repartidor.id
        and solicitud.parada_ruta.ruta.unidad_operativa_id == unidad.id
        and solicitud.parada_ruta.ruta.estatus
        in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}
    ):
        return previous_route_id, previous_route_id

    if (
        solicitud.parada_ruta_id
        and solicitud.parada_ruta.ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA
    ):
        raise DomicilioAssignmentError(
            "La parada ya pertenece a una ruta iniciada y no puede moverse.",
            409,
        )

    route = _route_for_assignment(
        repartidor=repartidor,
        unidad=unidad,
        created_by=created_by,
    )
    direccion = solicitud.direccion_cliente
    point, _ = PuntoLogistico.objects.select_for_update().get_or_create(
        direccion_cliente=direccion,
        defaults={
            "nombre": f"{solicitud.cliente_nombre} · {direccion.alias or 'Domicilio'}",
            "tipo": PuntoLogistico.TIPO_DOMICILIO,
            "latitud": direccion.latitud,
            "longitud": direccion.longitud,
            "radio_geocerca_metros": 80,
            "notas": direccion.referencias,
        },
    )
    point_updates = []
    canonical_values = {
        "nombre": f"{solicitud.cliente_nombre} · {direccion.alias or 'Domicilio'}",
        "tipo": PuntoLogistico.TIPO_DOMICILIO,
        "latitud": direccion.latitud,
        "longitud": direccion.longitud,
        "notas": direccion.referencias,
        "activo": True,
    }
    for field, value in canonical_values.items():
        if getattr(point, field) != value:
            setattr(point, field, value)
            point_updates.append(field)
    if point_updates:
        point.save(update_fields=[*point_updates, "actualizado_en"])

    sequence = solicitud.route_sequence
    if sequence is None:
        sequence = (
            SolicitudDomicilio.objects.filter(repartidor=repartidor)
            .aggregate(value=Max("route_sequence"))
            .get("value")
            or 0
        ) + 1
        solicitud.route_sequence = sequence

    if solicitud.parada_ruta_id:
        parada = ParadaRuta.objects.select_for_update().get(
            pk=solicitud.parada_ruta_id,
        )
        parada.ruta = route
        parada.punto = point
        parada.orden = sequence
        parada.punto_nombre_snapshot = point.nombre
        parada.latitud_geocerca = point.latitud
        parada.longitud_geocerca = point.longitud
        parada.radio_geocerca_metros = point.radio_geocerca_metros
        parada.save(
            update_fields=[
                "ruta",
                "punto",
                "orden",
                "punto_nombre_snapshot",
                "latitud_geocerca",
                "longitud_geocerca",
                "radio_geocerca_metros",
                "actualizado_en",
            ]
        )
        solicitud.parada_ruta = parada
    else:
        parada = ParadaRuta.objects.create(
            ruta=route,
            punto=point,
            orden=sequence,
            notas=solicitud.instrucciones_entrega or solicitud.notas,
        )
        solicitud.parada_ruta = parada

    solicitud.save(update_fields=["parada_ruta", "route_sequence"])

    if previous_route_id and previous_route_id != route.id:
        previous_route = RutaEntrega.objects.select_for_update().get(
            pk=previous_route_id,
        )
        if (
            previous_route.estatus == RutaEntrega.ESTATUS_PLANEADA
            and not previous_route.paradas.exists()
        ):
            previous_route.estatus = RutaEntrega.ESTATUS_CANCELADA
            previous_route.notas = (
                f"{previous_route.notas}\nCancelada automáticamente al reasignar su última parada."
            ).strip()
            previous_route.save(update_fields=["estatus", "notas", "updated_at"])
        transaction.on_commit(
            lambda route_id=previous_route_id: _refresh_programmed_route(route_id)
        )
    transaction.on_commit(
        lambda route_id=route.id: _refresh_programmed_route(route_id)
    )
    return previous_route_id, route.id


def assign_domicilio(
    *,
    solicitud_id: int,
    repartidor_id: int,
    audit_user=None,
    audit_metadata: dict[str, Any] | None = None,
    owner_api_client=None,
    unidad=None,
) -> dict[str, Any]:
    with transaction.atomic():
        solicitudes = SolicitudDomicilio.objects.select_for_update()
        if owner_api_client is not None:
            solicitudes = solicitudes.filter(
                pedido_cliente__public_api_client=owner_api_client,
            )
        solicitud = get_object_or_404(solicitudes, pk=solicitud_id)
        if (
            owner_api_client is not None
            and not owner_api_client.repartidores_logistica_autorizados.filter(
                pk=repartidor_id,
            ).exists()
        ):
            raise DomicilioAssignmentError("Repartidor no disponible.", 400)
        repartidor_solicitado = (
            Repartidor.objects.select_for_update(of=("self",))
            .select_related("unidad_asignada", "sucursal")
            .filter(pk=repartidor_id)
            .first()
        )
        if repartidor_solicitado is None:
            raise DomicilioAssignmentError("Repartidor no disponible.", 400)
        if unidad is None:
            unidad = repartidor_solicitado.unidad_asignada
            if unidad is None or not unidad.activa:
                raise DomicilioAssignmentError(
                    "El repartidor no tiene una unidad activa asignada.",
                    400,
                )
            if unidad.sucursal_id != repartidor_solicitado.sucursal_id:
                raise DomicilioAssignmentError(
                    "La unidad asignada al repartidor no pertenece a su sucursal.",
                    400,
                )
        if solicitud.estatus in SolicitudDomicilio.TERMINAL_STATUSES:
            if (
                solicitud.repartidor_id == repartidor_id
                and solicitud.unidad_id == unidad.id
            ):
                result = {
                    "id": solicitud.id,
                    "repartidor_id": repartidor_id,
                    "estatus": solicitud.estatus,
                    "revision": solicitud.revision,
                    "sequence": solicitud.route_sequence,
                    "idempotent": True,
                }
                result.update(_route_payload(solicitud))
                return result
            if solicitud.repartidor_id == repartidor_id:
                raise DomicilioAssignmentError(
                    "El domicilio terminal no coincide con la unidad solicitada.",
                    409,
                )
            raise DomicilioAssignmentError(
                "El domicilio ya no admite asignación.",
                409,
            )
        if solicitud.estatus == SolicitudDomicilio.ESTATUS_EN_RUTA:
            raise DomicilioAssignmentError(
                "El domicilio ya no admite asignación.",
                409,
            )
        repartidor = repartidores_disponibles_queryset().filter(
            pk=repartidor_id
        ).first()
        if repartidor is None:
            raise DomicilioAssignmentError("Repartidor no disponible.", 400)
        if unidad is None or not unidades_disponibles_queryset().filter(pk=unidad.pk).exists():
            raise DomicilioAssignmentError("Unidad activa obligatoria.", 400)
        if unidad.sucursal_id != repartidor.sucursal_id:
            raise DomicilioAssignmentError(
                "La unidad y el repartidor deben pertenecer a la misma sucursal.",
                409,
            )
        if (
            solicitud.repartidor_id == repartidor_id
            and solicitud.unidad_id == unidad.id
        ):
            if solicitud.route_sequence is None:
                max_sequence = (
                    SolicitudDomicilio.objects.filter(repartidor_id=repartidor_id)
                    .aggregate(value=Max("route_sequence"))
                    .get("value")
                    or 0
                )
                solicitud.route_sequence = max_sequence + 1
                solicitud.save(update_fields=["route_sequence"])
            _link_solicitud_to_route(
                solicitud=solicitud,
                repartidor=repartidor,
                unidad=unidad,
                created_by=audit_user,
            )
            result = {
                "id": solicitud.id,
                "repartidor_id": repartidor_id,
                "estatus": solicitud.estatus,
                "sequence": solicitud.route_sequence,
                "idempotent": True,
            }
            result.update(_route_payload(solicitud))
            return result
        anterior_id = solicitud.repartidor_id
        unidad_anterior_id = solicitud.unidad_id
        unidad_anterior_codigo = (
            solicitud.unidad.codigo if solicitud.unidad_id else None
        )
        solicitud.repartidor = repartidor
        solicitud.unidad = unidad
        if (
            anterior_id != repartidor_id
            or solicitud.route_sequence is None
        ):
            max_sequence = (
                SolicitudDomicilio.objects.filter(repartidor_id=repartidor_id)
                .aggregate(value=Max("route_sequence"))
                .get("value")
                or 0
            )
            solicitud.route_sequence = max_sequence + 1
        solicitud.asignado_en = timezone.now()
        solicitud.revision += 1
        update_fields = [
            "repartidor",
            "route_sequence",
            "asignado_en",
            "revision",
        ]
        update_fields.append("unidad")
        solicitud.save(update_fields=update_fields)
        ruta_anterior_id, ruta_nueva_id = _link_solicitud_to_route(
            solicitud=solicitud,
            repartidor=repartidor,
            unidad=unidad,
            created_by=audit_user,
        )
        unidad_nueva = unidad if unidad is not None else solicitud.unidad
        payload = dict(audit_metadata or {})
        payload.update({
            "repartidor_anterior_id": anterior_id,
            "repartidor_nuevo_id": repartidor.id,
            "route_sequence": solicitud.route_sequence,
            "unidad_anterior_id": unidad_anterior_id,
            "unidad_anterior_codigo": unidad_anterior_codigo,
            "unidad_nueva_id": unidad_nueva.id if unidad_nueva else None,
            "unidad_nueva_codigo": unidad_nueva.codigo if unidad_nueva else None,
            "ruta_anterior_id": ruta_anterior_id,
            "ruta_nueva_id": ruta_nueva_id,
            "parada_ruta_id": solicitud.parada_ruta_id,
        })
        log_event(
            audit_user,
            "ASSIGN",
            "logistica.SolicitudDomicilio",
            solicitud.id,
            payload,
        )
        result = {
            "id": solicitud.id,
            "repartidor_id": repartidor.id,
            "estatus": solicitud.estatus,
            "revision": solicitud.revision,
            "sequence": solicitud.route_sequence,
            "idempotent": False,
        }
        result.update(_route_payload(solicitud))
        return result
