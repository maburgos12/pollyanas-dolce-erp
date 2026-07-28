from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django.utils import timezone

from core.audit import log_event
from logistica.models import Repartidor, SolicitudDomicilio, Unidad
from rrhh.services_identidad import nombre_operativo_usuario


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
            Repartidor.objects.select_related("unidad_asignada", "sucursal")
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
                return {
                    "id": solicitud.id,
                    "repartidor_id": repartidor_id,
                    "estatus": solicitud.estatus,
                    "revision": solicitud.revision,
                    "idempotent": True,
                }
            if solicitud.repartidor_id == repartidor_id:
                raise DomicilioAssignmentError(
                    "El domicilio terminal no coincide con la unidad solicitada.",
                    409,
                )
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
            return {
                "id": solicitud.id,
                "repartidor_id": repartidor_id,
                "estatus": solicitud.estatus,
                "idempotent": True,
            }
        anterior_id = solicitud.repartidor_id
        unidad_anterior_id = solicitud.unidad_id
        unidad_anterior_codigo = (
            solicitud.unidad.codigo if solicitud.unidad_id else None
        )
        solicitud.repartidor = repartidor
        solicitud.unidad = unidad
        solicitud.asignado_en = timezone.now()
        solicitud.revision += 1
        update_fields = ["repartidor", "asignado_en", "revision"]
        update_fields.append("unidad")
        solicitud.save(update_fields=update_fields)
        unidad_nueva = unidad if unidad is not None else solicitud.unidad
        payload = dict(audit_metadata or {})
        payload.update({
            "repartidor_anterior_id": anterior_id,
            "repartidor_nuevo_id": repartidor.id,
            "unidad_anterior_id": unidad_anterior_id,
            "unidad_anterior_codigo": unidad_anterior_codigo,
            "unidad_nueva_id": unidad_nueva.id if unidad_nueva else None,
            "unidad_nueva_codigo": unidad_nueva.codigo if unidad_nueva else None,
        })
        log_event(
            audit_user,
            "ASSIGN",
            "logistica.SolicitudDomicilio",
            solicitud.id,
            payload,
        )
        return {
            "id": solicitud.id,
            "repartidor_id": repartidor.id,
            "estatus": solicitud.estatus,
            "revision": solicitud.revision,
            "idempotent": False,
        }
