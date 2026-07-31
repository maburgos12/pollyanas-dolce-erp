from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Iterable
from uuid import UUID

from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from core.models import Sucursal, sucursales_operativas_q
from logistica.models import PuntoLogistico
from logistica.services_rutas_control import distancia_metros

from .checklist import CHECKLIST_BASE
from .models import ChecklistVisita, VisitaSucursal


class AuditoriaVisitaError(ValueError):
    pass


def sucursal_es_visitable(sucursal: Sucursal | None) -> bool:
    if not sucursal:
        return False
    return (
        Sucursal.objects.filter(sucursales_operativas_q(), pk=sucursal.pk)
        .exclude(codigo__iexact="CEDIS")
        .exclude(nombre__iexact="CEDIS")
        .exists()
    )


def crear_checklist_base(visita: VisitaSucursal) -> None:
    ChecklistVisita.objects.bulk_create(
        ChecklistVisita(visita=visita, categoria=categoria, titulo=titulo, orden=index)
        for index, (_key, categoria, titulo) in enumerate(CHECKLIST_BASE, start=1)
    )


def programaciones_pendientes(sucursal: Sucursal):
    return VisitaSucursal.objects.filter(
        sucursal=sucursal,
        estatus=VisitaSucursal.ESTATUS_PROGRAMADA,
        fecha_programada__isnull=False,
    ).exclude(tipo=VisitaSucursal.TIPO_EXTRAORDINARIA).order_by("fecha_programada", "id")


@transaction.atomic
def crear_borrador_extraordinario(
    *,
    user,
    sucursal: Sucursal,
    motivo: str,
    detalle: str,
    clave_idempotencia: UUID,
) -> VisitaSucursal:
    if not sucursal_es_visitable(sucursal):
        raise AuditoriaVisitaError("La sucursal seleccionada no está disponible para auditoría.")

    existente = VisitaSucursal.objects.filter(clave_idempotencia=clave_idempotencia).first()
    if existente:
        if existente.sucursal_id != sucursal.pk or existente.creado_por_id != user.pk:
            raise AuditoriaVisitaError(
                "La clave de reintento no corresponde a esta sucursal y auditor."
            )
        return existente

    visita = VisitaSucursal(
        sucursal=sucursal,
        tipo=VisitaSucursal.TIPO_EXTRAORDINARIA,
        estatus=VisitaSucursal.ESTATUS_BORRADOR,
        fecha_programada=None,
        motivo_extraordinaria=motivo,
        detalle_extraordinaria=detalle.strip(),
        clave_idempotencia=clave_idempotencia,
        creado_por=user,
    )
    visita.full_clean()
    visita.save()
    crear_checklist_base(visita)
    log_event(
        user,
        "visita_sucursal_extraordinaria_iniciada",
        "VisitaSucursal",
        str(visita.pk),
        {"sucursal_id": sucursal.pk, "motivo": motivo},
    )
    return visita


def _punto_logistico_sucursal(sucursal: Sucursal) -> PuntoLogistico | None:
    return (
        PuntoLogistico.objects.filter(
            sucursal=sucursal,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            activo=True,
        )
        .order_by("id")
        .first()
    )


def _validar_gps(
    *,
    sucursal: Sucursal,
    latitud: Decimal | None,
    longitud: Decimal | None,
    precision_m: Decimal | None,
) -> tuple[PuntoLogistico, int]:
    if latitud is None or longitud is None:
        raise AuditoriaVisitaError("Activa la ubicación para ejecutar la auditoría.")
    if not (Decimal("-90") <= latitud <= Decimal("90")):
        raise AuditoriaVisitaError("La latitud reportada no es válida.")
    if not (Decimal("-180") <= longitud <= Decimal("180")):
        raise AuditoriaVisitaError("La longitud reportada no es válida.")
    if precision_m is None or precision_m <= 0 or precision_m > 100:
        raise AuditoriaVisitaError("La precisión del GPS debe ser de 100 metros o menos.")

    punto = _punto_logistico_sucursal(sucursal)
    if not punto:
        raise AuditoriaVisitaError("La sucursal no tiene una geocerca activa configurada.")
    distancia = distancia_metros(latitud, longitud, punto.latitud, punto.longitud)
    if distancia > punto.radio_geocerca_metros:
        raise AuditoriaVisitaError(
            f"La ubicación está fuera de la geocerca de {sucursal.nombre}: {distancia} m."
        )
    return punto, distancia


@transaction.atomic
def ejecutar_auditoria(
    *,
    visita_id: int,
    sucursal: Sucursal,
    user,
    latitud: Decimal | None,
    longitud: Decimal | None,
    precision_m: Decimal | None,
    respuestas: dict[int, tuple[str, str]],
    observaciones: str = "",
    personal_ids: Iterable[int] = (),
    fecha_real: date | None = None,
    realizada_en: datetime | None = None,
) -> VisitaSucursal:
    visita = VisitaSucursal.objects.select_for_update().get(pk=visita_id)
    if visita.sucursal_id != sucursal.pk:
        raise AuditoriaVisitaError("La visita no corresponde a la sucursal seleccionada.")
    if visita.estatus not in {
        VisitaSucursal.ESTATUS_PROGRAMADA,
        VisitaSucursal.ESTATUS_BORRADOR,
    }:
        raise AuditoriaVisitaError("La visita ya no está pendiente y no puede ejecutarse otra vez.")
    if not sucursal_es_visitable(sucursal):
        raise AuditoriaVisitaError("La sucursal seleccionada no está disponible para auditoría.")

    punto, distancia = _validar_gps(
        sucursal=sucursal,
        latitud=latitud,
        longitud=longitud,
        precision_m=precision_m,
    )

    items = {item.pk: item for item in visita.checklist.select_for_update()}
    respuestas_validas = {value for value, _label in ChecklistVisita.RESPUESTA_CHOICES}
    for item_id, (respuesta, detalle) in respuestas.items():
        item = items.get(int(item_id))
        if not item:
            raise AuditoriaVisitaError("El checklist contiene un punto que no pertenece a la visita.")
        if respuesta not in respuestas_validas:
            raise AuditoriaVisitaError("El checklist contiene una respuesta no válida.")
        item.respuesta = respuesta
        item.observaciones = detalle.strip()
        item.save(update_fields=["respuesta", "observaciones"])

    ahora = realizada_en or timezone.now()
    visita.observaciones = observaciones.strip()
    visita.estatus = VisitaSucursal.ESTATUS_REALIZADA
    visita.fecha_real = fecha_real or timezone.localdate()
    visita.realizada_por = user
    visita.realizada_en = ahora
    visita.auditor = visita.auditor or user
    visita.gps_latitud = latitud
    visita.gps_longitud = longitud
    visita.gps_precision_m = precision_m
    visita.gps_distancia_sucursal_m = distancia
    visita.gps_dentro_geocerca = True
    visita.gps_radio_geocerca_m = punto.radio_geocerca_metros
    visita.save(
        update_fields=[
            "observaciones",
            "estatus",
            "fecha_real",
            "realizada_por",
            "realizada_en",
            "auditor",
            "gps_latitud",
            "gps_longitud",
            "gps_precision_m",
            "gps_distancia_sucursal_m",
            "gps_dentro_geocerca",
            "gps_radio_geocerca_m",
            "actualizado_en",
        ]
    )
    visita.personal_presente.set(personal_ids)
    log_event(
        user,
        "visita_sucursal_realizada",
        "VisitaSucursal",
        str(visita.pk),
        {
            "sucursal_id": sucursal.pk,
            "fecha_programada": str(visita.fecha_programada) if visita.fecha_programada else None,
            "fecha_real": str(visita.fecha_real),
            "distancia_m": distancia,
            "precision_m": str(precision_m),
            "radio_geocerca_m": punto.radio_geocerca_metros,
        },
    )
    return visita
