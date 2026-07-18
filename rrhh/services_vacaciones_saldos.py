from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q, Sum

from .models import (
    AplicacionGoceVacaciones,
    MovimientoVacaciones,
    PeriodoVacacional,
    SolicitudVacaciones,
)


@dataclass(frozen=True)
class SaldoPeriodoVacacional:
    periodo_id: int
    aniversario: date
    dias_generados: Decimal
    reservado: Decimal
    gozado: Decimal
    disponible_goce: Decimal


def saldo_periodo_vacacional(periodo: PeriodoVacacional) -> SaldoPeriodoVacacional:
    totales = periodo.aplicaciones_goce.aggregate(
        reservado=Sum(
            "dias",
            filter=Q(estado=AplicacionGoceVacaciones.ESTADO_RESERVADA),
            default=Decimal("0"),
        ),
        gozado=Sum(
            "dias",
            filter=Q(estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA),
            default=Decimal("0"),
        ),
    )
    reservado = totales["reservado"] or Decimal("0")
    gozado = totales["gozado"] or Decimal("0")
    disponible = max(periodo.dias_generados - reservado - gozado, Decimal("0"))
    return SaldoPeriodoVacacional(
        periodo_id=periodo.pk,
        aniversario=periodo.aniversario,
        dias_generados=periodo.dias_generados,
        reservado=reservado,
        gozado=gozado,
        disponible_goce=disponible,
    )


@transaction.atomic
def reservar_goce_fifo(
    solicitud: SolicitudVacaciones,
    dias: Decimal,
    actor=None,
) -> list[AplicacionGoceVacaciones]:
    dias = Decimal(dias)
    if dias <= 0:
        raise ValidationError("Los días a reservar deben ser mayores que cero.")

    solicitud = SolicitudVacaciones.objects.select_for_update().get(pk=solicitud.pk)
    if solicitud.estado != SolicitudVacaciones.ESTADO_SOLICITADA:
        raise ValidationError(
            "Solo se puede reservar goce para solicitudes en estado solicitada."
        )
    periodos = list(
        PeriodoVacacional.objects.select_for_update()
        .filter(empleado_id=solicitud.empleado_id)
        .order_by("aniversario", "id")
    )
    if AplicacionGoceVacaciones.objects.filter(solicitud=solicitud).exists():
        raise ValidationError("La solicitud ya tiene aplicaciones de goce.")

    pendiente = dias
    aplicaciones = []
    for periodo in periodos:
        disponible = saldo_periodo_vacacional(periodo).disponible_goce
        aplicado = min(disponible, pendiente)
        if aplicado > 0:
            aplicaciones.append(
                AplicacionGoceVacaciones.objects.create(
                    solicitud=solicitud,
                    periodo=periodo,
                    dias=aplicado,
                    estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
                    actor=actor,
                )
            )
            pendiente -= aplicado
        if pendiente == 0:
            return aplicaciones

    raise ValidationError(f"Saldo vacacional insuficiente. Faltan {pendiente} días.")


def aplicaciones_reservadas_bloqueadas(
    solicitud: SolicitudVacaciones,
) -> list[AplicacionGoceVacaciones]:
    """Devuelve las reservas vigentes bloqueadas en orden FIFO estable."""
    return list(
        AplicacionGoceVacaciones.objects.select_for_update(of=("self",))
        .select_related("periodo")
        .filter(
            solicitud_id=solicitud.pk,
            estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )
        .order_by("periodo__aniversario", "id")
    )


def _crear_movimiento_aplicacion(
    aplicacion: AplicacionGoceVacaciones,
    *,
    tipo: str,
    actor,
    descripcion: str,
) -> None:
    MovimientoVacaciones.objects.create(
        empleado_id=aplicacion.solicitud.empleado_id,
        solicitud_id=aplicacion.solicitud_id,
        tipo=tipo,
        dias=aplicacion.dias,
        periodo_anio=aplicacion.periodo.aniversario.year,
        descripcion=descripcion,
        actor=actor,
    )


def liberar_reservas_goce(
    solicitud: SolicitudVacaciones,
    *,
    actor,
    descripcion: str,
) -> list[AplicacionGoceVacaciones]:
    aplicaciones = aplicaciones_reservadas_bloqueadas(solicitud)
    for aplicacion in aplicaciones:
        aplicacion.estado = AplicacionGoceVacaciones.ESTADO_LIBERADA
        aplicacion.save(update_fields=["estado"])
        _crear_movimiento_aplicacion(
            aplicacion,
            tipo=MovimientoVacaciones.TIPO_LIBERADO,
            actor=actor,
            descripcion=descripcion,
        )
    return aplicaciones


def consumir_reservas_goce(
    solicitud: SolicitudVacaciones,
    *,
    actor,
) -> list[AplicacionGoceVacaciones]:
    aplicaciones = aplicaciones_reservadas_bloqueadas(solicitud)
    if not aplicaciones:
        raise ValidationError("La solicitud no tiene reservas de goce vigentes.")

    total_reservado = sum((aplicacion.dias for aplicacion in aplicaciones), Decimal("0"))
    if total_reservado != solicitud.dias_laborables:
        raise ValidationError(
            "La suma de reservas de goce no coincide con los días de la solicitud."
        )

    for aplicacion in aplicaciones:
        aplicacion.estado = AplicacionGoceVacaciones.ESTADO_CONSUMIDA
        aplicacion.save(update_fields=["estado"])
        _crear_movimiento_aplicacion(
            aplicacion,
            tipo=MovimientoVacaciones.TIPO_LIBERADO,
            actor=actor,
            descripcion=f"Cierre de reserva por aprobación {solicitud.folio}",
        )
        _crear_movimiento_aplicacion(
            aplicacion,
            tipo=MovimientoVacaciones.TIPO_CONSUMIDO,
            actor=actor,
            descripcion=f"Consumo por aprobación {solicitud.folio}",
        )
    return aplicaciones
