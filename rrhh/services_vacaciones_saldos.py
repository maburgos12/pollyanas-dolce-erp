from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q, Sum

from .models import AplicacionGoceVacaciones, PeriodoVacacional, SolicitudVacaciones


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
