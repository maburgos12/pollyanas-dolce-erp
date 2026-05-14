from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal

from .models import Prestamo, PrestamoCuota


def _siguiente_quincena(f: date) -> date:
    if f.day <= 15:
        ultimo = calendar.monthrange(f.year, f.month)[1]
        return date(f.year, f.month, ultimo)
    mes = f.month + 1 if f.month < 12 else 1
    anio = f.year if f.month < 12 else f.year + 1
    return date(anio, mes, 15)


def generar_cuotas(prestamo: Prestamo) -> list[PrestamoCuota]:
    """
    Genera las cuotas proyectadas a partir de fecha_deposito o fecha_solicitud.
    La primera cuota cae en la siguiente quincena de la fecha base.
    """
    cuotas = []
    fecha = _siguiente_quincena(prestamo.fecha_deposito or prestamo.fecha_solicitud)

    for i in range(1, prestamo.num_quincenas + 1):
        cuotas.append(
            PrestamoCuota(
                prestamo=prestamo,
                numero_quincena=i,
                fecha_quincena=fecha,
                monto_esperado=prestamo.descuento_quincenal,
                estado=PrestamoCuota.ESTADO_PENDIENTE,
            )
        )
        fecha = _siguiente_quincena(fecha)

    PrestamoCuota.objects.bulk_create(cuotas, ignore_conflicts=True)
    return cuotas


def aplicar_cobro_manual(
    cuota: PrestamoCuota,
    monto: Decimal,
    user,
    nota: str = "",
    *,
    fuente: str = PrestamoCuota.FUENTE_MANUAL,
) -> PrestamoCuota:
    """
    Registra un cobro sobre una cuota y recalcula el saldo del préstamo.
    """
    monto = Decimal(str(monto or "0")).quantize(Decimal("0.01"))
    cuota.monto_cobrado = monto
    cuota.fecha_cobro = date.today()
    cuota.registrado_por = user
    cuota.fuente = fuente
    cuota.nota = nota

    if monto >= cuota.monto_esperado:
        cuota.estado = PrestamoCuota.ESTADO_COBRADO
    elif monto > 0:
        cuota.estado = PrestamoCuota.ESTADO_PARCIAL
    else:
        cuota.estado = PrestamoCuota.ESTADO_OMITIDO

    cuota.save()
    cuota.prestamo.recalcular_saldo()
    return cuota
