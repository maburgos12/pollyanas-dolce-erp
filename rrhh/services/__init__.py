from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from django.db import transaction

from rrhh.models import AsistenciaEmpleado, HoraExtra, NominaLinea, NominaPeriodo

TIEMPO_COMIDA_MINUTOS = 35


def usuario_jefe_directo_de_empleado(empleado):
    if not empleado or not empleado.jefe_directo_id:
        return None
    return getattr(empleado.jefe_directo, "usuario_erp", None)


def asistencia_descuenta_comida(asistencia: AsistenciaEmpleado) -> bool:
    """
    La comida solo descuenta jornada cuando el checador trae salida y regreso.
    Point solo entrega entrada/salida, por lo que no se infiere comida.
    """
    if asistencia.fuente == AsistenciaEmpleado.FUENTE_POINT:
        return False
    return bool(asistencia.salida_comida and asistencia.regreso_comida)


def minutos_jornada_programada(asistencia: AsistenciaEmpleado) -> int:
    if not asistencia.turno:
        return 0

    turno = asistencia.turno
    inicio = datetime.combine(asistencia.fecha, turno.hora_entrada)
    fin = datetime.combine(asistencia.fecha, turno.hora_salida)
    if fin <= inicio:
        fin += timedelta(days=1)

    minutos_jornada = int((fin - inicio).total_seconds() // 60)
    if asistencia_descuenta_comida(asistencia):
        minutos_jornada = max(minutos_jornada - TIEMPO_COMIDA_MINUTOS, 0)
    return minutos_jornada


def calcular_horas_extra(asistencia: AsistenciaEmpleado) -> Decimal:
    """
    Calcula horas extra a partir de la asistencia diaria.
    Regla: minutos trabajados - jornada de turno > tolerancia.
    """
    if not asistencia.turno or not asistencia.entrada or not asistencia.salida:
        return Decimal("0")

    minutos_jornada = minutos_jornada_programada(asistencia)
    excedente = int(asistencia.minutos_trabajados or 0) - minutos_jornada
    if excedente > int(asistencia.turno.tolerancia_minutos or 0):
        return Decimal(str(round(excedente / 60, 2))).quantize(Decimal("0.01"))
    return Decimal("0")


def generar_horas_extra_automatico(asistencia: AsistenciaEmpleado) -> HoraExtra | None:
    """
    Crea o actualiza la HoraExtra derivada de una asistencia.
    No modifica registros ya autorizados, rechazados o pagados.
    """
    horas = calcular_horas_extra(asistencia)
    if horas <= 0:
        return None

    he, creado = HoraExtra.objects.get_or_create(
        asistencia=asistencia,
        defaults={
            "empleado": asistencia.empleado,
            "fecha": asistencia.fecha,
            "horas": horas,
            "jefe_directo": usuario_jefe_directo_de_empleado(asistencia.empleado),
        },
    )
    if not creado and he.estado == HoraExtra.ESTADO_PENDIENTE:
        he.horas = horas
        if not he.jefe_directo_id:
            he.jefe_directo = usuario_jefe_directo_de_empleado(asistencia.empleado)
        he.save(update_fields=["horas", "jefe_directo"])
    return he


def calcular_monto_hora_extra(he: HoraExtra) -> Decimal:
    """
    Monto = horas x (salario_diario / jornada_horas) x tasa_extra.
    """
    try:
        salario_hora = Decimal(str(he.empleado.salario_diario or "0")) / Decimal("8")
        monto = Decimal(str(he.horas or "0")) * salario_hora * Decimal(str(he.tasa_extra or "0"))
        he.monto_calculado = monto.quantize(Decimal("0.01"))
        he.save(update_fields=["monto_calculado"])
        return he.monto_calculado
    except Exception:
        return Decimal("0")


@transaction.atomic
def aplicar_horas_extra_a_nomina(periodo: NominaPeriodo) -> int:
    """
    Precalcula horas extra autorizadas del periodo dentro de las líneas de nómina existentes.
    """
    actualizadas = 0
    horas_por_empleado: dict[int, Decimal] = {}
    for he in HoraExtra.objects.filter(
        estado=HoraExtra.ESTADO_AUTORIZADO,
        fecha__gte=periodo.fecha_inicio,
        fecha__lte=periodo.fecha_fin,
    ).select_related("empleado"):
        horas_por_empleado[he.empleado_id] = horas_por_empleado.get(he.empleado_id, Decimal("0")) + he.horas

    for empleado_id, horas in horas_por_empleado.items():
        linea, _ = NominaLinea.objects.get_or_create(periodo=periodo, empleado_id=empleado_id)
        linea.horas_extra = horas
        linea.save()
        actualizadas += 1
    periodo.recompute_totals()
    periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
    return actualizadas
