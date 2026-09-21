from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from django.db import transaction

from core.access import can_manage_rrhh

from rrhh.models import AsistenciaEmpleado, HoraExtra, NominaLinea, NominaPeriodo
from rrhh.services_permisos import permiso_requiere_autorizacion_direccion, usuario_direccion_general_para_autorizacion
from rrhh.services_extra_conciliacion import (
    detectar_minutos_extra, diagnosticar_horas_extra, saldo_automatico_esperado,
    NOTA_EXTRA_AUTOMATICA, NOTA_SALDO_CUBIERTO,
)
from rrhh.services_extra_bloqueos import bloquear_jornadas_extra

TIEMPO_COMIDA_MINUTOS = 35


def usuario_jefe_directo_de_empleado(empleado):
    if permiso_requiere_autorizacion_direccion(empleado):
        direccion = usuario_direccion_general_para_autorizacion()
        if direccion:
            return direccion
    if not empleado or not empleado.jefe_directo_id:
        return None
    return getattr(empleado.jefe_directo, "usuario_erp", None)


def can_edit_incidencia(user, incidencia) -> bool:
    if not user or not getattr(user, "is_authenticated", False) or not incidencia:
        return False
    if can_manage_rrhh(user):
        return True
    empleado = getattr(incidencia, "empleado", None)
    jefe = usuario_jefe_directo_de_empleado(empleado)
    return getattr(jefe, "id", None) == user.id


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
    minutos = detectar_minutos_extra(asistencia)
    return (Decimal(minutos or 0) / 60).quantize(Decimal('0.01'))


@transaction.atomic
def generar_horas_extra_automatico(asistencia: AsistenciaEmpleado) -> HoraExtra | None:
    """
    Crea o actualiza la HoraExtra derivada de una asistencia.
    No modifica registros ya autorizados, rechazados o pagados.
    """
    identidad = AsistenciaEmpleado.objects.get(pk=asistencia.pk)
    bloquear_jornadas_extra([(identidad.empleado_id, identidad.fecha)])
    # Serializa eventos del mismo día, incluso cuando aún no existe extra.
    asistencia = AsistenciaEmpleado.objects.select_for_update(of=('self',)).select_related(
        'empleado__jefe_directo__usuario_erp', 'turno').get(pk=asistencia.pk)
    if (asistencia.empleado_id, asistencia.fecha) != (identidad.empleado_id, identidad.fecha):
        return None  # La corrección de jornada requiere una nueva evaluación.
    vinculada = HoraExtra.objects.filter(asistencia_id=asistencia.pk).first()
    if vinculada and (vinculada.empleado_id, vinculada.fecha) != (asistencia.empleado_id, asistencia.fecha):
        return vinculada  # No recrear ni trasladar un vínculo corregido manualmente.
    diagnostico = diagnosticar_horas_extra(asistencia)
    registros = list(HoraExtra.objects.select_for_update(of=('self',)).filter(
        empleado_id=asistencia.empleado_id, fecha=asistencia.fecha).order_by('pk'))
    he = next((r for r in registros if r.asistencia_id == asistencia.pk), None)
    saldo = saldo_automatico_esperado(diagnostico, registros, he)
    if saldo is None:
        return he
    if he and he.estado == HoraExtra.ESTADO_PENDIENTE and he.ajuste_autorizacion:
        # Preservar la decisión humana; si cambió la jornada, el autorizador
        # verá que la evidencia caducó y deberá reevaluarla.
        return he
    reactivar = bool(he and saldo > 0 and he.estado == HoraExtra.ESTADO_CANCELADO
        and he.notas.startswith(NOTA_EXTRA_AUTOMATICA) and he.notas.endswith(NOTA_SALDO_CUBIERTO))
    if he and he.estado != HoraExtra.ESTADO_PENDIENTE and not reactivar:
        return he  # Autorización, rechazo, pago y cancelación se conservan.
    if saldo <= 0:
        if he and he.notas.startswith(NOTA_EXTRA_AUTOMATICA):
            he.estado = HoraExtra.ESTADO_CANCELADO
            he.notas += '\n' + NOTA_SALDO_CUBIERTO
            he.save(update_fields=['estado', 'notas'])
        return next((r for r in registros if r != he and r.estado != HoraExtra.ESTADO_CANCELADO), he)
    if he is None:
        return HoraExtra.objects.create(asistencia=asistencia, empleado_id=asistencia.empleado_id,
            fecha=asistencia.fecha, horas=saldo,
            notas=f'{NOTA_EXTRA_AUTOMATICA} Jornada con comida incluida. Saldo no cubierto por otros registros.',
            jefe_directo=usuario_jefe_directo_de_empleado(asistencia.empleado))
    if he.horas != saldo or reactivar:
        he.horas = saldo
        if reactivar:
            he.estado = HoraExtra.ESTADO_PENDIENTE
            he.notas += '\nSaldo automático pendiente nuevamente por cambio de cobertura.'
        if not he.jefe_directo_id:
            he.jefe_directo = usuario_jefe_directo_de_empleado(asistencia.empleado)
        he.save(update_fields=['horas', 'jefe_directo', 'estado', 'notas'])
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
