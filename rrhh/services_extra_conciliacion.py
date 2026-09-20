"""Detección y conciliación de extra; consultar no cambia registros."""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.utils import timezone

from .models import AsistenciaEmpleado, Empleado, HoraExtra

COMIDA_INCLUIDA_MINUTOS = 35
NOTA_EXTRA_AUTOMATICA = '[Detección automática]'
NOTA_SALDO_CUBIERTO = '[Saldo automático cubierto o checada corregida]'


@dataclass(frozen=True)
class DiagnosticoHoraExtra:
    minutos: int | None
    codigo: str
    detalle: str
    modalidad: str
    comida_observable: bool
    requiere_revision: bool
    duracion_minutos: int | None = None


def modalidad_marcaje_efectiva(asistencia):
    if not asistencia or not asistencia.empleado_id:
        return Empleado.MARCAJE_CUATRO_MARCAS
    empleado = asistencia.empleado
    if empleado.modalidad_marcaje != Empleado.MARCAJE_AUTO:
        return empleado.modalidad_marcaje
    if (empleado.puesto_operativo or "").strip().upper() == "REPARTIDOR":
        return Empleado.MARCAJE_RUTA
    if asistencia.fuente == AsistenciaEmpleado.FUENTE_POINT:
        return Empleado.MARCAJE_DOS_MARCAS
    return Empleado.MARCAJE_CUATRO_MARCAS


def formato_minutos(value):
    if value is None:
        return 'N/D'
    return f'{value // 60} h {value % 60:02d} min'


def horas_a_minutos(value):
    return int((Decimal(value) * 60).quantize(Decimal('1'), rounding=ROUND_HALF_UP))


def diagnosticar_horas_extra(asistencia):
    """Diagnóstico de solo lectura contra el turno real, con comida incluida."""
    modalidad = modalidad_marcaje_efectiva(asistencia)
    comida_observable = bool(asistencia and asistencia.salida_comida and asistencia.regreso_comida)
    contexto = {'modalidad': modalidad, 'comida_observable': comida_observable}
    if not asistencia or not asistencia.entrada or not asistencia.salida:
        return DiagnosticoHoraExtra(None, 'marcaje_incompleto', 'Falta entrada o salida final.',
            requiere_revision=True, **contexto)
    intervalo = asistencia.salida - asistencia.entrada
    duracion_cruda = int(intervalo.total_seconds() // 60)
    if intervalo <= timedelta(0):
        return DiagnosticoHoraExtra(None, 'intervalo_invalido', 'La salida final debe ser posterior a la entrada.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)
    if intervalo > timedelta(days=1):
        return DiagnosticoHoraExtra(None, 'intervalo_invalido', 'El intervalo excede 24 horas.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)
    if bool(asistencia.salida_comida) != bool(asistencia.regreso_comida):
        return DiagnosticoHoraExtra(None, 'marcaje_comida_incompleto', 'Falta una marca de comida.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)
    if not asistencia.turno_id:
        return DiagnosticoHoraExtra(None, 'sin_turno', 'Falta asignar el turno de esta jornada.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)

    turno = asistencia.turno
    inicio_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_entrada))
    fin_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_salida))
    if fin_turno <= inicio_turno:
        fin_turno += timedelta(days=1)
    jornada = int((fin_turno - inicio_turno).total_seconds() // 60)
    inicio, fin = max(asistencia.entrada, inicio_turno), asistencia.salida
    if fin <= inicio:
        return DiagnosticoHoraExtra(None, 'intervalo_invalido', 'La salida final no supera el inicio del turno.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)
    duracion = int((fin - inicio).total_seconds() // 60)
    # Los primeros 35 minutos de comida forman parte de la jornada programada.
    if comida_observable:
        comida_inicio, comida_fin = asistencia.salida_comida, asistencia.regreso_comida
        if not asistencia.entrada <= comida_inicio < comida_fin <= fin:
            return DiagnosticoHoraExtra(None, 'marcaje_comida_invalido', 'Las marcas de comida no forman un intervalo válido dentro de la jornada.',
                requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)
        comida = int((comida_fin - comida_inicio).total_seconds() // 60)
        duracion -= max(comida - COMIDA_INCLUIDA_MINUTOS, 0)
    excedente = max(0, duracion - jornada)
    tolerancia = int(turno.tolerancia_minutos or 0)
    minutos = excedente if excedente > tolerancia else 0
    detalle = 'Comida registrada.' if comida_observable else 'La comida no es observable en las marcas.'
    return DiagnosticoHoraExtra(minutos, 'calculado', detalle,
        requiere_revision=not comida_observable, duracion_minutos=duracion_cruda, **contexto)


def detectar_minutos_extra(asistencia):
    """Compatibilidad para consumidores que solo necesitan los minutos."""
    return diagnosticar_horas_extra(asistencia).minutos


def conciliar_extra_diario(asistencia, registros):
    registros = list(registros)
    detectado = detectar_minutos_extra(asistencia)
    autorizado = horas_a_minutos(sum((r.horas for r in registros if r.estado in
        {HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO}), Decimal('0')))
    rechazado = horas_a_minutos(sum((r.horas for r in registros if r.estado == HoraExtra.ESTADO_RECHAZADO), Decimal('0')))
    solicitado = horas_a_minutos(sum((r.horas for r in registros if r.estado == HoraExtra.ESTADO_PENDIENTE), Decimal('0')))
    pendiente = max(detectado - autorizado - rechazado, 0) if detectado is not None else None
    diferencia = detectado - autorizado if detectado is not None else None
    if detectado is None:
        estado = 'No calculable: faltan checadas o intervalo válido'
    elif rechazado:
        estado = 'Con tiempo rechazado' if not pendiente else 'Con rechazo y diferencia pendiente'
    elif pendiente:
        estado = 'Pendiente de autorización' if not autorizado else 'Autorización parcial'
    elif autorizado > detectado:
        estado = 'Autorizado superior a lo detectado; revisar'
    elif autorizado:
        estado = 'Conciliado'
    else:
        estado = 'Sin extra detectado'
    return {
        'detectado_minutos': detectado, 'autorizado_minutos': autorizado,
        'pendiente_minutos': pendiente, 'rechazado_minutos': rechazado,
        'solicitado_minutos': solicitado, 'diferencia_minutos': diferencia,
        'detectado': formato_minutos(detectado), 'autorizado': formato_minutos(autorizado),
        'pendiente': formato_minutos(pendiente), 'rechazado': formato_minutos(rechazado),
        'estado': estado, 'registros': registros,
        'base': 'Turno de la asistencia; comida incluida' if asistencia and asistencia.turno_id
            else 'Jornada de 8 h con comida incluida; por duración',
    }
