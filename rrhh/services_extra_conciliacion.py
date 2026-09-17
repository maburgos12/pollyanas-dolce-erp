"""Detección y conciliación de extra; consultar no cambia registros."""
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.utils import timezone

from .models import HoraExtra

JORNADA_DIARIA_MINUTOS = 8 * 60
TOLERANCIA_EXTRA_MINUTOS = 10
COMIDA_INCLUIDA_MINUTOS = 35
NOTA_EXTRA_AUTOMATICA = '[Detección automática]'
NOTA_SALDO_CUBIERTO = '[Saldo automático cubierto o checada corregida]'


def formato_minutos(value):
    if value is None:
        return 'N/D'
    return f'{value // 60} h {value % 60:02d} min'


def horas_a_minutos(value):
    return int((Decimal(value) * 60).quantize(Decimal('1'), rounding=ROUND_HALF_UP))


def detectar_minutos_extra(asistencia):
    """Comida incluida. Sin turno se usa la jornada de 8h ratificada.

    Con turno, una entrada anticipada no amplía la jornada desde antes de
    su inicio. Sin turno se mide duración; no se presume puntualidad.
    """
    if not asistencia or not asistencia.entrada or not asistencia.salida:
        return None
    inicio, fin = asistencia.entrada, asistencia.salida
    jornada = JORNADA_DIARIA_MINUTOS
    tolerancia = TOLERANCIA_EXTRA_MINUTOS
    if asistencia.turno_id:
        turno = asistencia.turno
        inicio_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_entrada))
        fin_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_salida))
        if fin_turno <= inicio_turno:
            fin_turno += timedelta(days=1)
        jornada = int((fin_turno - inicio_turno).total_seconds() // 60)
        inicio = max(inicio, inicio_turno)
        tolerancia = int(turno.tolerancia_minutos or 0)
    if fin <= inicio:
        return None
    duracion = int((fin - inicio).total_seconds() // 60)
    if duracion > 24 * 60:
        return None
    # Los 35 min previstos cuentan en las 8h. Un descanso mayor no crea
    # tiempo trabajado: solo su exceso se descuenta si hay ambas marcas.
    if asistencia.salida_comida and asistencia.regreso_comida and asistencia.fuente != 'point':
        comida_inicio, comida_fin = asistencia.salida_comida, asistencia.regreso_comida
        if not asistencia.entrada <= comida_inicio < comida_fin <= fin:
            return None
        comida = int((comida_fin - comida_inicio).total_seconds() // 60)
        duracion -= max(comida - COMIDA_INCLUIDA_MINUTOS, 0)
    excedente = max(0, duracion - jornada)
    return excedente if excedente > tolerancia else 0


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
