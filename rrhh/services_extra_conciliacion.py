"""Detección y conciliación de extra; consultar no cambia registros."""
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.utils import timezone

from .models import AsistenciaEmpleado, Empleado, HoraExtra

NOTA_EXTRA_AUTOMATICA = '[Detección automática]'
NOTA_SALDO_CUBIERTO = '[Saldo automático cubierto o checada corregida]'
UMBRAL_SOLICITUD_EXTRA_MINUTOS = 50
BLOQUE_AUTORIZACION_EXTRA_MINUTOS = 30


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


def minutos_a_horas(minutos):
    return (Decimal(minutos) / Decimal('60')).quantize(Decimal('0.01'))


def formatear_duracion_minutos(minutos):
    horas, resto = divmod(int(minutos), 60)
    partes = []
    if horas:
        partes.append(f'{horas} h')
    if resto or not partes:
        partes.append(f'{resto} min')
    return ' '.join(partes)


def formatear_duracion_horas(horas):
    return formatear_duracion_minutos(horas_a_minutos(horas))


def es_bloque_extra_autorizable(horas):
    minutos = horas_a_minutos(horas)
    return minutos > 0 and minutos % BLOQUE_AUTORIZACION_EXTRA_MINUTOS == 0


def diagnosticar_horas_extra(asistencia):
    """Calcula únicamente el tiempo posterior a la salida programada."""
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
    if not asistencia.turno_id:
        return DiagnosticoHoraExtra(None, 'sin_turno', 'Falta asignar el turno de esta jornada.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)

    turno = asistencia.turno
    inicio_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_entrada))
    fin_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_salida))
    if fin_turno <= inicio_turno:
        fin_turno += timedelta(days=1)
    if asistencia.salida <= asistencia.entrada:
        return DiagnosticoHoraExtra(None, 'intervalo_invalido', 'La salida final no supera el inicio del turno.',
            requiere_revision=True, duracion_minutos=duracion_cruda, **contexto)

    requiere_revision = False
    detalle = 'Comida registrada.' if comida_observable else 'La comida no es observable en las marcas.'
    codigo = 'calculado'
    if bool(asistencia.salida_comida) != bool(asistencia.regreso_comida):
        requiere_revision = True
        codigo = 'calculado_con_revision_comida'
        detalle = 'Tiempo extra calculado por salida programada; falta una marca de comida.'
    if comida_observable:
        comida_inicio, comida_fin = asistencia.salida_comida, asistencia.regreso_comida
        if not asistencia.entrada <= comida_inicio < comida_fin <= asistencia.salida:
            requiere_revision = True
            codigo = 'calculado_con_revision_comida'
            detalle = 'Tiempo extra calculado por salida programada; las marcas de comida requieren revisión.'

    excedente = max(0, int((asistencia.salida - fin_turno).total_seconds() // 60))
    minutos = excedente
    return DiagnosticoHoraExtra(minutos, codigo, detalle,
        requiere_revision=requiere_revision or not comida_observable,
        duracion_minutos=duracion_cruda, **contexto)


def detectar_minutos_extra(asistencia):
    """Compatibilidad para consumidores que solo necesitan los minutos."""
    return diagnosticar_horas_extra(asistencia).minutos


def es_hora_extra_automatica(hora_extra):
    """El vínculo del generador define el origen; las notas son editables."""
    return bool(hora_extra.asistencia_id)


def saldo_automatico_minutos(diagnostico, registros, hora_extra=None):
    """Saldo crudo no cubierto; conserva minutos menores al umbral para auditoría."""
    if diagnostico.minutos is None:
        return None
    if diagnostico.minutos <= 0:
        return 0
    cobertura = sum(
        (r.horas for r in registros if r != hora_extra and r.estado != HoraExtra.ESTADO_CANCELADO),
        Decimal("0"),
    )
    return max(diagnostico.minutos - horas_a_minutos(cobertura), 0)


def saldo_automatico_esperado(diagnostico, registros, hora_extra=None):
    """Saldo en horas con el redondeo del generador; None significa no calculable."""
    saldo_minutos = saldo_automatico_minutos(diagnostico, registros, hora_extra)
    if saldo_minutos is None:
        return None
    if saldo_minutos < UMBRAL_SOLICITUD_EXTRA_MINUTOS:
        return Decimal('0')
    return minutos_a_horas(saldo_minutos)


def huella_calculo_extra(asistencia):
    """Identifica los insumos usados al justificar una cantidad distinta al saldo."""
    turno = asistencia.turno if asistencia.turno_id else None
    insumos = {
        "empleado": asistencia.empleado_id,
        "fecha": asistencia.fecha.isoformat(),
        "fuente": asistencia.fuente,
        "modalidad": modalidad_marcaje_efectiva(asistencia),
        "entrada": asistencia.entrada.isoformat() if asistencia.entrada else None,
        "salida_comida": asistencia.salida_comida.isoformat() if asistencia.salida_comida else None,
        "regreso_comida": asistencia.regreso_comida.isoformat() if asistencia.regreso_comida else None,
        "salida": asistencia.salida.isoformat() if asistencia.salida else None,
        "turno": asistencia.turno_id,
        "hora_entrada_turno": turno.hora_entrada.isoformat() if turno else None,
        "hora_salida_turno": turno.hora_salida.isoformat() if turno else None,
        "tolerancia": turno.tolerancia_minutos if turno else None,
    }
    return hashlib.sha256(json.dumps(insumos, sort_keys=True).encode()).hexdigest()


def evidencia_ajuste_extra(hora_extra, saldo, motivo, usuario):
    """Congela una corrección humana para esta asistencia y este saldo calculado."""
    return {
        "horas": f"{hora_extra.horas:.2f}",
        "saldo": f"{saldo:.2f}",
        "huella": huella_calculo_extra(hora_extra.asistencia),
        "motivo": motivo.strip(),
        "usuario": usuario,
        "registrado_en": timezone.now().isoformat(),
    }


def contexto_hora_extra(hora_extra, registros_dia=None):
    """Explica el cálculo actual sin modificar propuestas ni autorizaciones."""
    if not es_hora_extra_automatica(hora_extra):
        return {
            "modalidad": "Manual",
            "comida": "No evaluada en captura manual",
            "turno": "No aplica al cálculo automático",
            "estado": "Captura manual",
            "puede_autorizar": True,
            "motivo_bloqueo": "",
            "requiere_revision": False,
        }

    asistencia = hora_extra.asistencia
    diagnostico = diagnosticar_horas_extra(asistencia)
    if registros_dia is None:
        registros_dia = HoraExtra.objects.filter(empleado_id=hora_extra.empleado_id, fecha=hora_extra.fecha)
    saldo = saldo_automatico_esperado(diagnostico, registros_dia, hora_extra)
    calculable_positivo = diagnostico.minutos is not None and diagnostico.minutos > 0
    ajuste = hora_extra.ajuste_autorizacion or {}
    ajuste_vigente = bool(
        ajuste.get("motivo")
        and ajuste.get("horas") == f"{hora_extra.horas:.2f}"
        and ajuste.get("saldo") == f"{saldo:.2f}"
        and ajuste.get("huella") == huella_calculo_extra(asistencia)
    )
    bloque_autorizable = es_bloque_extra_autorizable(hora_extra.horas)
    puede_autorizar = calculable_positivo and saldo > 0 and bloque_autorizable and (
        hora_extra.horas == saldo or ajuste_vigente
    )
    requiere_revision = diagnostico.requiere_revision or not puede_autorizar or ajuste_vigente
    motivo_bloqueo = ""
    if not puede_autorizar:
        if calculable_positivo and saldo and not bloque_autorizable:
            motivo_bloqueo = "Ajusta el tiempo a bloques de 30 minutos antes de autorizar."
        elif calculable_positivo:
            motivo_bloqueo = "La propuesta no coincide con el saldo automático vigente."
        elif diagnostico.minutos == 0:
            motivo_bloqueo = "No se detectan horas extra en la asistencia actual."
        else:
            motivo_bloqueo = diagnostico.detalle
        if diagnostico.codigo == "sin_turno":
            motivo_bloqueo += " Asigna el turno y reevalúa la asistencia antes de autorizar."
        elif bloque_autorizable:
            motivo_bloqueo += " Corrige y reevalúa la asistencia antes de autorizar."

    if not puede_autorizar and hora_extra.estado in {HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO}:
        estado = "Revisión recomendada"
    elif not puede_autorizar:
        estado = "No calculable"
    elif ajuste_vigente and hora_extra.horas != saldo:
        estado = "Ajuste justificado"
    elif requiere_revision:
        estado = "Requiere revisión"
    else:
        estado = "Calculado"
    return {
        "modalidad": {
            Empleado.MARCAJE_CUATRO_MARCAS: "4 marcas",
            Empleado.MARCAJE_DOS_MARCAS: "2 marcas",
            Empleado.MARCAJE_RUTA: "Ruta",
        }[diagnostico.modalidad],
        "comida": "Comida registrada" if diagnostico.comida_observable else "Comida no observable",
        "turno": asistencia.turno.nombre if asistencia.turno_id else "Sin turno asignado",
        "estado": estado,
        "puede_autorizar": puede_autorizar,
        "motivo_bloqueo": motivo_bloqueo,
        "requiere_revision": requiere_revision,
        "ajuste_justificado": ajuste_vigente and hora_extra.horas != saldo,
        "saldo_detectado": str(saldo) if saldo is not None else None,
    }


def conciliar_extra_diario(asistencia, registros):
    registros = list(registros)
    diagnostico = diagnosticar_horas_extra(asistencia)
    detectado = diagnostico.minutos
    autorizado = horas_a_minutos(sum((r.horas for r in registros if r.estado in
        {HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO}), Decimal('0')))
    rechazado = horas_a_minutos(sum((r.horas for r in registros if r.estado == HoraExtra.ESTADO_RECHAZADO), Decimal('0')))
    solicitado = horas_a_minutos(sum((r.horas for r in registros if r.estado == HoraExtra.ESTADO_PENDIENTE), Decimal('0')))
    pendiente = max(detectado - autorizado - rechazado, 0) if detectado is not None else None
    diferencia = detectado - autorizado if detectado is not None else None
    ajuste_autorizado = any(
        r.ajuste_autorizacion and contexto_hora_extra(r, registros).get('ajuste_justificado')
        for r in registros if r.estado in {HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO}
    )
    if diagnostico.codigo == 'sin_turno':
        estado = 'No calculable: falta asignar turno'
    elif detectado is None:
        estado = 'No calculable: faltan checadas o intervalo válido'
    elif rechazado:
        estado = 'Con tiempo rechazado' if not pendiente else 'Con rechazo y diferencia pendiente'
    elif ajuste_autorizado:
        estado = 'Ajuste justificado: diferencia pendiente' if pendiente else 'Ajuste justificado autorizado'
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
        'base': 'Salida programada del turno' if detectado is not None else diagnostico.detalle,
        'modalidad': diagnostico.modalidad,
        'comida_observable': diagnostico.comida_observable,
        'requiere_revision': diagnostico.requiere_revision,
        'codigo': diagnostico.codigo,
        'ajuste_autorizado': ajuste_autorizado,
    }
