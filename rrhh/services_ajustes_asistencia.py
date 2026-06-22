from datetime import datetime

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from rrhh.models import AjusteAsistencia, AsistenciaEmpleado
from rrhh.services_asistencia_reglas import evaluar_dia_empleado


TIPOS_A_CAMPOS = {
    AjusteAsistencia.TIPO_ENTRADA: "entrada",
    AjusteAsistencia.TIPO_SALIDA: "salida",
    AjusteAsistencia.TIPO_SALIDA_COMIDA: "salida_comida",
    AjusteAsistencia.TIPO_REGRESO_COMIDA: "regreso_comida",
}


def _serializar_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if timezone.is_aware(value):
            value = timezone.localtime(value)
        return value.isoformat()
    return str(value)


def _parsear_datetime(value, field_name: str):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = parse_datetime(str(value))
    if dt is None:
        raise ValidationError({field_name: "Debe ser una fecha y hora ISO valida."})
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _campo_para_tipo(tipo_ajuste: str) -> str:
    try:
        return TIPOS_A_CAMPOS[tipo_ajuste]
    except KeyError as exc:
        raise ValidationError({"tipo_ajuste": "Tipo de ajuste de asistencia no soportado."}) from exc


def _validar_pendiente(ajuste: AjusteAsistencia) -> None:
    if ajuste.estado != AjusteAsistencia.ESTADO_PENDIENTE:
        raise ValidationError({"estado": "Solo se pueden resolver ajustes pendientes."})


def crear_ajuste_asistencia(empleado, fecha, tipo_ajuste, valores_propuestos, motivo, solicitado_por):
    motivo = (motivo or "").strip()
    if not motivo:
        raise ValidationError({"motivo": "El motivo es obligatorio."})
    if empleado.fecha_ingreso and fecha < empleado.fecha_ingreso:
        raise ValidationError({"fecha": "No se pueden capturar ajustes antes de la fecha de ingreso del empleado."})

    campo = _campo_para_tipo(tipo_ajuste)
    asistencia, _ = AsistenciaEmpleado.objects.get_or_create(
        empleado=empleado,
        fecha=fecha,
    )

    return AjusteAsistencia.objects.create(
        empleado=empleado,
        fecha=fecha,
        asistencia=asistencia,
        tipo_ajuste=tipo_ajuste,
        estado=AjusteAsistencia.ESTADO_PENDIENTE,
        valores_anteriores={campo: _serializar_datetime(getattr(asistencia, campo))},
        valores_propuestos=valores_propuestos or {},
        motivo=motivo,
        solicitado_por=solicitado_por,
    )


@transaction.atomic
def aprobar_ajuste_asistencia(ajuste, user, comentario=""):
    ajuste = (
        AjusteAsistencia.objects.select_for_update()
        .select_related("empleado")
        .get(pk=ajuste.pk)
    )
    _validar_pendiente(ajuste)

    campo = _campo_para_tipo(ajuste.tipo_ajuste)
    asistencia = (
        AsistenciaEmpleado.objects.select_for_update()
        .get(pk=ajuste.asistencia_id)
        if ajuste.asistencia_id
        else AsistenciaEmpleado.objects.select_for_update().get(
            empleado=ajuste.empleado,
            fecha=ajuste.fecha,
        )
    )
    valor = _parsear_datetime(ajuste.valores_propuestos.get(campo), campo)
    setattr(asistencia, campo, valor)
    asistencia.save(update_fields=[campo])

    now = timezone.now()
    ajuste.asistencia = asistencia
    ajuste.estado = AjusteAsistencia.ESTADO_APLICADO
    ajuste.autorizado_por = user
    ajuste.aplicado_por = user
    ajuste.autorizado_en = now
    ajuste.aplicado_en = now
    ajuste.comentario_autorizacion = (comentario or "").strip()
    ajuste.valores_aplicados = {campo: _serializar_datetime(valor)}
    ajuste.save(
        update_fields=[
            "asistencia",
            "estado",
            "autorizado_por",
            "aplicado_por",
            "autorizado_en",
            "aplicado_en",
            "comentario_autorizacion",
            "valores_aplicados",
            "actualizado_en",
        ]
    )

    evaluar_dia_empleado(ajuste.empleado, ajuste.fecha)
    return ajuste


@transaction.atomic
def rechazar_ajuste_asistencia(ajuste, user, comentario=""):
    ajuste = AjusteAsistencia.objects.select_for_update().get(pk=ajuste.pk)
    _validar_pendiente(ajuste)

    ajuste.estado = AjusteAsistencia.ESTADO_RECHAZADO
    ajuste.autorizado_por = user
    ajuste.autorizado_en = timezone.now()
    ajuste.comentario_autorizacion = (comentario or "").strip()
    ajuste.save(
        update_fields=[
            "estado",
            "autorizado_por",
            "autorizado_en",
            "comentario_autorizacion",
            "actualizado_en",
        ]
    )
    return ajuste
