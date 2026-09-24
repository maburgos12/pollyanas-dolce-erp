"""Resuelve horarios confirmados por persona y fecha sin inferirlos de la llegada."""

from dataclasses import dataclass

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .models import AsignacionJornadaEmpleado, AsignacionTurnoEmpleado, Empleado, Turno


ESTADO_LABORABLE = "laborable"
ESTADO_DESCANSO = "descanso"
ESTADO_SIN_ASIGNACION = "sin_asignacion"


@dataclass(frozen=True)
class HorarioProgramado:
    estado: str
    turno: Turno | None
    asignacion: AsignacionJornadaEmpleado | AsignacionTurnoEmpleado | None


@transaction.atomic
def asignar_jornada_empleado(*, empleado, jornada, fecha_inicio, fecha_fin, motivo, actor):
    Empleado.objects.select_for_update().get(pk=empleado.pk)
    list(
        AsignacionJornadaEmpleado.objects.select_for_update()
        .filter(empleado=empleado)
    )
    asignacion = AsignacionJornadaEmpleado(
        empleado=empleado,
        jornada=jornada,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        motivo=motivo,
        creado_por=actor,
    )
    asignacion.full_clean()
    asignacion.save()
    return asignacion


def _asignacion_legacy_para_fecha(empleado, fecha):
    if not empleado or not fecha:
        return None
    asignaciones = list(
        AsignacionTurnoEmpleado.objects.select_related("turno")
        .filter(empleado=empleado, fecha_inicio__lte=fecha)
        .filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=fecha))
        .order_by("-fecha_inicio")[:2]
    )
    if len(asignaciones) > 1:
        raise ValidationError("Hay turnos asignados que se traslapan; revisa la vigencia del empleado.")
    return asignaciones[0] if asignaciones else None


def _turno_legacy_asignado_para_fecha(empleado, fecha):
    asignacion = _asignacion_legacy_para_fecha(empleado, fecha)
    return asignacion.turno if asignacion else None


def horario_programado_para_fecha(empleado, fecha):
    if not empleado or not fecha:
        return HorarioProgramado(ESTADO_SIN_ASIGNACION, None, None)

    asignaciones = list(
        AsignacionJornadaEmpleado.objects.select_related("jornada")
        .filter(empleado=empleado, fecha_inicio__lte=fecha)
        .filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=fecha))
        .order_by("-fecha_inicio")[:2]
    )
    if len(asignaciones) > 1:
        raise ValidationError("Hay jornadas semanales asignadas que se traslapan; revisa la vigencia del empleado.")

    if asignaciones:
        asignacion = asignaciones[0]
        dias = list(asignacion.jornada.dias.select_related("turno"))
        if len(dias) != 7 or {dia.dia_semana for dia in dias} != set(range(7)):
            raise ValidationError("La jornada semanal debe tener exactamente un detalle para cada día de lunes a domingo.")
        turno = next(dia.turno for dia in dias if dia.dia_semana == fecha.weekday())
        estado = ESTADO_LABORABLE if turno else ESTADO_DESCANSO
        return HorarioProgramado(estado, turno, asignacion)

    asignacion_legacy = _asignacion_legacy_para_fecha(empleado, fecha)
    if asignacion_legacy is None:
        return HorarioProgramado(ESTADO_SIN_ASIGNACION, None, None)
    return HorarioProgramado(ESTADO_LABORABLE, asignacion_legacy.turno, asignacion_legacy)


def turno_asignado_para_fecha(empleado, fecha):
    horario = horario_programado_para_fecha(empleado, fecha)
    return horario.turno if horario.estado == ESTADO_LABORABLE else None


def es_jornada_historica_antes_de_asignacion(asistencia):
    """Evita efectos disciplinarios o propuestas nuevas al releer historia preasignación."""
    if not asistencia:
        return False
    for modelo in (AsignacionJornadaEmpleado, AsignacionTurnoEmpleado):
        asignacion = (
            modelo.objects.filter(
                empleado_id=asistencia.empleado_id,
                proteger_reingesta_historica=True,
                fecha_inicio__lte=asistencia.fecha,
            )
            .filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=asistencia.fecha))
            .order_by("-fecha_inicio")
            .first()
        )
        if asignacion and asistencia.fecha < timezone.localtime(asignacion.creado_en).date():
            return True
    return False
