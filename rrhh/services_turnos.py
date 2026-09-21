"""Resuelve horarios confirmados por persona y fecha sin inferirlos de la llegada."""

from django.core.exceptions import ValidationError
from django.db.models import Q
from django.utils import timezone

from .models import AsignacionTurnoEmpleado


def turno_asignado_para_fecha(empleado, fecha):
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
    return asignaciones[0].turno if asignaciones else None


def es_jornada_historica_antes_de_asignacion(asistencia):
    """Evita efectos disciplinarios o propuestas nuevas al releer historia preasignación."""
    if not asistencia:
        return False
    asignacion = (
        AsignacionTurnoEmpleado.objects.filter(
            empleado_id=asistencia.empleado_id,
            proteger_reingesta_historica=True,
            fecha_inicio__lte=asistencia.fecha,
        )
        .filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=asistencia.fecha))
        .order_by("-fecha_inicio")
        .first()
    )
    return bool(asignacion and asistencia.fecha < timezone.localtime(asignacion.creado_en).date())
