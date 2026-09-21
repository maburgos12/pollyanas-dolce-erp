"""Mantiene las detecciones pendientes bajo la jefatura actual del empleado."""

from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from core.models import Notificacion
from core.notificaciones import notificar_hora_extra_solicitada

from .models import HoraExtra
from .services_extra_conciliacion import es_hora_extra_automatica


def jefe_vigente_hora_extra(hora_extra):
    """Las detecciones pendientes siguen la ficha actual; las resueltas guardan historia."""
    if hora_extra.estado != HoraExtra.ESTADO_PENDIENTE or not es_hora_extra_automatica(hora_extra):
        return hora_extra.jefe_directo
    from .services import usuario_jefe_directo_de_empleado

    return usuario_jefe_directo_de_empleado(hora_extra.empleado)


def jefatura_hora_extra_actualizada(hora_extra):
    jefe = jefe_vigente_hora_extra(hora_extra)
    return hora_extra.jefe_directo_id == getattr(jefe, "pk", None)


def actualizar_jefe_hora_extra_pendiente(hora_extra, *, actor=None):
    """Reasigna una detección sin alterar horas, estado ni autorizaciones."""
    if hora_extra.estado != HoraExtra.ESTADO_PENDIENTE or not es_hora_extra_automatica(hora_extra):
        return False
    jefe = jefe_vigente_hora_extra(hora_extra)
    nuevo_id = getattr(jefe, "pk", None)
    anterior_id = hora_extra.jefe_directo_id
    if anterior_id == nuevo_id:
        return False
    HoraExtra.objects.filter(pk=hora_extra.pk, estado=HoraExtra.ESTADO_PENDIENTE).update(jefe_directo_id=nuevo_id)
    hora_extra.jefe_directo = jefe
    notificaciones_anteriores = Notificacion.objects.filter(
        objeto_tipo="rrhh.HoraExtra", objeto_id=str(hora_extra.pk),
        usuario_id=anterior_id, leida=False,
    )
    habia_aviso = notificaciones_anteriores.exists()
    notificaciones_anteriores.update(leida=True, leido_en=timezone.now())
    if jefe and habia_aviso:
        notificar_hora_extra_solicitada(hora_extra, actor=actor)
    log_event(actor, "UPDATE", "rrhh.HoraExtra", str(hora_extra.pk), {
        "origen": "sincronizacion_jefatura_deteccion_automatica",
        "jefe_anterior_id": anterior_id,
        "jefe_nuevo_id": nuevo_id,
        "empleado_id": hora_extra.empleado_id,
        "fecha": hora_extra.fecha.isoformat(),
    })
    return True


@transaction.atomic
def sincronizar_jefe_horas_extra_pendientes(empleado, *, actor=None):
    registros = HoraExtra.objects.select_for_update(of=("self",)).select_related(
        "empleado__jefe_directo__usuario_erp", "jefe_directo",
    ).filter(empleado=empleado, estado=HoraExtra.ESTADO_PENDIENTE, asistencia__isnull=False)
    return sum(actualizar_jefe_hora_extra_pendiente(he, actor=actor) for he in registros)
