"""Cambios de autorización concilian solo extra; nunca nómina o faltas."""
from contextvars import ContextVar

from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.db.models.signals import post_delete, post_save, pre_delete, pre_save
from django.dispatch import receiver

from .models import AsistenciaEmpleado, HoraExtra, IncidenciaAsistencia

_conciliando = ContextVar('rrhh_conciliando_extra', default=False)


@receiver(pre_delete, sender=AsistenciaEmpleado)
def proteger_origen_extra(sender, instance, using, **kwargs):
    """SET_NULL no debe convertir una propuesta automática en captura manual."""
    from .services_extra_bloqueos import bloquear_jornadas_extra
    bloquear_jornadas_extra([(instance.empleado_id, instance.fecha)])
    vinculadas = list(HoraExtra.objects.using(using).filter(asistencia_id=instance.pk))
    if vinculadas:
        raise ProtectedError("No se puede eliminar una asistencia vinculada a horas extra.", vinculadas)


@receiver(pre_save, sender=HoraExtra)
def guardar_fecha_anterior_extra(sender, instance, raw=False, **kwargs):
    instance._dia_extra_anterior = None
    if not raw and instance.pk and not _conciliando.get():
        instance._dia_extra_anterior = sender.objects.filter(pk=instance.pk).values_list('empleado_id', 'fecha').first()


@transaction.atomic
def conciliar_dia_extra(empleado_id, fecha, *, generar=True):
    if _conciliando.get():
        return
    from .services_extra_bloqueos import bloquear_jornadas_extra
    bloquear_jornadas_extra([(empleado_id, fecha)])
    token = _conciliando.set(True)
    try:
        from .services_asistencia_reglas import _evaluar_hora_extra
        asistencia = AsistenciaEmpleado.objects.select_related('turno', 'empleado').filter(empleado_id=empleado_id, fecha=fecha).first()
        touched = set()
        if asistencia:
            _evaluar_hora_extra(asistencia, touched, generar=generar)
        if not touched:
            IncidenciaAsistencia.objects.filter(empleado_id=empleado_id, fecha=fecha,
                tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE, editado_manual=False).exclude(
                estado=IncidenciaAsistencia.ESTADO_RESUELTO).update(estado=IncidenciaAsistencia.ESTADO_RESUELTO)
    finally:
        _conciliando.reset(token)


@receiver(post_save, sender=HoraExtra)
def conciliar_cambio_extra(sender, instance, raw=False, update_fields=None, **kwargs):
    if raw or _conciliando.get():
        return
    if update_fields is not None and not {'horas', 'estado', 'fecha', 'empleado', 'empleado_id', 'asistencia', 'asistencia_id'}.intersection(update_fields):
        return
    anterior = getattr(instance, '_dia_extra_anterior', None)
    if anterior and anterior != (instance.empleado_id, instance.fecha):
        conciliar_dia_extra(*anterior, generar=False)
    conciliar_dia_extra(instance.empleado_id, instance.fecha)


@receiver(post_delete, sender=HoraExtra)
def conciliar_eliminacion_extra(sender, instance, **kwargs):
    # Eliminar una solicitud no debe recrearla automáticamente en ese instante.
    conciliar_dia_extra(instance.empleado_id, instance.fecha, generar=False)
