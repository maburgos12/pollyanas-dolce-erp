"""Cambios de autorización concilian solo extra; nunca nómina o faltas."""
from contextvars import ContextVar

from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.db.models.signals import post_delete, post_save, pre_delete, pre_save
from django.dispatch import receiver

from .models import AsistenciaEmpleado, HoraExtra, IncidenciaAsistencia

_conciliando = ContextVar('rrhh_conciliando_extra', default=False)


@receiver(pre_delete, sender=AsistenciaEmpleado)
def proteger_origen_extra(sender, instance, using, origin=None, **kwargs):
    """SET_NULL no debe convertir una propuesta automática en captura manual."""
    from .services_extra_bloqueos import preparar_eliminacion_asistencia
    preparar_eliminacion_asistencia(instance, origin=origin, using=using)
    vinculadas = list(HoraExtra.objects.using(using).filter(asistencia_id=instance.pk))
    if vinculadas:
        raise ProtectedError("No se puede eliminar una asistencia vinculada a horas extra.", vinculadas)


@receiver(pre_save, sender=HoraExtra)
def guardar_fecha_anterior_extra(sender, instance, raw=False, using="default", update_fields=None, **kwargs):
    instance._dia_extra_anterior = None
    if raw:
        return  # Fixtures se cargan sin conciliación ni consultas a relaciones.
    from .services_extra_bloqueos import preparar_guardado_extra
    preparar_guardado_extra(instance, using=using, update_fields=update_fields)


@receiver(pre_delete, sender=HoraExtra)
def bloquear_eliminacion_extra(sender, instance, origin=None, using="default", **kwargs):
    from .services_extra_bloqueos import preparar_eliminacion_extra
    preparar_eliminacion_extra(instance, origin=origin, using=using)


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
    actual = instance._dia_extra_actual
    if anterior and anterior != actual:
        conciliar_dia_extra(*anterior, generar=False)
    conciliar_dia_extra(*actual)


@receiver(post_delete, sender=HoraExtra)
def conciliar_eliminacion_extra(sender, instance, **kwargs):
    # Eliminar una solicitud no debe recrearla automáticamente en ese instante.
    conciliar_dia_extra(instance.empleado_id, instance.fecha, generar=False)
