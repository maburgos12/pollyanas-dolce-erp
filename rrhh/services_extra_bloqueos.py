"""Serialización PostgreSQL por jornada, incluso antes de existir asistencia."""
from hashlib import blake2b

from django.core.exceptions import ValidationError
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.models import Q
from django.http import Http404
from django.shortcuts import get_object_or_404

from .models import AsistenciaEmpleado, HoraExtra


class JornadaExtraConflict(Exception):
    """El registro existe, pero su identidad cambió durante la espera."""


def bloquear_jornadas_extra(jornadas, *, using=DEFAULT_DB_ALIAS):
    """Advisory xact locks ordenados; adquirir ANTES de cualquier bloqueo de fila.

    El namespace evita compartir claves con otros dominios. No usar hash() de
    Python: cambia entre procesos. Una colisión solo serializa jornadas de más.
    """
    connection = connections[using]
    if not connection.in_atomic_block:
        raise RuntimeError("El bloqueo de jornada requiere transaction.atomic().")
    claves = {
        int.from_bytes(blake2b(
            f"rrhh:extra:{empleado_id}:{fecha}".encode(), digest_size=8,
        ).digest(), "big", signed=True)
        for empleado_id, fecha in jornadas
    }
    with connection.cursor() as cursor:
        for clave in sorted(claves):
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [clave])


def _bloquear_asistencias(jornadas, *, using):
    if not jornadas:
        return
    filtro = Q()
    for empleado_id, fecha in jornadas:
        filtro |= Q(empleado_id=empleado_id, fecha=fecha)
    list(AsistenciaEmpleado.objects.using(using).select_for_update(of=("self",)).filter(filtro).order_by("pk"))


def preparar_guardado_extra(instance, *, using, update_fields):
    """Adquiere origen/destino antes del SQL y detecta identidad movida al esperar."""
    registros = HoraExtra.objects.using(using)
    observada = registros.filter(pk=instance.pk).values_list("empleado_id", "fecha", "asistencia_id").first() if instance.pk else None
    campos = {"empleado_id", "fecha", "asistencia_id"} if update_fields is None else set(update_fields)
    destino = (
        instance.empleado_id if observada is None or {"empleado", "empleado_id"} & campos else observada[0],
        HoraExtra._meta.get_field("fecha").to_python(instance.fecha) if observada is None or "fecha" in campos else observada[1],
        instance.asistencia_id if observada is None or {"asistencia", "asistencia_id"} & campos else observada[2],
    )
    jornadas = {destino[:2]}
    if observada:
        jornadas.add(observada[:2])
    vinculadas = {destino[2], observada[2] if observada else None} - {None}
    jornadas.update(AsistenciaEmpleado.objects.using(using).filter(pk__in=vinculadas).values_list("empleado_id", "fecha"))
    bloquear_jornadas_extra(jornadas, using=using)
    _bloquear_asistencias(jornadas, using=using)
    actual = registros.select_for_update().filter(pk=instance.pk).values_list("empleado_id", "fecha", "asistencia_id").first() if instance.pk else None
    if actual != observada:
        raise ValidationError("La jornada cambió mientras se guardaba. Recarga y reintenta.")
    instance._dia_extra_anterior = observada[:2] if observada else None
    instance._dia_extra_actual = destino[:2]


def preparar_eliminacion_extra(instance, *, origin, using):
    """El collector emite pre_delete antes de SQL; bloquea el lote completo."""
    from django.db.models import QuerySet
    from .models import Empleado

    registros = HoraExtra.objects.using(using)
    seleccion = Q(pk=instance.pk)
    if isinstance(origin, QuerySet) and origin.model is HoraExtra:
        seleccion |= Q(pk__in=origin.values("pk"))
    elif isinstance(origin, QuerySet) and origin.model is Empleado:
        seleccion |= Q(empleado_id__in=origin.values("pk"))
    elif isinstance(origin, Empleado):
        seleccion |= Q(empleado_id=origin.pk)
    observadas = list(registros.filter(seleccion).order_by("pk").values_list("pk", "empleado_id", "fecha"))
    jornadas = {(empleado_id, fecha) for _, empleado_id, fecha in observadas}
    jornadas.update(_jornadas_cascada_empleado(origin, using=using))
    bloquear_jornadas_extra(jornadas, using=using)
    _bloquear_asistencias(jornadas, using=using)
    actuales = list(registros.select_for_update().filter(pk__in=[r[0] for r in observadas]).order_by("pk").values_list("pk", "empleado_id", "fecha"))
    fecha = HoraExtra._meta.get_field("fecha").to_python(instance.fecha)
    if actuales != observadas or (instance.pk, instance.empleado_id, fecha) not in actuales:
        raise ValidationError("La jornada cambió mientras se eliminaba. Recarga y reintenta.")


def _jornadas_cascada_empleado(origin, *, using):
    """Ambos modelos del collector comparten el conjunto antes de tomar filas."""
    from django.db.models import QuerySet
    from .models import Empleado

    if isinstance(origin, QuerySet) and origin.model is Empleado:
        empleados = origin.values("pk")
    elif isinstance(origin, Empleado):
        empleados = [origin.pk]
    else:
        return set()
    jornadas = set(AsistenciaEmpleado.objects.using(using).filter(empleado_id__in=empleados).values_list("empleado_id", "fecha"))
    jornadas.update(HoraExtra.objects.using(using).filter(empleado_id__in=empleados).values_list("empleado_id", "fecha"))
    return jornadas


def preparar_eliminacion_asistencia(instance, *, origin, using):
    """Bloquea todo el lote/cascada en orden, no en el orden de PK del collector."""
    from django.db.models import QuerySet
    from .models import Empleado

    registros = AsistenciaEmpleado.objects.using(using)
    seleccion = Q(pk=instance.pk)
    if isinstance(origin, QuerySet) and origin.model is AsistenciaEmpleado:
        seleccion |= Q(pk__in=origin.values("pk"))
    elif isinstance(origin, QuerySet) and origin.model is Empleado:
        seleccion |= Q(empleado_id__in=origin.values("pk"))
    elif isinstance(origin, Empleado):
        seleccion |= Q(empleado_id=origin.pk)
    observadas = list(registros.filter(seleccion).order_by("pk").values_list("pk", "empleado_id", "fecha"))
    jornadas = {(empleado_id, fecha) for _, empleado_id, fecha in observadas}
    jornadas.update(_jornadas_cascada_empleado(origin, using=using))
    bloquear_jornadas_extra(jornadas, using=using)
    actuales = list(registros.select_for_update().filter(pk__in=[r[0] for r in observadas]).order_by("pk").values_list("pk", "empleado_id", "fecha"))
    fecha = AsistenciaEmpleado._meta.get_field("fecha").to_python(instance.fecha)
    if actuales != observadas or (instance.pk, instance.empleado_id, fecha) not in actuales:
        raise JornadaExtraConflict("La jornada cambió mientras se eliminaba. Recarga y reintenta.")


def bloquear_hora_extra(hora_extra_id, *, jornadas_adicionales=()):
    """Relee bajo bloqueos jornada → asistencias → extras, ambos por pk."""
    identidad = get_object_or_404(
        HoraExtra.objects.select_related("asistencia"), pk=hora_extra_id,
    )
    jornadas = {(identidad.empleado_id, identidad.fecha), *jornadas_adicionales}
    if identidad.asistencia_id:
        jornadas.add((identidad.asistencia.empleado_id, identidad.asistencia.fecha))
    bloquear_jornadas_extra(jornadas)
    filtro = Q()
    for empleado_id, fecha in jornadas:
        filtro |= Q(empleado_id=empleado_id, fecha=fecha)
    list(AsistenciaEmpleado.objects.select_for_update(of=("self",)).filter(filtro).order_by("pk"))
    registros = list(HoraExtra.objects.select_for_update(of=("self",)).filter(filtro).select_related(
        "empleado__sucursal_ref", "jefe_directo", "asistencia__empleado", "asistencia__turno",
    ).order_by("pk"))
    he = next((registro for registro in registros if registro.pk == identidad.pk), None)
    if he is None and not HoraExtra.objects.filter(pk=identidad.pk).exists():
        raise Http404("La hora extra ya no existe.")
    if he is None or (he.empleado_id, he.fecha, he.asistencia_id) != (
        identidad.empleado_id, identidad.fecha, identidad.asistencia_id,
    ):
        raise JornadaExtraConflict("La jornada cambió. Recarga el registro antes de continuar.")
    if he.asistencia_id and (he.asistencia.empleado_id, he.asistencia.fecha) not in jornadas:
        raise JornadaExtraConflict("La jornada de asistencia cambió. Recarga antes de continuar.")
    return he, [r for r in registros if (r.empleado_id, r.fecha) == (he.empleado_id, he.fecha)]
