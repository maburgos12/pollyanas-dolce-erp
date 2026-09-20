"""Serialización PostgreSQL por jornada, incluso antes de existir asistencia."""
from hashlib import blake2b

from django.db import connection
from django.db.models import Q
from django.http import Http404
from django.shortcuts import get_object_or_404

from .models import AsistenciaEmpleado, HoraExtra


def bloquear_jornadas_extra(jornadas):
    """Advisory xact locks ordenados; adquirir ANTES de cualquier bloqueo de fila.

    El namespace evita compartir claves con otros dominios. No usar hash() de
    Python: cambia entre procesos. Una colisión solo serializa jornadas de más.
    """
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
    if he is None or (he.empleado_id, he.fecha, he.asistencia_id) != (
        identidad.empleado_id, identidad.fecha, identidad.asistencia_id,
    ):
        raise Http404("La jornada cambió. Recarga el registro antes de continuar.")
    if he.asistencia_id and (he.asistencia.empleado_id, he.asistencia.fecha) not in jornadas:
        raise Http404("La jornada de asistencia cambió. Recarga antes de continuar.")
    return he, [r for r in registros if (r.empleado_id, r.fecha) == (he.empleado_id, he.fecha)]
