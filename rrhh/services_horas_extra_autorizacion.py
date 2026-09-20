"""Resolución de horas extra compartida por web y APIs, con bloqueo por jornada."""
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import Http404
from django.shortcuts import get_object_or_404
from django.utils import timezone

from .models import AsistenciaEmpleado, HoraExtra
from .services_extra_conciliacion import contexto_hora_extra


@transaction.atomic
def resolver_hora_extra(hora_extra_id, action, usuario, *, permitir_superusuario=True):
    """Bloquea asistencia antes de extras, también para capturas manuales.

    Las señales de HoraExtra pueden invocar el generador; comparten este orden.
    Los adaptadores conservan su alcance de consulta y formato de respuesta.
    """
    identidad = get_object_or_404(
        HoraExtra.objects.only("empleado_id", "fecha", "asistencia_id"), pk=hora_extra_id,
    )
    asistencias_dia = list(
        AsistenciaEmpleado.objects.select_for_update(of=("self",)).filter(
            empleado_id=identidad.empleado_id, fecha=identidad.fecha,
        ).order_by("pk")
    )
    registros_dia = list(
        HoraExtra.objects.select_for_update(of=("self",)).filter(
            empleado_id=identidad.empleado_id, fecha=identidad.fecha,
        ).select_related(
            "empleado__sucursal_ref", "jefe_directo", "asistencia__empleado", "asistencia__turno",
        ).order_by("pk")
    )
    he = next((registro for registro in registros_dia if registro.pk == identidad.pk), None)
    if he is None:
        raise Http404("La hora extra ya no está disponible en esta jornada.")
    if he.jefe_directo_id != usuario.id and not (permitir_superusuario and usuario.is_superuser):
        verbo = "rechazar" if action == "rechazar" else "autorizar"
        raise PermissionDenied(f"Solo el jefe directo asignado puede {verbo} esta hora extra.")
    if he.asistencia_id != identidad.asistencia_id:
        return he, "", "La asistencia vinculada cambió. Recarga y reevalúa antes de autorizar."
    if he.asistencia_id and he.asistencia_id not in {asistencia.pk for asistencia in asistencias_dia}:
        return he, "", "La asistencia vinculada no corresponde al empleado y fecha. Corrige y reevalúa antes de autorizar."
    if action not in {"autorizar", "rechazar"}:
        return he, "", "La acción solicitada no es válida."
    if he.estado != HoraExtra.ESTADO_PENDIENTE:
        return he, "", "La hora extra ya no está pendiente. Recarga la lista para revisar su estado actual."
    if action == "autorizar":
        contexto = contexto_hora_extra(he, registros_dia)
        if not contexto["puede_autorizar"]:
            return he, "", contexto["motivo_bloqueo"]
        from .services import calcular_monto_hora_extra

        calcular_monto_hora_extra(he)
        he.estado = HoraExtra.ESTADO_AUTORIZADO
        message = f"Hora extra autorizada para {he.empleado.nombre}."
    else:
        he.estado = HoraExtra.ESTADO_RECHAZADO
        message = f"Hora extra rechazada para {he.empleado.nombre}."
    he.autorizado_por = usuario
    he.fecha_autorizacion_jefe = timezone.now()
    he.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
    return he, message, ""
