"""Resolución de horas extra compartida por web y APIs, con bloqueo por jornada."""
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.utils import timezone

from .models import HoraExtra
from .services_extra_bloqueos import bloquear_hora_extra
from .services_extra_conciliacion import contexto_hora_extra
from .services_horas_extra_jefatura import jefatura_hora_extra_actualizada


@transaction.atomic
def resolver_hora_extra(hora_extra_id, action, usuario, *, permitir_superusuario=True):
    """Bloquea asistencia antes de extras, también para capturas manuales.

    Las señales de HoraExtra pueden invocar el generador; comparten este orden.
    Los adaptadores conservan su alcance de consulta y formato de respuesta.
    """
    he, registros_dia = bloquear_hora_extra(hora_extra_id)
    if not jefatura_hora_extra_actualizada(he):
        return he, "", "La jefatura de esta detección cambió. Sincroniza el jefe directo antes de resolverla."
    if he.jefe_directo_id != usuario.id and not (permitir_superusuario and usuario.is_superuser):
        verbo = "rechazar" if action == "rechazar" else "autorizar"
        raise PermissionDenied(f"Solo el jefe directo asignado puede {verbo} esta hora extra.")
    if action not in {"autorizar", "rechazar"}:
        return he, "", "La acción solicitada no es válida."
    if he.estado != HoraExtra.ESTADO_PENDIENTE:
        return he, "", "La hora extra ya no está pendiente. Recarga la lista para revisar su estado actual."
    if action == "autorizar":
        if he.asistencia_id and (he.asistencia.empleado_id, he.asistencia.fecha) != (he.empleado_id, he.fecha):
            return he, "", "La asistencia vinculada no corresponde al empleado y fecha. Corrige y reevalúa antes de autorizar."
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
