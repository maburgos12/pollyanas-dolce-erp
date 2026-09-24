"""Gestión de la jornada semanal desde la ficha de un empleado."""

from dataclasses import dataclass
from datetime import date, timedelta
import re

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from core.access import can_manage_rrhh
from core.audit import log_event

from .models import AsignacionJornadaEmpleado, Empleado, JornadaSemanal
from .services_turnos import asignar_jornada_empleado


@dataclass(frozen=True)
class ResultadoJornada:
    cambio: bool
    mensaje: str
    asignacion: AsignacionJornadaEmpleado | None


def _fecha_desde_post(valor):
    if not valor or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", valor):
        raise ValidationError({"jornada_fecha_inicio": "Indica una fecha de inicio válida (AAAA-MM-DD)."})
    try:
        return date.fromisoformat(valor)
    except ValueError as exc:
        raise ValidationError({"jornada_fecha_inicio": "Indica una fecha de inicio válida (AAAA-MM-DD)."}) from exc


@transaction.atomic
def aplicar_jornada_desde_post(*, empleado, post, actor, creacion=False):
    if not can_manage_rrhh(actor):
        raise PermissionDenied("No tienes permisos para gestionar jornadas de RRHH")
    if "jornada_gestion_presente" not in post:
        return ResultadoJornada(False, "Jornada sin cambios.", None)

    jornada_id = (post.get("jornada_id") or "").strip()
    if not jornada_id and creacion:
        return ResultadoJornada(False, "Empleado registrado sin jornada semanal.", None)

    # La ficha envía el selector vigente aun al editar otros datos. Sin fecha ni
    # motivo explícitos, ese POST conserva la jornada y su historial.
    fecha_raw = (post.get("jornada_fecha_inicio") or "").strip()
    motivo_raw = (post.get("jornada_motivo") or "").strip()
    if not creacion and not fecha_raw and not motivo_raw:
        if not jornada_id:
            return ResultadoJornada(False, "Jornada sin cambios.", None)
        hoy = timezone.localdate()
        vigente_actual = AsignacionJornadaEmpleado.objects.filter(
            empleado_id=empleado.pk, fecha_inicio__lte=hoy
        ).filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=hoy)).order_by("-fecha_inicio", "-pk").first()
        if vigente_actual and jornada_id == str(vigente_actual.jornada_id):
            return ResultadoJornada(False, "Jornada sin cambios.", vigente_actual)

    Empleado.objects.select_for_update().get(pk=empleado.pk)
    if not jornada_id and not fecha_raw and not AsignacionJornadaEmpleado.objects.filter(empleado_id=empleado.pk).exists():
        return ResultadoJornada(False, "El empleado ya estaba sin jornada semanal.", None)

    if creacion and jornada_id and not fecha_raw and empleado.fecha_ingreso:
        fecha_inicio = empleado.fecha_ingreso
    else:
        fecha_inicio = _fecha_desde_post(fecha_raw)
    if empleado.fecha_ingreso and fecha_inicio < empleado.fecha_ingreso:
        raise ValidationError({"jornada_fecha_inicio": "El inicio de la jornada no puede ser anterior al ingreso."})

    vigentes = list(
        AsignacionJornadaEmpleado.objects.select_for_update()
        .filter(empleado_id=empleado.pk, fecha_inicio__lte=fecha_inicio)
        .filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=fecha_inicio))
        .order_by("-fecha_inicio", "-pk")[:2]
    )
    if len(vigentes) > 1:
        raise ValidationError({"jornada_fecha_inicio": "Hay jornadas traslapadas; revisa el historial antes de cambiarla."})
    vigente = vigentes[0] if vigentes else None
    if not jornada_id and not vigente:
        return ResultadoJornada(False, "El empleado ya estaba sin jornada semanal.", None)

    motivo = (post.get("jornada_motivo") or "").strip()
    if not motivo:
        raise ValidationError({"jornada_motivo": "Indica el motivo del cambio de jornada."})

    jornada = None
    if jornada_id:
        if not jornada_id.isdigit():
            raise ValidationError({"jornada_id": "Selecciona una jornada semanal válida."})
        jornada = JornadaSemanal.objects.filter(pk=int(jornada_id), activo=True).first()
        if jornada is None:
            raise ValidationError({"jornada_id": "Selecciona una jornada semanal activa."})

    if vigente and fecha_inicio <= vigente.fecha_inicio:
        if jornada and fecha_inicio == vigente.fecha_inicio and vigente.jornada_id == jornada.pk:
            return ResultadoJornada(False, "La jornada ya estaba asignada.", vigente)
        raise ValidationError({"jornada_fecha_inicio": "El cambio debe iniciar después de la jornada vigente."})

    if vigente:
        fin_anterior = vigente.fecha_fin
        vigente.fecha_fin = fecha_inicio - timedelta(days=1)
        vigente.full_clean()
        vigente.save(update_fields=["fecha_fin"])
        log_event(actor, "UPDATE", "rrhh.AsignacionJornadaEmpleado", str(vigente.pk), {
            "tipo": "cambio" if jornada else "cierre",
            "empleado_id": empleado.pk,
            "asignacion_id": vigente.pk,
            "jornada_id": vigente.jornada_id,
            "fecha_inicio": vigente.fecha_inicio.isoformat(),
            "fecha_fin_anterior": fin_anterior.isoformat() if fin_anterior else None,
            "fecha_fin_nueva": vigente.fecha_fin.isoformat(),
            "motivo": motivo,
        })

    if jornada is None:
        return ResultadoJornada(True, "Jornada semanal cerrada.", None)

    asignacion = asignar_jornada_empleado(
        empleado=empleado, jornada=jornada, fecha_inicio=fecha_inicio,
        fecha_fin=None, motivo=motivo, actor=actor,
    )
    log_event(actor, "CREATE", "rrhh.AsignacionJornadaEmpleado", str(asignacion.pk), {
        "tipo": "asignacion",
        "empleado_id": empleado.pk,
        "asignacion_id": asignacion.pk,
        "jornada_id": jornada.pk,
        "fecha_inicio": fecha_inicio.isoformat(),
        "fecha_fin": None,
        "motivo": motivo,
    })
    return ResultadoJornada(True, "Jornada semanal actualizada.", asignacion)
