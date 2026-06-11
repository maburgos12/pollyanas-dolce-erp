from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.core.management import call_command
from django.db.models import Q
from django.utils import timezone

from core.models import Notificacion
from core.access import can_review_seguimiento_global
from core.notificaciones import crear_notificacion
from rrhh.models import Empleado

from .models import ActividadCalendario, SeguimientoChecklistItem, SeguimientoItem

logger = logging.getLogger(__name__)


def _enviar_whatsapp_maya(telefono: str, mensaje: str):
    try:
        import httpx

        httpx.post(
            "https://api.pollyanasdolce.com/api/send-message/",
            json={"phone": telefono, "message": mensaje},
            timeout=10,
        )
    except Exception as exc:
        logger.warning("[seguimiento] WhatsApp Maya error: %s", exc)


def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")


def _day_bounds(start, end):
    tz = timezone.get_current_timezone()
    start_dt = timezone.make_aware(datetime.combine(start, datetime.min.time()), tz)
    end_dt = timezone.make_aware(datetime.combine(end, datetime.max.time()), tz)
    return start_dt, end_dt


def _empleado_de_usuario(user):
    empleado = getattr(user, "empleado_rrhh", None)
    if empleado:
        return empleado
    if user.email:
        return Empleado.objects.filter(email__iexact=user.email).first()
    return None


def _items_para_usuario(user):
    empleado = _empleado_de_usuario(user)
    filters = Q(responsable_user=user) | Q(participantes_user=user)
    if empleado:
        filters |= Q(responsable_empleado=empleado) | Q(participantes_empleado=empleado)
    return SeguimientoItem.objects.filter(filters).distinct()


def _conteos_recordatorio_usuario(user, hoy, manana) -> dict[str, int]:
    start_dt, end_dt = _day_bounds(hoy, manana)
    items = _items_para_usuario(user).exclude(
        estatus__in=[SeguimientoItem.ESTATUS_COMPLETADO, SeguimientoItem.ESTATUS_CANCELADO]
    )
    items_count = items.filter(fecha_limite__gte=start_dt, fecha_limite__lte=end_dt).count()
    pasos_count = SeguimientoChecklistItem.objects.filter(
        seguimiento__in=items.values("pk"),
        vence__gte=start_dt,
        vence__lte=end_dt,
        completado=False,
    ).count()
    actividad_filters = Q(usuario=user) | Q(creado_por=user) | Q(invitado_user=user)
    if can_review_seguimiento_global(user):
        actividad_filters |= Q(direccion_general=True)
    actividades_count = ActividadCalendario.objects.filter(
        actividad_filters,
        activo=True,
        estatus=ActividadCalendario.ESTATUS_PENDIENTE,
        fecha__gte=hoy,
        fecha__lte=manana,
    ).distinct().count()
    return {
        "seguimientos": items_count,
        "pasos": pasos_count,
        "actividades": actividades_count,
    }


def _mensaje_recordatorio(conteos: dict[str, int], hoy, manana) -> str:
    partes = []
    if conteos["seguimientos"]:
        partes.append(f"{conteos['seguimientos']} seguimiento(s)")
    if conteos["pasos"]:
        partes.append(f"{conteos['pasos']} paso(s)")
    if conteos["actividades"]:
        partes.append(f"{conteos['actividades']} actividad(es)")
    resumen = ", ".join(partes)
    return f"Tienes {resumen} con vencimiento entre {hoy:%d/%m/%Y} y {manana:%d/%m/%Y}."


@shared_task(
    name="seguimiento.importar_agente_dg",
    bind=True,
    acks_late=True,
    max_retries=0,
    time_limit=900,
    soft_time_limit=840,
)
def task_importar_agente_dg_seguimiento(self, *, limit: int = 0) -> dict[str, object]:
    stdout = io.StringIO()
    call_command("importar_agente_dg_seguimiento", limit=int(limit or 0), stdout=stdout)
    return {
        "ok": True,
        "task_id": getattr(getattr(self, "request", None), "id", None),
        "output": stdout.getvalue()[-4000:],
    }


@shared_task(name="seguimiento.recordatorios_calendario")
def recordatorios_calendario() -> dict[str, int]:
    hoy = timezone.localdate()
    manana = hoy + timedelta(days=1)
    conteos = {"usuarios_notificados": 0, "correos": 0, "whatsapps": 0}
    User = get_user_model()

    for user in User.objects.filter(is_active=True).select_related("userprofile").order_by("id"):
        resumen = _conteos_recordatorio_usuario(user, hoy, manana)
        if not any(resumen.values()):
            continue
        mensaje = _mensaje_recordatorio(resumen, hoy, manana)
        crear_notificacion(
            usuario=user,
            titulo="Recordatorio de calendario",
            mensaje=mensaje,
            url="/seguimiento/calendario/",
            tipo=Notificacion.TIPO_SEGUIMIENTO,
        )
        conteos["usuarios_notificados"] += 1

        if user.email:
            send_mail(
                "Recordatorio de calendario",
                f"{mensaje}\n\nAbrir calendario: https://erp.pollyanasdolce.com/seguimiento/calendario/",
                _from_email(),
                [user.email],
                fail_silently=True,
            )
            conteos["correos"] += 1

        telefono = (getattr(getattr(user, "userprofile", None), "telefono", "") or "").strip()
        if telefono:
            _enviar_whatsapp_maya(telefono, f"Recordatorio de calendario: {mensaje}")
            conteos["whatsapps"] += 1

    return conteos
