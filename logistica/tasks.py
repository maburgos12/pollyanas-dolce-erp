from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.html import strip_tags

from .models import ReporteUnidad


def _emails_de_grupo(nombre_grupo: str) -> list[str]:
    return list(
        get_user_model()
        .objects.filter(groups__name=nombre_grupo, email__isnull=False)
        .exclude(email="")
        .values_list("email", flat=True)
        .distinct()
    )


def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")


@shared_task
def notificar_reporte_nuevo(reporte_id):
    try:
        reporte = ReporteUnidad.objects.select_related("unidad", "repartidor__user").get(pk=reporte_id)
    except ReporteUnidad.DoesNotExist:
        return {"enviado": False, "motivo": "reporte_no_encontrado", "reporte_id": reporte_id}

    context = {
        "reporte": reporte,
        "ticket_url": f"/logistica/tickets/?ticket={reporte.id}",
    }
    html_message = render_to_string("logistica/emails/reporte_nuevo.html", context)
    plain_message = strip_tags(html_message)
    from_email = _from_email()

    compras_emails = _emails_de_grupo("compras_logistica")
    dg_emails = _emails_de_grupo("dg") if reporte.severidad in ["urgente", "critico"] else []

    enviados = 0
    if compras_emails:
        send_mail(
            subject=f"Nuevo reporte de unidad {reporte.unidad.codigo}",
            message=plain_message,
            from_email=from_email,
            recipient_list=compras_emails,
            html_message=html_message,
            fail_silently=False,
        )
        enviados += 1

    if dg_emails:
        send_mail(
            subject=f"Reporte {reporte.get_severidad_display()} de unidad {reporte.unidad.codigo}",
            message=plain_message,
            from_email=from_email,
            recipient_list=dg_emails,
            html_message=html_message,
            fail_silently=False,
        )
        enviados += 1

    return {"enviado": bool(enviados), "grupos_notificados": enviados, "reporte_id": reporte.id}


@shared_task
def escalar_tickets_sin_respuesta():
    limite = timezone.now() - timedelta(hours=2)
    tickets = ReporteUnidad.objects.select_related("unidad", "repartidor__user").filter(
        estatus=ReporteUnidad.ESTATUS_ABIERTO,
        severidad__in=[ReporteUnidad.SEVERIDAD_URGENTE, ReporteUnidad.SEVERIDAD_CRITICO],
        fecha_reporte__lte=limite,
        notificacion_escalada=False,
    )
    ticket_ids = list(tickets.values_list("id", flat=True))
    if not ticket_ids:
        return {"escalados": 0}

    dg_emails = _emails_de_grupo("dg")
    if dg_emails:
        html_message = render_to_string("logistica/emails/escalado.html", {"tickets": tickets})
        send_mail(
            subject="Escalado de tickets de logística sin respuesta",
            message=strip_tags(html_message),
            from_email=_from_email(),
            recipient_list=dg_emails,
            html_message=html_message,
            fail_silently=False,
        )

    actualizados = ReporteUnidad.objects.filter(id__in=ticket_ids).update(notificacion_escalada=True)
    return {"escalados": actualizados}
