import logging

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail

logger = logging.getLogger(__name__)

EMOJI_PRIORIDAD = {
    "baja": "[Baja]",
    "media": "[Media]",
    "alta": "[Alta]",
    "critica": "[Critica]",
}

EMOJI_ESTATUS = {
    "abierto": "[Nuevo]",
    "en_revision": "[Revision]",
    "en_proceso": "[Proceso]",
    "resuelto": "[Resuelto]",
    "cerrado": "[Cerrado]",
    "cancelado": "[Cancelado]",
}


def _emails_de_grupo(nombre_grupo: str) -> list[str]:
    return list(
        get_user_model()
        .objects.filter(groups__name=nombre_grupo, is_active=True, email__isnull=False)
        .exclude(email="")
        .values_list("email", flat=True)
        .distinct()
    )


def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")


def _enviar_whatsapp_maya(telefono: str, mensaje: str):
    try:
        import httpx

        httpx.post(
            "https://api.pollyanasdolce.com/api/send-message/",
            json={"phone": telefono, "message": mensaje},
            timeout=10,
        )
    except Exception as exc:
        logger.warning("[fallas] WhatsApp Maya error: %s", exc)


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def notificar_nuevo_reporte(self, reporte_pk: int):
    """Notifica a compras cuando llega un nuevo reporte de falla."""

    from .models import ReporteFalla

    try:
        reporte = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por").get(pk=reporte_pk)
    except ReporteFalla.DoesNotExist:
        logger.error("[fallas] Reporte %s no encontrado.", reporte_pk)
        return {"enviado": False, "motivo": "reporte_no_encontrado", "reporte_id": reporte_pk}

    etiqueta = EMOJI_PRIORIDAD.get(reporte.prioridad, "[Falla]")
    asunto = f"{etiqueta} Nueva falla reportada - {reporte.sucursal.nombre} [{reporte.get_prioridad_display()}]"
    cuerpo = (
        "Se ha registrado una nueva falla:\n\n"
        f"Sucursal: {reporte.sucursal.nombre}\n"
        f"Categoria: {reporte.categoria}\n"
        f"Titulo: {reporte.titulo}\n"
        f"Prioridad: {reporte.get_prioridad_display()}\n"
        f"Reportado por: {reporte.reportado_por.get_full_name() or reporte.reportado_por.username}\n"
        f"Fecha: {reporte.fecha_reporte:%d/%m/%Y %H:%M}\n\n"
        f"Descripcion:\n{reporte.descripcion}\n\n"
        f"Ver reporte: https://erp.pollyanasdolce.com/fallas/reportes/{reporte.pk}/"
    )
    destinatarios = _emails_de_grupo("compras_logistica")
    if reporte.prioridad == ReporteFalla.PRIORIDAD_CRITICA:
        destinatarios += _emails_de_grupo("dg")
    destinatarios = sorted(set(destinatarios))

    if destinatarios:
        try:
            send_mail(
                subject=asunto,
                message=cuerpo,
                from_email=_from_email(),
                recipient_list=destinatarios,
                fail_silently=False,
            )
        except Exception as exc:
            logger.error("[fallas] Error enviando email: %s", exc)
            raise self.retry(exc=exc)

    try:
        encargados = get_user_model().objects.filter(groups__name="compras_logistica", is_active=True).distinct()
        for encargado in encargados:
            telefono = getattr(getattr(encargado, "profile", None), "telefono", None)
            if telefono:
                msg = (
                    f"{etiqueta} *Nueva falla - {reporte.sucursal.nombre}*\n"
                    f"{reporte.titulo}\n"
                    f"Prioridad: {reporte.get_prioridad_display()}\n"
                    f"Por: {reporte.reportado_por.get_full_name() or reporte.reportado_por.username}"
                )
                _enviar_whatsapp_maya(telefono, msg)
    except Exception as exc:
        logger.warning("[fallas] WhatsApp encargados: %s", exc)

    return {"enviado": bool(destinatarios), "destinatarios": len(destinatarios), "reporte_id": reporte.pk}


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def notificar_cambio_estatus(self, reporte_pk: int, nuevo_estatus: str, usuario_pk: int):
    """Notifica al reportador cuando cambia el estatus de su reporte."""

    from .models import ReporteFalla

    try:
        reporte = ReporteFalla.objects.select_related("sucursal", "reportado_por").get(pk=reporte_pk)
    except ReporteFalla.DoesNotExist:
        return {"enviado": False, "motivo": "reporte_no_encontrado", "reporte_id": reporte_pk}

    reportador = reporte.reportado_por
    if not reportador.email:
        return {"enviado": False, "motivo": "sin_email", "reporte_id": reporte.pk}

    etiqueta = EMOJI_ESTATUS.get(nuevo_estatus, "[Actualizacion]")
    asunto = f"{etiqueta} Actualización en tu reporte de falla - {reporte.get_estatus_display()}"
    cuerpo = (
        f"Hola {reportador.first_name or reportador.username},\n\n"
        "Tu reporte de falla ha sido actualizado:\n\n"
        f"Sucursal: {reporte.sucursal.nombre}\n"
        f"Falla: {reporte.titulo}\n"
        f"Nuevo estatus: {reporte.get_estatus_display()}\n\n"
        f"Detalle: https://erp.pollyanasdolce.com/fallas/reportes/{reporte.pk}/"
    )

    try:
        send_mail(
            subject=asunto,
            message=cuerpo,
            from_email=_from_email(),
            recipient_list=[reportador.email],
            fail_silently=False,
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    return {"enviado": True, "reporte_id": reporte.pk, "usuario_id": usuario_pk}
