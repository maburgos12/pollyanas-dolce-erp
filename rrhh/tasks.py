from __future__ import annotations

from celery import shared_task
from django.utils import timezone


@shared_task
def sync_asistencia_point():
    """
    Futura integración: jalar checadas de PointMeUp via pos_bridge.
    """
    return {"ok": True, "procesados": 0, "fuente": "point_placeholder"}


@shared_task
def alertar_he_pendientes():
    """
    Envía email si hay horas extra en estado pendiente por más de 24 horas.
    """
    from datetime import timedelta

    from django.core.mail import send_mail

    from .models import HoraExtra

    umbral = timezone.now() - timedelta(hours=24)
    count = HoraExtra.objects.filter(estado=HoraExtra.ESTADO_PENDIENTE, creado_en__lt=umbral).count()
    if count <= 0:
        return {"ok": True, "pendientes": 0}

    send_mail(
        subject=f"[ERP] {count} horas extra pendientes de autorización",
        message=f"Hay {count} registros de horas extra esperando autorización en el ERP.",
        from_email="no-reply@pollyanasdolce.com",
        recipient_list=["mauricio@pollyanasdolce.com"],
    )
    return {"ok": True, "pendientes": count}
