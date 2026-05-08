from __future__ import annotations

import logging
from io import BytesIO
from datetime import date

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage
from django.utils import timezone

from recetas.models import ConsolidadoNocturnoCEDIS
from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService
from recetas.views.reabasto import _build_solicitudes_sucursal_workbook


logger = logging.getLogger(__name__)


def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")


def _consolidado_cedis_recipients() -> list[str]:
    recipients: list[str] = []
    User = get_user_model()
    user = (
        User.objects.filter(username__iexact="carolina.cayetano").first()
        or User.objects.filter(username__iexact="produccion.carolina@pollyanasdolce.com").first()
        or User.objects.filter(email__iexact="produccion.carolina@pollyanasdolce.com").first()
    )
    if user and (user.email or "").strip():
        recipients.append(user.email.strip())
    recipients.extend(getattr(settings, "CONSOLIDADO_CEDIS_EXPORT_RECIPIENTS", []) or [])
    deduped: list[str] = []
    seen: set[str] = set()
    for email in recipients:
        normalized = (email or "").strip().lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(email.strip())
    return deduped


def enviar_solicitudes_sucursal_cedis(
    *,
    consolidado: ConsolidadoNocturnoCEDIS,
    forzar_envio: bool = False,
) -> dict:
    metadata = consolidado.metadata or {}
    if metadata.get("solicitudes_sucursal_email_sent_at") and not forzar_envio:
        return {
            "status": "omitido",
            "reason": "ya_enviado",
            "sent_at": metadata.get("solicitudes_sucursal_email_sent_at"),
            "recipients": metadata.get("solicitudes_sucursal_email_recipients", []),
        }

    recipients = _consolidado_cedis_recipients()
    if not recipients:
        logger.warning(
            "No se envio solicitudes CEDIS: Carolina Cayetano no tiene correo y no hay CONSOLIDADO_CEDIS_EXPORT_RECIPIENTS."
        )
        return {"status": "omitido", "reason": "sin_destinatarios", "recipients": []}

    workbook = _build_solicitudes_sucursal_workbook(consolidado.fecha_operacion)
    attachment = BytesIO()
    workbook.save(attachment)
    attachment.seek(0)
    filename = f"cedis_solicitudes_sucursales_{consolidado.fecha_operacion.isoformat()}.xlsx"
    subject = f"Solicitudes por sucursal CEDIS - {consolidado.fecha_operacion:%d/%m/%Y}"
    body = (
        "Carolina,\n\n"
        "Adjunto va el Excel de solicitudes por sucursal generado automaticamente por el ERP.\n\n"
        f"Fecha de solicitud: {consolidado.fecha_operacion:%d/%m/%Y}\n"
        f"Productos consolidados: {consolidado.productos_consolidados}\n"
        f"Sucursales con solicitud: {consolidado.sucursales_con_solicitud}/{consolidado.sucursales_esperadas}\n\n"
        "Tambien puedes revisarlo en el ERP:\n"
        "https://erp.pollyanasdolce.com/recetas/consolidado-cedis/\n\n"
        "-- ERP Pollyana's Dolce"
    )
    email = EmailMessage(
        subject=subject,
        body=body,
        from_email=_from_email() or recipients[0],
        to=recipients,
    )
    email.attach(
        filename,
        attachment.getvalue(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    email.send(fail_silently=False)

    sent_at = timezone.now().isoformat()
    metadata.update(
        {
            "solicitudes_sucursal_email_sent_at": sent_at,
            "solicitudes_sucursal_email_recipients": recipients,
            "solicitudes_sucursal_email_filename": filename,
        }
    )
    ConsolidadoNocturnoCEDIS.objects.filter(pk=consolidado.pk).update(metadata=metadata)
    return {"status": "enviado", "sent_at": sent_at, "recipients": recipients, "filename": filename}


@shared_task(name="recetas.consolidado_nocturno_cedis")
def consolidado_nocturno_cedis(
    fecha_operacion: str | None = None,
    sincronizar_point: bool = True,
    sincronizar_inventario_cedis: bool = True,
    forzar_recalculo: bool = True,
    enviar_excel_carolina: bool = True,
    forzar_envio_excel: bool = False,
) -> dict:
    target_date = date.fromisoformat(fecha_operacion) if fecha_operacion else timezone.localdate()
    consolidado = ConsolidadoNocturnoCedisService().consolidar(
        fecha_operacion=target_date,
        sincronizar_point=sincronizar_point,
        sincronizar_inventario_cedis=sincronizar_inventario_cedis,
        forzar_recalculo=forzar_recalculo,
    )
    email_result = {"status": "desactivado"}
    if enviar_excel_carolina:
        try:
            email_result = enviar_solicitudes_sucursal_cedis(
                consolidado=consolidado,
                forzar_envio=forzar_envio_excel,
            )
        except Exception as exc:  # noqa: BLE001 - el correo no debe invalidar el consolidado ya generado
            logger.exception("Error enviando Excel de solicitudes CEDIS para %s", target_date)
            email_result = {"status": "error_envio", "error": str(exc)}
    return {
        "consolidado_id": consolidado.id,
        "fecha_operacion": consolidado.fecha_operacion.isoformat(),
        "estado": consolidado.estado,
        "plan_produccion_id": consolidado.plan_produccion_id,
        "inventory_sync_job_id": (consolidado.metadata or {}).get("inventory_sync_job_id"),
        "sync_job_id": consolidado.sync_job_id,
        "sucursales_esperadas": consolidado.sucursales_esperadas,
        "sucursales_con_solicitud": consolidado.sucursales_con_solicitud,
        "productos_consolidados": consolidado.productos_consolidados,
        "total_plan_produccion": str(consolidado.total_plan_produccion),
        "email_solicitudes_sucursal": email_result,
    }
