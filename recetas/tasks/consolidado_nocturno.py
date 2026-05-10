from __future__ import annotations

import logging
import time
from io import BytesIO
from datetime import date, timedelta

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage
from django.utils import timezone

from config.email_backends import retrieve_resend_email
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


def _dedupe_emails(emails: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for email in emails:
        normalized = (email or "").strip().lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(email.strip())
    return deduped


def _consolidado_cedis_cc(recipients: list[str]) -> list[str]:
    recipient_set = {(email or "").strip().lower() for email in recipients}
    cc = [
        email
        for email in (getattr(settings, "CONSOLIDADO_CEDIS_EXPORT_CC", []) or [])
        if (email or "").strip().lower() not in recipient_set
    ]
    return _dedupe_emails(cc)


def _retrieve_resend_last_event(email_id: str) -> str:
    if not email_id:
        return ""
    last_error = None
    for attempt in range(5):
        try:
            resend_status = retrieve_resend_email(email_id)
            return str(resend_status.get("last_event") or "")
        except Exception as exc:  # noqa: BLE001 - Resend puede tardar segundos en indexar el correo
            last_error = exc
            if attempt < 4:
                time.sleep(3)
    raise RuntimeError(str(last_error))


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
    cc_recipients = _consolidado_cedis_cc(recipients)
    if not recipients:
        logger.warning(
            "No se envio solicitudes CEDIS: Carolina Cayetano no tiene correo y no hay CONSOLIDADO_CEDIS_EXPORT_RECIPIENTS."
        )
        return {"status": "omitido", "reason": "sin_destinatarios", "recipients": []}

    metadata = consolidado.metadata or {}
    transfer_request_date_raw = metadata.get("transfer_request_date") or (
        consolidado.fecha_operacion - timedelta(days=1)
    ).isoformat()
    try:
        transfer_request_date_label = date.fromisoformat(str(transfer_request_date_raw)).strftime("%d/%m/%Y")
    except ValueError:
        transfer_request_date_label = str(transfer_request_date_raw)
    workbook = _build_solicitudes_sucursal_workbook(consolidado.fecha_operacion)
    attachment = BytesIO()
    workbook.save(attachment)
    attachment.seek(0)
    filename = f"cedis_solicitudes_sucursales_{consolidado.fecha_operacion.isoformat()}.xlsx"
    subject = f"Solicitudes por sucursal CEDIS - {consolidado.fecha_operacion:%d/%m/%Y}"
    body = (
        "Carolina,\n\n"
        "Adjunto va el Excel de solicitudes por sucursal generado automaticamente por el ERP.\n\n"
        f"Fecha de solicitud: {transfer_request_date_label}\n"
        f"Fecha de aplicación del plan: {consolidado.fecha_operacion:%d/%m/%Y}\n"
        f"Productos consolidados: {consolidado.productos_consolidados}\n"
        f"Sucursales con solicitud: {consolidado.sucursales_con_solicitud}/{consolidado.sucursales_esperadas}\n\n"
        "Tambien puedes revisarlo en el ERP:\n"
        "https://erp.pollyanasdolce.com/recetas/consolidado-cedis/\n\n"
        "-- ERP Pollyana's Dolce"
    )
    attachment_bytes = attachment.getvalue()
    delivery_results: list[dict] = []

    def send_copy(target: str, role: str) -> dict:
        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=_from_email() or recipients[0],
            to=[target],
        )
        email.attach(
            filename,
            attachment_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        email.send(fail_silently=False)

        resend_email_id = getattr(email, "resend_email_id", "")
        resend_last_event = ""
        if resend_email_id:
            try:
                resend_last_event = _retrieve_resend_last_event(resend_email_id)
            except Exception as exc:  # noqa: BLE001 - no bloquea el reporte ya aceptado por Resend
                logger.warning(
                    "No se pudo verificar estatus Resend del consolidado CEDIS %s para %s: %s",
                    consolidado.id,
                    target,
                    exc,
                )
        return {
            "email": target,
            "role": role,
            "resend_email_id": resend_email_id,
            "resend_last_event": resend_last_event,
        }

    for recipient in recipients:
        delivery_results.append(send_copy(recipient, "primary"))

    for cc_recipient in cc_recipients:
        try:
            delivery_results.append(send_copy(cc_recipient, "copy"))
        except Exception as exc:  # noqa: BLE001 - una copia no debe impedir el envio principal
            logger.warning("No se pudo enviar copia CEDIS a %s: %s", cc_recipient, exc)
            delivery_results.append(
                {
                    "email": cc_recipient,
                    "role": "copy",
                    "error": str(exc),
                }
            )

    sent_at = timezone.now().isoformat()
    primary_delivery = next((row for row in delivery_results if row.get("role") == "primary"), {})
    resend_email_id = primary_delivery.get("resend_email_id", "")
    resend_last_event = primary_delivery.get("resend_last_event", "")
    metadata.update(
        {
            "solicitudes_sucursal_email_sent_at": sent_at,
            "solicitudes_sucursal_email_recipients": recipients,
            "solicitudes_sucursal_email_cc": cc_recipients,
            "solicitudes_sucursal_email_filename": filename,
            "solicitudes_sucursal_resend_email_id": resend_email_id,
            "solicitudes_sucursal_resend_last_event": resend_last_event,
            "solicitudes_sucursal_email_deliveries": delivery_results,
        }
    )
    ConsolidadoNocturnoCEDIS.objects.filter(pk=consolidado.pk).update(metadata=metadata)
    status = "entregado" if resend_last_event == "delivered" else "enviado"
    return {
        "status": status,
        "sent_at": sent_at,
        "recipients": recipients,
        "cc": cc_recipients,
        "filename": filename,
        "resend_email_id": resend_email_id,
        "resend_last_event": resend_last_event,
        "deliveries": delivery_results,
    }


@shared_task(name="recetas.consolidado_nocturno_cedis")
def consolidado_nocturno_cedis(
    fecha_operacion: str | None = None,
    sincronizar_point: bool = True,
    sincronizar_inventario_cedis: bool = True,
    forzar_recalculo: bool = True,
    enviar_excel_carolina: bool = True,
    forzar_envio_excel: bool = False,
) -> dict:
    local_today = timezone.localdate()
    target_date = date.fromisoformat(fecha_operacion) if fecha_operacion else local_today + timedelta(days=1)
    transfer_request_date = target_date - timedelta(days=1)
    consolidado = ConsolidadoNocturnoCedisService().consolidar(
        fecha_operacion=target_date,
        sincronizar_point=sincronizar_point,
        sincronizar_inventario_cedis=sincronizar_inventario_cedis,
        forzar_recalculo=forzar_recalculo,
        fecha_transferencias=transfer_request_date,
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
