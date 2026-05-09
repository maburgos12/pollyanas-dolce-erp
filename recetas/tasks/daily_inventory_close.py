from __future__ import annotations

import logging
from datetime import date, timedelta
from io import BytesIO

from celery import shared_task
from django.core.mail import EmailMessage
from django.utils import timezone

from pos_bridge.services.daily_inventory_close_service import DailyInventoryCloseService
from recetas.tasks.consolidado_nocturno import _consolidado_cedis_recipients, _from_email


logger = logging.getLogger(__name__)


@shared_task(name="recetas.inventario_final_cierre_email")
def inventario_final_cierre_email(fecha_operacion: str | None = None) -> dict:
    local_today = timezone.localdate()
    target_date = date.fromisoformat(fecha_operacion) if fecha_operacion else local_today - timedelta(days=1)
    recipients = _consolidado_cedis_recipients()
    if not recipients:
        logger.warning("No se envio inventario final cierre: Carolina Cayetano no tiene correo configurado.")
        return {"status": "omitido", "reason": "sin_destinatarios", "recipients": []}

    service = DailyInventoryCloseService()
    payload = service.build_close(fecha_operacion=target_date)
    workbook = service.build_workbook(payload)
    xlsx_buffer = BytesIO()
    workbook.save(xlsx_buffer)
    xlsx_buffer.seek(0)
    pdf_bytes = service.build_pdf_bytes(payload)

    subject = f"Inventario final al cierre - {target_date:%d/%m/%Y}"
    last_capture = payload["last_capture_at"].strftime("%d/%m/%Y %H:%M") if payload["last_capture_at"] else "Sin captura"
    body = (
        "Carolina,\n\n"
        "Adjunto va el inventario final al cierre del dia, tomado de los snapshots de Point en el ERP.\n\n"
        f"Fecha operativa: {target_date:%d/%m/%Y}\n"
        f"Zona horaria: {payload['timezone_name']}\n"
        f"Ultima captura Point usada: {last_capture}\n"
        f"Productos incluidos: {len(payload['rows'])}\n"
    )
    if payload["missing_branch_codes"]:
        body += f"Sucursales sin captura: {', '.join(payload['missing_branch_codes'])}\n"
    body += "\n-- ERP Pollyana's Dolce"

    email = EmailMessage(
        subject=subject,
        body=body,
        from_email=_from_email() or recipients[0],
        to=recipients,
    )
    filename_base = f"inventario_final_cierre_{target_date.isoformat()}"
    email.attach(
        f"{filename_base}.xlsx",
        xlsx_buffer.getvalue(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    email.attach(f"{filename_base}.pdf", pdf_bytes, "application/pdf")
    email.send(fail_silently=False)
    return {
        "status": "enviado",
        "fecha_operacion": target_date.isoformat(),
        "recipients": recipients,
        "rows": len(payload["rows"]),
        "missing_branch_codes": payload["missing_branch_codes"],
    }
