from __future__ import annotations

import logging
from datetime import date, timedelta
from io import BytesIO

from celery import shared_task
from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage
from django.utils import timezone

from pos_bridge.services.daily_inventory_close_service import DAILY_CLOSE_CATEGORY_ORDER, DailyInventoryCloseService
from recetas.tasks.consolidado_nocturno import _consolidado_cedis_recipients, _from_email


logger = logging.getLogger(__name__)
INVENTARIO_CIERRE_PRIMARY_USERNAMES = ("carolina.cayetano", "produccion.carolina@pollyanasdolce.com")
INVENTARIO_CIERRE_CC_USERNAMES = ("johana.lopez",)
INVENTARIO_CIERRE_CC_EMAILS = ("ventas.johanna@pollyanasdolce.com", "maburgos12@pollyanasdolce.com")


def _dedupe_emails(values: list[str]) -> list[str]:
    seen = set()
    emails = []
    for value in values:
        email = (value or "").strip()
        key = email.lower()
        if not email or key in seen:
            continue
        seen.add(key)
        emails.append(email)
    return emails


def _emails_for_usernames(usernames: tuple[str, ...]) -> list[str]:
    User = get_user_model()
    users = User.objects.filter(username__in=usernames, is_active=True).only("username", "email")
    return [user.email for user in users if user.email]


def _inventario_cierre_recipient_groups() -> tuple[list[str], list[str]]:
    primary = _dedupe_emails(
        [
            *_consolidado_cedis_recipients(),
            *_emails_for_usernames(INVENTARIO_CIERRE_PRIMARY_USERNAMES),
            "produccion.carolina@pollyanasdolce.com",
        ]
    )
    cc = _dedupe_emails([*_emails_for_usernames(INVENTARIO_CIERRE_CC_USERNAMES), *INVENTARIO_CIERRE_CC_EMAILS])
    primary_keys = {email.lower() for email in primary}
    cc = [email for email in cc if email.lower() not in primary_keys]
    return primary, cc


@shared_task(name="recetas.inventario_final_cierre_email")
def inventario_final_cierre_email(
    fecha_operacion: str | None = None,
    category_filter: list[str] | str | None = None,
) -> dict:
    local_today = timezone.localdate()
    target_date = date.fromisoformat(fecha_operacion) if fecha_operacion else local_today - timedelta(days=1)
    if category_filter == "reposteria_cierre":
        category_filter = DAILY_CLOSE_CATEGORY_ORDER
    elif isinstance(category_filter, str):
        category_filter = [item.strip() for item in category_filter.split(",") if item.strip()]
    recipients, cc = _inventario_cierre_recipient_groups()
    if not recipients:
        logger.warning("No se envio inventario final cierre: no hay destinatarios configurados.")
        return {"status": "omitido", "reason": "sin_destinatarios", "recipients": [], "cc": []}

    service = DailyInventoryCloseService()
    payload = service.build_close(fecha_operacion=target_date, category_filter=category_filter)
    workbook = service.build_workbook(payload)
    xlsx_buffer = BytesIO()
    workbook.save(xlsx_buffer)
    xlsx_buffer.seek(0)
    pdf_bytes = service.build_pdf_bytes(payload)

    subject = f"Inventario final al cierre - {target_date:%d/%m/%Y}"
    if category_filter:
        subject += " - categorias clave"
    last_capture = payload["last_capture_at"].strftime("%d/%m/%Y %H:%M") if payload["last_capture_at"] else "Sin captura"
    body = (
        "Carolina,\n\n"
        "Adjunto va el inventario final al cierre del dia, tomado de los snapshots de Point en el ERP.\n\n"
        f"Fecha operativa: {target_date:%d/%m/%Y}\n"
        f"Zona horaria: {payload['timezone_name']}\n"
        f"Ultima captura Point usada: {last_capture}\n"
        f"Productos incluidos: {len(payload['rows'])}\n"
    )
    if category_filter:
        body += "Categorias incluidas: " + ", ".join(category_filter) + "\n"
    if payload["missing_branch_codes"]:
        body += f"Sucursales sin captura: {', '.join(payload['missing_branch_codes'])}\n"
    body += "\n-- ERP Pollyana's Dolce"

    email = EmailMessage(
        subject=subject,
        body=body,
        from_email=_from_email() or recipients[0],
        to=recipients,
        cc=cc,
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
        "cc": cc,
        "rows": len(payload["rows"]),
        "category_filter": category_filter or [],
        "missing_branch_codes": payload["missing_branch_codes"],
    }
