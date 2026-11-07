"""Proyección de comunicaciones; el despacho pertenece al servicio de acuerdos."""
from django.contrib import messages
import hashlib
import logging
import os
from django.db import transaction

from seguimiento import agente_dg_client


CHANNELS = {"email": "Correo", "whatsapp": "WhatsApp"}
STATUS_LABELS = {
    "not_requested": "no solicitado", "pending": "pendiente", "in_progress": "en proceso",
    "accepted": "aceptado por el proveedor", "delivered": "entregado", "read": "leído",
    "failed": "fallido", "unknown": "resultado incierto; consultar antes de reintentar",
}
logger = logging.getLogger(__name__)


def _store_result(item, result):
    with transaction.atomic():
        current = type(item).objects.select_for_update().get(pk=item.pk)
        metadata = dict(current.metadata or {})
        identity = lambda row: (row.get("content_version"), row.get("event_type"), row.get("event_key"))
        previous = next((row for row in metadata.get("communications", []) if isinstance(row, dict) and identity(row) == identity(result)), {})
        result = {**result, "channels": {channel: (previous.get("channels", {}).get(channel, state) if state.get("status") == "not_requested" else state)
                                          for channel, state in result.get("channels", {}).items() if isinstance(state, dict)}}
        events = [row for row in metadata.get("communications", []) if isinstance(row, dict)
                  and identity(row) != identity(result)]
        events.append(result)
        metadata["communications"] = events
        current.metadata = metadata
        current.save(update_fields=["metadata", "updated_at"])
        from seguimiento.services import AgenteDGSeguimientoImporter
        AgenteDGSeguimientoImporter()._sync_communication_notifications(current)
        item.metadata = current.metadata


def dispatch_event(item, event_type, *, actor=None, message=""):
    """Un envío de minuta siempre pasa por el mismo servicio, incluido el flujo ERP."""
    metadata = item.metadata or {}
    if metadata.get("source_table") != "minute_agreements":
        return False
    if os.getenv("AGENTE_DG_WRITEBACK_ENABLED", "").lower() not in {"1", "true", "yes", "on"}:
        return False
    from core.models import AuditLog
    audit = AuditLog.objects.filter(model="SeguimientoItem", object_id=str(item.pk)).order_by("-id").first()
    stamp = f"audit:{audit.pk}" if audit else hashlib.sha256(f"{item.updated_at.isoformat()}:{message}".encode()).hexdigest()
    event_key = f"erp:{item.pk}:{event_type}:{stamp}"
    actor_name = (actor.get_full_name() or actor.username) if actor else ""
    event_message = f"{actor_name}: {message}" if actor_name and message else message
    try:
        if not agente_dg_client.is_configured():
            raise agente_dg_client.AgenteDGError("Integración no configurada")
        if "send_email_on_create" not in metadata or "send_whatsapp_on_create" not in metadata:
            source = agente_dg_client.get_minute_agreement(metadata["source_id"])
            metadata = {**metadata, "send_email_on_create": source.get("send_email_on_create", False),
                        "send_whatsapp_on_create": source.get("send_whatsapp_on_create", False)}
        result = agente_dg_client.send_minute_agreement(
            metadata["source_id"], send_email=True if event_type in {"submitted", "evidence_added", "extension_requested"} else bool(metadata.get("send_email_on_create")),
            send_whatsapp=False if event_type in {"submitted", "evidence_added", "extension_requested"} else bool(metadata.get("send_whatsapp_on_create")), event_type=event_type,
            event_key=event_key, event_message=event_message,
        )
    except (agente_dg_client.AgenteDGError, OSError):
        logger.warning("No se confirmó comunicación de minuta ERP %s, evento %s", item.pk, event_type)
        result = {"minute_id": metadata.get("source_id"), "event_type": event_type, "event_key": event_key,
                  "channels": {channel: {"status": "unknown"} for channel in CHANNELS}}
    _store_result(item, result)
    return True


def communication_rows(item):
    rows = []
    for event in (item.metadata or {}).get("communications", []):
        if not isinstance(event, dict):
            continue
        for channel, label in CHANNELS.items():
            state = (event.get("channels") or {}).get(channel, {})
            if not isinstance(state, dict):
                continue
            rows.append({"channel": label, "status": STATUS_LABELS.get(state.get("status"), "sin confirmación"),
                         "event_type": event.get("event_type", ""), "event_key": event.get("event_key", ""),
                         "updated_at": state.get("updated_at") or "",
                         "error": _public_error(state.get("error"))})
    return rows


def _public_error(error):
    text = str(error or "")[:240]
    if any(secret in text.lower() for secret in ("password", "token", "api_key", "authorization", "bearer", "sk-")):
        return "El proveedor no confirmó la operación. Consulta el historial técnico autorizado."
    return text


def notify_agreement(request, item, *, source_id=None, event_type="assignment", event_key=None, force_resend=False, retry_channels=None):
    metadata = (item.metadata or {}) if item else {}
    source_id = source_id or metadata.get("source_id")
    try:
        result = agente_dg_client.send_minute_agreement(
            source_id, send_email=(retry_channels.get("email", False) if retry_channels is not None else request.POST.get("send_email") == "on"),
            send_whatsapp=(retry_channels.get("whatsapp", False) if retry_channels is not None else request.POST.get("send_whatsapp") == "on"),
            event_type=event_type, event_key=event_key,
            force_resend=force_resend, resend_reason=request.POST.get("motivo_reenvio", ""), request_id=request.POST.get("request_id"),
        )
    except (agente_dg_client.AgenteDGError, OSError):
        messages.warning(request, "El acuerdo está guardado. No se confirmó el envío; consulta sus comunicaciones antes de reintentar.")
        return None
    # Solo espejo del resultado; el registro atómico de envío está en el origen.
    if item:
        _store_result(item, result)
    summaries = []
    incomplete = False
    for channel, label in CHANNELS.items():
        state = result.get("channels", {}).get(channel, {}).get("status", "unknown")
        summaries.append(f"{label}: {STATUS_LABELS.get(state, 'sin confirmación')}")
        incomplete |= state in {"failed", "unknown", "pending", "in_progress"}
    (messages.warning if incomplete else messages.success)(request, "; ".join(summaries) + ".")
    return result
