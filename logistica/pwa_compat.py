from __future__ import annotations

from datetime import datetime, time

from django.conf import settings
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime


def v59_compat_deadline() -> datetime | None:
    """Return the explicit v59 replay deadline; an empty value disables it."""
    raw = str(getattr(settings, "LOGISTICA_PWA_V59_COMPAT_UNTIL", "") or "").strip()
    if not raw:
        return None
    deadline = parse_datetime(raw)
    if deadline is None:
        parsed_date = parse_date(raw)
        if parsed_date is None:
            raise ValueError("LOGISTICA_PWA_V59_COMPAT_UNTIL debe ser una fecha o fecha-hora ISO 8601.")
        deadline = datetime.combine(parsed_date, time.max)
    if timezone.is_naive(deadline):
        deadline = timezone.make_aware(deadline, timezone.get_current_timezone())
    return deadline


def v59_compat_active(*, now: datetime | None = None) -> bool:
    try:
        deadline = v59_compat_deadline()
    except ValueError:
        return False
    if deadline is None:
        return False
    current = now or timezone.now()
    if timezone.is_naive(current):
        current = timezone.make_aware(current, timezone.get_current_timezone())
    return current <= deadline


def is_exact_v59_replay_contract(*, queue_id, client_event_id, client_context) -> bool:
    queue_id = str(queue_id or "").strip()
    if not queue_id or len(queue_id) > 60:
        return False
    if any(not (char.isalnum() or char in "._:-") for char in queue_id):
        return False
    if not isinstance(client_context, dict):
        return False
    return bool(
        client_event_id == f"offline-v59-{queue_id}"
        and client_context.get("client_version") == "pwa-v59-offline"
        and client_context.get("causa") == "GPS_SIN_SENAL"
        and client_context.get("client_timestamp")
    )
