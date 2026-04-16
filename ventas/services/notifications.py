from __future__ import annotations

from ventas.models import EventoVenta, EventoVentaNotification


def create_notification(event: EventoVenta, message: str, severity: str = EventoVentaNotification.SEVERITY_INFO) -> EventoVentaNotification:
    return EventoVentaNotification.objects.create(
        sales_event=event,
        message=(message or "").strip()[:255],
        severity=severity,
    )


def create_unique_notification(event: EventoVenta, message: str, severity: str = EventoVentaNotification.SEVERITY_INFO) -> EventoVentaNotification | None:
    normalized = (message or "").strip()[:255]
    if not normalized:
        return None
    existing = EventoVentaNotification.objects.filter(
        sales_event=event,
        message=normalized,
        severity=severity,
        status=EventoVentaNotification.STATUS_PENDIENTE,
    ).first()
    if existing:
        return existing
    return create_notification(event, normalized, severity)
