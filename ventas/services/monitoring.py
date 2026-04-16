from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.utils import timezone

from ventas.models import EventoVenta, EventoVentaNotification
from ventas.services.notifications import create_unique_notification
from ventas.services.postmortem import build_postmortem


ACTIVE_STATUSES = {
    EventoVenta.STATUS_APROBADO,
    EventoVenta.STATUS_APROBADO_AJUSTES,
    EventoVenta.STATUS_ENVIADO_PROD,
    EventoVenta.STATUS_VALIDADO_PROD,
    EventoVenta.STATUS_ENVIADO_COMPRAS,
    EventoVenta.STATUS_EN_EJECUCION,
    EventoVenta.STATUS_CERRADO,
}


def monitor_active_events() -> dict:
    today = timezone.localdate()
    window_start = today - timedelta(days=7)
    window_end = today + timedelta(days=2)
    events = EventoVenta.objects.filter(
        status__in=ACTIVE_STATUSES,
        analysis_end_date__gte=window_start,
        analysis_start_date__lte=window_end,
    ).order_by("main_date", "id")

    processed = 0
    warnings = 0
    for event in events:
        if event.analysis_start_date <= today <= event.analysis_end_date and event.status != EventoVenta.STATUS_EN_EJECUCION:
            event.status = EventoVenta.STATUS_EN_EJECUCION
            event.save(update_fields=["status", "updated_at"])
            create_unique_notification(event, "Evento en ejecucion: monitoreo diario activo.")

        result = build_postmortem(event)
        summary = result.get("summary", {})
        forecast_total = Decimal(str(summary.get("forecast_total_qty") or 0))
        actual_total = Decimal(str(summary.get("actual_total_qty") or 0))
        if forecast_total > 0:
            deviation = abs(actual_total - forecast_total) / forecast_total
            if deviation >= Decimal("0.20"):
                warnings += 1
                create_unique_notification(
                    event,
                    f"Desviacion diaria relevante: forecast {forecast_total:.2f} vs real {actual_total:.2f}.",
                    EventoVentaNotification.SEVERITY_WARN,
                )
        processed += 1

    return {"processed": processed, "warnings": warnings}
