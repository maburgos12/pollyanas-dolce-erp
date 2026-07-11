from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.utils import timezone


MAZATLAN = ZoneInfo("America/Mazatlan")

STATUS_MAP = {
    "orden": {
        "PENDIENTE": "abierto",
        "EN_PROCESO": "en_proceso",
        "CERRADA": "cerrado",
        "CANCELADA": "cancelado",
    },
    "reporte_unidad": {
        "ABIERTO": "abierto",
        "EN_PROCESO": "en_proceso",
        "PROGRAMADO": "programado",
        "CERRADO": "cerrado",
        "CANCELADO": "cancelado",
    },
}


def canonical_status(source, value):
    return STATUS_MAP[source][value]


def period_bounds(period, *, now=None):
    local_now = (now or timezone.now()).astimezone(MAZATLAN)
    today = local_now.date()
    next_day = datetime.combine(today + timedelta(days=1), datetime.min.time(), MAZATLAN)

    if period == "30d":
        return next_day - timedelta(days=30), next_day
    if period == "90d":
        return next_day - timedelta(days=90), next_day
    if period == "semana":
        start = datetime.combine(
            today - timedelta(days=today.weekday()),
            datetime.min.time(),
            MAZATLAN,
        )
        return start, start + timedelta(days=7)
    if period == "mes":
        start = datetime(today.year, today.month, 1, tzinfo=MAZATLAN)
        if today.month == 12:
            end = datetime(today.year + 1, 1, 1, tzinfo=MAZATLAN)
        else:
            end = datetime(today.year, today.month + 1, 1, tzinfo=MAZATLAN)
        return start, end
    if period == "todo":
        return None, next_day
    raise ValueError("Periodo no soportado")
