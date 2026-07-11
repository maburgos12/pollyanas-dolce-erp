from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.utils import timezone

from activos.models import OrdenMantenimiento
from fallas.models import ReporteFalla
from logistica.models import ReporteUnidad
from mantenimiento.services_access import authorized_fallas, authorized_orders, authorized_unit_reports


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


def _aware_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if timezone.is_aware(value) else timezone.make_aware(value, MAZATLAN)
    return datetime.combine(value, datetime.min.time(), MAZATLAN)


def _in_period(value, start, end):
    event = _aware_date(value)
    if event is None:
        return start is None
    return (start is None or event >= start) and event < end


def inbox_rows(user, *, period, origin):
    """Return authorized, normalized inbox rows with a fixed query count."""
    start, end = period_bounds(period)
    rows = []
    if origin in {"sucursales", "todos"}:
        fallas = authorized_fallas(user).exclude(estatus=ReporteFalla.ESTATUS_CANCELADO).values(
            "id", "titulo", "descripcion", "prioridad", "estatus", "fecha_reporte",
            "fecha_resolucion", "fecha_cierre", "sucursal_id", "sucursal__nombre",
        )
        for row in fallas:
            state = {
                ReporteFalla.ESTATUS_ABIERTO: "abierto",
                ReporteFalla.ESTATUS_REVISION: "en_proceso",
                ReporteFalla.ESTATUS_PROCESO: "en_proceso",
                ReporteFalla.ESTATUS_RESUELTO: "cerrado",
                ReporteFalla.ESTATUS_CERRADO: "cerrado",
            }.get(row["estatus"])
            event = (row["fecha_cierre"] or row["fecha_resolucion"]) if state == "cerrado" else row["fecha_reporte"]
            if state and _in_period(event, start, end):
                rows.append(_row_payload(
                    uid=f"falla:{row['id']}", pk=row["id"], kind="falla", origin="sucursales",
                    state=state, critical=row["prioridad"] == ReporteFalla.PRIORIDAD_CRITICA,
                    event=event, title=row["titulo"], description=row["descripcion"],
                    branch_id=row["sucursal_id"], branch_name=row["sucursal__nombre"],
                ))

        orders = authorized_orders(user).exclude(estatus=OrdenMantenimiento.ESTATUS_CANCELADA).values(
            "id", "folio", "descripcion", "prioridad", "estatus", "creado_en", "fecha_cierre",
            "activo_ref__sucursal_id", "activo_ref__sucursal__nombre", "activo_ref_id", "activo_ref__nombre",
        )
        for row in orders:
            state = canonical_status("orden", row["estatus"])
            event = row["fecha_cierre"] if state == "cerrado" else row["creado_en"]
            if _in_period(event, start, end):
                rows.append(_row_payload(
                    uid=f"orden:{row['id']}", pk=row["id"], kind="orden", origin="sucursales",
                    state=state, critical=row["prioridad"] == OrdenMantenimiento.PRIORIDAD_CRITICA,
                    event=event, title=row["folio"], description=row["descripcion"],
                    branch_id=row["activo_ref__sucursal_id"], branch_name=row["activo_ref__sucursal__nombre"],
                    subject_id=row["activo_ref_id"], subject=row["activo_ref__nombre"],
                ))

    if origin in {"logistica", "todos"}:
        reports = authorized_unit_reports(user).values(
            "id", "tipo", "descripcion", "severidad", "estatus", "fecha_reporte",
            "fecha_cierre",
            "unidad_id", "unidad__codigo", "unidad__sucursal_id", "unidad__sucursal__nombre",
        )
        for row in reports:
            state = canonical_status("reporte_unidad", row["estatus"])
            event = row["fecha_cierre"] if state == "cerrado" else row["fecha_reporte"]
            if state != "cancelado" and _in_period(event, start, end):
                rows.append(_row_payload(
                    uid=f"reporte_unidad:{row['id']}", pk=row["id"], kind="reporte_unidad", origin="logistica",
                    state=state, critical=row["severidad"] == ReporteUnidad.SEVERIDAD_CRITICO,
                    event=event, title=row["tipo"], description=row["descripcion"],
                    branch_id=row["unidad__sucursal_id"], branch_name=row["unidad__sucursal__nombre"],
                    subject_id=row["unidad_id"], subject=row["unidad__codigo"],
                ))
    return rows


def _row_payload(*, uid, pk, kind, origin, state, critical, event, title, description,
                 branch_id, branch_name, subject_id=None, subject=""):
    event = _aware_date(event)
    return {
        "uid": uid, "id": pk, "tipo": kind, "origen": origin,
        "estado": state, "critico": critical, "fecha_evento": event,
        "titulo": title or "", "descripcion": description or "",
        "sucursal": {"id": branch_id, "nombre": branch_name or ""},
        "sujeto": {"id": subject_id, "nombre": subject or ""} if subject_id else None,
    }
