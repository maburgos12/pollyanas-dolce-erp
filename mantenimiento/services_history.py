from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.utils import timezone
from django.db.models import Prefetch
from django.http import Http404

from activos.models import OrdenMantenimiento
from fallas.models import BitacoraFalla, EvidenciaSeguimientoFalla, ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad
from mantenimiento.services_access import (
    authorized_fallas, authorized_orders, authorized_repairs, authorized_unit_reports,
    authorized_unit_services,
)


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
    return STATUS_MAP[source][value.upper()]


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


def _history_actor(user_id, full_name, username):
    if not user_id:
        return {"id": None, "label": "Sin autor registrado"}
    return {"id": user_id, "label": (full_name or username or "Usuario")}


def unified_history_rows(user, *, period):
    """Normalize authorized maintenance facts; one database row becomes one UID."""
    start, end = period_bounds(period)
    rows = []

    fallas = authorized_fallas(user).select_related("reportado_por").values(
        "id", "titulo", "descripcion", "estatus", "fecha_reporte", "fecha_cierre",
        "sucursal_id", "sucursal__nombre", "activo_relacionado_id", "activo_relacionado__nombre",
        "reportado_por_id", "reportado_por__first_name", "reportado_por__last_name", "reportado_por__username",
    )
    falla_states = {
        ReporteFalla.ESTATUS_ABIERTO: "abierto", ReporteFalla.ESTATUS_REVISION: "en_proceso",
        ReporteFalla.ESTATUS_PROCESO: "en_proceso", ReporteFalla.ESTATUS_RESUELTO: "cerrado",
        ReporteFalla.ESTATUS_CERRADO: "cerrado", ReporteFalla.ESTATUS_CANCELADO: "cancelado",
    }
    for item in fallas:
        state = falla_states[item["estatus"]]
        event = item["fecha_cierre"] if state == "cerrado" and item["fecha_cierre"] else item["fecha_reporte"]
        if _in_period(event, start, end):
            rows.append(_history_payload(
                uid=f"falla:{item['id']}", event=event, kind="reporte", state=state,
                branch_id=item["sucursal_id"], branch=item["sucursal__nombre"],
                subject_id=item["activo_relacionado_id"], subject=item["activo_relacionado__nombre"] or "",
                actor=_history_actor(item["reportado_por_id"], " ".join(filter(None, [item["reportado_por__first_name"], item["reportado_por__last_name"]])), item["reportado_por__username"]),
                origin="falla", title=item["titulo"], description=item["descripcion"],
                asset_id=item["activo_relacionado_id"],
            ))

    orders = authorized_orders(user).select_related("creado_por").values(
        "id", "folio", "descripcion", "estatus", "creado_en", "fecha_cierre", "origen", "plan_ref_id",
        "activo_ref_id", "activo_ref__nombre", "activo_ref__sucursal_id", "activo_ref__sucursal__nombre",
        "creado_por_id", "creado_por__first_name", "creado_por__last_name", "creado_por__username",
    )
    from activos.models import SolicitudFalla
    linked_order_ids = set(SolicitudFalla.objects.filter(orden_atencion_id__in=[x["id"] for x in orders]).values_list("orden_atencion_id", flat=True))
    for item in orders:
        state = canonical_status("orden", item["estatus"])
        event = item["fecha_cierre"] if state == "cerrado" and item["fecha_cierre"] else item["creado_en"]
        if _in_period(event, start, end):
            unreported = item["origen"] in {OrdenMantenimiento.ORIGEN_EMERGENCIA, OrdenMantenimiento.ORIGEN_INICIATIVA} and not item["plan_ref_id"] and item["id"] not in linked_order_ids
            rows.append(_history_payload(
                uid=f"orden:{item['id']}", event=event, kind="orden", state=state,
                branch_id=item["activo_ref__sucursal_id"], branch=item["activo_ref__sucursal__nombre"],
                subject_id=item["activo_ref_id"], subject=item["activo_ref__nombre"],
                actor=_history_actor(item["creado_por_id"], " ".join(filter(None, [item["creado_por__first_name"], item["creado_por__last_name"]])), item["creado_por__username"]),
                origin="sin_reporte" if unreported else item["origen"].lower(), title=item["folio"],
                description=item["descripcion"], asset_id=item["activo_ref_id"],
            ))

    reports = authorized_unit_reports(user).values(
        "id", "tipo", "descripcion", "estatus", "fecha_reporte", "fecha_cierre", "repartidor__user_id",
        "repartidor__user__first_name", "repartidor__user__last_name", "repartidor__user__username",
        "unidad_id", "unidad__codigo", "unidad__sucursal_id", "unidad__sucursal__nombre",
    )
    for item in reports:
        state = canonical_status("reporte_unidad", item["estatus"])
        if state == "programado": state = "en_proceso"
        event = item["fecha_cierre"] if state == "cerrado" and item["fecha_cierre"] else item["fecha_reporte"]
        if _in_period(event, start, end):
            rows.append(_history_payload(uid=f"reporte_unidad:{item['id']}", event=event, kind="reporte", state=state,
                branch_id=item["unidad__sucursal_id"], branch=item["unidad__sucursal__nombre"], subject_id=item["unidad_id"], subject=item["unidad__codigo"],
                actor=_history_actor(item["repartidor__user_id"], " ".join(filter(None, [item["repartidor__user__first_name"], item["repartidor__user__last_name"]])), item["repartidor__user__username"]),
                origin="reporte_unidad", title=item["tipo"], description=item["descripcion"], unit_id=item["unidad_id"]))

    for kind, queryset, date_field in (("reparacion", authorized_repairs(user), "fecha_ingreso"), ("servicio_unidad", authorized_unit_services(user), "fecha_servicio")):
        queryset = queryset.select_related("registrado_por", "unidad", "unidad__sucursal")
        for obj in queryset:
            event = getattr(obj, date_field)
            if not _in_period(event, start, end): continue
            is_service = kind == "servicio_unidad"
            rows.append(_history_payload(uid=f"{kind}:{obj.pk}", event=event, kind=kind, state="cerrado",
                branch_id=obj.unidad.sucursal_id, branch=obj.unidad.sucursal.nombre, subject_id=obj.unidad_id, subject=obj.unidad.codigo,
                actor=_history_actor(obj.registrado_por_id, obj.registrado_por.get_full_name() if obj.registrado_por else "", obj.registrado_por.get_username() if obj.registrado_por else ""),
                origin=kind, parent_uid=(f"reporte_unidad:{obj.reporte_origen_id}" if kind == "reparacion" and obj.reporte_origen_id else None),
                title=(obj.tipo_servicio.nombre if is_service else obj.descripcion_falla), description=(obj.notas or ("" if is_service else obj.descripcion_reparacion)),
                unit_id=obj.unidad_id, direct=is_service, invoice=(obj.archivo_factura.name if obj.archivo_factura else "")))
    return rows


def _history_payload(*, uid, event, kind, state, branch_id, branch, subject_id, subject, actor, origin,
                     title, description, asset_id=None, unit_id=None, parent_uid=None, direct=False, invoice=""):
    return {"uid": uid, "fecha_evento": _aware_date(event), "tipo": kind, "estado": state,
            "sucursal": {"id": branch_id, "label": branch or ""},
            "sujeto": ({"id": subject_id, "label": subject or ""} if subject_id else None), "actor": actor,
            "origen": origin, "parent_uid": parent_uid, "captura_directa": direct,
            "titulo": title or "", "descripcion": description or "", "activo_id": asset_id,
            "unidad_id": unit_id, "factura": invoice or ""}


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


def _person(user):
    if user is None:
        return None
    return {"id": user.pk, "nombre": user.get_full_name() or user.get_username()}


def _iso(value):
    if value is None:
        return None
    value = _aware_date(value)
    return value.astimezone(MAZATLAN).isoformat()


def _evidence_payload(kind, pk, file_field, display_name=""):
    if not file_field:
        return None
    import mimetypes
    from pathlib import PurePath
    name = PurePath(display_name or file_field.name).name
    return {
        "id": f"{kind}:{pk}", "nombre": name,
        "mime": mimetypes.guess_type(name)[0] or "application/octet-stream",
        "url": f"/api/mantenimiento/v2/evidencias/{kind}/{pk}/",
    }


def item_detail(user, kind, pk):
    if kind == "falla":
        evidence_qs = EvidenciaSeguimientoFalla.objects.order_by("creado_en", "id")
        log_qs = BitacoraFalla.objects.select_related("usuario").prefetch_related(
            Prefetch("evidencias", queryset=evidence_qs)
        ).order_by("timestamp", "id")
        report = authorized_fallas(user).select_related(
            "sucursal", "categoria", "activo_relacionado", "reportado_por", "asignado_a", "cerrado_por"
        ).prefetch_related(Prefetch("bitacora", queryset=log_qs)).filter(pk=pk).first()
        if report is None:
            raise Http404
        state = {
            ReporteFalla.ESTATUS_ABIERTO: "abierto", ReporteFalla.ESTATUS_REVISION: "en_proceso",
            ReporteFalla.ESTATUS_PROCESO: "en_proceso", ReporteFalla.ESTATUS_RESUELTO: "cerrado",
            ReporteFalla.ESTATUS_CERRADO: "cerrado", ReporteFalla.ESTATUS_CANCELADO: "cancelado",
        }[report.estatus]
        return {
            "schema_version": 2, "uid": f"falla:{report.pk}", "tipo": "falla",
            "estado": {"codigo": state, "etiqueta": report.get_estatus_display(), "grupo": state},
            "prioridad": {"codigo": report.prioridad, "etiqueta": report.get_prioridad_display().split(" - ")[0]},
            "reporte_inicial": {
                "titulo": report.titulo or "", "descripcion": report.descripcion or "",
                "foto": _evidence_payload("falla_inicial", report.pk, report.foto_evidencia),
                "reportado_por": _person(report.reportado_por), "fecha": _iso(report.fecha_reporte),
                "sucursal": {"id": report.sucursal_id, "nombre": report.sucursal.nombre or ""},
                "categoria": report.categoria.nombre or "", "area": report.get_area_display(),
                "activo": ({"id": report.activo_relacionado_id, "nombre": report.activo_relacionado.nombre}
                           if report.activo_relacionado_id else None),
            },
            "fechas": {"reporte": _iso(report.fecha_reporte), "asignacion": _iso(report.fecha_asignacion),
                       "resolucion": _iso(report.fecha_resolucion), "cierre": _iso(report.fecha_cierre)},
            "responsables": {"asignado_a": _person(report.asignado_a), "cerrado_por": _person(report.cerrado_por)},
            "seguimiento": [{
                "id": row.pk, "fecha": _iso(row.timestamp), "usuario": _person(row.usuario),
                "estatus_anterior": row.estatus_anterior or "", "estatus_nuevo": row.estatus_nuevo or "",
                "comentario": row.comentario or "",
                "evidencias": [_evidence_payload("seguimiento_falla", item.pk, item.archivo, item.nombre)
                                for item in row.evidencias.all()],
            } for row in report.bitacora.all()],
        }

    definitions = {
        "orden": (authorized_orders, ("activo_ref", "activo_ref__sucursal")),
        "reporte_unidad": (authorized_unit_reports, ("unidad", "unidad__sucursal")),
        "reparacion": (authorized_repairs, ("unidad", "unidad__sucursal", "registrado_por")),
        "servicio_unidad": (authorized_unit_services, ("unidad", "unidad__sucursal", "tipo_servicio", "registrado_por")),
    }
    if kind not in definitions:
        raise Http404
    authorize, related = definitions[kind]
    obj = authorize(user).select_related(*related).filter(pk=pk).first()
    if obj is None:
        raise Http404
    return {"schema_version": 2, "uid": f"{kind}:{obj.pk}", "tipo": kind, "detalle": {
        "id": obj.pk, "sucursal": {"id": (obj.activo_ref.sucursal_id if kind == "orden" else obj.unidad.sucursal_id),
                                    "nombre": (obj.activo_ref.sucursal.nombre if kind == "orden" else obj.unidad.sucursal.nombre)},
    }}
