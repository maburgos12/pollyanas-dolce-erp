from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.utils import timezone
from django.db.models import Case, Exists, F, OuterRef, Prefetch, Q, When
from django.db.models.functions import Coalesce
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


def unified_history_rows(user, *, period, include_costs=False, filters=None, candidate_limit=None):
    """Normalize authorized maintenance facts; one database row becomes one UID."""
    start, end = period_bounds(period)
    filters = filters or {}
    rows = []
    total = 0

    fallas = authorized_fallas(user).select_related("reportado_por")
    if filters.get("tipo") not in {None, "todo", "reporte"}:
        fallas = fallas.none()
    if filters.get("sucursal"): fallas = fallas.filter(sucursal_id=filters["sucursal"])
    if filters.get("activo"): fallas = fallas.filter(activo_relacionado_id=filters["activo"])
    if filters.get("q"): fallas = fallas.filter(Q(titulo__icontains=filters["q"]) | Q(descripcion__icontains=filters["q"]))
    falla_statuses = {"abierto": [ReporteFalla.ESTATUS_ABIERTO], "en_proceso": [ReporteFalla.ESTATUS_REVISION, ReporteFalla.ESTATUS_PROCESO], "cerrado": [ReporteFalla.ESTATUS_RESUELTO, ReporteFalla.ESTATUS_CERRADO], "cancelado": [ReporteFalla.ESTATUS_CANCELADO]}
    if filters.get("estado") not in {None, "todo"}: fallas = fallas.filter(estatus__in=falla_statuses[filters["estado"]])
    closed = [ReporteFalla.ESTATUS_RESUELTO, ReporteFalla.ESTATUS_CERRADO]
    fq = Q(estatus__in=closed, fecha_cierre__gte=start, fecha_cierre__lt=end) if start else Q(estatus__in=closed, fecha_cierre__lt=end)
    rq = Q(estatus__in=closed, fecha_cierre__isnull=True, fecha_resolucion__gte=start, fecha_resolucion__lt=end) if start else Q(estatus__in=closed, fecha_cierre__isnull=True, fecha_resolucion__lt=end)
    oq = Q(fecha_reporte__gte=start, fecha_reporte__lt=end) if start else Q(fecha_reporte__lt=end)
    fallas = fallas.filter(fq | rq | (~Q(estatus__in=closed) & oq))
    total += fallas.count()
    fallas = fallas.annotate(effective_event=Case(
        When(estatus__in=closed, then=Coalesce("fecha_cierre", "fecha_resolucion", "fecha_reporte")),
        default=F("fecha_reporte"),
    )).order_by("-effective_event", "-id").values(
        "id", "titulo", "descripcion", "estatus", "fecha_reporte", "fecha_resolucion", "fecha_cierre",
        "sucursal_id", "sucursal__nombre", "activo_relacionado_id", "activo_relacionado__nombre",
        "reportado_por_id", "reportado_por__first_name", "reportado_por__last_name", "reportado_por__username",
    )[:candidate_limit]
    falla_states = {
        ReporteFalla.ESTATUS_ABIERTO: "abierto", ReporteFalla.ESTATUS_REVISION: "en_proceso",
        ReporteFalla.ESTATUS_PROCESO: "en_proceso", ReporteFalla.ESTATUS_RESUELTO: "cerrado",
        ReporteFalla.ESTATUS_CERRADO: "cerrado", ReporteFalla.ESTATUS_CANCELADO: "cancelado",
    }
    for item in fallas:
        state = falla_states[item["estatus"]]
        event = ((item["fecha_cierre"] or item.get("fecha_resolucion"))
                 if state == "cerrado" else item["fecha_reporte"])
        if _in_period(event, start, end):
            rows.append(_history_payload(
                uid=f"falla:{item['id']}", event=event, kind="reporte", state=state,
                branch_id=item["sucursal_id"], branch=item["sucursal__nombre"],
                subject_id=item["activo_relacionado_id"], subject=item["activo_relacionado__nombre"] or "",
                actor=_history_actor(item["reportado_por_id"], " ".join(filter(None, [item["reportado_por__first_name"], item["reportado_por__last_name"]])), item["reportado_por__username"]),
                origin="falla", title=item["titulo"], description=item["descripcion"],
                asset_id=item["activo_relacionado_id"],
            ))

    from activos.models import SolicitudFalla
    orders = authorized_orders(user).select_related("creado_por").annotate(
        has_linked_request=Exists(SolicitudFalla.objects.filter(orden_atencion_id=OuterRef("pk")))
    )
    if filters.get("tipo") not in {None, "todo", "orden", "sin_reporte"}: orders = orders.none()
    if filters.get("sucursal"): orders = orders.filter(activo_ref__sucursal_id=filters["sucursal"])
    if filters.get("activo"): orders = orders.filter(activo_ref_id=filters["activo"])
    if filters.get("q"): orders = orders.filter(Q(folio__icontains=filters["q"]) | Q(descripcion__icontains=filters["q"]) | Q(activo_ref__nombre__icontains=filters["q"]))
    unreported = Q(origen__in=[OrdenMantenimiento.ORIGEN_EMERGENCIA, OrdenMantenimiento.ORIGEN_INICIATIVA], plan_ref__isnull=True, has_linked_request=False)
    if filters.get("tipo") == "sin_reporte": orders = orders.filter(unreported)
    elif filters.get("tipo") == "orden": orders = orders.exclude(unreported)
    order_statuses = {"abierto": [OrdenMantenimiento.ESTATUS_PENDIENTE], "en_proceso": [OrdenMantenimiento.ESTATUS_EN_PROCESO], "cerrado": [OrdenMantenimiento.ESTATUS_CERRADA], "cancelado": [OrdenMantenimiento.ESTATUS_CANCELADA]}
    if filters.get("estado") not in {None, "todo"}: orders = orders.filter(estatus__in=order_statuses[filters["estado"]])
    cq = Q(estatus=OrdenMantenimiento.ESTATUS_CERRADA, fecha_cierre__gte=start.date(), fecha_cierre__lt=end.date()) if start else Q(estatus=OrdenMantenimiento.ESTATUS_CERRADA, fecha_cierre__lt=end.date())
    nq = Q(fecha_programada__gte=start.date(), fecha_programada__lt=end.date()) if start else Q(fecha_programada__lt=end.date())
    orders = orders.filter(cq | (~Q(estatus=OrdenMantenimiento.ESTATUS_CERRADA) & nq))
    total += orders.count()
    orders = orders.annotate(effective_event=Case(
        When(estatus=OrdenMantenimiento.ESTATUS_CERRADA, then=F("fecha_cierre")),
        default=F("fecha_programada"),
    )).order_by("-effective_event", "-id").values(
        "id", "folio", "descripcion", "estatus", "creado_en", "fecha_programada", "fecha_cierre", "origen", "plan_ref_id", "numero_factura", "factura_archivo", "costo_repuestos", "costo_mano_obra", "costo_otros",
        "activo_ref_id", "activo_ref__nombre", "activo_ref__sucursal_id", "activo_ref__sucursal__nombre",
        "creado_por_id", "creado_por__first_name", "creado_por__last_name", "creado_por__username",
    )[:candidate_limit]
    scoped_order_ids = authorized_orders(user).values_list("id", flat=True)
    linked_order_ids = set(SolicitudFalla.objects.filter(
        orden_atencion_id__in=scoped_order_ids,
    ).values_list("orden_atencion_id", flat=True))
    for item in orders:
        state = canonical_status("orden", item["estatus"])
        event = item["fecha_cierre"] if state == "cerrado" and item["fecha_cierre"] else item["fecha_programada"]
        if _in_period(event, start, end):
            unreported = item["origen"] in {OrdenMantenimiento.ORIGEN_EMERGENCIA, OrdenMantenimiento.ORIGEN_INICIATIVA} and not item["plan_ref_id"] and item["id"] not in linked_order_ids
            rows.append(_history_payload(
                uid=f"orden:{item['id']}", event=event, kind=("sin_reporte" if unreported else "orden"), state=state,
                branch_id=item["activo_ref__sucursal_id"], branch=item["activo_ref__sucursal__nombre"],
                subject_id=item["activo_ref_id"], subject=item["activo_ref__nombre"],
                actor=_history_actor(item["creado_por_id"], " ".join(filter(None, [item["creado_por__first_name"], item["creado_por__last_name"]])), item["creado_por__username"]),
                origin="sin_reporte" if unreported else item["origen"].lower(), title=item["folio"],
                description=item["descripcion"], asset_id=item["activo_ref_id"], direct=unreported,
                invoice=_invoice("orden_factura", item["id"], item["factura_archivo"], item["numero_factura"]),
                cost=((item["costo_repuestos"] or 0) + (item["costo_mano_obra"] or 0) + (item["costo_otros"] or 0)) if include_costs else None,
            ))

    reports = authorized_unit_reports(user)
    if filters.get("tipo") not in {None, "todo", "reporte"}: reports = reports.none()
    if filters.get("sucursal"): reports = reports.filter(unidad__sucursal_id=filters["sucursal"])
    if filters.get("unidad"): reports = reports.filter(unidad_id=filters["unidad"])
    if filters.get("q"): reports = reports.filter(Q(tipo__icontains=filters["q"]) | Q(descripcion__icontains=filters["q"]) | Q(unidad__codigo__icontains=filters["q"]))
    report_statuses = {"abierto": [ReporteUnidad.ESTATUS_ABIERTO], "en_proceso": [ReporteUnidad.ESTATUS_EN_PROCESO, ReporteUnidad.ESTATUS_PROGRAMADO], "cerrado": [ReporteUnidad.ESTATUS_CERRADO], "cancelado": []}
    if filters.get("estado") not in {None, "todo"}: reports = reports.filter(estatus__in=report_statuses[filters["estado"]])
    rcq = Q(estatus=ReporteUnidad.ESTATUS_CERRADO, fecha_cierre__gte=start, fecha_cierre__lt=end) if start else Q(estatus=ReporteUnidad.ESTATUS_CERRADO, fecha_cierre__lt=end)
    roq = Q(fecha_reporte__gte=start, fecha_reporte__lt=end) if start else Q(fecha_reporte__lt=end)
    reports = reports.filter(rcq | (Q(estatus=ReporteUnidad.ESTATUS_CERRADO, fecha_cierre__isnull=True) & roq) | (~Q(estatus=ReporteUnidad.ESTATUS_CERRADO) & roq))
    total += reports.count()
    reports = reports.annotate(effective_event=Case(
        When(estatus=ReporteUnidad.ESTATUS_CERRADO, then=Coalesce("fecha_cierre", "fecha_reporte")),
        default=F("fecha_reporte"),
    )).order_by("-effective_event", "-id").values(
        "id", "tipo", "descripcion", "estatus", "fecha_reporte", "fecha_cierre", "repartidor__user_id",
        "repartidor__user__first_name", "repartidor__user__last_name", "repartidor__user__username",
        "unidad_id", "unidad__codigo", "unidad__sucursal_id", "unidad__sucursal__nombre",
    )[:candidate_limit]
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
        if filters.get("tipo") not in {None, "todo", kind}:
            continue
        if filters.get("sucursal"): queryset = queryset.filter(unidad__sucursal_id=filters["sucursal"])
        if filters.get("unidad"): queryset = queryset.filter(unidad_id=filters["unidad"])
        if filters.get("q"):
            if kind == "reparacion": queryset = queryset.filter(Q(descripcion_falla__icontains=filters["q"]) | Q(descripcion_reparacion__icontains=filters["q"]) | Q(unidad__codigo__icontains=filters["q"]))
            else: queryset = queryset.filter(Q(tipo_servicio__nombre__icontains=filters["q"]) | Q(notas__icontains=filters["q"]) | Q(unidad__codigo__icontains=filters["q"]))
        if start: queryset = queryset.filter(**{f"{date_field}__gte": start.date()})
        queryset = queryset.filter(**{f"{date_field}__lt": end.date()})
        if filters.get("estado") not in {None, "todo", "cerrado"}: queryset = queryset.none()
        total += queryset.count()
        queryset = queryset.select_related("registrado_por", "unidad", "unidad__sucursal")
        if kind == "servicio_unidad": queryset = queryset.select_related("tipo_servicio")
        queryset = queryset.order_by(f"-{date_field}", "-id")[:candidate_limit]
        for obj in queryset:
            event = getattr(obj, date_field)
            if not _in_period(event, start, end): continue
            is_service = kind == "servicio_unidad"
            rows.append(_history_payload(uid=f"{kind}:{obj.pk}", event=event, kind=kind, state="cerrado",
                branch_id=obj.unidad.sucursal_id, branch=obj.unidad.sucursal.nombre, subject_id=obj.unidad_id, subject=obj.unidad.codigo,
                actor=_history_actor(obj.registrado_por_id, obj.registrado_por.get_full_name() if obj.registrado_por else "", obj.registrado_por.get_username() if obj.registrado_por else ""),
                origin=kind, parent_uid=(f"reporte_unidad:{obj.reporte_origen_id}" if kind == "reparacion" and obj.reporte_origen_id else None),
                title=(obj.tipo_servicio.nombre if is_service else obj.descripcion_falla), description=(obj.notas or ("" if is_service else obj.descripcion_reparacion)),
                unit_id=obj.unidad_id, direct=(is_service or (kind == "reparacion" and not obj.reporte_origen_id)),
                invoice=_invoice(f"{kind}_factura" if kind == "reparacion" else "servicio_unidad_factura", obj.pk, obj.archivo_factura),
                cost=((obj.costo if is_service else obj.costo_total) if include_costs else None)))
    return rows, total


def filtered_history_count(user, *, period, filters):
    """Count a single concrete source without materializing its rows."""
    kind = filters.get("tipo")
    definitions = {
        "servicio_unidad": (authorized_unit_services(user), "fecha_servicio", "tipo_servicio__nombre", "notas"),
        "reparacion": (authorized_repairs(user), "fecha_ingreso", "descripcion_falla", "descripcion_reparacion"),
    }
    if kind not in definitions:
        return None
    queryset, date_field, title_field, description_field = definitions[kind]
    start, end = period_bounds(period)
    if filters.get("sucursal"): queryset = queryset.filter(unidad__sucursal_id=filters["sucursal"])
    if filters.get("unidad"): queryset = queryset.filter(unidad_id=filters["unidad"])
    if start: queryset = queryset.filter(**{f"{date_field}__gte": start.date()})
    queryset = queryset.filter(**{f"{date_field}__lt": end.date()})
    if filters.get("q"):
        queryset = queryset.filter(
            Q(**{f"{title_field}__icontains": filters["q"]}) |
            Q(**{f"{description_field}__icontains": filters["q"]}) |
            Q(unidad__codigo__icontains=filters["q"])
        )
    if filters.get("estado") not in {None, "todo", "cerrado"}:
        return 0
    return queryset.count()


def _history_payload(*, uid, event, kind, state, branch_id, branch, subject_id, subject, actor, origin,
                     title, description, asset_id=None, unit_id=None, parent_uid=None, direct=False, invoice=None, cost=None):
    return {"uid": uid, "fecha_evento": _aware_date(event), "tipo": kind, "estado": state,
            "sucursal": {"id": branch_id, "label": branch or ""},
            "sujeto": ({"id": subject_id, "label": subject or ""} if subject_id else None), "actor": actor,
            "origen": origin, "parent_uid": parent_uid, "captura_directa": direct,
            "titulo": title or "", "descripcion": description or "", "activo_id": asset_id,
            "unidad_id": unit_id, "factura": invoice, "costo": cost}


def _invoice(kind, pk, file_value, number=""):
    name = getattr(file_value, "name", file_value) or ""
    if not name:
        return None
    return {"numero": number or "", "nombre": name.rsplit("/", 1)[-1],
            "url": f"/api/mantenimiento/v2/evidencias/{kind}/{pk}/"}


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
