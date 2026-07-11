from pathlib import PurePath

from django.http import FileResponse, Http404
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response

from fallas.models import EvidenciaSeguimientoFalla
from mantenimiento.services_access import (
    authorized_fallas, authorized_orders, authorized_repairs, authorized_unit_reports,
    authorized_unit_services, can_view_costs,
)
from mantenimiento.services_history import inbox_rows, item_detail, unified_history_rows
from mantenimiento.serializers import MaintenanceHistoryEventSerializer
from mantenimiento.views import AUTH, EsMantenimiento


ALLOWED = {
    "estado": {"abiertos", "cerrados", "todos"},
    "periodo": {"semana", "mes", "30d", "90d", "todo"},
    "origen": {"sucursales", "logistica", "todos"},
}

HISTORY_ALLOWED = {
    "tipo": {"todo", "reporte", "orden", "reparacion", "servicio_unidad", "sin_reporte"},
    "estado": {"todo", "abierto", "en_proceso", "cerrado", "cancelado"},
    "periodo": {"semana", "mes", "30d", "90d", "todo"},
}


def _positive_int(raw, default):
    try:
        value = int(raw if raw is not None else default)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def bandeja_v2(request):
    state = (request.query_params.get("estado") or "abiertos").lower()
    period = (request.query_params.get("periodo") or ("30d" if state == "cerrados" else "todo")).lower()
    origin = (request.query_params.get("origen") or "todos").lower()
    values = {"estado": state, "periodo": period, "origen": origin}
    for name, value in values.items():
        if value not in ALLOWED[name]:
            return Response({"error": f"{name.capitalize()} no válido."}, status=400)
    page = _positive_int(request.query_params.get("page"), 1)
    page_size = _positive_int(request.query_params.get("page_size"), 25)
    if page is None or page_size is None:
        return Response({"error": "Paginación no válida."}, status=400)
    page_size = min(page_size, 100)

    period_rows = inbox_rows(request.user, period=period, origin=origin)
    open_rows = inbox_rows(request.user, period="todo", origin=origin)
    closed_rows = period_rows
    counts = {
        "abiertos": sum(row["estado"] in {"abierto", "en_proceso", "programado"} for row in open_rows),
        "en_proceso": sum(row["estado"] == "en_proceso" for row in open_rows),
        "criticos": (sum(row["critico"] and row["estado"] in {"abierto", "en_proceso", "programado"} for row in open_rows)
                     + sum(row["critico"] and row["estado"] == "cerrado" for row in closed_rows)),
        "cerrados": sum(row["estado"] == "cerrado" for row in closed_rows),
    }
    rows = period_rows
    if state == "abiertos":
        rows = [row for row in open_rows if row["estado"] in {"abierto", "en_proceso", "programado"}]
    elif state == "cerrados":
        rows = [row for row in rows if row["estado"] == "cerrado"]
    else:
        rows = ([row for row in open_rows if row["estado"] in {"abierto", "en_proceso", "programado"}]
                + [row for row in closed_rows if row["estado"] == "cerrado"])
    rows.sort(key=lambda row: (row["fecha_evento"] is not None, row["fecha_evento"] or "", row["uid"]), reverse=True)
    total = len(rows)
    start = (page - 1) * page_size
    results = rows[start:start + page_size]
    for row in results:
        row["fecha_evento"] = row["fecha_evento"].isoformat() if row["fecha_evento"] else None
    return Response({
        "counts": counts,
        "schema_version": 2,
        "results": results,
        "pagination": {
            "page": page, "page_size": page_size, "total": total,
            "has_next": start + page_size < total,
        },
    })


@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def historial_v2(request):
    filters = {name: (request.query_params.get(name) or "todo").lower() for name in HISTORY_ALLOWED}
    if any(value not in HISTORY_ALLOWED[name] for name, value in filters.items()):
        return Response({"error": "Filtro no válido."}, status=400)
    page = _positive_int(request.query_params.get("page"), 1)
    page_size = _positive_int(request.query_params.get("page_size"), 25)
    if page is None or page_size is None:
        return Response({"error": "Paginación no válida."}, status=400)
    page_size = min(page_size, 100)
    sql_filters = {
        "tipo": filters["tipo"], "estado": filters["estado"],
        "sucursal": request.query_params.get("sucursal"), "activo": request.query_params.get("activo"),
        "unidad": request.query_params.get("unidad"), "q": (request.query_params.get("q") or "").strip(),
    }
    for key in ("sucursal", "activo", "unidad"):
        if sql_filters[key]:
            try: sql_filters[key] = int(sql_filters[key])
            except ValueError: return Response({"error": f"{key.capitalize()} no válido."}, status=400)
    rows, total = unified_history_rows(
        request.user, period=filters["periodo"], include_costs=can_view_costs(request.user),
        filters=sql_filters, candidate_limit=page * page_size,
    )
    tipo = filters["tipo"]
    if tipo != "todo":
        rows = [row for row in rows if row["tipo"] == tipo]
    if filters["estado"] != "todo": rows = [row for row in rows if row["estado"] == filters["estado"]]
    for key, field in (("sucursal", "sucursal"), ("activo", "activo_id"), ("unidad", "unidad_id")):
        raw = request.query_params.get(key)
        if raw:
            try: wanted = int(raw)
            except ValueError: return Response({"error": f"{key.capitalize()} no válido."}, status=400)
            rows = [row for row in rows if (row[field]["id"] if field == "sucursal" else row[field]) == wanted]
    query = (request.query_params.get("q") or "").strip().casefold()
    if query:
        rows = [row for row in rows if query in " ".join((row["titulo"], row["descripcion"], (row["sujeto"] or {}).get("label", ""), row["actor"]["label"])).casefold()]
    rows.sort(key=lambda row: (
        row["fecha_evento"], row["uid"].split(":", 1)[0], int(row["uid"].split(":", 1)[1])
    ), reverse=True)
    start = (page - 1) * page_size
    results = MaintenanceHistoryEventSerializer(rows[start:start + page_size], many=True).data
    return Response({"schema_version": 2, "results": results, "pagination": {"page": page, "page_size": page_size, "total": total, "has_next": start + page_size < total}})


@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def item_v2(request, tipo, pk):
    return Response(item_detail(request.user, tipo, pk))


def _authorized_file(user, kind, pk):
    if kind == "falla_inicial":
        parent = authorized_fallas(user).only("foto_evidencia").filter(pk=pk).first()
        return (parent.foto_evidencia, "") if parent else (None, "")
    if kind == "seguimiento_falla":
        evidence = EvidenciaSeguimientoFalla.objects.select_related("bitacora").filter(
            pk=pk, bitacora__reporte_id__in=authorized_fallas(user).values("pk")
        ).first()
        return (evidence.archivo, evidence.nombre) if evidence else (None, "")
    definitions = {
        "reporte_unidad": (authorized_unit_reports, "foto"),
        "orden_factura": (authorized_orders, "factura_archivo"),
        "reparacion_factura": (authorized_repairs, "archivo_factura"),
        "reparacion_foto": (authorized_repairs, "foto_nota"),
        "servicio_unidad_factura": (authorized_unit_services, "archivo_factura"),
    }
    if kind not in definitions:
        return None, ""
    authorize, field = definitions[kind]
    parent = authorize(user).only(field).filter(pk=pk).first()
    return (getattr(parent, field), "") if parent else (None, "")


@api_view(["GET", "HEAD"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def evidencia_v2(request, tipo, pk):
    file_field, supplied_name = _authorized_file(request.user, tipo, pk)
    if not file_field or not file_field.name:
        raise Http404
    try:
        if not file_field.storage.exists(file_field.name):
            raise Http404
        stream = file_field.open("rb")
    except (FileNotFoundError, OSError, ValueError):
        raise Http404
    raw_name = (supplied_name or file_field.name).splitlines()[0]
    safe_name = PurePath(raw_name).name
    safe_name = "".join(ch for ch in safe_name if ch.isprintable() and ch not in {'"', "\\"}) or "evidencia"
    extension = PurePath(safe_name).suffix.lower()
    signature = stream.read(16)
    stream.seek(0)
    safe_types = {
        ".jpg": ("image/jpeg", signature.startswith(b"\xff\xd8\xff")),
        ".jpeg": ("image/jpeg", signature.startswith(b"\xff\xd8\xff")),
        ".png": ("image/png", signature.startswith(b"\x89PNG\r\n\x1a\n")),
        ".webp": ("image/webp", signature.startswith(b"RIFF") and signature[8:12] == b"WEBP"),
        ".pdf": ("application/pdf", signature.startswith(b"%PDF-")),
    }
    declared_mime, valid_signature = safe_types.get(extension, ("application/octet-stream", False))
    inline = valid_signature
    mime = declared_mime if inline else "application/octet-stream"
    response = FileResponse(stream, content_type=mime, as_attachment=not inline, filename=safe_name)
    response["Cache-Control"] = "private, no-store"
    response["X-Content-Type-Options"] = "nosniff"
    return response
