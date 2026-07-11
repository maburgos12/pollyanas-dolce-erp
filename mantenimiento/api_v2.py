from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response

from mantenimiento.services_history import inbox_rows
from mantenimiento.views import AUTH, EsMantenimiento


ALLOWED = {
    "estado": {"abiertos", "cerrados", "todos"},
    "periodo": {"semana", "mes", "30d", "90d", "todo"},
    "origen": {"sucursales", "logistica", "todos"},
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

    rows = inbox_rows(request.user, period=period, origin=origin)
    if state == "abiertos":
        rows = [row for row in rows if row["estado"] in {"abierto", "en_proceso", "programado"}]
    elif state == "cerrados":
        rows = [row for row in rows if row["estado"] == "cerrado"]
    rows.sort(key=lambda row: (row["fecha_evento"], row["uid"]), reverse=True)
    counts = {
        "abiertos": sum(row["estado"] in {"abierto", "en_proceso", "programado"} for row in rows),
        "en_proceso": sum(row["estado"] == "en_proceso" for row in rows),
        "criticos": sum(row["critico"] for row in rows),
        "cerrados": sum(row["estado"] == "cerrado" for row in rows),
    }
    total = len(rows)
    start = (page - 1) * page_size
    results = rows[start:start + page_size]
    for row in results:
        row["fecha_evento"] = row["fecha_evento"].isoformat()
    return Response({
        "counts": counts,
        "schema_version": 2,
        "results": results,
        "pagination": {
            "page": page, "page_size": page_size, "total": total,
            "has_next": start + page_size < total,
        },
    })
