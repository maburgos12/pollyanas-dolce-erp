import csv
from io import BytesIO
from datetime import timedelta
from decimal import Decimal, InvalidOperation

from openpyxl import Workbook
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Sum, Max
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils import timezone

from core.access import can_view_reportes
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo
from recetas.models import Receta, LineaReceta


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _consumo_rows(date_from: str, date_to: str, tipo: str):
    movimientos = MovimientoInventario.objects.select_related("insumo").filter(
        fecha__date__gte=date_from,
        fecha__date__lte=date_to,
    )
    if tipo != "ALL":
        movimientos = movimientos.filter(tipo=tipo)

    resumen = list(
        movimientos.values("insumo_id", "insumo__nombre")
        .annotate(
            cantidad_total=Sum("cantidad"),
            ultima_fecha=Max("fecha"),
        )
        .order_by("-cantidad_total", "insumo__nombre")
    )
    return movimientos, resumen


def _export_consumo_csv(rows, date_from: str, date_to: str, tipo: str) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="reporte_consumo_{date_from}_{date_to}.csv"'
    writer = csv.writer(response)
    writer.writerow(["Insumo", "Cantidad total", "Ultimo movimiento", "Filtro tipo", "Desde", "Hasta"])
    for row in rows:
        writer.writerow(
            [
                row["insumo__nombre"],
                row["cantidad_total"],
                row["ultima_fecha"].strftime("%Y-%m-%d %H:%M") if row["ultima_fecha"] else "",
                tipo,
                date_from,
                date_to,
            ]
        )
    return response


def _export_consumo_xlsx(rows, date_from: str, date_to: str, tipo: str) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Consumo"
    ws.append(["Insumo", "Cantidad total", "Ultimo movimiento", "Filtro tipo", "Desde", "Hasta"])
    for row in rows:
        ws.append(
            [
                row["insumo__nombre"],
                float(row["cantidad_total"] or 0),
                row["ultima_fecha"].strftime("%Y-%m-%d %H:%M") if row["ultima_fecha"] else "",
                tipo,
                date_from,
                date_to,
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="reporte_consumo_{date_from}_{date_to}.xlsx"'
    return response


def _faltantes_rows(nivel: str):
    existencias = list(
        ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base").order_by("insumo__nombre")[:500]
    )

    criticos_count = 0
    bajo_count = 0
    rows = []
    for e in existencias:
        stock = e.stock_actual
        reorden = e.punto_reorden
        if stock <= 0:
            e.criticidad = "Alta"
            e.criticidad_badge = "bg-danger"
            e.nivel = "critico"
            criticos_count += 1
        elif stock < reorden:
            e.criticidad = "Media"
            e.criticidad_badge = "bg-warning"
            e.nivel = "bajo"
            bajo_count += 1
        else:
            e.criticidad = "Sin riesgo"
            e.criticidad_badge = "bg-success"
            e.nivel = "ok"

        e.sugerencia_compra = max(reorden - stock, 0)

        include = False
        if nivel == "all":
            include = True
        elif nivel == "alerta":
            include = e.nivel in {"critico", "bajo"}
        else:
            include = e.nivel == nivel

        if include:
            rows.append(e)

    return rows, criticos_count, bajo_count


def _export_faltantes_csv(rows, nivel: str) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="reporte_faltantes.csv"'
    writer = csv.writer(response)
    writer.writerow(["Insumo", "Unidad", "Stock actual", "Punto reorden", "Sugerencia compra", "Criticidad", "Nivel filtro"])
    for row in rows:
        writer.writerow(
            [
                row.insumo.nombre,
                row.insumo.unidad_base.codigo if row.insumo.unidad_base else "-",
                row.stock_actual,
                row.punto_reorden,
                row.sugerencia_compra,
                row.criticidad,
                nivel,
            ]
        )
    return response


def _export_faltantes_xlsx(rows, nivel: str) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Faltantes"
    ws.append(["Insumo", "Unidad", "Stock actual", "Punto reorden", "Sugerencia compra", "Criticidad", "Nivel filtro"])
    for row in rows:
        ws.append(
            [
                row.insumo.nombre,
                row.insumo.unidad_base.codigo if row.insumo.unidad_base else "-",
                float(row.stock_actual or 0),
                float(row.punto_reorden or 0),
                float(row.sugerencia_compra or 0),
                row.criticidad,
                nivel,
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="reporte_faltantes.xlsx"'
    return response


@login_required
def costo_receta(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")

    margen_pct = _to_decimal(request.GET.get("margen"), "35")
    if margen_pct < 0:
        margen_pct = Decimal("0")
    if margen_pct > 500:
        margen_pct = Decimal("500")
    factor_margen = Decimal("1") + (margen_pct / Decimal("100"))

    recetas = list(Receta.objects.prefetch_related("lineas", "lineas__insumo").order_by("nombre")[:500])
    lineas_insumo_ids = []
    for receta in recetas:
        for linea in receta.lineas.all():
            if linea.insumo_id:
                lineas_insumo_ids.append(linea.insumo_id)

    latest_cost_by_insumo = {}
    for cost in CostoInsumo.objects.filter(insumo_id__in=lineas_insumo_ids).order_by("insumo_id", "-fecha", "-id"):
        if cost.insumo_id not in latest_cost_by_insumo:
            latest_cost_by_insumo[cost.insumo_id] = cost

    rows = []
    total_costo = Decimal("0")
    with_costo = 0
    for receta in recetas:
        costo_total = Decimal("0")
        lineas_total = 0
        lineas_costeadas = 0
        for linea in receta.lineas.all():
            if linea.match_status == LineaReceta.STATUS_REJECTED:
                continue
            lineas_total += 1

            costo_linea = Decimal("0")
            if linea.costo_linea_excel is not None:
                costo_linea = _to_decimal(linea.costo_linea_excel, "0")
            elif linea.cantidad is not None and linea.costo_unitario_snapshot is not None:
                costo_linea = _to_decimal(linea.cantidad, "0") * _to_decimal(linea.costo_unitario_snapshot, "0")
            elif linea.cantidad is not None and linea.insumo_id and linea.insumo_id in latest_cost_by_insumo:
                costo_linea = _to_decimal(linea.cantidad, "0") * _to_decimal(latest_cost_by_insumo[linea.insumo_id].costo_unitario, "0")

            if costo_linea > 0:
                lineas_costeadas += 1
                costo_total += costo_linea

        cobertura_pct = (Decimal("100") * Decimal(lineas_costeadas) / Decimal(lineas_total)) if lineas_total else Decimal("0")
        precio_sugerido = costo_total * factor_margen
        row = {
            "receta": receta,
            "costo_total": costo_total,
            "margen_pct": margen_pct,
            "precio_sugerido": precio_sugerido,
            "lineas_total": lineas_total,
            "lineas_costeadas": lineas_costeadas,
            "cobertura_pct": cobertura_pct,
        }
        rows.append(row)
        total_costo += costo_total
        if costo_total > 0:
            with_costo += 1

    rows.sort(key=lambda r: (r["costo_total"], r["receta"].nombre), reverse=True)

    context = {
        "rows": rows,
        "margen_pct": margen_pct,
        "total_recetas": len(rows),
        "recetas_con_costo": with_costo,
        "total_costo": total_costo,
    }
    return render(request, "reportes/costo_receta.html", context)


@login_required
def consumo(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    today = timezone.localdate()
    default_from = today - timedelta(days=30)

    date_from = request.GET.get("date_from") or default_from.isoformat()
    date_to = request.GET.get("date_to") or today.isoformat()
    tipo = (request.GET.get("tipo") or "all").upper()
    valid_tipos = {"ALL", "CONSUMO", "SALIDA", "ENTRADA"}
    if tipo not in valid_tipos:
        tipo = "ALL"

    movimientos, resumen = _consumo_rows(date_from, date_to, tipo)

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_consumo_csv(resumen, date_from, date_to, tipo)
    if export_format == "xlsx":
        return _export_consumo_xlsx(resumen, date_from, date_to, tipo)

    context = {
        "rows": resumen,
        "total_movimientos": movimientos.count(),
        "total_insumos": len(resumen),
        "total_cantidad": sum((row["cantidad_total"] or 0) for row in resumen),
        "filters": {
            "date_from": date_from,
            "date_to": date_to,
            "tipo": tipo,
        },
    }
    return render(request, "reportes/consumo.html", context)


@login_required
def faltantes(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")
    nivel = (request.GET.get("nivel") or "alerta").lower()
    valid_levels = {"alerta", "critico", "bajo", "all"}
    if nivel not in valid_levels:
        nivel = "alerta"

    rows, criticos_count, bajo_count = _faltantes_rows(nivel)

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_faltantes_csv(rows, nivel)
    if export_format == "xlsx":
        return _export_faltantes_xlsx(rows, nivel)

    context = {
        "rows": rows,
        "nivel": nivel,
        "criticos_count": criticos_count,
        "bajo_count": bajo_count,
        "alertas_count": criticos_count + bajo_count,
    }
    return render(request, "reportes/faltantes.html", context)
