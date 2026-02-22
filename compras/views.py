import csv
import calendar
from io import BytesIO
from decimal import Decimal, InvalidOperation
from io import StringIO
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q, Sum
from django.urls import reverse
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST
from openpyxl import Workbook, load_workbook

from core.access import can_manage_compras, can_view_compras
from core.audit import log_event
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo, Insumo, Proveedor
from recetas.models import PlanProduccion
from recetas.utils.matching import match_insumo
from recetas.utils.normalizacion import normalizar_nombre

from .models import (
    OrdenCompra,
    PresupuestoCompraCategoria,
    PresupuestoCompraPeriodo,
    PresupuestoCompraProveedor,
    RecepcionCompra,
    SolicitudCompra,
)

IMPORT_PREVIEW_SESSION_KEY = "compras_solicitudes_import_preview"


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


def _build_import_preview_context(import_preview_payload) -> dict | None:
    if not isinstance(import_preview_payload, dict):
        return None

    preview_rows = [x for x in (import_preview_payload.get("rows") or []) if isinstance(x, dict)]
    try:
        preview_score_min = int(import_preview_payload.get("score_min") or 90)
    except (TypeError, ValueError):
        preview_score_min = 90

    preview_ready = 0
    preview_with_issues = 0
    preview_duplicates = 0
    preview_without_match = 0
    preview_invalid_qty = 0
    for row in preview_rows:
        if bool(row.get("include")):
            preview_ready += 1
        if row.get("notes"):
            preview_with_issues += 1
        if bool(row.get("duplicate")):
            preview_duplicates += 1
        if not str(row.get("insumo_id") or "").strip():
            preview_without_match += 1
        try:
            cantidad_preview = _to_decimal(str(row.get("cantidad") or "0"), "0")
        except Exception:
            cantidad_preview = Decimal("0")
        if cantidad_preview <= 0:
            preview_invalid_qty += 1

    return {
        "rows": preview_rows,
        "count": len(preview_rows),
        "evitar_duplicados": bool(import_preview_payload.get("evitar_duplicados")),
        "score_min": preview_score_min,
        "ready_count": preview_ready,
        "excluded_count": max(0, len(preview_rows) - preview_ready),
        "issues_count": preview_with_issues,
        "duplicates_count": preview_duplicates,
        "without_match_count": preview_without_match,
        "invalid_qty_count": preview_invalid_qty,
        "file_name": str(import_preview_payload.get("file_name") or "").strip(),
        "generated_at": str(import_preview_payload.get("generated_at") or "").strip(),
    }


def _map_import_header(name: str) -> str:
    n = normalizar_nombre(name or "").replace("_", " ")
    if n in {"insumo", "nombre insumo", "insumo nombre", "materia prima", "articulo", "item", "producto", "descripcion"}:
        return "insumo"
    if n in {"cantidad", "cant", "qty", "cantidad requerida", "requerido"}:
        return "cantidad"
    if n in {"proveedor", "proveedor sugerido"}:
        return "proveedor"
    if n in {"fecha", "fecha requerida", "fecha requerida compra", "fecha requerida compras"}:
        return "fecha_requerida"
    if n in {"area", "area solicitante", "departamento"}:
        return "area"
    if n in {"solicitante", "responsable", "usuario"}:
        return "solicitante"
    if n in {"estatus", "estado"}:
        return "estatus"
    if n in {"periodo tipo", "tipo periodo", "tipo"}:
        return "periodo_tipo"
    if n in {"periodo mes", "mes", "periodo"}:
        return "periodo_mes"
    if n in {"monto objetivo", "presupuesto objetivo", "objetivo", "monto", "presupuesto"}:
        return "monto_objetivo"
    if n in {"monto objetivo proveedor", "objetivo proveedor", "presupuesto proveedor", "monto proveedor"}:
        return "monto_objetivo_proveedor"
    if n in {"categoria", "categoria insumo", "familia", "linea", "grupo"}:
        return "categoria"
    if n in {"monto objetivo categoria", "objetivo categoria", "presupuesto categoria", "monto categoria"}:
        return "monto_objetivo_categoria"
    if n in {"nota", "notas", "comentario", "comentarios"}:
        return "notas"
    return n


def _read_import_rows(uploaded) -> list[dict]:
    ext = Path(uploaded.name or "").suffix.lower()
    rows: list[dict] = []

    if ext in {".xlsx", ".xlsm"}:
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        first_row = next(rows_iter, None)
        if not first_row:
            return []
        headers = [_map_import_header(str(h or "")) for h in first_row]
        for raw in rows_iter:
            row = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                row[header] = raw[idx] if idx < len(raw) else None
            rows.append(row)
        return rows

    if ext == ".csv":
        uploaded.seek(0)
        content = uploaded.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(StringIO(content))
        for raw in reader:
            row = {}
            for k, v in raw.items():
                if not k:
                    continue
                row[_map_import_header(k)] = v
            rows.append(row)
        return rows

    raise ValueError("Formato no soportado. Usa .xlsx, .xlsm o .csv.")


@login_required
def descargar_plantilla_solicitudes(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para descargar plantilla de importación.")

    export_format = (request.GET.get("format") or "xlsx").strip().lower()
    headers = ["insumo", "cantidad", "proveedor", "fecha_requerida", "area", "solicitante", "estatus"]
    sample_rows = [
        ["Harina Pastelera", "12.500", "Proveedor A", date.today().isoformat(), "Compras", request.user.username, SolicitudCompra.STATUS_BORRADOR],
        ["Mantequilla", "8.000", "Proveedor B", (date.today() + timedelta(days=2)).isoformat(), "Produccion", request.user.username, SolicitudCompra.STATUS_BORRADOR],
    ]

    if export_format == "csv":
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="plantilla_solicitudes_compras.csv"'
        writer = csv.writer(response)
        writer.writerow(headers)
        writer.writerows(sample_rows)
        return response

    if export_format not in {"xlsx", "xlsm"}:
        messages.error(request, "Formato de plantilla no soportado. Usa csv o xlsx.")
        return redirect("compras:solicitudes")

    wb = Workbook()
    ws = wb.active
    ws.title = "solicitudes_import"
    ws.append(headers)
    for row in sample_rows:
        ws.append(row)
    for col in ("A", "B", "C", "D", "E", "F", "G"):
        ws.column_dimensions[col].width = 24

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="plantilla_solicitudes_compras.xlsx"'
    return response


def _parse_periodo_tipo_value(raw) -> str | None:
    value = normalizar_nombre(str(raw or ""))
    if value in {"mes", "mensual"}:
        return "mes"
    if value in {"q1", "1ra quincena", "primera quincena", "quincena 1", "q 1"}:
        return "q1"
    if value in {"q2", "2da quincena", "segunda quincena", "quincena 2", "q 2"}:
        return "q2"
    return None


def _parse_periodo_mes_value(raw) -> str | None:
    if not raw:
        return None
    if isinstance(raw, date):
        return f"{raw.year:04d}-{raw.month:02d}"
    text = str(raw).strip()
    if not text:
        return None
    text = text.replace("/", "-")
    try:
        y, m = text.split("-")[:2]
        yi = int(y)
        mi = int(m)
        if 1 <= mi <= 12:
            return f"{yi:04d}-{mi:02d}"
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(text, fmt).date()
            return f"{dt.year:04d}-{dt.month:02d}"
        except ValueError:
            continue
    return None


def _default_fecha_requerida(periodo_tipo: str, periodo_mes: str) -> date:
    if periodo_tipo == "all":
        return timezone.localdate()
    year, month = periodo_mes.split("-")
    y = int(year)
    m = int(month)
    if periodo_tipo == "q1":
        return date(y, m, 15)
    if periodo_tipo == "q2":
        return date(y, m, calendar.monthrange(y, m)[1])
    return date(y, m, 1)


def _parse_date_value(raw_value, fallback: date) -> date:
    if not raw_value:
        return fallback
    if isinstance(raw_value, date):
        return raw_value
    text = str(raw_value).strip()
    if not text:
        return fallback
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return fallback


def _resolve_proveedor_name(raw: str, providers_by_norm: dict[str, Proveedor]) -> Proveedor | None:
    name = (raw or "").strip()
    if not name:
        return None
    return providers_by_norm.get(normalizar_nombre(name))


def _normalize_categoria_text(raw: str) -> str:
    return normalizar_nombre((raw or "").strip())


def _sanitize_categoria_filter(raw: str) -> str:
    return " ".join((raw or "").strip().split())


def _sanitize_consumo_ref_filter(raw: str) -> str:
    value = (raw or "all").strip().lower()
    if value not in {"all", "plan_ref"}:
        return "all"
    return value


def _resolve_insumo_categoria(insumo: Insumo) -> str:
    categoria = " ".join((getattr(insumo, "categoria", "") or "").strip().split())
    if categoria:
        return categoria
    unidad = getattr(insumo, "unidad_base", None)
    if not unidad:
        return "Sin categoría"
    tipo = (unidad.tipo or "").strip().upper()
    if tipo == "MASS":
        return "Masa"
    if tipo == "VOLUME":
        return "Volumen"
    if tipo == "UNIT":
        return "Pieza"
    return "Sin categoría"


def _write_import_pending_csv(rows: list[dict]) -> str:
    ts = timezone.localtime().strftime("%Y%m%d_%H%M%S")
    filepath = Path("logs") / f"compras_import_pendientes_{ts}.csv"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "row",
        "insumo_origen",
        "cantidad_origen",
        "score",
        "metodo",
        "sugerencia",
        "motivo",
    ]
    with filepath.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h, "") for h in headers})
    return str(filepath)


def _export_import_preview_csv(import_preview: dict) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="compras_preview_solicitudes_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "row_id",
            "source_row",
            "include",
            "insumo_origen",
            "insumo_sugerencia",
            "insumo_id",
            "cantidad",
            "area",
            "solicitante",
            "fecha_requerida",
            "estatus",
            "proveedor_id",
            "score",
            "metodo",
            "duplicate",
            "notes",
        ]
    )
    for row in import_preview.get("rows", []):
        writer.writerow(
            [
                row.get("row_id", ""),
                row.get("source_row", ""),
                "1" if row.get("include") else "0",
                row.get("insumo_origen", ""),
                row.get("insumo_sugerencia", ""),
                row.get("insumo_id", ""),
                row.get("cantidad", ""),
                row.get("area", ""),
                row.get("solicitante", ""),
                row.get("fecha_requerida", ""),
                row.get("estatus", ""),
                row.get("proveedor_id", ""),
                row.get("score", ""),
                row.get("metodo", ""),
                "1" if row.get("duplicate") else "0",
                row.get("notes", ""),
            ]
        )
    return response


def _export_import_preview_xlsx(import_preview: dict) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    wb = Workbook()
    ws = wb.active
    ws.title = "preview_import"
    ws.append(
        [
            "row_id",
            "source_row",
            "include",
            "insumo_origen",
            "insumo_sugerencia",
            "insumo_id",
            "cantidad",
            "area",
            "solicitante",
            "fecha_requerida",
            "estatus",
            "proveedor_id",
            "score",
            "metodo",
            "duplicate",
            "notes",
        ]
    )
    for row in import_preview.get("rows", []):
        ws.append(
            [
                row.get("row_id", ""),
                row.get("source_row", ""),
                1 if row.get("include") else 0,
                row.get("insumo_origen", ""),
                row.get("insumo_sugerencia", ""),
                row.get("insumo_id", ""),
                row.get("cantidad", ""),
                row.get("area", ""),
                row.get("solicitante", ""),
                row.get("fecha_requerida", ""),
                row.get("estatus", ""),
                row.get("proveedor_id", ""),
                row.get("score", ""),
                row.get("metodo", ""),
                1 if row.get("duplicate") else 0,
                row.get("notes", ""),
            ]
        )
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    response = HttpResponse(
        bio.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="compras_preview_solicitudes_{now_str}.xlsx"'
    return response


def _active_solicitud_statuses() -> set[str]:
    return {
        SolicitudCompra.STATUS_BORRADOR,
        SolicitudCompra.STATUS_EN_REVISION,
        SolicitudCompra.STATUS_APROBADA,
    }


def _can_transition_solicitud(current: str, new: str) -> bool:
    transitions = {
        SolicitudCompra.STATUS_BORRADOR: {SolicitudCompra.STATUS_EN_REVISION, SolicitudCompra.STATUS_APROBADA, SolicitudCompra.STATUS_RECHAZADA},
        SolicitudCompra.STATUS_EN_REVISION: {SolicitudCompra.STATUS_APROBADA, SolicitudCompra.STATUS_RECHAZADA},
        SolicitudCompra.STATUS_APROBADA: set(),
        SolicitudCompra.STATUS_RECHAZADA: set(),
    }
    return new in transitions.get(current, set())


def _can_transition_orden(current: str, new: str) -> bool:
    transitions = {
        OrdenCompra.STATUS_BORRADOR: {OrdenCompra.STATUS_ENVIADA},
        OrdenCompra.STATUS_ENVIADA: {OrdenCompra.STATUS_CONFIRMADA, OrdenCompra.STATUS_PARCIAL},
        OrdenCompra.STATUS_CONFIRMADA: {OrdenCompra.STATUS_PARCIAL, OrdenCompra.STATUS_CERRADA},
        OrdenCompra.STATUS_PARCIAL: {OrdenCompra.STATUS_CERRADA},
        OrdenCompra.STATUS_CERRADA: set(),
    }
    return new in transitions.get(current, set())


def _can_transition_recepcion(current: str, new: str) -> bool:
    transitions = {
        RecepcionCompra.STATUS_PENDIENTE: {RecepcionCompra.STATUS_DIFERENCIAS, RecepcionCompra.STATUS_CERRADA},
        RecepcionCompra.STATUS_DIFERENCIAS: {RecepcionCompra.STATUS_CERRADA},
        RecepcionCompra.STATUS_CERRADA: set(),
    }
    return new in transitions.get(current, set())


def _build_insumo_options():
    insumos = list(Insumo.objects.filter(activo=True).order_by("nombre")[:200])
    insumo_ids = [i.id for i in insumos]
    existencias = {e.insumo_id: e for e in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids)}

    en_transito_by_insumo: dict[int, Decimal] = {}
    for orden in (
        OrdenCompra.objects.filter(
            estatus__in=[
                OrdenCompra.STATUS_ENVIADA,
                OrdenCompra.STATUS_CONFIRMADA,
                OrdenCompra.STATUS_PARCIAL,
            ],
            solicitud__isnull=False,
            solicitud__insumo_id__in=insumo_ids,
        )
        .select_related("solicitud")
        .only("solicitud__insumo_id", "solicitud__cantidad")
    ):
        solicitud = orden.solicitud
        if not solicitud or not solicitud.insumo_id:
            continue
        qty = _to_decimal(str(solicitud.cantidad or 0), "0")
        if qty <= 0:
            continue
        en_transito_by_insumo[solicitud.insumo_id] = en_transito_by_insumo.get(solicitud.insumo_id, Decimal("0")) + qty

    options = []
    for insumo in insumos:
        ex = existencias.get(insumo.id)
        stock_actual = ex.stock_actual if ex else Decimal("0")
        punto_reorden = ex.punto_reorden if ex else Decimal("0")
        stock_seguridad = ex.stock_minimo if ex else Decimal("0")
        consumo_diario = ex.consumo_diario_promedio if ex else Decimal("0")
        lead_time_dias = int(ex.dias_llegada_pedido or 0) if ex else 0
        if lead_time_dias <= 0 and insumo.proveedor_principal_id:
            lead_time_dias = int(insumo.proveedor_principal.lead_time_dias or 0)
        lead_time_dias = max(lead_time_dias, 0)

        demanda_lead_time = consumo_diario * Decimal(str(lead_time_dias))
        en_transito = en_transito_by_insumo.get(insumo.id, Decimal("0"))
        recomendado = (demanda_lead_time + stock_seguridad) - (stock_actual + en_transito)
        if recomendado < 0:
            recomendado = Decimal("0")

        options.append(
            {
                "id": insumo.id,
                "nombre": insumo.nombre,
                "proveedor_sugerido": insumo.proveedor_principal.nombre if insumo.proveedor_principal_id else "",
                "stock_actual": stock_actual,
                "punto_reorden": punto_reorden,
                "stock_seguridad": stock_seguridad,
                "demanda_lead_time": demanda_lead_time,
                "en_transito": en_transito,
                "lead_time_dias": lead_time_dias,
                "recomendado": recomendado,
            }
        )
    return options


def _solicitudes_print_folio() -> str:
    now = timezone.localtime()
    return f"SC-{now.strftime('%Y%m%d-%H%M%S')}"


def _parse_period_filters(periodo_tipo_raw: str, periodo_mes_raw: str) -> tuple[str, str, str]:
    tipo = (periodo_tipo_raw or "all").strip().lower()
    if tipo not in {"all", "mes", "q1", "q2"}:
        tipo = "all"

    now = timezone.localdate()
    default_mes = f"{now.year:04d}-{now.month:02d}"
    periodo_mes = (periodo_mes_raw or default_mes).strip()
    try:
        y, m = periodo_mes.split("-")
        y_int = int(y)
        m_int = int(m)
        if not (1 <= m_int <= 12):
            raise ValueError
        periodo_mes = f"{y_int:04d}-{m_int:02d}"
    except Exception:
        periodo_mes = default_mes

    if tipo == "mes":
        label = f"Mensual ({periodo_mes})"
    elif tipo == "q1":
        label = f"1ra Quincena ({periodo_mes})"
    elif tipo == "q2":
        label = f"2da Quincena ({periodo_mes})"
    else:
        label = "Todos"
    return tipo, periodo_mes, label


def _periodo_bounds(periodo_tipo: str, periodo_mes: str) -> tuple[date | None, date | None]:
    if periodo_tipo == "all":
        return None, None

    year, month = periodo_mes.split("-")
    y = int(year)
    m = int(month)
    last_day = calendar.monthrange(y, m)[1]
    start = date(y, m, 1)
    end = date(y, m, last_day)

    if periodo_tipo == "q1":
        end = date(y, m, 15)
    elif periodo_tipo == "q2":
        start = date(y, m, 16)
    return start, end


def _filter_ordenes_by_scope(ordenes_qs, source_filter: str, plan_filter: str):
    if source_filter == "plan":
        ordenes_qs = ordenes_qs.filter(solicitud__area__startswith="PLAN_PRODUCCION:")
    elif source_filter == "manual":
        ordenes_qs = ordenes_qs.exclude(solicitud__area__startswith="PLAN_PRODUCCION:")

    if plan_filter:
        ordenes_qs = ordenes_qs.filter(solicitud__area=f"PLAN_PRODUCCION:{plan_filter}")
    return ordenes_qs


def _filter_solicitudes_by_scope(solicitudes_qs, source_filter: str, plan_filter: str):
    if source_filter == "plan":
        solicitudes_qs = solicitudes_qs.filter(area__startswith="PLAN_PRODUCCION:")
    elif source_filter == "manual":
        solicitudes_qs = solicitudes_qs.exclude(area__startswith="PLAN_PRODUCCION:")
    if plan_filter:
        solicitudes_qs = solicitudes_qs.filter(area=f"PLAN_PRODUCCION:{plan_filter}")
    return solicitudes_qs


def _filter_solicitudes_by_categoria(solicitudes_qs, categoria_filter: str):
    categoria = _sanitize_categoria_filter(categoria_filter)
    if not categoria:
        return solicitudes_qs

    categoria_norm = _normalize_categoria_text(categoria)
    categoria_unit_map = {
        "masa": "MASS",
        "volumen": "VOLUME",
        "pieza": "UNIT",
    }
    unidad_tipo = categoria_unit_map.get(categoria_norm)
    if unidad_tipo:
        return solicitudes_qs.filter(
            Q(insumo__categoria__iexact=categoria)
            | (
                (Q(insumo__categoria="") | Q(insumo__categoria__isnull=True))
                & Q(insumo__unidad_base__tipo=unidad_tipo)
            )
        )
    return solicitudes_qs.filter(insumo__categoria__iexact=categoria)


def _filter_ordenes_by_categoria(ordenes_qs, categoria_filter: str):
    categoria = _sanitize_categoria_filter(categoria_filter)
    if not categoria:
        return ordenes_qs

    categoria_norm = _normalize_categoria_text(categoria)
    categoria_unit_map = {
        "masa": "MASS",
        "volumen": "VOLUME",
        "pieza": "UNIT",
    }
    unidad_tipo = categoria_unit_map.get(categoria_norm)
    if unidad_tipo:
        return ordenes_qs.filter(
            Q(solicitud__insumo__categoria__iexact=categoria)
            | (
                (Q(solicitud__insumo__categoria="") | Q(solicitud__insumo__categoria__isnull=True))
                & Q(solicitud__insumo__unidad_base__tipo=unidad_tipo)
            )
        )
    return ordenes_qs.filter(solicitud__insumo__categoria__iexact=categoria)


def _filter_movimientos_by_scope(
    movimientos_qs,
    source_filter: str,
    plan_filter: str,
    consumo_ref_filter: str = "all",
):
    if source_filter == "plan":
        movimientos_qs = movimientos_qs.filter(referencia__icontains="PLAN_PRODUCCION:")
    elif source_filter == "manual":
        movimientos_qs = movimientos_qs.exclude(referencia__icontains="PLAN_PRODUCCION:")
    if plan_filter:
        movimientos_qs = movimientos_qs.filter(referencia__icontains=f"PLAN_PRODUCCION:{plan_filter}")
    if _sanitize_consumo_ref_filter(consumo_ref_filter) == "plan_ref":
        movimientos_qs = movimientos_qs.filter(referencia__icontains="PLAN_PRODUCCION:")
    return movimientos_qs


def _filter_movimientos_by_categoria(movimientos_qs, categoria_filter: str):
    categoria = _sanitize_categoria_filter(categoria_filter)
    if not categoria:
        return movimientos_qs

    categoria_norm = _normalize_categoria_text(categoria)
    categoria_unit_map = {
        "masa": "MASS",
        "volumen": "VOLUME",
        "pieza": "UNIT",
    }
    unidad_tipo = categoria_unit_map.get(categoria_norm)
    if unidad_tipo:
        return movimientos_qs.filter(
            Q(insumo__categoria__iexact=categoria)
            | (
                (Q(insumo__categoria="") | Q(insumo__categoria__isnull=True))
                & Q(insumo__unidad_base__tipo=unidad_tipo)
            )
        )
    return movimientos_qs.filter(insumo__categoria__iexact=categoria)


def _shift_month(periodo_mes: str, delta_months: int) -> str:
    year, month = periodo_mes.split("-")
    y = int(year)
    m = int(month)
    total = y * 12 + (m - 1) - delta_months
    shifted_y = total // 12
    shifted_m = (total % 12) + 1
    return f"{shifted_y:04d}-{shifted_m:02d}"


def _compute_budget_period_summary(
    periodo_tipo: str,
    periodo_mes: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
) -> dict:
    start_date, end_date = _periodo_bounds(periodo_tipo, periodo_mes)
    if not start_date or not end_date:
        return {
            "periodo_tipo": periodo_tipo,
            "periodo_mes": periodo_mes,
            "objetivo": Decimal("0"),
            "estimado": Decimal("0"),
            "ejecutado": Decimal("0"),
            "ratio_pct": None,
            "estado_label": "Sin periodo",
            "estado_badge": "bg-warning",
        }

    presupuesto = PresupuestoCompraPeriodo.objects.filter(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
    ).first()
    objetivo = presupuesto.monto_objetivo if presupuesto else Decimal("0")

    solicitudes_qs = SolicitudCompra.objects.filter(fecha_requerida__range=(start_date, end_date))
    solicitudes_qs = _filter_solicitudes_by_scope(solicitudes_qs, source_filter, plan_filter)
    solicitudes_qs = _filter_solicitudes_by_categoria(solicitudes_qs, categoria_filter)
    solicitudes_vals = list(solicitudes_qs.values("insumo_id", "cantidad"))
    insumo_ids = [x["insumo_id"] for x in solicitudes_vals]

    latest_cost_by_insumo: dict[int, Decimal] = {}
    if insumo_ids:
        for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
            if c.insumo_id not in latest_cost_by_insumo:
                latest_cost_by_insumo[c.insumo_id] = c.costo_unitario

    estimado = sum(
        ((row.get("cantidad") or Decimal("0")) * latest_cost_by_insumo.get(row["insumo_id"], Decimal("0")))
        for row in solicitudes_vals
    )

    ordenes_qs = OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR).filter(
        fecha_emision__range=(start_date, end_date)
    )
    ordenes_qs = _filter_ordenes_by_scope(ordenes_qs, source_filter, plan_filter)
    ordenes_qs = _filter_ordenes_by_categoria(ordenes_qs, categoria_filter)
    ejecutado = ordenes_qs.aggregate(total=Sum("monto_estimado"))["total"] or Decimal("0")

    base = max(estimado, ejecutado)
    ratio_pct = ((base * Decimal("100")) / objetivo) if objetivo > 0 else None
    if objetivo <= 0:
        estado_label = "Sin objetivo"
        estado_badge = "bg-warning"
    elif ratio_pct <= Decimal("90"):
        estado_label = "Verde"
        estado_badge = "bg-success"
    elif ratio_pct <= Decimal("100"):
        estado_label = "Amarillo"
        estado_badge = "bg-warning"
    else:
        estado_label = "Rojo"
        estado_badge = "bg-danger"

    return {
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
        "objetivo": objetivo,
        "estimado": estimado,
        "ejecutado": ejecutado,
        "ratio_pct": ratio_pct,
        "estado_label": estado_label,
        "estado_badge": estado_badge,
    }


def _build_budget_history(periodo_mes: str, source_filter: str, plan_filter: str, categoria_filter: str) -> list[dict]:
    rows: list[dict] = []
    for delta in range(0, 6):
        month_value = _shift_month(periodo_mes, delta)
        summary = _compute_budget_period_summary("mes", month_value, source_filter, plan_filter, categoria_filter)
        rows.append(summary)
    return rows


def _build_provider_dashboard(
    periodo_mes: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    current_rows: list[dict],
) -> dict:
    months_desc = [_shift_month(periodo_mes, d) for d in range(0, 6)]
    months_asc = list(reversed(months_desc))

    monthly_provider_data: dict[str, dict[str, dict[str, Decimal]]] = {}
    provider_score: dict[str, Decimal] = {}

    for month_value in months_desc:
        start_date, end_date = _periodo_bounds("mes", month_value)
        solicitudes_qs = SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido").filter(
            fecha_requerida__range=(start_date, end_date)
        )
        solicitudes_qs = _filter_solicitudes_by_scope(solicitudes_qs, source_filter, plan_filter)
        solicitudes_qs = _filter_solicitudes_by_categoria(solicitudes_qs, categoria_filter)
        solicitudes = list(solicitudes_qs)

        insumo_ids = [s.insumo_id for s in solicitudes]
        latest_cost_by_insumo: dict[int, Decimal] = {}
        if insumo_ids:
            for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
                if c.insumo_id not in latest_cost_by_insumo:
                    latest_cost_by_insumo[c.insumo_id] = c.costo_unitario

        estimado_by_provider: dict[str, Decimal] = {}
        for s in solicitudes:
            proveedor_nombre = (
                s.proveedor_sugerido.nombre
                if s.proveedor_sugerido_id
                else (
                    s.insumo.proveedor_principal.nombre
                    if getattr(s.insumo, "proveedor_principal_id", None)
                    else "Sin proveedor"
                )
            )
            estimado_by_provider[proveedor_nombre] = estimado_by_provider.get(proveedor_nombre, Decimal("0")) + (
                (s.cantidad or Decimal("0")) * latest_cost_by_insumo.get(s.insumo_id, Decimal("0"))
            )

        ordenes_qs = OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR).filter(
            fecha_emision__range=(start_date, end_date)
        )
        ordenes_qs = _filter_ordenes_by_scope(ordenes_qs, source_filter, plan_filter)
        ordenes_qs = _filter_ordenes_by_categoria(ordenes_qs, categoria_filter)
        ejecutado_by_provider: dict[str, Decimal] = {}
        for row in ordenes_qs.values("proveedor__nombre").annotate(total=Sum("monto_estimado")):
            provider_name = row["proveedor__nombre"] or "Sin proveedor"
            ejecutado_by_provider[provider_name] = row["total"] or Decimal("0")

        providers = set(estimado_by_provider.keys()) | set(ejecutado_by_provider.keys())
        for provider_name in providers:
            estimado = estimado_by_provider.get(provider_name, Decimal("0"))
            ejecutado = ejecutado_by_provider.get(provider_name, Decimal("0"))
            variacion = ejecutado - estimado
            monthly_provider_data.setdefault(provider_name, {})[month_value] = {
                "estimado": estimado,
                "ejecutado": ejecutado,
                "variacion": variacion,
            }
            provider_score[provider_name] = provider_score.get(provider_name, Decimal("0")) + abs(variacion)

    top_providers = [p for p, _ in sorted(provider_score.items(), key=lambda x: x[1], reverse=True)[:6]]

    trend_rows: list[dict] = []
    for provider_name in top_providers:
        for month_value in months_asc:
            data = monthly_provider_data.get(provider_name, {}).get(
                month_value,
                {"estimado": Decimal("0"), "ejecutado": Decimal("0"), "variacion": Decimal("0")},
            )
            trend_rows.append(
                {
                    "proveedor": provider_name,
                    "mes": month_value,
                    "estimado": data["estimado"],
                    "ejecutado": data["ejecutado"],
                    "variacion": data["variacion"],
                }
            )

    top_desviaciones = sorted(
        [r for r in current_rows if (r["variacion"] or Decimal("0")) != Decimal("0")],
        key=lambda x: abs(x["variacion"]),
        reverse=True,
    )[:12]

    return {
        "top_desviaciones": top_desviaciones,
        "trend_rows": trend_rows,
        "trend_months": months_asc,
        "trend_providers": top_providers,
    }


def _build_category_dashboard(
    periodo_mes: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    current_rows: list[dict],
) -> dict:
    months_desc = [_shift_month(periodo_mes, d) for d in range(0, 6)]
    months_asc = list(reversed(months_desc))

    monthly_category_data: dict[str, dict[str, dict[str, Decimal]]] = {}
    category_score: dict[str, Decimal] = {}

    for month_value in months_desc:
        start_date, end_date = _periodo_bounds("mes", month_value)
        solicitudes_qs = (
            SolicitudCompra.objects.select_related("insumo", "insumo__unidad_base", "proveedor_sugerido")
            .filter(fecha_requerida__range=(start_date, end_date))
        )
        solicitudes_qs = _filter_solicitudes_by_scope(solicitudes_qs, source_filter, plan_filter)
        solicitudes_qs = _filter_solicitudes_by_categoria(solicitudes_qs, categoria_filter)
        solicitudes = list(solicitudes_qs)

        insumo_ids = [s.insumo_id for s in solicitudes]
        latest_cost_by_insumo: dict[int, Decimal] = {}
        if insumo_ids:
            for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
                if c.insumo_id not in latest_cost_by_insumo:
                    latest_cost_by_insumo[c.insumo_id] = c.costo_unitario

        estimado_by_categoria: dict[str, Decimal] = {}
        for s in solicitudes:
            categoria_nombre = _resolve_insumo_categoria(s.insumo)
            estimado = getattr(s, "presupuesto_estimado", None)
            if estimado is None:
                estimado = (s.cantidad or Decimal("0")) * latest_cost_by_insumo.get(s.insumo_id, Decimal("0"))
            estimado_by_categoria[categoria_nombre] = estimado_by_categoria.get(categoria_nombre, Decimal("0")) + (
                estimado or Decimal("0")
            )

        ordenes_qs = (
            OrdenCompra.objects.select_related("solicitud", "solicitud__insumo", "solicitud__insumo__unidad_base")
            .exclude(estatus=OrdenCompra.STATUS_BORRADOR)
            .filter(fecha_emision__range=(start_date, end_date))
        )
        ordenes_qs = _filter_ordenes_by_scope(ordenes_qs, source_filter, plan_filter)
        ordenes_qs = _filter_ordenes_by_categoria(ordenes_qs, categoria_filter)
        ejecutado_by_categoria: dict[str, Decimal] = {}
        for orden in ordenes_qs:
            categoria_nombre = "Sin categoría"
            if orden.solicitud_id and getattr(orden.solicitud, "insumo_id", None):
                categoria_nombre = _resolve_insumo_categoria(orden.solicitud.insumo)
            ejecutado_by_categoria[categoria_nombre] = ejecutado_by_categoria.get(categoria_nombre, Decimal("0")) + (
                orden.monto_estimado or Decimal("0")
            )

        categorias = set(estimado_by_categoria.keys()) | set(ejecutado_by_categoria.keys())
        for categoria_nombre in categorias:
            estimado = estimado_by_categoria.get(categoria_nombre, Decimal("0"))
            ejecutado = ejecutado_by_categoria.get(categoria_nombre, Decimal("0"))
            variacion = ejecutado - estimado
            monthly_category_data.setdefault(categoria_nombre, {})[month_value] = {
                "estimado": estimado,
                "ejecutado": ejecutado,
                "variacion": variacion,
            }
            category_score[categoria_nombre] = category_score.get(categoria_nombre, Decimal("0")) + abs(variacion)

    top_categorias = [c for c, _ in sorted(category_score.items(), key=lambda x: x[1], reverse=True)[:6]]

    trend_rows: list[dict] = []
    for categoria_nombre in top_categorias:
        for month_value in months_asc:
            data = monthly_category_data.get(categoria_nombre, {}).get(
                month_value,
                {"estimado": Decimal("0"), "ejecutado": Decimal("0"), "variacion": Decimal("0")},
            )
            trend_rows.append(
                {
                    "categoria": categoria_nombre,
                    "mes": month_value,
                    "estimado": data["estimado"],
                    "ejecutado": data["ejecutado"],
                    "variacion": data["variacion"],
                }
            )

    top_desviaciones = sorted(
        [r for r in current_rows if (r["variacion"] or Decimal("0")) != Decimal("0")],
        key=lambda x: abs(x["variacion"]),
        reverse=True,
    )[:12]

    return {
        "top_desviaciones": top_desviaciones,
        "trend_rows": trend_rows,
        "trend_months": months_asc,
        "trend_categories": top_categorias,
    }


def _build_consumo_vs_plan_dashboard(
    periodo_tipo: str,
    periodo_mes: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    consumo_ref_filter: str = "all",
) -> dict:
    start_date, end_date = _periodo_bounds(periodo_tipo, periodo_mes)
    consumo_ref_filter = _sanitize_consumo_ref_filter(consumo_ref_filter)
    if not start_date or not end_date:
        # Evita consultas históricas abiertas cuando el filtro está en "Todos".
        end_date = timezone.localdate()
        start_date = end_date - timedelta(days=90)

    plan_qs = PlanProduccion.objects.prefetch_related(
        "items__receta__lineas__insumo__unidad_base"
    ).filter(fecha_produccion__range=(start_date, end_date))
    if plan_filter and plan_filter.isdigit():
        plan_qs = plan_qs.filter(id=int(plan_filter))
    if source_filter == "manual":
        plan_qs = plan_qs.none()

    plan_qty_by_insumo: dict[int, Decimal] = {}
    insumo_meta: dict[int, dict] = {}
    for plan in plan_qs:
        for item in plan.items.all():
            multiplicador = Decimal(str(item.cantidad or 0))
            if multiplicador <= 0:
                continue
            for linea in item.receta.lineas.all():
                if not linea.insumo_id:
                    continue
                qty_base = Decimal(str(linea.cantidad or 0))
                if qty_base <= 0:
                    continue
                qty_requerida = qty_base * multiplicador
                if qty_requerida <= 0:
                    continue
                if categoria_filter and _sanitize_categoria_filter(categoria_filter):
                    if _normalize_categoria_text(_resolve_insumo_categoria(linea.insumo)) != _normalize_categoria_text(
                        _sanitize_categoria_filter(categoria_filter)
                    ):
                        continue
                plan_qty_by_insumo[linea.insumo_id] = plan_qty_by_insumo.get(linea.insumo_id, Decimal("0")) + qty_requerida
                if linea.insumo_id not in insumo_meta:
                    insumo_meta[linea.insumo_id] = {
                        "insumo": linea.insumo.nombre,
                        "unidad": linea.insumo.unidad_base.codigo if linea.insumo.unidad_base_id else "-",
                        "categoria": _resolve_insumo_categoria(linea.insumo),
                    }

    movimientos_qs = (
        MovimientoInventario.objects.select_related("insumo", "insumo__unidad_base")
        .filter(tipo__in=[MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO])
        .filter(fecha__date__range=(start_date, end_date))
    )
    movimientos_qs = _filter_movimientos_by_scope(
        movimientos_qs,
        source_filter,
        plan_filter,
        consumo_ref_filter,
    )
    movimientos_qs = _filter_movimientos_by_categoria(movimientos_qs, categoria_filter)

    actual_qty_by_insumo: dict[int, Decimal] = {}
    for mov in movimientos_qs:
        if not mov.insumo_id:
            continue
        qty = abs(Decimal(str(mov.cantidad or 0)))
        if qty <= 0:
            continue
        actual_qty_by_insumo[mov.insumo_id] = actual_qty_by_insumo.get(mov.insumo_id, Decimal("0")) + qty
        if mov.insumo_id not in insumo_meta:
            insumo_meta[mov.insumo_id] = {
                "insumo": mov.insumo.nombre,
                "unidad": mov.insumo.unidad_base.codigo if mov.insumo.unidad_base_id else "-",
                "categoria": _resolve_insumo_categoria(mov.insumo),
            }

    insumo_ids = list(set(plan_qty_by_insumo.keys()) | set(actual_qty_by_insumo.keys()))
    latest_cost_by_insumo: dict[int, Decimal] = {}
    if insumo_ids:
        for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
            if c.insumo_id not in latest_cost_by_insumo:
                latest_cost_by_insumo[c.insumo_id] = c.costo_unitario

    rows: list[dict] = []
    totals = {
        "plan_qty_total": Decimal("0"),
        "consumo_real_qty_total": Decimal("0"),
        "plan_cost_total": Decimal("0"),
        "consumo_real_cost_total": Decimal("0"),
        "variacion_cost_total": Decimal("0"),
        "sin_costo_count": 0,
        "semaforo_verde_count": 0,
        "semaforo_amarillo_count": 0,
        "semaforo_rojo_count": 0,
    }
    for insumo_id in insumo_ids:
        plan_qty = plan_qty_by_insumo.get(insumo_id, Decimal("0"))
        real_qty = actual_qty_by_insumo.get(insumo_id, Decimal("0"))
        costo_unitario = latest_cost_by_insumo.get(insumo_id, Decimal("0"))

        plan_cost = plan_qty * costo_unitario
        real_cost = real_qty * costo_unitario
        variacion_qty = real_qty - plan_qty
        variacion_cost = real_cost - plan_cost
        consumo_pct = None
        if plan_qty > 0:
            consumo_pct = (real_qty * Decimal("100")) / plan_qty
        sin_costo = costo_unitario <= 0 and (plan_qty > 0 or real_qty > 0)

        estado = "OK"
        semaforo = "VERDE"
        if plan_qty <= 0 and real_qty > 0:
            estado = "SIN_PLAN"
            semaforo = "ROJO"
        elif plan_qty > 0 and real_qty <= 0:
            estado = "SIN_CONSUMO"
            semaforo = "AMARILLO"
        elif consumo_pct is not None and consumo_pct > Decimal("110"):
            estado = "SOBRECONSUMO"
            semaforo = "ROJO"
        elif consumo_pct is not None and consumo_pct < Decimal("90"):
            estado = "BAJO_CONSUMO"
            semaforo = "AMARILLO"

        if sin_costo and semaforo == "VERDE":
            semaforo = "AMARILLO"

        if semaforo == "ROJO":
            totals["semaforo_rojo_count"] += 1
        elif semaforo == "AMARILLO":
            totals["semaforo_amarillo_count"] += 1
        else:
            totals["semaforo_verde_count"] += 1

        if sin_costo:
            totals["sin_costo_count"] += 1

        meta = insumo_meta.get(insumo_id, {"insumo": f"Insumo {insumo_id}", "unidad": "-", "categoria": "Sin categoría"})
        rows.append(
            {
                "insumo_id": insumo_id,
                "insumo": meta["insumo"],
                "categoria": meta["categoria"],
                "unidad": meta["unidad"],
                "cantidad_plan": plan_qty,
                "cantidad_real": real_qty,
                "variacion_qty": variacion_qty,
                "costo_unitario": costo_unitario,
                "costo_plan": plan_cost,
                "costo_real": real_cost,
                "variacion_cost": variacion_cost,
                "consumo_pct": consumo_pct,
                "estado": estado,
                "semaforo": semaforo,
                "sin_costo": sin_costo,
                "alerta": "Sin costo unitario" if sin_costo else "",
            }
        )
        totals["plan_qty_total"] += plan_qty
        totals["consumo_real_qty_total"] += real_qty
        totals["plan_cost_total"] += plan_cost
        totals["consumo_real_cost_total"] += real_cost
        totals["variacion_cost_total"] += variacion_cost

    rows = sorted(rows, key=lambda x: abs(x["variacion_cost"]), reverse=True)
    totals["cobertura_pct"] = (
        (totals["consumo_real_qty_total"] * Decimal("100")) / totals["plan_qty_total"]
        if totals["plan_qty_total"] > 0
        else None
    )
    return {
        "rows": rows[:30],
        "totals": totals,
        "period_start": start_date,
        "period_end": end_date,
    }


def _build_budget_context(
    solicitudes,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    periodo_tipo: str,
    periodo_mes: str,
) -> dict:
    total_estimado = sum((s.presupuesto_estimado for s in solicitudes), Decimal("0"))

    start_date, end_date = _periodo_bounds(periodo_tipo, periodo_mes)
    ordenes_qs = (
        OrdenCompra.objects.select_related("proveedor", "solicitud", "solicitud__insumo", "solicitud__insumo__unidad_base")
        .exclude(estatus=OrdenCompra.STATUS_BORRADOR)
    )
    if start_date and end_date:
        ordenes_qs = ordenes_qs.filter(fecha_emision__range=(start_date, end_date))
    ordenes_qs = _filter_ordenes_by_scope(ordenes_qs, source_filter, plan_filter)
    ordenes_qs = _filter_ordenes_by_categoria(ordenes_qs, categoria_filter)

    total_ejecutado = ordenes_qs.aggregate(total=Sum("monto_estimado"))["total"] or Decimal("0")
    variacion_ejecutado_vs_estimado = total_ejecutado - total_estimado

    presupuesto_periodo = None
    objetivo = None
    variacion_objetivo = None
    variacion_objetivo_pct = None
    avance_objetivo_pct = None
    objetivos_proveedor_by_name: dict[str, PresupuestoCompraProveedor] = {}
    objetivos_categoria_by_norm: dict[str, PresupuestoCompraCategoria] = {}
    if periodo_tipo != "all":
        presupuesto_periodo = PresupuestoCompraPeriodo.objects.filter(
            periodo_tipo=periodo_tipo,
            periodo_mes=periodo_mes,
        ).first()
        objetivo = presupuesto_periodo.monto_objetivo if presupuesto_periodo else Decimal("0")
        variacion_objetivo = total_estimado - objetivo
        if objetivo > 0:
            variacion_objetivo_pct = (variacion_objetivo * Decimal("100")) / objetivo
            avance_objetivo_pct = (total_ejecutado * Decimal("100")) / objetivo
        if presupuesto_periodo:
            for objetivo_prov in (
                PresupuestoCompraProveedor.objects.select_related("proveedor")
                .filter(presupuesto_periodo=presupuesto_periodo)
                .only("id", "proveedor__nombre", "monto_objetivo")
            ):
                objetivos_proveedor_by_name[objetivo_prov.proveedor.nombre] = objetivo_prov
            for objetivo_cat in (
                PresupuestoCompraCategoria.objects.filter(presupuesto_periodo=presupuesto_periodo)
                .only("id", "categoria", "categoria_normalizada", "monto_objetivo")
            ):
                objetivos_categoria_by_norm[objetivo_cat.categoria_normalizada] = objetivo_cat

    estimado_by_proveedor: dict[str, Decimal] = {}
    estimado_by_categoria: dict[str, Decimal] = {}
    for s in solicitudes:
        proveedor_nombre = (
            s.proveedor_sugerido.nombre
            if s.proveedor_sugerido_id
            else (
                s.insumo.proveedor_principal.nombre
                if getattr(s.insumo, "proveedor_principal_id", None)
                else "Sin proveedor"
            )
        )
        estimado_by_proveedor[proveedor_nombre] = estimado_by_proveedor.get(proveedor_nombre, Decimal("0")) + (
            s.presupuesto_estimado or Decimal("0")
        )
        categoria_nombre = _resolve_insumo_categoria(s.insumo)
        estimado_by_categoria[categoria_nombre] = estimado_by_categoria.get(categoria_nombre, Decimal("0")) + (
            s.presupuesto_estimado or Decimal("0")
        )

    ejecutado_by_proveedor: dict[str, Decimal] = {}
    for row in ordenes_qs.values("proveedor__nombre").annotate(total=Sum("monto_estimado")):
        proveedor_nombre = row["proveedor__nombre"] or "Sin proveedor"
        ejecutado_by_proveedor[proveedor_nombre] = row["total"] or Decimal("0")

    ejecutado_by_categoria: dict[str, Decimal] = {}
    for orden in ordenes_qs:
        categoria_nombre = "Sin categoría"
        if orden.solicitud_id and getattr(orden.solicitud, "insumo_id", None):
            categoria_nombre = _resolve_insumo_categoria(orden.solicitud.insumo)
        ejecutado_by_categoria[categoria_nombre] = ejecutado_by_categoria.get(categoria_nombre, Decimal("0")) + (
            orden.monto_estimado or Decimal("0")
        )

    proveedores = (
        set(estimado_by_proveedor.keys())
        | set(ejecutado_by_proveedor.keys())
        | set(objetivos_proveedor_by_name.keys())
    )
    rows_proveedor = []
    for proveedor_nombre in proveedores:
        estimado = estimado_by_proveedor.get(proveedor_nombre, Decimal("0"))
        ejecutado = ejecutado_by_proveedor.get(proveedor_nombre, Decimal("0"))
        variacion = ejecutado - estimado
        objetivo_proveedor_obj = objetivos_proveedor_by_name.get(proveedor_nombre)
        objetivo_proveedor = (
            objetivo_proveedor_obj.monto_objetivo if objetivo_proveedor_obj else Decimal("0")
        )
        base_control = max(estimado, ejecutado)
        uso_objetivo_pct = (
            (base_control * Decimal("100")) / objetivo_proveedor
            if objetivo_proveedor > 0
            else None
        )
        objetivo_estado = "sin_objetivo"
        if objetivo_proveedor > 0:
            if base_control > objetivo_proveedor:
                objetivo_estado = "excedido"
            elif base_control >= (objetivo_proveedor * Decimal("0.90")):
                objetivo_estado = "preventivo"
            else:
                objetivo_estado = "ok"
        share = (estimado * Decimal("100") / total_estimado) if total_estimado > 0 else Decimal("0")
        rows_proveedor.append(
            {
                "proveedor": proveedor_nombre,
                "estimado": estimado,
                "ejecutado": ejecutado,
                "variacion": variacion,
                "participacion_pct": share,
                "objetivo_proveedor": objetivo_proveedor,
                "uso_objetivo_pct": uso_objetivo_pct,
                "objetivo_estado": objetivo_estado,
            }
        )
    rows_proveedor.sort(
        key=lambda r: max(
            r["estimado"] or Decimal("0"),
            r["ejecutado"] or Decimal("0"),
            r["objetivo_proveedor"] or Decimal("0"),
        ),
        reverse=True,
    )

    categorias = (
        set(estimado_by_categoria.keys())
        | set(ejecutado_by_categoria.keys())
        | set(
            obj.categoria
            for obj in objetivos_categoria_by_norm.values()
            if (obj.categoria or "").strip()
        )
    )
    rows_categoria = []
    for categoria_nombre in categorias:
        categoria_display = " ".join((categoria_nombre or "").strip().split()) or "Sin categoría"
        categoria_norm = _normalize_categoria_text(categoria_display)
        estimado = estimado_by_categoria.get(categoria_display, Decimal("0"))
        ejecutado = ejecutado_by_categoria.get(categoria_display, Decimal("0"))
        variacion = ejecutado - estimado
        objetivo_categoria_obj = objetivos_categoria_by_norm.get(categoria_norm)
        objetivo_categoria = (
            objetivo_categoria_obj.monto_objetivo if objetivo_categoria_obj else Decimal("0")
        )
        base_control = max(estimado, ejecutado)
        uso_objetivo_pct = (
            (base_control * Decimal("100")) / objetivo_categoria
            if objetivo_categoria > 0
            else None
        )
        objetivo_estado = "sin_objetivo"
        if objetivo_categoria > 0:
            if base_control > objetivo_categoria:
                objetivo_estado = "excedido"
            elif base_control >= (objetivo_categoria * Decimal("0.90")):
                objetivo_estado = "preventivo"
            else:
                objetivo_estado = "ok"
        share = (estimado * Decimal("100") / total_estimado) if total_estimado > 0 else Decimal("0")
        rows_categoria.append(
            {
                "categoria": categoria_display,
                "estimado": estimado,
                "ejecutado": ejecutado,
                "variacion": variacion,
                "participacion_pct": share,
                "objetivo_categoria": objetivo_categoria,
                "uso_objetivo_pct": uso_objetivo_pct,
                "objetivo_estado": objetivo_estado,
            }
        )
    rows_categoria.sort(
        key=lambda r: max(
            r["estimado"] or Decimal("0"),
            r["ejecutado"] or Decimal("0"),
            r["objetivo_categoria"] or Decimal("0"),
        ),
        reverse=True,
    )

    alertas: list[dict] = []
    if periodo_tipo != "all":
        if objetivo is not None and objetivo > 0:
            if total_estimado > objetivo:
                alertas.append(
                    {
                        "nivel": "alto",
                        "tipo": "periodo_estimado",
                        "titulo": "Estimado supera objetivo",
                        "detalle": f"Estimado ${total_estimado:.2f} > Objetivo ${objetivo:.2f}",
                    }
                )
            if total_ejecutado > objetivo:
                alertas.append(
                    {
                        "nivel": "alto",
                        "tipo": "periodo_ejecutado",
                        "titulo": "Ejecutado supera objetivo",
                        "detalle": f"Ejecutado ${total_ejecutado:.2f} > Objetivo ${objetivo:.2f}",
                    }
                )

        for row in rows_proveedor:
            estimado = row["estimado"] or Decimal("0")
            ejecutado = row["ejecutado"] or Decimal("0")
            variacion = row["variacion"] or Decimal("0")
            objetivo_proveedor = row["objetivo_proveedor"] or Decimal("0")
            uso_objetivo_pct = row["uso_objetivo_pct"]
            if objetivo_proveedor > 0 and uso_objetivo_pct is not None:
                if uso_objetivo_pct > Decimal("100"):
                    alertas.append(
                        {
                            "nivel": "alto",
                            "tipo": "proveedor_objetivo_excedido",
                            "titulo": f"{row['proveedor']}: supera objetivo proveedor",
                            "detalle": f"${max(estimado, ejecutado):.2f} > ${objetivo_proveedor:.2f} ({uso_objetivo_pct:.2f}%)",
                        }
                    )
                elif uso_objetivo_pct >= Decimal("90"):
                    alertas.append(
                        {
                            "nivel": "medio",
                            "tipo": "proveedor_objetivo_preventivo",
                            "titulo": f"{row['proveedor']}: cerca de objetivo proveedor",
                            "detalle": f"${max(estimado, ejecutado):.2f} de ${objetivo_proveedor:.2f} ({uso_objetivo_pct:.2f}%)",
                        }
                    )
            if ejecutado <= 0:
                continue
            if estimado <= 0 and ejecutado > 0:
                alertas.append(
                    {
                        "nivel": "medio",
                        "tipo": "proveedor_sin_base",
                        "titulo": f"{row['proveedor']}: sin base estimada",
                        "detalle": f"Ejecutado ${ejecutado:.2f} sin estimado en solicitudes",
                    }
                )
                continue
            if variacion > 0:
                pct = (variacion * Decimal("100")) / estimado if estimado > 0 else Decimal("0")
                alertas.append(
                    {
                        "nivel": "medio",
                        "tipo": "proveedor_desviado",
                        "titulo": f"{row['proveedor']}: ejecutado arriba de estimado",
                        "detalle": f"+${variacion:.2f} ({pct:.2f}%) sobre estimado",
                    }
                )

        for row in rows_categoria:
            estimado = row["estimado"] or Decimal("0")
            ejecutado = row["ejecutado"] or Decimal("0")
            variacion = row["variacion"] or Decimal("0")
            objetivo_categoria = row["objetivo_categoria"] or Decimal("0")
            uso_objetivo_pct = row["uso_objetivo_pct"]
            if objetivo_categoria > 0 and uso_objetivo_pct is not None:
                if uso_objetivo_pct > Decimal("100"):
                    alertas.append(
                        {
                            "nivel": "alto",
                            "tipo": "categoria_objetivo_excedido",
                            "titulo": f"{row['categoria']}: supera objetivo categoría",
                            "detalle": f"${max(estimado, ejecutado):.2f} > ${objetivo_categoria:.2f} ({uso_objetivo_pct:.2f}%)",
                        }
                    )
                elif uso_objetivo_pct >= Decimal("90"):
                    alertas.append(
                        {
                            "nivel": "medio",
                            "tipo": "categoria_objetivo_preventivo",
                            "titulo": f"{row['categoria']}: cerca de objetivo categoría",
                            "detalle": f"${max(estimado, ejecutado):.2f} de ${objetivo_categoria:.2f} ({uso_objetivo_pct:.2f}%)",
                        }
                    )
            if ejecutado <= 0:
                continue
            if estimado <= 0 and ejecutado > 0:
                alertas.append(
                    {
                        "nivel": "medio",
                        "tipo": "categoria_sin_base",
                        "titulo": f"{row['categoria']}: sin base estimada",
                        "detalle": f"Ejecutado ${ejecutado:.2f} sin estimado en solicitudes",
                    }
                )
                continue
            if variacion > 0:
                pct = (variacion * Decimal("100")) / estimado if estimado > 0 else Decimal("0")
                alertas.append(
                    {
                        "nivel": "medio",
                        "tipo": "categoria_desviada",
                        "titulo": f"{row['categoria']}: ejecutado arriba de estimado",
                        "detalle": f"+${variacion:.2f} ({pct:.2f}%) sobre estimado",
                    }
                )
    alertas.sort(key=lambda x: (0 if x["nivel"] == "alto" else 1, x["titulo"]))

    return {
        "presupuesto_periodo": presupuesto_periodo,
        "presupuesto_objetivo": objetivo,
        "presupuesto_estimado_total": total_estimado,
        "presupuesto_ejecutado_total": total_ejecutado,
        "presupuesto_variacion_objetivo": variacion_objetivo,
        "presupuesto_variacion_objetivo_pct": variacion_objetivo_pct,
        "presupuesto_avance_objetivo_pct": avance_objetivo_pct,
        "presupuesto_variacion_ejecutado_estimado": variacion_ejecutado_vs_estimado,
        "presupuesto_rows_proveedor": rows_proveedor,
        "presupuesto_rows_categoria": rows_categoria,
        "presupuesto_objetivos_proveedor_total": len(objetivos_proveedor_by_name),
        "presupuesto_objetivos_categoria_total": len(objetivos_categoria_by_norm),
        "presupuesto_alertas": alertas[:25],
        "presupuesto_alertas_total": len(alertas),
        "presupuesto_alertas_altas": sum(1 for a in alertas if a["nivel"] == "alto"),
        "presupuesto_alertas_medias": sum(1 for a in alertas if a["nivel"] == "medio"),
        "presupuesto_alertas_preventivas": sum(
            1
            for a in alertas
            if a["tipo"] in {"proveedor_objetivo_preventivo", "categoria_objetivo_preventivo"}
        ),
        "presupuesto_alertas_excedidas": sum(
            1
            for a in alertas
            if a["tipo"]
            in {
                "proveedor_objetivo_excedido",
                "categoria_objetivo_excedido",
                "periodo_estimado",
                "periodo_ejecutado",
            }
        ),
    }


def _export_solicitudes_csv(
    solicitudes,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    reabasto_filter: str,
    periodo_tipo: str,
    periodo_mes: str,
    periodo_label: str,
) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="solicitudes_compras_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "Folio",
            "Area",
            "Solicitante",
            "Origen",
            "Plan",
            "Insumo",
            "Proveedor sugerido",
            "Cantidad",
            "Costo unitario",
            "Presupuesto estimado",
            "Fecha requerida",
            "Estatus",
            "Reabasto",
            "Detalle reabasto",
            "Filtro origen",
            "Filtro plan",
            "Filtro categoria",
            "Filtro reabasto",
            "Filtro periodo",
            "Filtro mes",
        ]
    )
    for s in solicitudes:
        writer.writerow(
            [
                s.folio,
                s.area,
                s.solicitante,
                "PLAN" if s.source_tipo == "plan" else "MANUAL",
                s.source_plan_id or "",
                s.insumo.nombre,
                s.proveedor_sugerido.nombre if s.proveedor_sugerido_id else "",
                s.cantidad,
                s.costo_unitario,
                s.presupuesto_estimado,
                s.fecha_requerida,
                s.get_estatus_display(),
                s.reabasto_texto,
                s.reabasto_detalle,
                source_filter,
                plan_filter or "",
                categoria_filter or "",
                reabasto_filter,
                periodo_label,
                periodo_mes if periodo_tipo != "all" else "",
            ]
        )
    return response


def _export_solicitudes_xlsx(
    solicitudes,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    reabasto_filter: str,
    periodo_tipo: str,
    periodo_mes: str,
    periodo_label: str,
) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Solicitudes"
    ws.append(
        [
            "Folio",
            "Area",
            "Solicitante",
            "Origen",
            "Plan",
            "Insumo",
            "Proveedor sugerido",
            "Cantidad",
            "Costo unitario",
            "Presupuesto estimado",
            "Fecha requerida",
            "Estatus",
            "Reabasto",
            "Detalle reabasto",
            "Filtro origen",
            "Filtro plan",
            "Filtro categoria",
            "Filtro reabasto",
            "Filtro periodo",
            "Filtro mes",
        ]
    )
    for s in solicitudes:
        ws.append(
            [
                s.folio,
                s.area,
                s.solicitante,
                "PLAN" if s.source_tipo == "plan" else "MANUAL",
                s.source_plan_id or "",
                s.insumo.nombre,
                s.proveedor_sugerido.nombre if s.proveedor_sugerido_id else "",
                float(s.cantidad or 0),
                float(s.costo_unitario or 0),
                float(s.presupuesto_estimado or 0),
                s.fecha_requerida.isoformat() if s.fecha_requerida else "",
                s.get_estatus_display(),
                s.reabasto_texto,
                s.reabasto_detalle,
                source_filter,
                plan_filter or "",
                categoria_filter or "",
                reabasto_filter,
                periodo_label,
                periodo_mes if periodo_tipo != "all" else "",
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="solicitudes_compras_{now_str}.xlsx"'
    return response


def _export_consolidado_csv(
    solicitudes,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    reabasto_filter: str,
    periodo_label: str,
    budget_ctx: dict,
) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="compras_consolidado_{now_str}.csv"'
    writer = csv.writer(response)

    writer.writerow(["RESUMEN EJECUTIVO COMPRAS"])
    writer.writerow(["Filtro periodo", periodo_label])
    writer.writerow(["Filtro origen", source_filter])
    writer.writerow(["Filtro plan", plan_filter or "-"])
    writer.writerow(["Filtro categoria", categoria_filter or "-"])
    writer.writerow(["Filtro reabasto", reabasto_filter])
    writer.writerow(["Objetivo presupuesto", budget_ctx.get("presupuesto_objetivo") or ""])
    writer.writerow(["Estimado solicitudes", budget_ctx["presupuesto_estimado_total"]])
    writer.writerow(["Ejecutado ordenes", budget_ctx["presupuesto_ejecutado_total"]])
    writer.writerow(["Variacion vs objetivo", budget_ctx.get("presupuesto_variacion_objetivo") or ""])
    writer.writerow(["Variacion ejecutado vs estimado", budget_ctx["presupuesto_variacion_ejecutado_estimado"]])
    writer.writerow([])
    writer.writerow(["ALERTAS"])
    writer.writerow(["Nivel", "Tipo", "Titulo", "Detalle"])
    for alerta in budget_ctx.get("presupuesto_alertas", []):
        writer.writerow([alerta.get("nivel"), alerta.get("tipo"), alerta.get("titulo"), alerta.get("detalle")])
    writer.writerow([])

    writer.writerow(["DESVIACION POR PROVEEDOR"])
    writer.writerow(
        [
            "Proveedor",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion estimado %",
            "Objetivo proveedor",
            "% Uso objetivo proveedor",
        ]
    )
    for row in budget_ctx["presupuesto_rows_proveedor"]:
        writer.writerow(
            [
                row["proveedor"],
                row["estimado"],
                row["ejecutado"],
                row["variacion"],
                round(float(row["participacion_pct"]), 2),
                row.get("objetivo_proveedor", Decimal("0")),
                round(float(row["uso_objetivo_pct"] or 0), 2) if row.get("uso_objetivo_pct") is not None else "",
            ]
        )
    writer.writerow([])

    writer.writerow(["DESVIACION POR CATEGORIA"])
    writer.writerow(
        [
            "Categoria",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion estimado %",
            "Objetivo categoria",
            "% Uso objetivo categoria",
        ]
    )
    for row in budget_ctx.get("presupuesto_rows_categoria", []):
        writer.writerow(
            [
                row["categoria"],
                row["estimado"],
                row["ejecutado"],
                row["variacion"],
                round(float(row["participacion_pct"]), 2),
                row.get("objetivo_categoria", Decimal("0")),
                round(float(row["uso_objetivo_pct"] or 0), 2) if row.get("uso_objetivo_pct") is not None else "",
            ]
        )
    writer.writerow([])

    writer.writerow(["DETALLE SOLICITUDES"])
    writer.writerow(
        [
            "Folio",
            "Area",
            "Solicitante",
            "Insumo",
            "Proveedor sugerido",
            "Cantidad",
            "Costo unitario",
            "Presupuesto",
            "Fecha requerida",
            "Estatus",
        ]
    )
    for s in solicitudes:
        writer.writerow(
            [
                s.folio,
                s.area,
                s.solicitante,
                s.insumo.nombre,
                s.proveedor_sugerido.nombre if s.proveedor_sugerido_id else "",
                s.cantidad,
                s.costo_unitario,
                s.presupuesto_estimado,
                s.fecha_requerida,
                s.get_estatus_display(),
            ]
        )
    return response


def _export_consolidado_xlsx(
    solicitudes,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    reabasto_filter: str,
    periodo_label: str,
    budget_ctx: dict,
) -> HttpResponse:
    wb = Workbook()

    ws_resumen = wb.active
    ws_resumen.title = "Resumen"
    ws_resumen.append(["RESUMEN EJECUTIVO COMPRAS"])
    ws_resumen.append(["Filtro periodo", periodo_label])
    ws_resumen.append(["Filtro origen", source_filter])
    ws_resumen.append(["Filtro plan", plan_filter or "-"])
    ws_resumen.append(["Filtro categoria", categoria_filter or "-"])
    ws_resumen.append(["Filtro reabasto", reabasto_filter])
    ws_resumen.append(["Objetivo presupuesto", float(budget_ctx["presupuesto_objetivo"] or 0)])
    ws_resumen.append(["Estimado solicitudes", float(budget_ctx["presupuesto_estimado_total"] or 0)])
    ws_resumen.append(["Ejecutado ordenes", float(budget_ctx["presupuesto_ejecutado_total"] or 0)])
    ws_resumen.append(["Variacion vs objetivo", float((budget_ctx.get("presupuesto_variacion_objetivo") or 0))])
    ws_resumen.append(["Variacion ejecutado vs estimado", float(budget_ctx["presupuesto_variacion_ejecutado_estimado"] or 0)])
    ws_resumen.append([])
    ws_resumen.append(["ALERTAS"])
    ws_resumen.append(["Nivel", "Tipo", "Titulo", "Detalle"])
    for alerta in budget_ctx.get("presupuesto_alertas", []):
        ws_resumen.append(
            [
                alerta.get("nivel"),
                alerta.get("tipo"),
                alerta.get("titulo"),
                alerta.get("detalle"),
            ]
        )
    ws_resumen.append([])
    ws_resumen.append(["DESVIACION POR PROVEEDOR"])
    ws_resumen.append(
        [
            "Proveedor",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion estimado %",
            "Objetivo proveedor",
            "% Uso objetivo proveedor",
        ]
    )
    for row in budget_ctx["presupuesto_rows_proveedor"]:
        ws_resumen.append(
            [
                row["proveedor"],
                float(row["estimado"] or 0),
                float(row["ejecutado"] or 0),
                float(row["variacion"] or 0),
                float(row["participacion_pct"] or 0),
                float(row.get("objetivo_proveedor") or 0),
                float(row["uso_objetivo_pct"] or 0) if row.get("uso_objetivo_pct") is not None else None,
            ]
        )
    ws_resumen.append([])
    ws_resumen.append(["DESVIACION POR CATEGORIA"])
    ws_resumen.append(
        [
            "Categoria",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion estimado %",
            "Objetivo categoria",
            "% Uso objetivo categoria",
        ]
    )
    for row in budget_ctx.get("presupuesto_rows_categoria", []):
        ws_resumen.append(
            [
                row["categoria"],
                float(row["estimado"] or 0),
                float(row["ejecutado"] or 0),
                float(row["variacion"] or 0),
                float(row["participacion_pct"] or 0),
                float(row.get("objetivo_categoria") or 0),
                float(row["uso_objetivo_pct"] or 0) if row.get("uso_objetivo_pct") is not None else None,
            ]
        )

    ws_solicitudes = wb.create_sheet(title="Solicitudes")
    ws_solicitudes.append(
        [
            "Folio",
            "Area",
            "Solicitante",
            "Insumo",
            "Proveedor sugerido",
            "Cantidad",
            "Costo unitario",
            "Presupuesto",
            "Fecha requerida",
            "Estatus",
        ]
    )
    for s in solicitudes:
        ws_solicitudes.append(
            [
                s.folio,
                s.area,
                s.solicitante,
                s.insumo.nombre,
                s.proveedor_sugerido.nombre if s.proveedor_sugerido_id else "",
                float(s.cantidad or 0),
                float(s.costo_unitario or 0),
                float(s.presupuesto_estimado or 0),
                s.fecha_requerida.isoformat() if s.fecha_requerida else "",
                s.get_estatus_display(),
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="compras_consolidado_{now_str}.xlsx"'
    return response


def _export_tablero_proveedor_csv(
    provider_dashboard: dict,
    periodo_label: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="compras_tablero_proveedor_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(["TABLERO PROVEEDOR - COMPRAS"])
    writer.writerow(["Periodo activo", periodo_label])
    writer.writerow(["Filtro origen", source_filter])
    writer.writerow(["Filtro plan", plan_filter or "-"])
    writer.writerow(["Filtro categoria", categoria_filter or "-"])
    writer.writerow([])

    writer.writerow(["TOP DESVIACIONES (PERIODO ACTIVO)"])
    writer.writerow(
        [
            "Proveedor",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion %",
            "Objetivo proveedor",
            "% Uso objetivo proveedor",
        ]
    )
    for row in provider_dashboard["top_desviaciones"]:
        writer.writerow(
            [
                row["proveedor"],
                row["estimado"],
                row["ejecutado"],
                row["variacion"],
                round(float(row["participacion_pct"] or 0), 2),
                row.get("objetivo_proveedor", Decimal("0")),
                round(float(row["uso_objetivo_pct"] or 0), 2) if row.get("uso_objetivo_pct") is not None else "",
            ]
        )
    writer.writerow([])

    writer.writerow(["TENDENCIA 6 MESES (TOP PROVEEDORES)"])
    writer.writerow(["Proveedor", "Mes", "Estimado", "Ejecutado", "Variacion"])
    for row in provider_dashboard["trend_rows"]:
        writer.writerow(
            [
                row["proveedor"],
                row["mes"],
                row["estimado"],
                row["ejecutado"],
                row["variacion"],
            ]
        )

    return response


def _export_tablero_proveedor_xlsx(
    provider_dashboard: dict,
    periodo_label: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Top desviaciones"
    ws.append(["TABLERO PROVEEDOR - COMPRAS"])
    ws.append(["Periodo activo", periodo_label])
    ws.append(["Filtro origen", source_filter])
    ws.append(["Filtro plan", plan_filter or "-"])
    ws.append(["Filtro categoria", categoria_filter or "-"])
    ws.append([])
    ws.append(
        [
            "Proveedor",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion %",
            "Objetivo proveedor",
            "% Uso objetivo proveedor",
        ]
    )
    for row in provider_dashboard["top_desviaciones"]:
        ws.append(
            [
                row["proveedor"],
                float(row["estimado"] or 0),
                float(row["ejecutado"] or 0),
                float(row["variacion"] or 0),
                float(row["participacion_pct"] or 0),
                float(row.get("objetivo_proveedor") or 0),
                float(row["uso_objetivo_pct"] or 0) if row.get("uso_objetivo_pct") is not None else None,
            ]
        )

    ws2 = wb.create_sheet(title="Tendencia 6m")
    ws2.append(["Proveedor", "Mes", "Estimado", "Ejecutado", "Variacion"])
    for row in provider_dashboard["trend_rows"]:
        ws2.append(
            [
                row["proveedor"],
                row["mes"],
                float(row["estimado"] or 0),
                float(row["ejecutado"] or 0),
                float(row["variacion"] or 0),
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="compras_tablero_proveedor_{now_str}.xlsx"'
    return response


def _export_tablero_categoria_csv(
    category_dashboard: dict,
    periodo_label: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="compras_tablero_categoria_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(["TABLERO CATEGORIA - COMPRAS"])
    writer.writerow(["Periodo activo", periodo_label])
    writer.writerow(["Filtro origen", source_filter])
    writer.writerow(["Filtro plan", plan_filter or "-"])
    writer.writerow(["Filtro categoria", categoria_filter or "-"])
    writer.writerow([])

    writer.writerow(["TOP DESVIACIONES (PERIODO ACTIVO)"])
    writer.writerow(
        [
            "Categoria",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion %",
            "Objetivo categoria",
            "% Uso objetivo categoria",
        ]
    )
    for row in category_dashboard["top_desviaciones"]:
        writer.writerow(
            [
                row["categoria"],
                row["estimado"],
                row["ejecutado"],
                row["variacion"],
                round(float(row["participacion_pct"] or 0), 2),
                row.get("objetivo_categoria", Decimal("0")),
                round(float(row["uso_objetivo_pct"] or 0), 2) if row.get("uso_objetivo_pct") is not None else "",
            ]
        )
    writer.writerow([])

    writer.writerow(["TENDENCIA 6 MESES (TOP CATEGORIAS)"])
    writer.writerow(["Categoria", "Mes", "Estimado", "Ejecutado", "Variacion"])
    for row in category_dashboard["trend_rows"]:
        writer.writerow(
            [
                row["categoria"],
                row["mes"],
                row["estimado"],
                row["ejecutado"],
                row["variacion"],
            ]
        )
    return response


def _export_tablero_categoria_xlsx(
    category_dashboard: dict,
    periodo_label: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Top desviaciones cat"
    ws.append(["TABLERO CATEGORIA - COMPRAS"])
    ws.append(["Periodo activo", periodo_label])
    ws.append(["Filtro origen", source_filter])
    ws.append(["Filtro plan", plan_filter or "-"])
    ws.append(["Filtro categoria", categoria_filter or "-"])
    ws.append([])
    ws.append(
        [
            "Categoria",
            "Estimado",
            "Ejecutado",
            "Variacion",
            "Participacion %",
            "Objetivo categoria",
            "% Uso objetivo categoria",
        ]
    )
    for row in category_dashboard["top_desviaciones"]:
        ws.append(
            [
                row["categoria"],
                float(row["estimado"] or 0),
                float(row["ejecutado"] or 0),
                float(row["variacion"] or 0),
                float(row["participacion_pct"] or 0),
                float(row.get("objetivo_categoria") or 0),
                float(row["uso_objetivo_pct"] or 0) if row.get("uso_objetivo_pct") is not None else None,
            ]
        )

    ws2 = wb.create_sheet(title="Tendencia 6m cat")
    ws2.append(["Categoria", "Mes", "Estimado", "Ejecutado", "Variacion"])
    for row in category_dashboard["trend_rows"]:
        ws2.append(
            [
                row["categoria"],
                row["mes"],
                float(row["estimado"] or 0),
                float(row["ejecutado"] or 0),
                float(row["variacion"] or 0),
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="compras_tablero_categoria_{now_str}.xlsx"'
    return response


def _export_consumo_plan_csv(
    consumo_dashboard: dict,
    periodo_label: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    consumo_ref_filter: str,
) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="compras_consumo_vs_plan_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(["CONSUMO REAL VS PLAN DE PRODUCCION"])
    writer.writerow(["Periodo activo", periodo_label])
    writer.writerow(["Rango considerado", f"{consumo_dashboard['period_start']} a {consumo_dashboard['period_end']}"])
    writer.writerow(["Filtro origen", source_filter])
    writer.writerow(["Filtro plan", plan_filter or "-"])
    writer.writerow(["Filtro categoria", categoria_filter or "-"])
    writer.writerow(
        [
            "Filtro consumo ref",
            "Solo referencia plan" if consumo_ref_filter == "plan_ref" else "Todos",
        ]
    )
    writer.writerow([])
    writer.writerow(["Totales"])
    writer.writerow(["Plan qty", consumo_dashboard["totals"]["plan_qty_total"]])
    writer.writerow(["Real qty", consumo_dashboard["totals"]["consumo_real_qty_total"]])
    writer.writerow(["Plan costo", consumo_dashboard["totals"]["plan_cost_total"]])
    writer.writerow(["Real costo", consumo_dashboard["totals"]["consumo_real_cost_total"]])
    writer.writerow(["Variacion costo", consumo_dashboard["totals"]["variacion_cost_total"]])
    writer.writerow(["Cobertura %", consumo_dashboard["totals"]["cobertura_pct"] or ""])
    writer.writerow([])
    writer.writerow(
        [
            "Insumo",
            "Categoria",
            "Unidad",
            "Cantidad plan",
            "Cantidad real",
            "Variacion qty",
            "Costo unitario",
            "Costo plan",
            "Costo real",
            "Variacion costo",
            "Consumo %",
            "Estado",
            "Semaforo",
            "Alerta costo",
        ]
    )
    for row in consumo_dashboard["rows"]:
        writer.writerow(
            [
                row["insumo"],
                row["categoria"],
                row["unidad"],
                row["cantidad_plan"],
                row["cantidad_real"],
                row["variacion_qty"],
                row["costo_unitario"],
                row["costo_plan"],
                row["costo_real"],
                row["variacion_cost"],
                row["consumo_pct"] if row["consumo_pct"] is not None else "",
                row["estado"],
                row["semaforo"],
                "SI" if row["sin_costo"] else "",
            ]
        )
    return response


def _export_consumo_plan_xlsx(
    consumo_dashboard: dict,
    periodo_label: str,
    source_filter: str,
    plan_filter: str,
    categoria_filter: str,
    consumo_ref_filter: str,
) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "Consumo vs plan"
    ws.append(["CONSUMO REAL VS PLAN DE PRODUCCION"])
    ws.append(["Periodo activo", periodo_label])
    ws.append(["Rango considerado", f"{consumo_dashboard['period_start']} a {consumo_dashboard['period_end']}"])
    ws.append(["Filtro origen", source_filter])
    ws.append(["Filtro plan", plan_filter or "-"])
    ws.append(["Filtro categoria", categoria_filter or "-"])
    ws.append(["Filtro consumo ref", "Solo referencia plan" if consumo_ref_filter == "plan_ref" else "Todos"])
    ws.append([])
    ws.append(["Plan qty", float(consumo_dashboard["totals"]["plan_qty_total"] or 0)])
    ws.append(["Real qty", float(consumo_dashboard["totals"]["consumo_real_qty_total"] or 0)])
    ws.append(["Plan costo", float(consumo_dashboard["totals"]["plan_cost_total"] or 0)])
    ws.append(["Real costo", float(consumo_dashboard["totals"]["consumo_real_cost_total"] or 0)])
    ws.append(["Variacion costo", float(consumo_dashboard["totals"]["variacion_cost_total"] or 0)])
    ws.append(["Cobertura %", float(consumo_dashboard["totals"]["cobertura_pct"] or 0)])
    ws.append([])
    ws.append(
        [
            "Insumo",
            "Categoria",
            "Unidad",
            "Cantidad plan",
            "Cantidad real",
            "Variacion qty",
            "Costo unitario",
            "Costo plan",
            "Costo real",
            "Variacion costo",
            "Consumo %",
            "Estado",
            "Semaforo",
            "Alerta costo",
        ]
    )
    for row in consumo_dashboard["rows"]:
        ws.append(
            [
                row["insumo"],
                row["categoria"],
                row["unidad"],
                float(row["cantidad_plan"] or 0),
                float(row["cantidad_real"] or 0),
                float(row["variacion_qty"] or 0),
                float(row["costo_unitario"] or 0),
                float(row["costo_plan"] or 0),
                float(row["costo_real"] or 0),
                float(row["variacion_cost"] or 0),
                float(row["consumo_pct"] or 0) if row["consumo_pct"] is not None else None,
                row["estado"],
                row["semaforo"],
                "SI" if row["sin_costo"] else "",
            ]
        )

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="compras_consumo_vs_plan_{now_str}.xlsx"'
    return response


def _filtered_solicitudes(
    source_filter_raw: str,
    plan_filter_raw: str,
    categoria_filter_raw: str,
    reabasto_filter_raw: str,
    periodo_tipo_raw: str,
    periodo_mes_raw: str,
):
    source_filter = (source_filter_raw or "all").lower()
    if source_filter not in {"all", "manual", "plan"}:
        source_filter = "all"
    plan_filter = (plan_filter_raw or "").strip()
    categoria_filter = _sanitize_categoria_filter(categoria_filter_raw)
    periodo_tipo, periodo_mes, periodo_label = _parse_period_filters(periodo_tipo_raw, periodo_mes_raw)

    solicitudes_qs = SolicitudCompra.objects.select_related("insumo", "insumo__unidad_base", "proveedor_sugerido").all()
    if source_filter == "plan":
        solicitudes_qs = solicitudes_qs.filter(area__startswith="PLAN_PRODUCCION:")
    elif source_filter == "manual":
        solicitudes_qs = solicitudes_qs.exclude(area__startswith="PLAN_PRODUCCION:")

    if plan_filter:
        solicitudes_qs = solicitudes_qs.filter(area=f"PLAN_PRODUCCION:{plan_filter}")
    solicitudes_qs = _filter_solicitudes_by_categoria(solicitudes_qs, categoria_filter)

    if periodo_tipo != "all":
        year, month = periodo_mes.split("-")
        y = int(year)
        m = int(month)
        solicitudes_qs = solicitudes_qs.filter(fecha_requerida__year=y, fecha_requerida__month=m)
        if periodo_tipo == "q1":
            solicitudes_qs = solicitudes_qs.filter(fecha_requerida__day__lte=15)
        elif periodo_tipo == "q2":
            solicitudes_qs = solicitudes_qs.filter(fecha_requerida__day__gte=16)

    solicitudes = list(solicitudes_qs[:300])
    insumo_ids = [s.insumo_id for s in solicitudes]
    existencias = {
        e.insumo_id: e
        for e in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids)
    }

    plan_ids = set()
    for s in solicitudes:
        if (s.area or "").startswith("PLAN_PRODUCCION:"):
            _, _, maybe_id = s.area.partition(":")
            if maybe_id.isdigit():
                plan_ids.add(int(maybe_id))
    planes_map = {
        p.id: p
        for p in PlanProduccion.objects.filter(id__in=plan_ids)
    }
    latest_cost_by_insumo: dict[int, Decimal] = {}
    for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
        if c.insumo_id not in latest_cost_by_insumo:
            latest_cost_by_insumo[c.insumo_id] = c.costo_unitario

    for s in solicitudes:
        ex = existencias.get(s.insumo_id)
        stock_actual = ex.stock_actual if ex else Decimal("0")
        punto_reorden = ex.punto_reorden if ex else Decimal("0")
        if stock_actual <= Decimal("0"):
            s.reabasto_nivel = "critico"
            s.reabasto_texto = "Sin stock"
        elif stock_actual < punto_reorden:
            s.reabasto_nivel = "bajo"
            s.reabasto_texto = "Bajo reorden"
        else:
            s.reabasto_nivel = "ok"
            s.reabasto_texto = "Stock suficiente"
        s.reabasto_detalle = f"Stock {stock_actual} / Reorden {punto_reorden}"
        s.source_tipo = "manual"
        s.source_plan_id = None
        s.source_plan_nombre = ""
        if (s.area or "").startswith("PLAN_PRODUCCION:"):
            _, _, maybe_id = s.area.partition(":")
            if maybe_id.isdigit():
                plan_id_int = int(maybe_id)
                s.source_tipo = "plan"
                s.source_plan_id = plan_id_int
                s.source_plan_nombre = planes_map.get(plan_id_int).nombre if plan_id_int in planes_map else f"Plan {plan_id_int}"
        s.costo_unitario = latest_cost_by_insumo.get(s.insumo_id, Decimal("0"))
        s.presupuesto_estimado = (s.cantidad or Decimal("0")) * (s.costo_unitario or Decimal("0"))

    open_orders_by_solicitud = {}
    solicitud_ids = [s.id for s in solicitudes]
    if solicitud_ids:
        for orden in (
            OrdenCompra.objects.filter(solicitud_id__in=solicitud_ids)
            .exclude(estatus=OrdenCompra.STATUS_CERRADA)
            .order_by("-creado_en")
        ):
            open_orders_by_solicitud.setdefault(orden.solicitud_id, orden)

    for s in solicitudes:
        open_order = open_orders_by_solicitud.get(s.id)
        s.has_open_order = bool(open_order)
        s.open_order_id = open_order.id if open_order else None
        s.open_order_folio = open_order.folio if open_order else ""

    reabasto_filter = (reabasto_filter_raw or "all").lower()
    if reabasto_filter in {"critico", "bajo", "ok"}:
        solicitudes = [s for s in solicitudes if s.reabasto_nivel == reabasto_filter]
    else:
        reabasto_filter = "all"

    plan_ids_all = set()
    for area_val in SolicitudCompra.objects.filter(area__startswith="PLAN_PRODUCCION:").values_list("area", flat=True).distinct():
        _, _, maybe_id = (area_val or "").partition(":")
        if maybe_id.isdigit():
            plan_ids_all.add(int(maybe_id))
    plan_options = list(PlanProduccion.objects.filter(id__in=plan_ids_all).order_by("-fecha_produccion", "-id")[:100])

    return (
        solicitudes,
        source_filter,
        plan_filter,
        categoria_filter,
        reabasto_filter,
        plan_options,
        periodo_tipo,
        periodo_mes,
        periodo_label,
    )


@login_required
def solicitudes(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    if request.method == "POST":
        if not can_manage_compras(request.user):
            raise PermissionDenied("No tienes permisos para crear solicitudes.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            insumo = get_object_or_404(Insumo, pk=insumo_id)
            solicitud = SolicitudCompra.objects.create(
                area=request.POST.get("area", "General").strip() or "General",
                solicitante=request.POST.get("solicitante", request.user.username).strip() or request.user.username,
                insumo=insumo,
                proveedor_sugerido=insumo.proveedor_principal,
                cantidad=_to_decimal(request.POST.get("cantidad"), "1"),
                fecha_requerida=request.POST.get("fecha_requerida") or date.today(),
                estatus=request.POST.get("estatus") or SolicitudCompra.STATUS_BORRADOR,
            )
            log_event(
                request.user,
                "CREATE",
                "compras.SolicitudCompra",
                solicitud.id,
                {"folio": solicitud.folio, "estatus": solicitud.estatus},
            )
        return redirect("compras:solicitudes")

    (
        solicitudes,
        source_filter,
        plan_filter,
        categoria_filter,
        reabasto_filter,
        plan_options,
        periodo_tipo,
        periodo_mes,
        periodo_label,
    ) = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
        request.GET.get("categoria"),
        request.GET.get("reabasto"),
        request.GET.get("periodo_tipo"),
        request.GET.get("periodo_mes"),
    )
    consumo_ref_filter = _sanitize_consumo_ref_filter(request.GET.get("consumo_ref"))
    budget_ctx = _build_budget_context(
        solicitudes,
        source_filter,
        plan_filter,
        categoria_filter,
        periodo_tipo,
        periodo_mes,
    )
    provider_dashboard = _build_provider_dashboard(
        periodo_mes,
        source_filter,
        plan_filter,
        categoria_filter,
        budget_ctx["presupuesto_rows_proveedor"],
    )
    category_dashboard = _build_category_dashboard(
        periodo_mes,
        source_filter,
        plan_filter,
        categoria_filter,
        budget_ctx.get("presupuesto_rows_categoria", []),
    )
    consumo_dashboard = _build_consumo_vs_plan_dashboard(
        periodo_tipo,
        periodo_mes,
        source_filter,
        plan_filter,
        categoria_filter,
        consumo_ref_filter,
    )
    total_presupuesto = budget_ctx["presupuesto_estimado_total"]
    import_preview = _build_import_preview_context(request.session.get(IMPORT_PREVIEW_SESSION_KEY))

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_solicitudes_csv(
            solicitudes,
            source_filter,
            plan_filter,
            categoria_filter,
            reabasto_filter,
            periodo_tipo,
            periodo_mes,
            periodo_label,
        )
    if export_format == "consolidado_csv":
        return _export_consolidado_csv(
            solicitudes,
            source_filter,
            plan_filter,
            categoria_filter,
            reabasto_filter,
            periodo_label,
            budget_ctx,
        )
    if export_format == "consolidado_xlsx":
        return _export_consolidado_xlsx(
            solicitudes,
            source_filter,
            plan_filter,
            categoria_filter,
            reabasto_filter,
            periodo_label,
            budget_ctx,
        )
    if export_format == "proveedor_csv":
        return _export_tablero_proveedor_csv(
            provider_dashboard,
            periodo_label,
            source_filter,
            plan_filter,
            categoria_filter,
        )
    if export_format == "proveedor_xlsx":
        return _export_tablero_proveedor_xlsx(
            provider_dashboard,
            periodo_label,
            source_filter,
            plan_filter,
            categoria_filter,
        )
    if export_format == "categoria_csv":
        return _export_tablero_categoria_csv(
            category_dashboard,
            periodo_label,
            source_filter,
            plan_filter,
            categoria_filter,
        )
    if export_format == "categoria_xlsx":
        return _export_tablero_categoria_xlsx(
            category_dashboard,
            periodo_label,
            source_filter,
            plan_filter,
            categoria_filter,
        )
    if export_format == "consumo_plan_csv":
        return _export_consumo_plan_csv(
            consumo_dashboard,
            periodo_label,
            source_filter,
            plan_filter,
            categoria_filter,
            consumo_ref_filter,
        )
    if export_format == "consumo_plan_xlsx":
        return _export_consumo_plan_xlsx(
            consumo_dashboard,
            periodo_label,
            source_filter,
            plan_filter,
            categoria_filter,
            consumo_ref_filter,
        )
    if export_format == "import_preview_csv":
        if not import_preview:
            messages.warning(request, "No hay vista previa de importación activa para exportar.")
            return redirect("compras:solicitudes")
        return _export_import_preview_csv(import_preview)
    if export_format == "import_preview_xlsx":
        if not import_preview:
            messages.warning(request, "No hay vista previa de importación activa para exportar.")
            return redirect("compras:solicitudes")
        return _export_import_preview_xlsx(import_preview)
    if export_format == "xlsx":
        return _export_solicitudes_xlsx(
            solicitudes,
            source_filter,
            plan_filter,
            categoria_filter,
            reabasto_filter,
            periodo_tipo,
            periodo_mes,
            periodo_label,
        )

    query_without_export = request.GET.copy()
    query_without_export.pop("export", None)

    context = {
        "solicitudes": solicitudes,
        "insumo_options": _build_insumo_options(),
        "proveedor_options": list(Proveedor.objects.filter(activo=True).only("id", "nombre").order_by("nombre")),
        "categoria_options": [
            c
            for c in sorted(
                {
                    *[
                        " ".join((x or "").strip().split())
                        for x in Insumo.objects.filter(activo=True)
                        .exclude(categoria="")
                        .values_list("categoria", flat=True)
                    ],
                    "Masa",
                    "Volumen",
                    "Pieza",
                }
            )
            if c
        ],
        "status_choices": SolicitudCompra.STATUS_CHOICES,
        "can_manage_compras": can_manage_compras(request.user),
        "reabasto_filter": reabasto_filter,
        "source_filter": source_filter,
        "plan_filter": plan_filter,
        "categoria_filter": categoria_filter,
        "plan_options": plan_options,
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
        "periodo_label": periodo_label,
        "consumo_ref_filter": consumo_ref_filter,
        "total_presupuesto": total_presupuesto,
        "current_query": query_without_export.urlencode(),
        "import_preview": import_preview,
        "presupuesto_historial": _build_budget_history(periodo_mes, source_filter, plan_filter, categoria_filter),
        "provider_dashboard": provider_dashboard,
        "category_dashboard": category_dashboard,
        "consumo_dashboard": consumo_dashboard,
        **budget_ctx,
    }
    return render(request, "compras/solicitudes.html", context)


@login_required
@require_POST
def guardar_presupuesto_periodo(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para gestionar presupuesto.")

    periodo_tipo, periodo_mes, _ = _parse_period_filters(
        request.POST.get("periodo_tipo"),
        request.POST.get("periodo_mes"),
    )
    if periodo_tipo == "all":
        messages.error(request, "Selecciona periodo mensual o quincenal para guardar presupuesto.")
        return redirect("compras:solicitudes")

    monto_objetivo = _to_decimal(request.POST.get("monto_objetivo"), "0")
    if monto_objetivo < 0:
        monto_objetivo = Decimal("0")
    notas = (request.POST.get("notas") or "").strip()

    presupuesto, created = PresupuestoCompraPeriodo.objects.update_or_create(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
        defaults={
            "monto_objetivo": monto_objetivo,
            "notas": notas,
            "actualizado_por": request.user,
        },
    )
    log_event(
        request.user,
        "CREATE" if created else "UPDATE",
        "compras.PresupuestoCompraPeriodo",
        presupuesto.id,
        {
            "periodo_tipo": periodo_tipo,
            "periodo_mes": periodo_mes,
            "monto_objetivo": str(monto_objetivo),
        },
    )
    messages.success(request, "Presupuesto del período actualizado.")

    params = {
        "source": (request.POST.get("source") or "all").strip() or "all",
        "plan_id": (request.POST.get("plan_id") or "").strip(),
        "categoria": _sanitize_categoria_filter(request.POST.get("categoria")),
        "reabasto": (request.POST.get("reabasto") or "all").strip() or "all",
        "consumo_ref": _sanitize_consumo_ref_filter(request.POST.get("consumo_ref")),
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
    }
    if not params["plan_id"]:
        params.pop("plan_id")
    if not params["categoria"]:
        params.pop("categoria")
    return redirect(f"{reverse('compras:solicitudes')}?{urlencode(params)}")


@login_required
@require_POST
def guardar_presupuesto_proveedor(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para gestionar presupuesto por proveedor.")

    periodo_tipo, periodo_mes, _ = _parse_period_filters(
        request.POST.get("periodo_tipo"),
        request.POST.get("periodo_mes"),
    )
    if periodo_tipo == "all":
        messages.error(request, "Selecciona periodo mensual o quincenal para guardar objetivo por proveedor.")
        return redirect("compras:solicitudes")

    proveedor_id_raw = (request.POST.get("proveedor_id") or "").strip()
    if not proveedor_id_raw.isdigit():
        messages.error(request, "Selecciona un proveedor válido.")
        return redirect("compras:solicitudes")

    proveedor = get_object_or_404(Proveedor, pk=int(proveedor_id_raw), activo=True)
    monto_objetivo = _to_decimal(request.POST.get("monto_objetivo_proveedor"), "0")
    if monto_objetivo < 0:
        monto_objetivo = Decimal("0")
    notas = (request.POST.get("notas_proveedor") or "").strip()

    presupuesto_periodo, _ = PresupuestoCompraPeriodo.objects.get_or_create(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
        defaults={"monto_objetivo": Decimal("0"), "actualizado_por": request.user},
    )
    objetivo_proveedor, created = PresupuestoCompraProveedor.objects.update_or_create(
        presupuesto_periodo=presupuesto_periodo,
        proveedor=proveedor,
        defaults={
            "monto_objetivo": monto_objetivo,
            "notas": notas,
            "actualizado_por": request.user,
        },
    )
    log_event(
        request.user,
        "CREATE" if created else "UPDATE",
        "compras.PresupuestoCompraProveedor",
        objetivo_proveedor.id,
        {
            "periodo_tipo": periodo_tipo,
            "periodo_mes": periodo_mes,
            "proveedor_id": proveedor.id,
            "proveedor_nombre": proveedor.nombre,
            "monto_objetivo": str(monto_objetivo),
        },
    )
    messages.success(request, f"Objetivo de proveedor actualizado: {proveedor.nombre}.")

    params = {
        "source": (request.POST.get("source") or "all").strip() or "all",
        "plan_id": (request.POST.get("plan_id") or "").strip(),
        "categoria": _sanitize_categoria_filter(request.POST.get("categoria")),
        "reabasto": (request.POST.get("reabasto") or "all").strip() or "all",
        "consumo_ref": _sanitize_consumo_ref_filter(request.POST.get("consumo_ref")),
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
    }
    if not params["plan_id"]:
        params.pop("plan_id")
    if not params["categoria"]:
        params.pop("categoria")
    return redirect(f"{reverse('compras:solicitudes')}?{urlencode(params)}")


@login_required
@require_POST
def guardar_presupuesto_categoria(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para gestionar presupuesto por categoría.")

    periodo_tipo, periodo_mes, _ = _parse_period_filters(
        request.POST.get("periodo_tipo"),
        request.POST.get("periodo_mes"),
    )
    if periodo_tipo == "all":
        messages.error(request, "Selecciona periodo mensual o quincenal para guardar objetivo por categoría.")
        return redirect("compras:solicitudes")

    categoria = " ".join((request.POST.get("categoria") or "").strip().split())
    if not categoria:
        messages.error(request, "Ingresa una categoría válida.")
        return redirect("compras:solicitudes")

    monto_objetivo = _to_decimal(request.POST.get("monto_objetivo_categoria"), "0")
    if monto_objetivo < 0:
        monto_objetivo = Decimal("0")
    notas = (request.POST.get("notas_categoria") or "").strip()

    presupuesto_periodo, _ = PresupuestoCompraPeriodo.objects.get_or_create(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
        defaults={"monto_objetivo": Decimal("0"), "actualizado_por": request.user},
    )
    objetivo_categoria, created = PresupuestoCompraCategoria.objects.update_or_create(
        presupuesto_periodo=presupuesto_periodo,
        categoria_normalizada=_normalize_categoria_text(categoria),
        defaults={
            "categoria": categoria,
            "monto_objetivo": monto_objetivo,
            "notas": notas,
            "actualizado_por": request.user,
        },
    )
    log_event(
        request.user,
        "CREATE" if created else "UPDATE",
        "compras.PresupuestoCompraCategoria",
        objetivo_categoria.id,
        {
            "periodo_tipo": periodo_tipo,
            "periodo_mes": periodo_mes,
            "categoria": categoria,
            "monto_objetivo": str(monto_objetivo),
        },
    )
    messages.success(request, f"Objetivo de categoría actualizado: {categoria}.")

    params = {
        "source": (request.POST.get("source") or "all").strip() or "all",
        "plan_id": (request.POST.get("plan_id") or "").strip(),
        "categoria": _sanitize_categoria_filter(request.POST.get("categoria")),
        "reabasto": (request.POST.get("reabasto") or "all").strip() or "all",
        "consumo_ref": _sanitize_consumo_ref_filter(request.POST.get("consumo_ref")),
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
    }
    if not params["plan_id"]:
        params.pop("plan_id")
    if not params["categoria"]:
        params.pop("categoria")
    return redirect(f"{reverse('compras:solicitudes')}?{urlencode(params)}")


@login_required
@require_POST
def importar_presupuestos_periodo(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para importar presupuesto.")

    archivo = request.FILES.get("archivo_presupuesto")
    if not archivo:
        messages.error(request, "Selecciona un archivo de presupuesto.")
        return redirect("compras:solicitudes")

    try:
        rows = _read_import_rows(archivo)
    except ValueError as exc:
        messages.error(request, str(exc))
        return redirect("compras:solicitudes")
    except Exception:
        messages.error(request, "No se pudo leer el archivo de presupuesto.")
        return redirect("compras:solicitudes")

    if not rows:
        messages.warning(request, "El archivo de presupuesto no tiene filas.")
        return redirect("compras:solicitudes")

    providers_by_norm = {
        normalizar_nombre(p.nombre): p
        for p in Proveedor.objects.filter(activo=True).only("id", "nombre")
    }

    created = 0
    updated = 0
    created_proveedor = 0
    updated_proveedor = 0
    created_categoria = 0
    updated_categoria = 0
    skipped = 0
    for idx, row in enumerate(rows, start=2):
        periodo_tipo = _parse_periodo_tipo_value(row.get("periodo_tipo"))
        periodo_mes = _parse_periodo_mes_value(row.get("periodo_mes"))
        monto_raw = row.get("monto_objetivo")
        monto_has_value = str(monto_raw).strip() != "" if monto_raw is not None else False
        monto = _to_decimal(str(monto_raw or "0"), "0")
        notas = str(row.get("notas") or "").strip()

        if not periodo_tipo or not periodo_mes:
            skipped += 1
            continue
        if monto_has_value and monto < 0:
            monto = Decimal("0")

        presupuesto, was_created = PresupuestoCompraPeriodo.objects.get_or_create(
            periodo_tipo=periodo_tipo,
            periodo_mes=periodo_mes,
            defaults={
                "monto_objetivo": monto,
                "notas": notas,
                "actualizado_por": request.user,
            },
        )
        if monto_has_value:
            presupuesto.monto_objetivo = monto
            presupuesto.notas = notas
            presupuesto.actualizado_por = request.user
            presupuesto.save(update_fields=["monto_objetivo", "notas", "actualizado_por", "actualizado_en"])
            log_event(
                request.user,
                "CREATE" if was_created else "UPDATE",
                "compras.PresupuestoCompraPeriodo",
                presupuesto.id,
                {
                    "source": "import",
                    "row": idx,
                    "periodo_tipo": periodo_tipo,
                    "periodo_mes": periodo_mes,
                    "monto_objetivo": str(monto),
                },
            )
            if was_created:
                created += 1
            else:
                updated += 1

        had_dimension_update = False

        proveedor_raw = str(row.get("proveedor") or "").strip()
        if proveedor_raw:
            proveedor = providers_by_norm.get(normalizar_nombre(proveedor_raw))
            if not proveedor:
                skipped += 1
            else:
                monto_proveedor_raw = row.get("monto_objetivo_proveedor")
                monto_proveedor_has_value = (
                    str(monto_proveedor_raw).strip() != "" if monto_proveedor_raw is not None else False
                )
                if not monto_proveedor_has_value:
                    monto_proveedor_raw = monto_raw
                    monto_proveedor_has_value = monto_has_value

                if not monto_proveedor_has_value:
                    skipped += 1
                else:
                    monto_proveedor = _to_decimal(str(monto_proveedor_raw or "0"), "0")
                    if monto_proveedor < 0:
                        monto_proveedor = Decimal("0")
                    objetivo_proveedor, was_created_proveedor = PresupuestoCompraProveedor.objects.update_or_create(
                        presupuesto_periodo=presupuesto,
                        proveedor=proveedor,
                        defaults={
                            "monto_objetivo": monto_proveedor,
                            "notas": notas,
                            "actualizado_por": request.user,
                        },
                    )
                    log_event(
                        request.user,
                        "CREATE" if was_created_proveedor else "UPDATE",
                        "compras.PresupuestoCompraProveedor",
                        objetivo_proveedor.id,
                        {
                            "source": "import",
                            "row": idx,
                            "periodo_tipo": periodo_tipo,
                            "periodo_mes": periodo_mes,
                            "proveedor_id": proveedor.id,
                            "proveedor_nombre": proveedor.nombre,
                            "monto_objetivo": str(monto_proveedor),
                        },
                    )
                    had_dimension_update = True
                    if was_created_proveedor:
                        created_proveedor += 1
                    else:
                        updated_proveedor += 1

        categoria_raw = " ".join((str(row.get("categoria") or "")).strip().split())
        if categoria_raw:
            categoria_norm = _normalize_categoria_text(categoria_raw)
            monto_categoria_raw = row.get("monto_objetivo_categoria")
            monto_categoria_has_value = (
                str(monto_categoria_raw).strip() != "" if monto_categoria_raw is not None else False
            )
            if not monto_categoria_has_value:
                monto_categoria_raw = monto_raw
                monto_categoria_has_value = monto_has_value

            if not monto_categoria_has_value:
                skipped += 1
            else:
                monto_categoria = _to_decimal(str(monto_categoria_raw or "0"), "0")
                if monto_categoria < 0:
                    monto_categoria = Decimal("0")
                objetivo_categoria, was_created_categoria = PresupuestoCompraCategoria.objects.update_or_create(
                    presupuesto_periodo=presupuesto,
                    categoria_normalizada=categoria_norm,
                    defaults={
                        "categoria": categoria_raw,
                        "monto_objetivo": monto_categoria,
                        "notas": notas,
                        "actualizado_por": request.user,
                    },
                )
                log_event(
                    request.user,
                    "CREATE" if was_created_categoria else "UPDATE",
                    "compras.PresupuestoCompraCategoria",
                    objetivo_categoria.id,
                    {
                        "source": "import",
                        "row": idx,
                        "periodo_tipo": periodo_tipo,
                        "periodo_mes": periodo_mes,
                        "categoria": categoria_raw,
                        "monto_objetivo": str(monto_categoria),
                    },
                )
                had_dimension_update = True
                if was_created_categoria:
                    created_categoria += 1
                else:
                    updated_categoria += 1

        if (not monto_has_value) and (not had_dimension_update):
            skipped += 1

    messages.success(
        request,
        (
            "Importación de presupuesto completada. "
            f"Periodo nuevos: {created}. "
            f"Periodo actualizados: {updated}. "
            f"Proveedor nuevos: {created_proveedor}. "
            f"Proveedor actualizados: {updated_proveedor}. "
            f"Categoría nuevas: {created_categoria}. "
            f"Categoría actualizadas: {updated_categoria}. "
            f"Omitidos: {skipped}."
        ),
    )

    params = {
        "source": (request.POST.get("source") or "all").strip() or "all",
        "plan_id": (request.POST.get("plan_id") or "").strip(),
        "categoria": _sanitize_categoria_filter(request.POST.get("categoria")),
        "reabasto": (request.POST.get("reabasto") or "all").strip() or "all",
        "periodo_tipo": (request.POST.get("periodo_tipo") or "mes").strip() or "mes",
        "periodo_mes": (request.POST.get("periodo_mes") or "").strip(),
    }
    if not params["plan_id"]:
        params.pop("plan_id")
    if not params["categoria"]:
        params.pop("categoria")
    if not params["periodo_mes"]:
        params.pop("periodo_mes")
    return redirect(f"{reverse('compras:solicitudes')}?{urlencode(params)}")


@login_required
@require_POST
def importar_solicitudes(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para importar solicitudes.")

    archivo = request.FILES.get("archivo")
    if not archivo:
        messages.error(request, "Debes seleccionar un archivo de importación (.xlsx o .csv).")
        return redirect("compras:solicitudes")

    periodo_tipo, periodo_mes, _ = _parse_period_filters(
        request.POST.get("periodo_tipo"),
        request.POST.get("periodo_mes"),
    )
    fecha_default = _default_fecha_requerida(periodo_tipo, periodo_mes)
    area_default = (request.POST.get("area") or "General").strip() or "General"
    solicitante_default = (request.POST.get("solicitante") or request.user.username).strip() or request.user.username
    estatus_default = (request.POST.get("estatus") or SolicitudCompra.STATUS_BORRADOR).strip().upper()
    valid_status = {x[0] for x in SolicitudCompra.STATUS_CHOICES}
    if estatus_default not in valid_status:
        estatus_default = SolicitudCompra.STATUS_BORRADOR
    evitar_duplicados = request.POST.get("evitar_duplicados") == "on"
    min_score_raw = request.POST.get("score_min") or "90"
    try:
        min_score = max(0, min(100, int(min_score_raw)))
    except ValueError:
        min_score = 90

    try:
        rows = _read_import_rows(archivo)
    except ValueError as exc:
        messages.error(request, str(exc))
        return redirect("compras:solicitudes")
    except Exception:
        messages.error(request, "No se pudo leer el archivo. Verifica formato y columnas.")
        return redirect("compras:solicitudes")

    if not rows:
        messages.warning(request, "El archivo no contiene filas de datos.")
        return redirect("compras:solicitudes")

    provider_map = {
        normalizar_nombre(p.nombre): p
        for p in Proveedor.objects.filter(activo=True).only("id", "nombre")
    }
    match_cache: dict[str, tuple[Insumo | None, float, str]] = {}
    parsed_rows: list[dict] = []
    duplicate_keys_to_check: set[tuple[str, int, date]] = set()

    for idx, row in enumerate(rows, start=2):
        insumo_raw = str(row.get("insumo") or "").strip()
        cantidad_raw = str(row.get("cantidad") or "").strip()
        cantidad = _to_decimal(cantidad_raw, "0")
        if insumo_raw:
            cache_key = normalizar_nombre(insumo_raw)
            insumo_match, score, method = match_cache.get(cache_key, (None, 0.0, "sin_match"))
            if cache_key not in match_cache:
                insumo_match, score, method = match_insumo(insumo_raw)
                match_cache[cache_key] = (insumo_match, score, method)
        else:
            insumo_match, score, method = (None, 0.0, "sin_match")
        insumo_id = int(insumo_match.id) if (insumo_match and score >= min_score) else 0

        area = str(row.get("area") or area_default).strip() or area_default
        solicitante = str(row.get("solicitante") or solicitante_default).strip() or solicitante_default
        fecha_requerida = _parse_date_value(row.get("fecha_requerida"), fecha_default)
        estatus = str(row.get("estatus") or estatus_default).strip().upper()
        if estatus not in valid_status:
            estatus = estatus_default
        proveedor = _resolve_proveedor_name(str(row.get("proveedor") or ""), provider_map)
        if not proveedor and insumo_match:
            proveedor = insumo_match.proveedor_principal

        parsed_rows.append(
            {
                "row_id": str(idx),
                "source_row": idx,
                "insumo_origen": insumo_raw,
                "insumo_sugerencia": insumo_match.nombre if insumo_match else "",
                "insumo_id": insumo_id,
                "cantidad": cantidad,
                "cantidad_origen": cantidad_raw,
                "area": area,
                "solicitante": solicitante,
                "fecha_requerida": fecha_requerida,
                "estatus": estatus,
                "proveedor_id": int(proveedor.id) if proveedor else 0,
                "score": float(score or 0),
                "metodo": method,
                "has_insumo_match": bool(insumo_match),
            }
        )
        if evitar_duplicados and insumo_id:
            duplicate_keys_to_check.add((area, insumo_id, fecha_requerida))

    duplicates_found: set[tuple[str, int, date]] = set()
    if duplicate_keys_to_check:
        areas = sorted({k[0] for k in duplicate_keys_to_check})
        insumo_ids = sorted({k[1] for k in duplicate_keys_to_check})
        fechas = sorted({k[2] for k in duplicate_keys_to_check})
        duplicates_found = {
            (area, int(insumo_id), fecha)
            for area, insumo_id, fecha in SolicitudCompra.objects.filter(
                area__in=areas,
                insumo_id__in=insumo_ids,
                fecha_requerida__in=fechas,
                estatus__in=_active_solicitud_statuses(),
            ).values_list("area", "insumo_id", "fecha_requerida")
        }

    preview_rows: list[dict] = []
    for parsed in parsed_rows:
        insumo_raw = str(parsed["insumo_origen"] or "").strip()
        cantidad = parsed["cantidad"]
        cantidad_raw = parsed["cantidad_origen"]
        insumo_id = parsed["insumo_id"]
        area = parsed["area"]
        solicitante = parsed["solicitante"]
        fecha_requerida = parsed["fecha_requerida"]
        estatus = parsed["estatus"]
        duplicate = bool(insumo_id and (area, insumo_id, fecha_requerida) in duplicates_found)

        notes: list[str] = []
        hard_error = False
        if not insumo_raw:
            notes.append("Insumo vacío en archivo.")
            hard_error = True
        if not insumo_id:
            if parsed["has_insumo_match"]:
                notes.append(f"Score de match insuficiente (<{min_score}).")
            else:
                notes.append("Sin match de insumo.")
            hard_error = True
        if cantidad <= 0:
            notes.append("Cantidad inválida (debe ser > 0).")
            hard_error = True
        if duplicate:
            notes.append("Posible duplicado con solicitud activa.")

        preview_rows.append(
            {
                "row_id": parsed["row_id"],
                "source_row": parsed["source_row"],
                "insumo_origen": insumo_raw,
                "insumo_sugerencia": parsed["insumo_sugerencia"],
                "insumo_id": str(insumo_id) if insumo_id else "",
                "cantidad": str(cantidad),
                "cantidad_origen": cantidad_raw,
                "area": area,
                "solicitante": solicitante,
                "fecha_requerida": fecha_requerida.isoformat(),
                "estatus": estatus,
                "proveedor_id": str(parsed["proveedor_id"]) if parsed["proveedor_id"] else "",
                "score": f"{parsed['score']:.1f}",
                "metodo": parsed["metodo"],
                "duplicate": duplicate,
                "notes": " | ".join(notes),
                "include": not hard_error,
            }
        )

    request.session[IMPORT_PREVIEW_SESSION_KEY] = {
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
        "evitar_duplicados": evitar_duplicados,
        "score_min": min_score,
        "file_name": archivo.name,
        "generated_at": timezone.localtime().strftime("%Y-%m-%d %H:%M"),
        "rows": preview_rows,
    }
    request.session.modified = True
    messages.info(
        request,
        f"Vista previa generada con {len(preview_rows)} filas. Edita, elimina filas y confirma la importación.",
    )

    return redirect(
        f"{reverse('compras:solicitudes')}?source=manual&periodo_tipo={periodo_tipo}&periodo_mes={periodo_mes}"
    )


@login_required
@require_POST
def confirmar_importacion_solicitudes(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para importar solicitudes.")

    preview_payload = request.session.get(IMPORT_PREVIEW_SESSION_KEY)
    if not preview_payload:
        messages.error(request, "No hay una vista previa activa para confirmar.")
        return redirect("compras:solicitudes")

    rows = preview_payload.get("rows") or []
    periodo_tipo = (preview_payload.get("periodo_tipo") or "mes").strip() or "mes"
    periodo_mes = (preview_payload.get("periodo_mes") or "").strip()
    evitar_duplicados = bool(preview_payload.get("evitar_duplicados"))
    default_fecha = _default_fecha_requerida(periodo_tipo, periodo_mes)
    valid_status = {x[0] for x in SolicitudCompra.STATUS_CHOICES}

    insumo_ids: set[int] = set()
    proveedor_ids: set[int] = set()
    for row in rows:
        row_id = str(row.get("row_id") or "")
        if request.POST.get(f"row_{row_id}_include") != "on":
            continue
        try:
            insumo_ids.add(int(request.POST.get(f"row_{row_id}_insumo_id") or "0"))
        except ValueError:
            pass
        try:
            proveedor_ids.add(int(request.POST.get(f"row_{row_id}_proveedor_id") or "0"))
        except ValueError:
            pass

    insumos_map = Insumo.objects.select_related("proveedor_principal").in_bulk(insumo_ids)
    proveedores_map = Proveedor.objects.filter(activo=True, id__in=proveedor_ids).in_bulk()

    existing_duplicate_keys: set[tuple[str, int, date]] = set()
    if evitar_duplicados:
        batch_keys: set[tuple[str, int, date]] = set()
        for row in rows:
            row_id = str(row.get("row_id") or "")
            if request.POST.get(f"row_{row_id}_include") != "on":
                continue
            area = (request.POST.get(f"row_{row_id}_area") or "").strip() or "General"
            try:
                insumo_id = int(request.POST.get(f"row_{row_id}_insumo_id") or "0")
            except ValueError:
                insumo_id = 0
            if insumo_id <= 0:
                continue
            fecha_requerida = _parse_date_value(request.POST.get(f"row_{row_id}_fecha_requerida"), default_fecha)
            batch_keys.add((area, insumo_id, fecha_requerida))
        if batch_keys:
            areas = sorted({k[0] for k in batch_keys})
            insumo_ids_batch = sorted({k[1] for k in batch_keys})
            fechas = sorted({k[2] for k in batch_keys})
            existing_duplicate_keys = {
                (area, int(insumo_id), fecha)
                for area, insumo_id, fecha in SolicitudCompra.objects.filter(
                    area__in=areas,
                    insumo_id__in=insumo_ids_batch,
                    fecha_requerida__in=fechas,
                    estatus__in=_active_solicitud_statuses(),
                ).values_list("area", "insumo_id", "fecha_requerida")
            }

    created = 0
    skipped_invalid = 0
    skipped_duplicate = 0
    skipped_removed = 0
    created_duplicate_keys: set[tuple[str, int, date]] = set()

    for row in rows:
        row_id = str(row.get("row_id") or "")
        if request.POST.get(f"row_{row_id}_include") != "on":
            skipped_removed += 1
            continue

        area = (request.POST.get(f"row_{row_id}_area") or "").strip() or "General"
        solicitante = (request.POST.get(f"row_{row_id}_solicitante") or "").strip() or request.user.username
        estatus = (request.POST.get(f"row_{row_id}_estatus") or SolicitudCompra.STATUS_BORRADOR).strip().upper()
        if estatus not in valid_status:
            estatus = SolicitudCompra.STATUS_BORRADOR

        try:
            insumo_id = int(request.POST.get(f"row_{row_id}_insumo_id") or "0")
        except ValueError:
            insumo_id = 0
        insumo = insumos_map.get(insumo_id)
        if not insumo:
            skipped_invalid += 1
            continue

        cantidad = _to_decimal(request.POST.get(f"row_{row_id}_cantidad"), "0")
        if cantidad <= 0:
            skipped_invalid += 1
            continue

        fecha_requerida = _parse_date_value(request.POST.get(f"row_{row_id}_fecha_requerida"), default_fecha)

        proveedor = None
        try:
            proveedor_id = int(request.POST.get(f"row_{row_id}_proveedor_id") or "0")
            proveedor = proveedores_map.get(proveedor_id)
        except ValueError:
            proveedor = None
        if not proveedor:
            proveedor = insumo.proveedor_principal

        duplicate_key = (area, int(insumo.id), fecha_requerida)
        if evitar_duplicados:
            if (duplicate_key in existing_duplicate_keys) or (duplicate_key in created_duplicate_keys):
                skipped_duplicate += 1
                continue

        solicitud = SolicitudCompra.objects.create(
            area=area,
            solicitante=solicitante,
            insumo=insumo,
            proveedor_sugerido=proveedor,
            cantidad=cantidad,
            fecha_requerida=fecha_requerida,
            estatus=estatus,
        )
        log_event(
            request.user,
            "CREATE",
            "compras.SolicitudCompra",
            solicitud.id,
            {
                "folio": solicitud.folio,
                "source": "import_preview_confirm",
            },
        )
        if evitar_duplicados:
            created_duplicate_keys.add(duplicate_key)
        created += 1

    request.session.pop(IMPORT_PREVIEW_SESSION_KEY, None)
    request.session.modified = True
    messages.success(
        request,
        (
            f"Importación confirmada. Creadas: {created}. "
            f"Eliminadas en vista previa: {skipped_removed}. "
            f"Duplicadas: {skipped_duplicate}. Inválidas: {skipped_invalid}."
        ),
    )
    return redirect(
        f"{reverse('compras:solicitudes')}?source=manual&periodo_tipo={periodo_tipo}&periodo_mes={periodo_mes}"
    )


@login_required
@require_POST
def cancelar_importacion_solicitudes(request: HttpRequest) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para importar solicitudes.")
    request.session.pop(IMPORT_PREVIEW_SESSION_KEY, None)
    request.session.modified = True
    messages.info(request, "Vista previa de importación eliminada.")
    return redirect("compras:solicitudes")


@login_required
def solicitudes_print(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    (
        solicitudes,
        source_filter,
        plan_filter,
        categoria_filter,
        reabasto_filter,
        _,
        periodo_tipo,
        periodo_mes,
        periodo_label,
    ) = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
        request.GET.get("categoria"),
        request.GET.get("reabasto"),
        request.GET.get("periodo_tipo"),
        request.GET.get("periodo_mes"),
    )

    total_cantidad = sum((s.cantidad for s in solicitudes), Decimal("0"))
    total_presupuesto = sum((s.presupuesto_estimado for s in solicitudes), Decimal("0"))
    criticos_count = sum(1 for s in solicitudes if s.reabasto_nivel == "critico")
    bajos_count = sum(1 for s in solicitudes if s.reabasto_nivel == "bajo")
    ok_count = sum(1 for s in solicitudes if s.reabasto_nivel == "ok")

    context = {
        "solicitudes": solicitudes,
        "source_filter": source_filter,
        "plan_filter": plan_filter or "-",
        "categoria_filter": categoria_filter or "-",
        "reabasto_filter": reabasto_filter,
        "periodo_label": periodo_label,
        "periodo_mes": periodo_mes if periodo_tipo != "all" else "-",
        "total_cantidad": total_cantidad,
        "total_presupuesto": total_presupuesto,
        "criticos_count": criticos_count,
        "bajos_count": bajos_count,
        "ok_count": ok_count,
        "generated_at": timezone.localtime(),
        "generated_by": request.user.username,
        "document_folio": _solicitudes_print_folio(),
        "status_autorizacion": "Pendiente de firmas",
        "return_query": request.GET.urlencode(),
    }
    return render(request, "compras/solicitudes_print.html", context)


@login_required
def solicitudes_resumen_api(request: HttpRequest) -> JsonResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    (
        solicitudes,
        source_filter,
        plan_filter,
        categoria_filter,
        reabasto_filter,
        _,
        periodo_tipo,
        periodo_mes,
        periodo_label,
    ) = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
        request.GET.get("categoria"),
        request.GET.get("reabasto"),
        request.GET.get("periodo_tipo"),
        request.GET.get("periodo_mes"),
    )
    consumo_ref_filter = _sanitize_consumo_ref_filter(request.GET.get("consumo_ref"))

    budget_ctx = _build_budget_context(
        solicitudes,
        source_filter,
        plan_filter,
        categoria_filter,
        periodo_tipo,
        periodo_mes,
    )
    provider_dashboard = _build_provider_dashboard(
        periodo_mes,
        source_filter,
        plan_filter,
        categoria_filter,
        budget_ctx["presupuesto_rows_proveedor"],
    )
    category_dashboard = _build_category_dashboard(
        periodo_mes,
        source_filter,
        plan_filter,
        categoria_filter,
        budget_ctx.get("presupuesto_rows_categoria", []),
    )
    consumo_dashboard = _build_consumo_vs_plan_dashboard(
        periodo_tipo,
        periodo_mes,
        source_filter,
        plan_filter,
        categoria_filter,
        consumo_ref_filter,
    )
    historial = _build_budget_history(periodo_mes, source_filter, plan_filter, categoria_filter)

    data = {
        "filters": {
            "source": source_filter,
            "plan_id": plan_filter or "",
            "categoria": categoria_filter or "",
            "reabasto": reabasto_filter,
            "consumo_ref": consumo_ref_filter,
            "periodo_tipo": periodo_tipo,
            "periodo_mes": periodo_mes,
            "periodo_label": periodo_label,
        },
        "totals": {
            "solicitudes_count": len(solicitudes),
            "presupuesto_estimado_total": float(budget_ctx["presupuesto_estimado_total"] or 0),
            "presupuesto_ejecutado_total": float(budget_ctx["presupuesto_ejecutado_total"] or 0),
            "presupuesto_objetivo": float((budget_ctx.get("presupuesto_objetivo") or 0)),
            "presupuesto_variacion_objetivo": float((budget_ctx.get("presupuesto_variacion_objetivo") or 0)),
            "alertas_total": int(budget_ctx.get("presupuesto_alertas_total") or 0),
            "alertas_excedidas": int(budget_ctx.get("presupuesto_alertas_excedidas") or 0),
            "alertas_preventivas": int(budget_ctx.get("presupuesto_alertas_preventivas") or 0),
        },
        "top_proveedores": [
            {
                "proveedor": row["proveedor"],
                "estimado": float(row["estimado"] or 0),
                "ejecutado": float(row["ejecutado"] or 0),
                "variacion": float(row["variacion"] or 0),
                "participacion_pct": float(row["participacion_pct"] or 0),
                "objetivo_proveedor": float((row.get("objetivo_proveedor") or 0)),
                "uso_objetivo_pct": (
                    float(row["uso_objetivo_pct"])
                    if row.get("uso_objetivo_pct") is not None
                    else None
                ),
            }
            for row in budget_ctx["presupuesto_rows_proveedor"][:10]
        ],
        "top_categorias": [
            {
                "categoria": row["categoria"],
                "estimado": float(row["estimado"] or 0),
                "ejecutado": float(row["ejecutado"] or 0),
                "variacion": float(row["variacion"] or 0),
                "participacion_pct": float(row["participacion_pct"] or 0),
                "objetivo_categoria": float((row.get("objetivo_categoria") or 0)),
                "uso_objetivo_pct": (
                    float(row["uso_objetivo_pct"])
                    if row.get("uso_objetivo_pct") is not None
                    else None
                ),
            }
            for row in budget_ctx.get("presupuesto_rows_categoria", [])[:10]
        ],
        "historial_6m": [
            {
                "periodo_mes": row["periodo_mes"],
                "objetivo": float(row["objetivo"] or 0),
                "estimado": float(row["estimado"] or 0),
                "ejecutado": float(row["ejecutado"] or 0),
                "ratio_pct": float(row["ratio_pct"]) if row.get("ratio_pct") is not None else None,
                "estado_label": row["estado_label"],
            }
            for row in historial
        ],
        "trend": {
            "proveedor_rows": [
                {
                    "proveedor": row["proveedor"],
                    "mes": row["mes"],
                    "estimado": float(row["estimado"] or 0),
                    "ejecutado": float(row["ejecutado"] or 0),
                    "variacion": float(row["variacion"] or 0),
                }
                for row in provider_dashboard["trend_rows"]
            ],
            "categoria_rows": [
                {
                    "categoria": row["categoria"],
                    "mes": row["mes"],
                    "estimado": float(row["estimado"] or 0),
                    "ejecutado": float(row["ejecutado"] or 0),
                    "variacion": float(row["variacion"] or 0),
                }
                for row in category_dashboard["trend_rows"]
            ],
        },
        "consumo_vs_plan": {
            "totals": {
                "plan_qty_total": float(consumo_dashboard["totals"]["plan_qty_total"] or 0),
                "consumo_real_qty_total": float(consumo_dashboard["totals"]["consumo_real_qty_total"] or 0),
                "plan_cost_total": float(consumo_dashboard["totals"]["plan_cost_total"] or 0),
                "consumo_real_cost_total": float(consumo_dashboard["totals"]["consumo_real_cost_total"] or 0),
                "variacion_cost_total": float(consumo_dashboard["totals"]["variacion_cost_total"] or 0),
                "sin_costo_count": int(consumo_dashboard["totals"]["sin_costo_count"] or 0),
                "semaforo_verde_count": int(consumo_dashboard["totals"]["semaforo_verde_count"] or 0),
                "semaforo_amarillo_count": int(consumo_dashboard["totals"]["semaforo_amarillo_count"] or 0),
                "semaforo_rojo_count": int(consumo_dashboard["totals"]["semaforo_rojo_count"] or 0),
                "cobertura_pct": (
                    float(consumo_dashboard["totals"]["cobertura_pct"])
                    if consumo_dashboard["totals"]["cobertura_pct"] is not None
                    else None
                ),
            },
            "period_start": str(consumo_dashboard["period_start"]),
            "period_end": str(consumo_dashboard["period_end"]),
            "rows": [
                {
                    "insumo_id": row["insumo_id"],
                    "insumo": row["insumo"],
                    "categoria": row["categoria"],
                    "unidad": row["unidad"],
                    "cantidad_plan": float(row["cantidad_plan"] or 0),
                    "cantidad_real": float(row["cantidad_real"] or 0),
                    "variacion_qty": float(row["variacion_qty"] or 0),
                    "costo_unitario": float(row["costo_unitario"] or 0),
                    "costo_plan": float(row["costo_plan"] or 0),
                    "costo_real": float(row["costo_real"] or 0),
                    "variacion_cost": float(row["variacion_cost"] or 0),
                    "consumo_pct": float(row["consumo_pct"]) if row["consumo_pct"] is not None else None,
                    "estado": row["estado"],
                    "semaforo": row["semaforo"],
                    "sin_costo": bool(row["sin_costo"]),
                    "alerta": row["alerta"],
                }
                for row in consumo_dashboard["rows"][:20]
            ],
        },
    }
    return JsonResponse(data)


@login_required
def solicitudes_consumo_vs_plan_api(request: HttpRequest) -> JsonResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    periodo_tipo, periodo_mes, periodo_label = _parse_period_filters(
        request.GET.get("periodo_tipo"),
        request.GET.get("periodo_mes"),
    )
    source_filter = (request.GET.get("source") or "all").lower()
    if source_filter not in {"all", "manual", "plan"}:
        source_filter = "all"
    plan_filter = (request.GET.get("plan_id") or "").strip()
    categoria_filter = _sanitize_categoria_filter(request.GET.get("categoria"))
    consumo_ref_filter = _sanitize_consumo_ref_filter(request.GET.get("consumo_ref"))

    dashboard = _build_consumo_vs_plan_dashboard(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
        source_filter=source_filter,
        plan_filter=plan_filter,
        categoria_filter=categoria_filter,
        consumo_ref_filter=consumo_ref_filter,
    )
    data = {
        "filters": {
            "source": source_filter,
            "plan_id": plan_filter,
            "categoria": categoria_filter or "",
            "consumo_ref": consumo_ref_filter,
            "periodo_tipo": periodo_tipo,
            "periodo_mes": periodo_mes,
            "periodo_label": periodo_label,
        },
        "period_start": str(dashboard["period_start"]),
        "period_end": str(dashboard["period_end"]),
        "totals": {
            "plan_qty_total": float(dashboard["totals"]["plan_qty_total"] or 0),
            "consumo_real_qty_total": float(dashboard["totals"]["consumo_real_qty_total"] or 0),
            "plan_cost_total": float(dashboard["totals"]["plan_cost_total"] or 0),
            "consumo_real_cost_total": float(dashboard["totals"]["consumo_real_cost_total"] or 0),
            "variacion_cost_total": float(dashboard["totals"]["variacion_cost_total"] or 0),
            "sin_costo_count": int(dashboard["totals"]["sin_costo_count"] or 0),
            "semaforo_verde_count": int(dashboard["totals"]["semaforo_verde_count"] or 0),
            "semaforo_amarillo_count": int(dashboard["totals"]["semaforo_amarillo_count"] or 0),
            "semaforo_rojo_count": int(dashboard["totals"]["semaforo_rojo_count"] or 0),
            "cobertura_pct": (
                float(dashboard["totals"]["cobertura_pct"])
                if dashboard["totals"]["cobertura_pct"] is not None
                else None
            ),
        },
        "rows": [
            {
                "insumo_id": row["insumo_id"],
                "insumo": row["insumo"],
                "categoria": row["categoria"],
                "unidad": row["unidad"],
                "cantidad_plan": float(row["cantidad_plan"] or 0),
                "cantidad_real": float(row["cantidad_real"] or 0),
                "variacion_qty": float(row["variacion_qty"] or 0),
                "costo_unitario": float(row["costo_unitario"] or 0),
                "costo_plan": float(row["costo_plan"] or 0),
                "costo_real": float(row["costo_real"] or 0),
                "variacion_cost": float(row["variacion_cost"] or 0),
                "consumo_pct": float(row["consumo_pct"]) if row["consumo_pct"] is not None else None,
                "estado": row["estado"],
                "semaforo": row["semaforo"],
                "sin_costo": bool(row["sin_costo"]),
                "alerta": row["alerta"],
            }
            for row in dashboard["rows"]
        ],
    }
    return JsonResponse(data)


def _parse_month_filter(raw: str) -> tuple[int, int] | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        year_str, month_str = value.split("-")
        year = int(year_str)
        month = int(month_str)
        if 1 <= month <= 12:
            return year, month
    except Exception:
        return None
    return None


def _export_ordenes_csv(ordenes_qs) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="ordenes_compra_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "folio",
            "solicitud_folio",
            "referencia",
            "proveedor",
            "fecha_emision",
            "fecha_entrega_estimada",
            "monto_estimado",
            "estatus",
        ]
    )
    for orden in ordenes_qs:
        writer.writerow(
            [
                orden.folio,
                orden.solicitud.folio if orden.solicitud else "",
                orden.referencia or "",
                orden.proveedor.nombre,
                orden.fecha_emision.isoformat() if orden.fecha_emision else "",
                orden.fecha_entrega_estimada.isoformat() if orden.fecha_entrega_estimada else "",
                f"{Decimal(orden.monto_estimado or 0):.2f}",
                orden.get_estatus_display(),
            ]
        )
    return response


def _export_ordenes_xlsx(ordenes_qs) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "ordenes_compra"
    ws.append(
        [
            "folio",
            "solicitud_folio",
            "referencia",
            "proveedor",
            "fecha_emision",
            "fecha_entrega_estimada",
            "monto_estimado",
            "estatus",
        ]
    )
    for orden in ordenes_qs:
        ws.append(
            [
                orden.folio,
                orden.solicitud.folio if orden.solicitud else "",
                orden.referencia or "",
                orden.proveedor.nombre,
                orden.fecha_emision.isoformat() if orden.fecha_emision else "",
                orden.fecha_entrega_estimada.isoformat() if orden.fecha_entrega_estimada else "",
                float(Decimal(orden.monto_estimado or 0)),
                orden.get_estatus_display(),
            ]
        )
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response["Content-Disposition"] = f'attachment; filename="ordenes_compra_{now_str}.xlsx"'
    return response


def _export_recepciones_csv(recepciones_qs) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="recepciones_compra_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "folio",
            "orden_folio",
            "proveedor",
            "fecha_recepcion",
            "conformidad_pct",
            "estatus",
            "observaciones",
        ]
    )
    for rec in recepciones_qs:
        writer.writerow(
            [
                rec.folio,
                rec.orden.folio,
                rec.orden.proveedor.nombre,
                rec.fecha_recepcion.isoformat() if rec.fecha_recepcion else "",
                f"{Decimal(rec.conformidad_pct or 0):.2f}",
                rec.get_estatus_display(),
                rec.observaciones or "",
            ]
        )
    return response


def _export_recepciones_xlsx(recepciones_qs) -> HttpResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "recepciones_compra"
    ws.append(
        [
            "folio",
            "orden_folio",
            "proveedor",
            "fecha_recepcion",
            "conformidad_pct",
            "estatus",
            "observaciones",
        ]
    )
    for rec in recepciones_qs:
        ws.append(
            [
                rec.folio,
                rec.orden.folio,
                rec.orden.proveedor.nombre,
                rec.fecha_recepcion.isoformat() if rec.fecha_recepcion else "",
                float(Decimal(rec.conformidad_pct or 0)),
                rec.get_estatus_display(),
                rec.observaciones or "",
            ]
        )
    out = BytesIO()
    wb.save(out)
    out.seek(0)
    response = HttpResponse(
        out.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response["Content-Disposition"] = f'attachment; filename="recepciones_compra_{now_str}.xlsx"'
    return response


@login_required
def ordenes(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    if request.method == "POST":
        if not can_manage_compras(request.user):
            raise PermissionDenied("No tienes permisos para crear órdenes.")
        proveedor_id = request.POST.get("proveedor_id")
        if proveedor_id:
            solicitud_raw = request.POST.get("solicitud_id")
            if not solicitud_raw:
                messages.error(request, "Debes seleccionar una solicitud aprobada para crear una orden.")
                return redirect("compras:ordenes")

            solicitud = get_object_or_404(SolicitudCompra, pk=solicitud_raw)
            if solicitud.estatus != SolicitudCompra.STATUS_APROBADA:
                messages.error(request, f"La solicitud {solicitud.folio} no está aprobada.")
                return redirect("compras:ordenes")

            orden = OrdenCompra.objects.create(
                proveedor_id=proveedor_id,
                solicitud=solicitud,
                referencia=f"SOLICITUD:{solicitud.folio}",
                fecha_emision=request.POST.get("fecha_emision") or None,
                fecha_entrega_estimada=request.POST.get("fecha_entrega_estimada") or None,
                monto_estimado=_to_decimal(request.POST.get("monto_estimado"), "0"),
                estatus=request.POST.get("estatus") or OrdenCompra.STATUS_BORRADOR,
            )
            log_event(
                request.user,
                "CREATE",
                "compras.OrdenCompra",
                orden.id,
                {"folio": orden.folio, "estatus": orden.estatus},
            )
        return redirect("compras:ordenes")

    ordenes_qs = OrdenCompra.objects.select_related("proveedor", "solicitud").order_by("-creado_en")
    proveedor_filter = (request.GET.get("proveedor_id") or "").strip()
    estatus_filter = (request.GET.get("estatus") or "all").strip().upper() or "all"
    mes_filter = (request.GET.get("mes") or "").strip()
    q_filter = (request.GET.get("q") or "").strip()

    if proveedor_filter.isdigit():
        ordenes_qs = ordenes_qs.filter(proveedor_id=int(proveedor_filter))
    if estatus_filter and estatus_filter != "ALL":
        ordenes_qs = ordenes_qs.filter(estatus=estatus_filter)
    parsed_month = _parse_month_filter(mes_filter)
    if parsed_month:
        y, m = parsed_month
        ordenes_qs = ordenes_qs.filter(fecha_emision__year=y, fecha_emision__month=m)
    if q_filter:
        ordenes_qs = ordenes_qs.filter(
            Q(folio__icontains=q_filter)
            | Q(referencia__icontains=q_filter)
            | Q(proveedor__nombre__icontains=q_filter)
            | Q(solicitud__folio__icontains=q_filter)
        )

    export_format = (request.GET.get("export") or "").strip().lower()
    if export_format == "csv":
        return _export_ordenes_csv(ordenes_qs)
    if export_format == "xlsx":
        return _export_ordenes_xlsx(ordenes_qs)

    query_without_export = request.GET.copy()
    query_without_export.pop("export", None)

    context = {
        "ordenes": ordenes_qs[:200],
        "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:200],
        "solicitudes": SolicitudCompra.objects.filter(estatus=SolicitudCompra.STATUS_APROBADA).order_by("-creado_en")[:200],
        "status_choices": OrdenCompra.STATUS_CHOICES,
        "proveedor_filter": proveedor_filter,
        "estatus_filter": estatus_filter,
        "mes_filter": mes_filter,
        "q_filter": q_filter,
        "current_query": query_without_export.urlencode(),
        "can_manage_compras": can_manage_compras(request.user),
    }
    return render(request, "compras/ordenes.html", context)


@login_required
def recepciones(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    if request.method == "POST":
        if not can_manage_compras(request.user):
            raise PermissionDenied("No tienes permisos para registrar recepciones.")
        orden_id = request.POST.get("orden_id")
        if orden_id:
            orden = get_object_or_404(OrdenCompra, pk=orden_id)
            if orden.estatus in {OrdenCompra.STATUS_BORRADOR, OrdenCompra.STATUS_CERRADA}:
                messages.error(request, f"La orden {orden.folio} no admite recepciones en estatus {orden.get_estatus_display()}.")
                return redirect("compras:recepciones")

            recepcion = RecepcionCompra.objects.create(
                orden=orden,
                fecha_recepcion=request.POST.get("fecha_recepcion") or None,
                conformidad_pct=_to_decimal(request.POST.get("conformidad_pct"), "100"),
                estatus=request.POST.get("estatus") or RecepcionCompra.STATUS_PENDIENTE,
                observaciones=request.POST.get("observaciones", "").strip(),
            )
            log_event(
                request.user,
                "CREATE",
                "compras.RecepcionCompra",
                recepcion.id,
                {"folio": recepcion.folio, "estatus": recepcion.estatus},
            )
            if recepcion.estatus == RecepcionCompra.STATUS_CERRADA and orden.estatus != OrdenCompra.STATUS_CERRADA:
                orden_prev = orden.estatus
                orden.estatus = OrdenCompra.STATUS_CERRADA
                orden.save(update_fields=["estatus"])
                log_event(
                    request.user,
                    "APPROVE",
                    "compras.OrdenCompra",
                    orden.id,
                    {"from": orden_prev, "to": OrdenCompra.STATUS_CERRADA, "folio": orden.folio, "source": recepcion.folio},
                )
        return redirect("compras:recepciones")

    recepciones_qs = RecepcionCompra.objects.select_related("orden", "orden__proveedor").order_by("-creado_en")
    proveedor_filter = (request.GET.get("proveedor_id") or "").strip()
    estatus_filter = (request.GET.get("estatus") or "all").strip().upper() or "all"
    mes_filter = (request.GET.get("mes") or "").strip()
    q_filter = (request.GET.get("q") or "").strip()

    if proveedor_filter.isdigit():
        recepciones_qs = recepciones_qs.filter(orden__proveedor_id=int(proveedor_filter))
    if estatus_filter and estatus_filter != "ALL":
        recepciones_qs = recepciones_qs.filter(estatus=estatus_filter)
    parsed_month = _parse_month_filter(mes_filter)
    if parsed_month:
        y, m = parsed_month
        recepciones_qs = recepciones_qs.filter(fecha_recepcion__year=y, fecha_recepcion__month=m)
    if q_filter:
        recepciones_qs = recepciones_qs.filter(
            Q(folio__icontains=q_filter)
            | Q(orden__folio__icontains=q_filter)
            | Q(orden__proveedor__nombre__icontains=q_filter)
            | Q(observaciones__icontains=q_filter)
        )

    export_format = (request.GET.get("export") or "").strip().lower()
    if export_format == "csv":
        return _export_recepciones_csv(recepciones_qs)
    if export_format == "xlsx":
        return _export_recepciones_xlsx(recepciones_qs)

    query_without_export = request.GET.copy()
    query_without_export.pop("export", None)

    context = {
        "recepciones": recepciones_qs[:200],
        "ordenes": OrdenCompra.objects.select_related("proveedor").exclude(estatus=OrdenCompra.STATUS_BORRADOR).exclude(estatus=OrdenCompra.STATUS_CERRADA).order_by("-creado_en")[:200],
        "status_choices": RecepcionCompra.STATUS_CHOICES,
        "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:200],
        "proveedor_filter": proveedor_filter,
        "estatus_filter": estatus_filter,
        "mes_filter": mes_filter,
        "q_filter": q_filter,
        "current_query": query_without_export.urlencode(),
        "can_manage_compras": can_manage_compras(request.user),
    }
    return render(request, "compras/recepciones.html", context)


@login_required
@require_POST
def actualizar_solicitud_estatus(request: HttpRequest, pk: int, estatus: str) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para aprobar/rechazar solicitudes.")

    solicitud = get_object_or_404(SolicitudCompra, pk=pk)
    prev = solicitud.estatus
    if _can_transition_solicitud(prev, estatus):
        solicitud.estatus = estatus
        solicitud.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.SolicitudCompra",
            solicitud.id,
            {"from": prev, "to": estatus, "folio": solicitud.folio},
        )
    return redirect("compras:solicitudes")


@login_required
@require_POST
def eliminar_solicitud(request: HttpRequest, pk: int) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para eliminar solicitudes.")

    solicitud = get_object_or_404(SolicitudCompra, pk=pk)
    has_open_order = OrdenCompra.objects.filter(solicitud=solicitud).exclude(estatus=OrdenCompra.STATUS_CERRADA).exists()
    return_query = (request.POST.get("return_query") or "").strip()
    if has_open_order:
        messages.error(request, f"No puedes eliminar {solicitud.folio}: tiene una orden de compra activa.")
    else:
        solicitud_id = solicitud.id
        folio = solicitud.folio
        solicitud.delete()
        log_event(
            request.user,
            "DELETE",
            "compras.SolicitudCompra",
            solicitud_id,
            {"folio": folio},
        )
        messages.success(request, f"Solicitud {folio} eliminada.")

    if return_query:
        return redirect(f"{reverse('compras:solicitudes')}?{return_query}")
    return redirect("compras:solicitudes")


@login_required
@require_POST
def actualizar_orden_estatus(request: HttpRequest, pk: int, estatus: str) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para operar órdenes.")

    orden = get_object_or_404(OrdenCompra, pk=pk)
    prev = orden.estatus

    if estatus == OrdenCompra.STATUS_CERRADA:
        has_closed_recepcion = RecepcionCompra.objects.filter(
            orden=orden,
            estatus=RecepcionCompra.STATUS_CERRADA,
        ).exists()
        if not has_closed_recepcion:
            messages.error(request, f"No puedes cerrar {orden.folio} sin al menos una recepción cerrada.")
            return redirect("compras:ordenes")

    if _can_transition_orden(prev, estatus):
        orden.estatus = estatus
        orden.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.OrdenCompra",
            orden.id,
            {"from": prev, "to": estatus, "folio": orden.folio},
        )
    return redirect("compras:ordenes")


@login_required
@require_POST
def actualizar_recepcion_estatus(request: HttpRequest, pk: int, estatus: str) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para cerrar recepciones.")

    recepcion = get_object_or_404(RecepcionCompra, pk=pk)
    prev = recepcion.estatus
    if _can_transition_recepcion(prev, estatus):
        recepcion.estatus = estatus
        recepcion.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.RecepcionCompra",
            recepcion.id,
            {"from": prev, "to": estatus, "folio": recepcion.folio},
        )

        # Si la recepción quedó cerrada, marcamos la orden cerrada automáticamente.
        if estatus == RecepcionCompra.STATUS_CERRADA and recepcion.orden.estatus != OrdenCompra.STATUS_CERRADA:
            orden_prev = recepcion.orden.estatus
            recepcion.orden.estatus = OrdenCompra.STATUS_CERRADA
            recepcion.orden.save(update_fields=["estatus"])
            log_event(
                request.user,
                "APPROVE",
                "compras.OrdenCompra",
                recepcion.orden.id,
                {"from": orden_prev, "to": OrdenCompra.STATUS_CERRADA, "folio": recepcion.orden.folio, "source": recepcion.folio},
            )
    return redirect("compras:recepciones")


@login_required
@require_POST
def crear_orden_desde_solicitud(request: HttpRequest, pk: int) -> HttpResponse:
    if not can_manage_compras(request.user):
        raise PermissionDenied("No tienes permisos para crear órdenes.")

    solicitud = get_object_or_404(SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido"), pk=pk)
    if solicitud.estatus != SolicitudCompra.STATUS_APROBADA:
        messages.error(request, f"La solicitud {solicitud.folio} no está aprobada.")
        return redirect("compras:solicitudes")

    has_open_order = OrdenCompra.objects.filter(solicitud=solicitud).exclude(estatus=OrdenCompra.STATUS_CERRADA).exists()
    if has_open_order:
        messages.info(request, f"La solicitud {solicitud.folio} ya tiene una orden activa.")
        return redirect("compras:ordenes")

    proveedor = solicitud.proveedor_sugerido or solicitud.insumo.proveedor_principal
    if not proveedor:
        messages.error(request, f"La solicitud {solicitud.folio} no tiene proveedor sugerido. Asigna uno y reintenta.")
        return redirect("compras:solicitudes")

    latest_cost = (
        CostoInsumo.objects.filter(insumo=solicitud.insumo)
        .order_by("-fecha", "-id")
        .first()
    )
    monto_estimado = (solicitud.cantidad or Decimal("0")) * (latest_cost.costo_unitario if latest_cost else Decimal("0"))

    orden = OrdenCompra.objects.create(
        solicitud=solicitud,
        proveedor=proveedor,
        referencia=f"SOLICITUD:{solicitud.folio}",
        fecha_emision=timezone.localdate(),
        fecha_entrega_estimada=solicitud.fecha_requerida,
        monto_estimado=monto_estimado,
        estatus=OrdenCompra.STATUS_BORRADOR,
    )
    log_event(
        request.user,
        "CREATE",
        "compras.OrdenCompra",
        orden.id,
        {"folio": orden.folio, "estatus": orden.estatus, "source": f"solicitud:{solicitud.folio}"},
    )
    messages.success(request, f"Orden {orden.folio} creada desde solicitud {solicitud.folio}.")
    return redirect("compras:ordenes")
