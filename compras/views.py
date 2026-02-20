import csv
import calendar
from io import BytesIO
from decimal import Decimal, InvalidOperation
from io import StringIO
from datetime import date, datetime
from pathlib import Path

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.urls import reverse
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST
from openpyxl import Workbook, load_workbook

from core.access import can_manage_compras, can_view_compras
from core.audit import log_event
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo, Proveedor
from recetas.models import PlanProduccion
from recetas.utils.matching import match_insumo
from recetas.utils.normalizacion import normalizar_nombre

from .models import OrdenCompra, RecepcionCompra, SolicitudCompra


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


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
    return n


def _read_import_rows(uploaded) -> list[dict]:
    ext = Path(uploaded.name or "").suffix.lower()
    rows: list[dict] = []

    if ext in {".xlsx", ".xlsm"}:
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []
        headers = [_map_import_header(str(h or "")) for h in values[0]]
        for raw in values[1:]:
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
    existencias = {
        e.insumo_id: e
        for e in ExistenciaInsumo.objects.filter(insumo_id__in=[i.id for i in insumos])
    }
    options = []
    for insumo in insumos:
        ex = existencias.get(insumo.id)
        stock_actual = ex.stock_actual if ex else Decimal("0")
        punto_reorden = ex.punto_reorden if ex else Decimal("0")
        recomendado = max(punto_reorden - stock_actual, Decimal("0"))
        options.append(
            {
                "id": insumo.id,
                "nombre": insumo.nombre,
                "proveedor_sugerido": insumo.proveedor_principal.nombre if insumo.proveedor_principal_id else "",
                "stock_actual": stock_actual,
                "punto_reorden": punto_reorden,
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


def _export_solicitudes_csv(
    solicitudes,
    source_filter: str,
    plan_filter: str,
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


def _filtered_solicitudes(
    source_filter_raw: str,
    plan_filter_raw: str,
    reabasto_filter_raw: str,
    periodo_tipo_raw: str,
    periodo_mes_raw: str,
):
    source_filter = (source_filter_raw or "all").lower()
    if source_filter not in {"all", "manual", "plan"}:
        source_filter = "all"
    plan_filter = (plan_filter_raw or "").strip()
    periodo_tipo, periodo_mes, periodo_label = _parse_period_filters(periodo_tipo_raw, periodo_mes_raw)

    solicitudes_qs = SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido").all()
    if source_filter == "plan":
        solicitudes_qs = solicitudes_qs.filter(area__startswith="PLAN_PRODUCCION:")
    elif source_filter == "manual":
        solicitudes_qs = solicitudes_qs.exclude(area__startswith="PLAN_PRODUCCION:")

    if plan_filter:
        solicitudes_qs = solicitudes_qs.filter(area=f"PLAN_PRODUCCION:{plan_filter}")

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

    return solicitudes, source_filter, plan_filter, reabasto_filter, plan_options, periodo_tipo, periodo_mes, periodo_label


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

    solicitudes, source_filter, plan_filter, reabasto_filter, plan_options, periodo_tipo, periodo_mes, periodo_label = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
        request.GET.get("reabasto"),
        request.GET.get("periodo_tipo"),
        request.GET.get("periodo_mes"),
    )
    total_presupuesto = sum((s.presupuesto_estimado for s in solicitudes), Decimal("0"))

    export_format = (request.GET.get("export") or "").lower()
    if export_format == "csv":
        return _export_solicitudes_csv(
            solicitudes,
            source_filter,
            plan_filter,
            reabasto_filter,
            periodo_tipo,
            periodo_mes,
            periodo_label,
        )
    if export_format == "xlsx":
        return _export_solicitudes_xlsx(
            solicitudes,
            source_filter,
            plan_filter,
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
        "status_choices": SolicitudCompra.STATUS_CHOICES,
        "can_manage_compras": can_manage_compras(request.user),
        "reabasto_filter": reabasto_filter,
        "source_filter": source_filter,
        "plan_filter": plan_filter,
        "plan_options": plan_options,
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
        "periodo_label": periodo_label,
        "total_presupuesto": total_presupuesto,
        "current_query": query_without_export.urlencode(),
    }
    return render(request, "compras/solicitudes.html", context)


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

    created = 0
    skipped_invalid = 0
    skipped_duplicate = 0
    skipped_match = 0
    pending_rows: list[dict] = []

    for idx, row in enumerate(rows, start=2):
        insumo_raw = str(row.get("insumo") or "").strip()
        cantidad = _to_decimal(str(row.get("cantidad") or ""), "0")
        if not insumo_raw or cantidad <= 0:
            skipped_invalid += 1
            continue

        insumo, score, method = match_insumo(insumo_raw)
        if not insumo or score < min_score:
            skipped_match += 1
            pending_rows.append(
                {
                    "row": idx,
                    "insumo_origen": insumo_raw,
                    "cantidad_origen": str(row.get("cantidad") or ""),
                    "score": f"{score:.1f}",
                    "metodo": method,
                    "sugerencia": insumo.nombre if insumo else "",
                    "motivo": f"score<{min_score}" if insumo else "sin_match",
                }
            )
            continue

        area = str(row.get("area") or area_default).strip() or area_default
        solicitante = str(row.get("solicitante") or solicitante_default).strip() or solicitante_default
        fecha_requerida = _parse_date_value(row.get("fecha_requerida"), fecha_default)
        estatus = str(row.get("estatus") or estatus_default).strip().upper()
        if estatus not in valid_status:
            estatus = estatus_default

        proveedor = _resolve_proveedor_name(str(row.get("proveedor") or ""), provider_map) or insumo.proveedor_principal

        if evitar_duplicados:
            exists = SolicitudCompra.objects.filter(
                area=area,
                insumo=insumo,
                fecha_requerida=fecha_requerida,
                estatus__in={
                    SolicitudCompra.STATUS_BORRADOR,
                    SolicitudCompra.STATUS_EN_REVISION,
                    SolicitudCompra.STATUS_APROBADA,
                },
            ).exists()
            if exists:
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
            {"folio": solicitud.folio, "source": "import", "score": round(score, 1), "method": method},
        )
        created += 1

    pending_path = ""
    if pending_rows:
        pending_path = _write_import_pending_csv(pending_rows)

    messages.success(
        request,
        (
            f"Importación completada. Creadas: {created}. "
            f"Sin match/score: {skipped_match}. Duplicadas: {skipped_duplicate}. "
            f"Inválidas: {skipped_invalid}."
        ),
    )
    if pending_path:
        messages.warning(request, f"Pendientes de homologación guardados en: {pending_path}")

    return redirect(
        f"{reverse('compras:solicitudes')}?source=manual&periodo_tipo={periodo_tipo}&periodo_mes={periodo_mes}"
    )


@login_required
def solicitudes_print(request: HttpRequest) -> HttpResponse:
    if not can_view_compras(request.user):
        raise PermissionDenied("No tienes permisos para ver Compras.")

    solicitudes, source_filter, plan_filter, reabasto_filter, _, periodo_tipo, periodo_mes, periodo_label = _filtered_solicitudes(
        request.GET.get("source"),
        request.GET.get("plan_id"),
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

    context = {
        "ordenes": OrdenCompra.objects.select_related("proveedor", "solicitud")[:50],
        "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:200],
        "solicitudes": SolicitudCompra.objects.filter(estatus=SolicitudCompra.STATUS_APROBADA).order_by("-creado_en")[:200],
        "status_choices": OrdenCompra.STATUS_CHOICES,
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

    context = {
        "recepciones": RecepcionCompra.objects.select_related("orden", "orden__proveedor")[:50],
        "ordenes": OrdenCompra.objects.select_related("proveedor").exclude(estatus=OrdenCompra.STATUS_BORRADOR).exclude(estatus=OrdenCompra.STATUS_CERRADA).order_by("-creado_en")[:200],
        "status_choices": RecepcionCompra.STATUS_CHOICES,
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
