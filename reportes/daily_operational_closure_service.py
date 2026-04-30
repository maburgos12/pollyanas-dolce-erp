from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.db.models import Sum

from control.models import MermaPOS
from core.models import AuditLog, sucursales_operativas
from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from pos_bridge.models import PointDailyBranchIndicator, PointDailySale, PointProductionLine, PointSyncJob, PointWasteLine
from recetas.models import PlanProduccion, PlanProduccionItem
from reportes.models import FactInventarioDiario, FactProduccionDiaria, FactVentaDiaria


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _latest_sync_job(job_type: str):
    return (
        PointSyncJob.objects.filter(job_type=job_type)
        .select_related("triggered_by")
        .order_by("-started_at", "-id")
        .first()
    )


def _latest_audit_for_trigger(trigger: str):
    return (
        AuditLog.objects.filter(
            model="reportes.AnalyticRefreshWindow",
            payload__trigger=trigger,
            action__in=[
                "INTEGRATIONS_ANALYTICS_REFRESH_COMPLETED",
                "INTEGRATIONS_ANALYTICS_REFRESH_FAILED",
            ],
        )
        .select_related("user")
        .order_by("-timestamp", "-id")
        .first()
    )


def _status_row(*, label: str, tone: str, detail: str) -> dict[str, str]:
    return {"label": label, "tone": tone, "detail": detail}


def _sales_domain(target_date: date) -> dict[str, object]:
    raw_qs = PointDailySale.objects.filter(sale_date=target_date)
    fact_qs = FactVentaDiaria.objects.filter(fecha=target_date)
    indicator_qs = PointDailyBranchIndicator.objects.filter(indicator_date=target_date)
    raw_max = PointDailySale.objects.order_by("-sale_date").values_list("sale_date", flat=True).first()
    fact_max = FactVentaDiaria.objects.order_by("-fecha").values_list("fecha", flat=True).first()

    raw_units = _to_decimal(raw_qs.aggregate(total=Sum("quantity")).get("total"))
    raw_amount = _to_decimal(raw_qs.aggregate(total=Sum("total_amount")).get("total"))
    fact_units = _to_decimal(fact_qs.aggregate(total=Sum("cantidad")).get("total"))
    fact_amount = _to_decimal(fact_qs.aggregate(total=Sum("venta_total")).get("total"))
    fact_tickets_raw = int(fact_qs.aggregate(total=Sum("tickets")).get("total") or 0)
    indicator_tickets = int(indicator_qs.aggregate(total=Sum("total_tickets")).get("total") or 0)
    fact_tickets = fact_tickets_raw if fact_tickets_raw > 0 else indicator_tickets

    latest_job = _latest_sync_job(PointSyncJob.JOB_TYPE_SALES)
    latest_audit = _latest_audit_for_trigger("point_daily_sales_sync")

    if raw_qs.exists() and fact_qs.exists():
        publication = _status_row(
            label="Publicado",
            tone="success",
            detail="Las ventas del día ya están materializadas en facts y listas para paneles visibles.",
        )
    elif raw_qs.exists():
        publication = _status_row(
            label="Pendiente",
            tone="warning",
            detail="Point ya tiene ventas del día, pero todavía no están materializadas en los facts visibles del ERP.",
        )
    else:
        publication = _status_row(
            label="Sin captura",
            tone="warning",
            detail="Point no trae ventas para la fecha auditada o la ventana todavía no ha sido sincronizada.",
        )

    return {
        "key": "ventas",
        "label": "Ventas",
        "source_truth": "PointDailySale -> FactVentaDiaria",
        "visible_source": "Reportes · Ventas / BI usan facts y corte canónico de ventas",
        "raw_max_date": raw_max,
        "fact_max_date": fact_max,
        "visible_max_date": fact_max,
        "raw_rows": raw_qs.count(),
        "fact_rows": fact_qs.count(),
        "raw_branch_count": raw_qs.exclude(branch__erp_branch_id__isnull=True).values("branch__erp_branch_id").distinct().count(),
        "fact_branch_count": fact_qs.exclude(sucursal_id__isnull=True).values("sucursal_id").distinct().count(),
        "raw_units": raw_units,
        "raw_amount": raw_amount,
        "fact_units": fact_units,
        "fact_amount": fact_amount,
        "fact_tickets": fact_tickets,
        "fact_tickets_raw": fact_tickets_raw,
        "indicator_tickets": indicator_tickets,
        "publication": publication,
        "latest_job": latest_job,
        "latest_audit": latest_audit,
    }


def _production_domain(target_date: date) -> dict[str, object]:
    raw_qs = PointProductionLine.objects.filter(
        production_date=target_date,
        is_insumo=False,
    )
    fact_qs = FactProduccionDiaria.objects.filter(fecha=target_date)
    plan_qs = PlanProduccion.objects.filter(fecha_produccion=target_date)
    plan_items_qs = PlanProduccionItem.objects.filter(plan__fecha_produccion=target_date)

    raw_max = PointProductionLine.objects.order_by("-production_date").values_list("production_date", flat=True).first()
    fact_max = FactProduccionDiaria.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    plan_max = PlanProduccion.objects.order_by("-fecha_produccion").values_list("fecha_produccion", flat=True).first()

    raw_units = _to_decimal(raw_qs.aggregate(total=Sum("produced_quantity")).get("total"))
    fact_produced = _to_decimal(fact_qs.aggregate(total=Sum("producido")).get("total"))
    fact_sold = _to_decimal(fact_qs.aggregate(total=Sum("vendido")).get("total"))
    fact_waste = _to_decimal(fact_qs.aggregate(total=Sum("merma")).get("total"))
    fact_transfer = _to_decimal(fact_qs.aggregate(total=Sum("transferido")).get("total"))
    plan_units = _to_decimal(plan_items_qs.aggregate(total=Sum("cantidad")).get("total"))

    latest_job = _latest_sync_job(PointSyncJob.JOB_TYPE_PRODUCTION)
    latest_audit = _latest_audit_for_trigger("point_production_sync")

    if raw_qs.exists() and fact_qs.exists():
        publication = _status_row(
            label="Publicado",
            tone="success",
            detail="La producción capturada en Point ya está convertida a facts diarios para comparación operativa.",
        )
    elif raw_qs.exists():
        publication = _status_row(
            label="Pendiente",
            tone="warning",
            detail="Point ya tiene producción del día, pero aún no se publica completamente a los facts visibles del ERP.",
        )
    else:
        publication = _status_row(
            label="Sin captura",
            tone="warning",
            detail="No hay producción Point para la fecha auditada o la sync aún no cerró esa jornada.",
        )

    plan_note = (
        "Las tarjetas visibles de plan de producción usan PlanProduccion, no PointProductionLine."
        if plan_max
        else "No existe plan cargado para la fecha auditada; la producción Point y el plan siguen siendo fuentes distintas."
    )

    return {
        "key": "produccion",
        "label": "Producción",
        "source_truth": "PointProductionLine -> FactProduccionDiaria",
        "visible_source": "Facts diarios para comparativa; tarjetas de plan usan PlanProduccion",
        "raw_max_date": raw_max,
        "fact_max_date": fact_max,
        "visible_max_date": fact_max,
        "plan_visible_max_date": plan_max,
        "raw_rows": raw_qs.count(),
        "fact_rows": fact_qs.count(),
        "plan_count": plan_qs.count(),
        "raw_branch_count": raw_qs.exclude(erp_branch_id__isnull=True).values("erp_branch_id").distinct().count(),
        "fact_branch_count": fact_qs.exclude(sucursal_id__isnull=True).values("sucursal_id").distinct().count(),
        "raw_units": raw_units,
        "fact_produced": fact_produced,
        "fact_sold": fact_sold,
        "fact_waste": fact_waste,
        "fact_transfer": fact_transfer,
        "plan_units": plan_units,
        "publication": publication,
        "plan_note": plan_note,
        "latest_job": latest_job,
        "latest_audit": latest_audit,
    }


def _waste_domain(target_date: date) -> dict[str, object]:
    raw_qs = PointWasteLine.objects.filter(movement_at__date=target_date)
    fact_qs = FactProduccionDiaria.objects.filter(fecha=target_date)
    branch_visible_qs = MermaPOS.objects.filter(fecha=target_date)
    cedis_visible_qs = MovimientoInventario.objects.filter(
        fecha__date=target_date,
        tipo=MovimientoInventario.TIPO_CONSUMO,
        referencia__startswith="MERMA|",
    )

    raw_latest_at = raw_qs.order_by("-movement_at").values_list("movement_at", flat=True).first()
    raw_max_at = PointWasteLine.objects.order_by("-movement_at").values_list("movement_at", flat=True).first()
    fact_max = FactProduccionDiaria.objects.filter(merma__gt=0).order_by("-fecha").values_list("fecha", flat=True).first()
    branch_visible_max = MermaPOS.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    cedis_visible_max = MovimientoInventario.objects.filter(
        tipo=MovimientoInventario.TIPO_CONSUMO,
        referencia__startswith="MERMA|",
    ).order_by("-fecha").values_list("fecha", flat=True).first()
    visible_max = max([value for value in [branch_visible_max, cedis_visible_max.date() if cedis_visible_max else None] if value], default=None)

    raw_units = _to_decimal(raw_qs.aggregate(total=Sum("quantity")).get("total"))
    raw_cost = _to_decimal(raw_qs.aggregate(total=Sum("total_cost")).get("total"))
    fact_units = _to_decimal(fact_qs.aggregate(total=Sum("merma")).get("total"))
    branch_visible_units = _to_decimal(branch_visible_qs.aggregate(total=Sum("cantidad")).get("total"))
    cedis_visible_units = _to_decimal(cedis_visible_qs.aggregate(total=Sum("cantidad")).get("total"))
    visible_units = branch_visible_units + cedis_visible_units

    latest_job = _latest_sync_job(PointSyncJob.JOB_TYPE_WASTE)
    latest_audit = _latest_audit_for_trigger("point_waste_sync")

    if raw_qs.exists() and visible_units > 0:
        publication = _status_row(
            label="Publicado",
            tone="success",
            detail="La merma del día ya impacta el resumen visible del ERP y la comparativa operativa.",
        )
    elif raw_qs.exists():
        publication = _status_row(
            label="Pendiente",
            tone="warning",
            detail="Point ya tiene merma del día, pero todavía no se refleja completa en la capa visible del ERP.",
        )
    else:
        publication = _status_row(
            label="Sin captura",
            tone="warning",
            detail="No hay merma Point registrada para la fecha auditada o la sync aún no cerró esa ventana.",
        )

    return {
        "key": "mermas",
        "label": "Mermas",
        "source_truth": "PointWasteLine; visible ERP combina MermaPOS y MovimientoInventario MERMA|",
        "visible_source": "Resumen de merma visible usa MermaPOS para sucursales y MovimientoInventario para CEDIS",
        "raw_max_date": raw_max_at.date() if raw_max_at else None,
        "raw_latest_at": raw_latest_at,
        "fact_max_date": fact_max,
        "visible_max_date": visible_max,
        "raw_rows": raw_qs.count(),
        "fact_rows": fact_qs.filter(merma__gt=0).count(),
        "visible_rows": branch_visible_qs.count() + cedis_visible_qs.count(),
        "raw_branch_count": raw_qs.exclude(erp_branch_id__isnull=True).values("erp_branch_id").distinct().count(),
        "visible_branch_count": branch_visible_qs.exclude(sucursal_id__isnull=True).values("sucursal_id").distinct().count(),
        "raw_units": raw_units,
        "raw_cost": raw_cost,
        "fact_units": fact_units,
        "branch_visible_units": branch_visible_units,
        "cedis_visible_units": cedis_visible_units,
        "visible_units": visible_units,
        "publication": publication,
        "latest_job": latest_job,
        "latest_audit": latest_audit,
    }


def _inventory_domain(target_date: date) -> dict[str, object]:
    fact_qs = FactInventarioDiario.objects.filter(fecha=target_date)
    moved_insumo_ids = list(fact_qs.values_list("insumo_id", flat=True))
    visible_qs = ExistenciaInsumo.objects.filter(insumo_id__in=moved_insumo_ids)
    movement_qs = MovimientoInventario.objects.filter(fecha__date=target_date)

    expected_stock = _to_decimal(fact_qs.aggregate(total=Sum("stock_final")).get("total"))
    visible_stock = _to_decimal(visible_qs.aggregate(total=Sum("stock_actual")).get("total"))
    entradas = _to_decimal(fact_qs.aggregate(total=Sum("entradas")).get("total"))
    salidas = _to_decimal(fact_qs.aggregate(total=Sum("salidas")).get("total"))
    delta_stock = visible_stock - expected_stock
    fact_max = FactInventarioDiario.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    movement_max = (
        MovimientoInventario.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    )
    movement_latest_at = movement_qs.order_by("-fecha").values_list("fecha", flat=True).first()
    visible_updated_max = visible_qs.order_by("-actualizado_en").values_list("actualizado_en", flat=True).first()
    visible_updated_min = visible_qs.order_by("actualizado_en").values_list("actualizado_en", flat=True).first()
    fact_updated_max = fact_qs.order_by("-actualizado_en").values_list("actualizado_en", flat=True).first()
    latest_sync_run = AlmacenSyncRun.objects.filter(status=AlmacenSyncRun.STATUS_OK).order_by("-started_at", "-id").first()
    latest_sync_any = AlmacenSyncRun.objects.order_by("-started_at", "-id").first()
    entradas_dia = _to_decimal(
        movement_qs.filter(tipo=MovimientoInventario.TIPO_ENTRADA).aggregate(total=Sum("cantidad")).get("total")
    )
    salidas_dia = _to_decimal(
        movement_qs.filter(tipo__in=[MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO])
        .aggregate(total=Sum("cantidad"))
        .get("total")
    )
    ajustes_netos = _to_decimal(
        movement_qs.filter(tipo=MovimientoInventario.TIPO_AJUSTE).aggregate(total=Sum("cantidad")).get("total")
    )
    visible_date = visible_updated_max.date() if visible_updated_max else None
    same_moment_comparison = bool(visible_date and visible_date == target_date)
    comparison_scope = (
        "Mismo día operativo"
        if same_moment_comparison
        else "Cruza cierre del día auditado contra stock vivo actual"
    )
    trace_counts: dict[str, int] = {}
    traceable_count = 0
    untraceable_count = 0
    for existencia in visible_qs.only("trazabilidad_stock"):
        trace = dict(existencia.trazabilidad_stock or {})
        label = str(trace.get("label") or "Sin traza suficiente")
        trace_counts[label] = trace_counts.get(label, 0) + 1
        if str(trace.get("source") or ""):
            traceable_count += 1
            if str(trace.get("source")) == "UNTRACED":
                untraceable_count += 1
        else:
            untraceable_count += 1
    trace_sources_text = ", ".join(f"{label}: {total}" for label, total in sorted(trace_counts.items())) or "Sin traza suficiente"

    if fact_qs.exists():
        if not same_moment_comparison:
            publication = _status_row(
                label="Cruza tiempos",
                tone="warning",
                detail="El cierre esperado sí corresponde al día auditado, pero el stock visible viene del inventario vivo actualizado después; no son saldos del mismo instante histórico.",
            )
        elif delta_stock == ZERO:
            publication = _status_row(
                label="Alineado",
                tone="success",
                detail="El stock visible coincide con el stock esperado para los insumos con movimiento del día.",
            )
        else:
            publication = _status_row(
                label="Desviación",
                tone="warning",
                detail="El stock visible difiere del esperado en los insumos que tuvieron movimiento durante el día.",
            )
    else:
        publication = _status_row(
            label="Sin fact diario",
            tone="warning",
            detail="No existe conciliación diaria de inventario para la fecha auditada; solo hay existencias vivas.",
        )

    return {
        "key": "inventario",
        "label": "Inventario",
        "source_truth": "FactInventarioDiario para movimiento diario; ExistenciaInsumo para stock visible actual",
        "visible_source": "ExistenciaInsumo muestra stock vivo actual, no un snapshot histórico completo por día",
        "raw_max_date": movement_max.date() if movement_max else None,
        "fact_max_date": fact_max,
        "visible_max_date": visible_date,
        "movement_rows": movement_qs.count(),
        "movement_insumo_count": fact_qs.values("insumo_id").distinct().count(),
        "expected_stock": expected_stock,
        "visible_stock": visible_stock,
        "delta_stock": delta_stock,
        "entradas": entradas,
        "salidas": salidas,
        "entradas_dia": entradas_dia,
        "salidas_dia": salidas_dia,
        "ajustes_netos": ajustes_netos,
        "movement_latest_at": movement_latest_at,
        "visible_updated_max": visible_updated_max,
        "visible_updated_min": visible_updated_min,
        "fact_updated_max": fact_updated_max,
        "same_moment_comparison": same_moment_comparison,
        "comparison_scope": comparison_scope,
        "traceable_count": traceable_count,
        "untraceable_count": untraceable_count,
        "trace_sources_text": trace_sources_text,
        "latest_sync_run": latest_sync_run,
        "latest_sync_any": latest_sync_any,
        "publication": publication,
    }


def _branch_rows(target_date: date) -> list[dict[str, object]]:
    sales_map = {
        int(row["sucursal_id"]): row
        for row in FactVentaDiaria.objects.filter(fecha=target_date, sucursal_id__isnull=False)
        .values("sucursal_id")
        .annotate(
            sales_units=Sum("cantidad"),
            sales_amount=Sum("venta_total"),
            tickets=Sum("tickets"),
        )
    }
    prod_map = {
        int(row["sucursal_id"]): row
        for row in FactProduccionDiaria.objects.filter(fecha=target_date, sucursal_id__isnull=False)
        .values("sucursal_id")
        .annotate(
            produced=Sum("producido"),
            sold=Sum("vendido"),
            waste=Sum("merma"),
            transferred=Sum("transferido"),
        )
    }
    waste_point_map = {
        int(row["erp_branch_id"]): row
        for row in PointWasteLine.objects.filter(movement_at__date=target_date, erp_branch_id__isnull=False)
        .values("erp_branch_id")
        .annotate(point_waste=Sum("quantity"))
    }
    waste_visible_map = {
        int(row["sucursal_id"]): row
        for row in MermaPOS.objects.filter(fecha=target_date, sucursal_id__isnull=False)
        .values("sucursal_id")
        .annotate(visible_waste=Sum("cantidad"))
    }
    indicator_map = {
        int(row["branch__erp_branch_id"]): row
        for row in PointDailyBranchIndicator.objects.filter(
            indicator_date=target_date,
            branch__erp_branch_id__isnull=False,
        )
        .values("branch__erp_branch_id")
        .annotate(tickets=Sum("total_tickets"))
    }

    rows: list[dict[str, object]] = []
    for branch in sucursales_operativas(reference_date=target_date):
        sales_row = sales_map.get(int(branch.id), {})
        indicator_row = indicator_map.get(int(branch.id), {})
        prod_row = prod_map.get(int(branch.id), {})
        point_waste_row = waste_point_map.get(int(branch.id), {})
        visible_waste_row = waste_visible_map.get(int(branch.id), {})
        sales_units = _to_decimal(sales_row.get("sales_units"))
        sales_amount = _to_decimal(sales_row.get("sales_amount"))
        produced = _to_decimal(prod_row.get("produced"))
        sold = _to_decimal(prod_row.get("sold"))
        fact_waste = _to_decimal(prod_row.get("waste"))
        transferred = _to_decimal(prod_row.get("transferred"))
        point_waste = _to_decimal(point_waste_row.get("point_waste"))
        visible_waste = _to_decimal(visible_waste_row.get("visible_waste"))
        net_units = produced + transferred - sold - visible_waste
        if produced > 0 and sales_units > produced + transferred:
            status = "Tensión"
            tone = "danger"
            detail = "La venta del día supera la producción/transferencia visible de la sucursal."
        elif sales_units > 0 or produced > 0 or visible_waste > 0:
            status = "Con actividad"
            tone = "success"
            detail = "Sucursal con señal operativa suficiente para conciliación diaria."
        else:
            status = "Sin actividad"
            tone = "warning"
            detail = "No hay actividad capturada en los dominios auditados para esta sucursal."
        rows.append(
            {
                "branch_code": branch.codigo,
                "branch_name": branch.nombre,
                "sales_units": sales_units,
                "sales_amount": sales_amount,
                "tickets": int(sales_row.get("tickets") or indicator_row.get("tickets") or 0),
                "produced_units": produced,
                "sold_units_fact": sold,
                "waste_units_fact": fact_waste,
                "waste_units_point": point_waste,
                "waste_units_visible": visible_waste,
                "transferred_units": transferred,
                "net_units": net_units,
                "status": status,
                "tone": tone,
                "detail": detail,
            }
        )
    rows.sort(
        key=lambda row: (
            0 if row["tone"] == "danger" else 1 if row["tone"] == "warning" else 2,
            -float(row["sales_amount"]),
            row["branch_code"],
        )
    )
    return rows


def _inventory_discrepancies(target_date: date, limit: int = 12) -> list[dict[str, object]]:
    fact_rows = list(
        FactInventarioDiario.objects.filter(fecha=target_date)
        .select_related("insumo")
        .order_by("insumo__nombre")
    )
    visible_map = {
        int(row.insumo_id): row
        for row in ExistenciaInsumo.objects.filter(insumo_id__in=[fact.insumo_id for fact in fact_rows]).select_related("insumo")
    }
    rows: list[dict[str, object]] = []
    for fact in fact_rows:
        visible = visible_map.get(int(fact.insumo_id))
        visible_stock = _to_decimal(getattr(visible, "stock_actual", ZERO))
        diff = visible_stock - _to_decimal(fact.stock_final)
        if diff == ZERO:
            continue
        rows.append(
            {
                "insumo_name": fact.insumo.nombre,
                "unit_code": getattr(getattr(fact.insumo, "unidad_base", None), "codigo", "") or "s/u",
                "unit_name": getattr(getattr(fact.insumo, "unidad_base", None), "nombre", "") or "Sin unidad base",
                "trace_label": str((getattr(visible, "trazabilidad_stock", {}) or {}).get("label") or "Sin traza suficiente"),
                "trace_effective_at": str((getattr(visible, "trazabilidad_stock", {}) or {}).get("effective_at") or ""),
                "expected_stock": _to_decimal(fact.stock_final),
                "visible_stock": visible_stock,
                "difference": diff,
                "movement_scope": _to_decimal(fact.entradas) + _to_decimal(fact.salidas),
            }
        )
    rows.sort(key=lambda row: abs(_to_decimal(row["difference"])), reverse=True)
    return rows[:limit]


def _build_alerts(*, target_date: date, sales: dict[str, object], production: dict[str, object], waste: dict[str, object], inventory: dict[str, object]) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []

    if sales["raw_rows"] > 0 and sales["fact_rows"] == 0:
        alerts.append(
            _status_row(
                label="Ventas sin publicar",
                tone="danger",
                detail=f"Point ya tiene {sales['raw_rows']} renglones de venta para {target_date.isoformat()}, pero FactVentaDiaria sigue vacío.",
            )
        )
    if production["raw_rows"] > 0 and production["fact_rows"] == 0:
        alerts.append(
            _status_row(
                label="Producción sin publicar",
                tone="danger",
                detail=f"Point ya tiene {production['raw_rows']} renglones de producción para {target_date.isoformat()}, pero FactProduccionDiaria no refleja esa jornada.",
            )
        )
    if waste["raw_rows"] > 0 and waste["visible_units"] <= 0:
        alerts.append(
            _status_row(
                label="Merma sin reflejo visible",
                tone="danger",
                detail=f"Point ya trae {waste['raw_rows']} renglones de merma para {target_date.isoformat()}, pero la capa visible del ERP no muestra unidades de merma publicadas.",
            )
        )
    if inventory["delta_stock"] != ZERO:
        alerts.append(
            _status_row(
                label="Desviación de stock",
                tone="warning",
                detail=(
                    f"El stock visible difiere en {inventory['delta_stock']} contra el stock esperado "
                    f"de los insumos con movimiento en {target_date.isoformat()}."
                ),
            )
        )
    if not inventory["same_moment_comparison"]:
        alerts.append(
            _status_row(
                label="Inventario cruza tiempos",
                tone="warning",
                detail=(
                    "El cierre auditado compara stock esperado del día contra ExistenciaInsumo actualizada después; úsalo como conciliación contra stock vivo, no como saldo histórico puro."
                ),
            )
        )
    if inventory["untraceable_count"] > 0:
        alerts.append(
            _status_row(
                label="Stock visible sin traza suficiente",
                tone="warning",
                detail=(
                    f"{inventory['untraceable_count']} insumos con movimiento del día todavía no tienen origen directo suficientemente trazado para el stock visible."
                ),
            )
        )
    if production["plan_count"] == 0:
        alerts.append(
            _status_row(
                label="Sin plan de producción",
                tone="warning",
                detail=(
                    "La producción Point puede estar publicada, pero la tarjeta visible de planeación sigue sin plan para esta fecha."
                ),
            )
        )
    if not alerts:
        alerts.append(
            _status_row(
                label="Sin alertas críticas",
                tone="success",
                detail="Los dominios auditados muestran publicación y conciliación suficiente para una lectura diaria inicial.",
            )
        )
    return alerts


def _overall_status(*, alerts: list[dict[str, str]]) -> dict[str, str]:
    if any(alert["tone"] == "danger" for alert in alerts):
        return _status_row(
            label="Inconsistente",
            tone="danger",
            detail="Existen dominios con dato capturado en Point que todavía no se refleja correctamente en el ERP visible.",
        )
    if any(alert["tone"] == "warning" for alert in alerts):
        return _status_row(
            label="Con rezago",
            tone="warning",
            detail="La jornada puede leerse, pero todavía presenta desviaciones o fuentes visibles no reconciliadas del todo.",
        )
    return _status_row(
        label="Completo",
        tone="success",
        detail="La jornada auditada ya refleja ventas, producción, mermas y stock comparativo sin alertas críticas abiertas.",
    )


def build_daily_operational_closure(*, target_date: date) -> dict[str, object]:
    sales = _sales_domain(target_date)
    production = _production_domain(target_date)
    waste = _waste_domain(target_date)
    inventory = _inventory_domain(target_date)
    alerts = _build_alerts(
        target_date=target_date,
        sales=sales,
        production=production,
        waste=waste,
        inventory=inventory,
    )
    overall = _overall_status(alerts=alerts)
    branch_rows = _branch_rows(target_date)
    inventory_differences = _inventory_discrepancies(target_date)

    domains = [sales, production, waste, inventory]
    publication_rows = [
        {
            "label": domain["label"],
            "raw_max_date": domain.get("raw_max_date"),
            "fact_max_date": domain.get("fact_max_date"),
            "visible_max_date": domain.get("visible_max_date"),
            "publication": domain["publication"],
        }
        for domain in domains
    ]

    return {
        "target_date": target_date,
        "overall_status": overall,
        "domains": domains,
        "sales": sales,
        "production": production,
        "waste": waste,
        "inventory": inventory,
        "alerts": alerts,
        "publication_rows": publication_rows,
        "branch_rows": branch_rows,
        "inventory_differences": inventory_differences,
        "source_notes": [
            {
                "label": "Ventas",
                "detail": "La lectura visible del cierre usa PointDailySale materializado en FactVentaDiaria y el corte comercial del módulo de ventas.",
            },
            {
                "label": "Producción",
                "detail": "La comparativa diaria usa PointProductionLine y FactProduccionDiaria; las tarjetas de planeación siguen usando PlanProduccion.",
            },
            {
                "label": "Mermas",
                "detail": "La merma Point se audita contra PointWasteLine, mientras la capa visible combina MermaPOS y MovimientoInventario con referencia MERMA|.",
            },
            {
                "label": "Inventario",
                "detail": "El comparativo diario de stock usa FactInventarioDiario como cierre esperado del día auditado y ExistenciaInsumo como stock vivo actual. Las cantidades se muestran en la unidad base configurada en cada insumo.",
            },
        ],
    }
