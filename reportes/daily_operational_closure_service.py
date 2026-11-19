from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.db.models import Sum
from django.utils import timezone

from control.models import MermaPOS
from core.models import AuditLog, sucursales_operativas
from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from inventario.canonical_point_inventory import CanonicalPointInventoryService, InventoryFreshness
from inventario.stock_trace import TRACE_LABELS, TRACE_UNTRACED
from maestros.models import Insumo
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


def _existence_origin_context(existencia: ExistenciaInsumo) -> dict[str, object]:
    """Describe existing provenance without accrediting an opening or balance."""
    trace = dict(existencia.trazabilidad_stock or {})
    source = str(trace.get("source") or "")
    pending = not source or source == TRACE_UNTRACED
    if not pending:
        group = "recorded"
        label = str(trace.get("label") or TRACE_LABELS.get(source) or "Procedencia registrada")
    elif trace.get("fuente") == "CONSUMO_BOM" and trace.get("creado_automaticamente") is True:
        group = "legacy_bom"
        label = "Registro automático por receta (BOM)"
    elif not trace:
        group = "empty_metadata"
        label = "Origen no registrado"
    else:
        group = "other_pending"
        label = str(trace.get("label") or "Procedencia por revisar")
    return {"source": source, "pending": pending, "origin_group": group, "origin_label": label}


def _inventory_domain(target_date: date) -> dict[str, object]:
    fact_qs = FactInventarioDiario.objects.filter(fecha=target_date, sucursal__isnull=True)
    moved_insumo_ids = list(fact_qs.values_list("insumo_id", flat=True))
    visible_qs = ExistenciaInsumo.objects.filter(insumo_id__in=moved_insumo_ids)
    inventory_items = list(Insumo.objects.filter(id__in=moved_insumo_ids))
    inventory_readings = CanonicalPointInventoryService().read_many(
        inventory_items,
        location="ALMACEN",
    )
    unavailable_count = sum(
        reading.freshness is not InventoryFreshness.FRESH
        for reading in inventory_readings.values()
    )
    movement_qs = MovimientoInventario.objects.filter(fecha__date=target_date)

    fact_max = FactInventarioDiario.objects.filter(sucursal__isnull=True).order_by("-fecha").values_list("fecha", flat=True).first()
    movement_max = (
        MovimientoInventario.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    )
    movement_latest_at = movement_qs.order_by("-fecha").values_list("fecha", flat=True).first()
    captured_values = [
        reading.captured_at
        for reading in inventory_readings.values()
        if reading.captured_at and reading.freshness in (InventoryFreshness.FRESH, InventoryFreshness.STALE)
    ]
    visible_updated_max = max(captured_values, default=None)
    visible_updated_min = min(captured_values, default=None)
    fact_updated_max = fact_qs.order_by("-actualizado_en").values_list("actualizado_en", flat=True).first()
    latest_sync_run = AlmacenSyncRun.objects.filter(status=AlmacenSyncRun.STATUS_OK).order_by("-started_at", "-id").first()
    latest_sync_any = AlmacenSyncRun.objects.order_by("-started_at", "-id").first()
    visible_date = timezone.localdate(visible_updated_max) if visible_updated_max else None
    same_operational_date = visible_date == target_date
    if visible_date is None:
        comparison_scope = "Sin fecha de lectura Point"
    elif same_operational_date:
        comparison_scope = "Mismo día operativo"
    else:
        comparison_scope = "Día auditado y captura Point de fechas distintas"
    trace_counts: dict[str, int] = {}
    traceable_count = 0
    untraceable_count = 0
    existence_count = 0
    pending_insumo_ids: set[int] = set()
    pending_origins = {"legacy_bom": 0, "empty_metadata": 0, "other_pending": 0}
    pending_locations: dict[str, dict[str, object]] = {}
    for existencia in visible_qs.only("insumo_id", "almacen", "trazabilidad_stock"):
        origin = _existence_origin_context(existencia)
        existence_count += 1
        label = origin["origin_label"]
        trace_counts[label] = trace_counts.get(label, 0) + 1
        # Preserve the previous source/pending counting rules, including UNTRACED.
        if origin["source"]:
            traceable_count += 1
        if origin["pending"]:
            untraceable_count += 1
            pending_insumo_ids.add(existencia.insumo_id)
            pending_origins[origin["origin_group"]] += 1
            location = pending_locations.setdefault(existencia.almacen, {
                "warehouse_code": existencia.almacen,
                "warehouse_name": existencia.get_almacen_display(),
                "count": 0,
            })
            location["count"] += 1
    trace_sources_text = ", ".join(f"{label}: {total}" for label, total in sorted(trace_counts.items())) or "Sin traza suficiente"

    if unavailable_count:
        publication = _status_row(
            label="Point no disponible",
            tone="warning",
            detail="El último ciclo canónico de ALMACÉN no tiene lecturas vigentes para todos los insumos.",
        )
    elif moved_insumo_ids:
        # The facts producer materializes GLOBAL_ERP from movements, while the
        # Point reader covers one warehouse. Matching dates do not align scopes.
        publication = _status_row(
            label="Referencias de distinto alcance",
            tone="warning",
            detail="El saldo de cierre es global en ERP. La captura de Point abarca ALMACÉN.",
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
        "source_truth": "FactInventarioDiario para movimiento diario; Point ALMACÉN para stock visible actual",
        "visible_source": "Point ALMACÉN muestra stock vivo actual, no un snapshot histórico completo por día",
        "raw_max_date": movement_max.date() if movement_max else None,
        "fact_max_date": fact_max,
        "visible_max_date": visible_date,
        "movement_rows": movement_qs.count(),
        "movement_insumo_count": fact_qs.values("insumo_id").distinct().count(),
        "inventory_unavailable_count": unavailable_count,
        "movement_latest_at": movement_latest_at,
        "visible_updated_max": visible_updated_max,
        "visible_updated_min": visible_updated_min,
        "fact_updated_max": fact_updated_max,
        "same_operational_date": same_operational_date,
        "comparison_scope": comparison_scope,
        "traceable_count": traceable_count,
        "untraceable_count": untraceable_count,
        "trace_sources_text": trace_sources_text,
        "existence_count": existence_count,
        "untraceable_insumo_count": len(pending_insumo_ids),
        "untraceable_legacy_bom_count": pending_origins["legacy_bom"],
        "untraceable_empty_metadata_count": pending_origins["empty_metadata"],
        "untraceable_other_count": pending_origins["other_pending"],
        "untraceable_locations": sorted(pending_locations.values(), key=lambda row: row["warehouse_name"]),
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


def _inventory_references(target_date: date) -> list[dict[str, object]]:
    fact_rows = list(
        FactInventarioDiario.objects.filter(fecha=target_date, sucursal__isnull=True)
        .select_related("insumo__unidad_base")
        .order_by("insumo__nombre")
    )
    insumo_ids = [fact.insumo_id for fact in fact_rows]
    inventory_readings = CanonicalPointInventoryService().read_many(
        [fact.insumo for fact in fact_rows],
        location="ALMACEN",
    )
    visible_map: dict[int, ExistenciaInsumo] = {}
    locations_map: dict[int, list[dict[str, object]]] = {}
    for row in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids).order_by(
        "insumo_id", "-actualizado_en", "-id"
    ).only("insumo_id", "almacen", "trazabilidad_stock", "actualizado_en"):
        visible_map.setdefault(int(row.insumo_id), row)
        origin = _existence_origin_context(row)
        locations_map.setdefault(row.insumo_id, []).append({
            "existence_id": row.pk,
            "warehouse_code": row.almacen,
            "warehouse_name": row.get_almacen_display(),
            "origin_label": origin["origin_label"],
            "pending": origin["pending"],
            "updated_at": row.actualizado_en,
        })
    rows: list[dict[str, object]] = []
    for fact in fact_rows:
        existence = visible_map.get(fact.insumo_id)
        trace = getattr(existence, "trazabilidad_stock", {}) or {}
        reading = inventory_readings[fact.insumo_id]
        ready = reading.freshness is InventoryFreshness.FRESH
        rows.append({
            "insumo_name": fact.insumo.nombre,
            "unit_code": getattr(fact.insumo.unidad_base, "codigo", "") or "s/u",
            "unit_name": getattr(fact.insumo.unidad_base, "nombre", "") or "Sin unidad base",
            "trace_label": str(trace.get("label") or "Sin traza suficiente"),
            "trace_effective_at": str(trace.get("effective_at") or ""),
            "trace_locations": locations_map.get(fact.insumo_id, []),
            "expected_stock": _to_decimal(fact.stock_final),
            "visible_stock": _to_decimal(reading.quantity_base) if ready else None,
            "inventory_ready": ready,
        })
    rows.sort(key=lambda row: row["inventory_ready"])
    return rows


def _build_alerts(*, target_date: date, sales: dict[str, object], production: dict[str, object], waste: dict[str, object], inventory: dict[str, object]) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []

    def add(label: str, tone: str, detail: str, review_area: str) -> None:
        alerts.append({"label": label, "tone": tone, "detail": detail, "review_area": review_area})

    if sales["raw_rows"] > 0 and sales["fact_rows"] == 0:
        add(
            "Ventas sin publicar", "danger",
            f"Point tiene {sales['raw_rows']} renglones de venta del {target_date.isoformat()} pendientes de publicación en el ERP.",
            "Integraciones Point",
        )
    if production["raw_rows"] > 0 and production["fact_rows"] == 0:
        add(
            "Producción sin publicar", "danger",
            f"Point tiene {production['raw_rows']} renglones de producción del {target_date.isoformat()} pendientes de publicación en el ERP.",
            "Integraciones Point",
        )
    if waste["raw_rows"] > 0 and waste["visible_units"] <= 0:
        add(
            "Merma pendiente de publicación", "danger",
            f"Point tiene {waste['raw_rows']} renglones de merma del {target_date.isoformat()} pendientes de reflejarse en el ERP.",
            "Control de mermas e Integraciones Point",
        )
    if inventory.get("inventory_unavailable_count"):
        add(
            "Point ALMACÉN no disponible", "warning",
            "Hace falta una captura vigente y completa de Point ALMACÉN para mostrar las existencias.",
            "Inventarios",
        )
    if inventory["visible_updated_max"] and not inventory["same_operational_date"]:
        add(
            "Fecha de la lectura de inventario", "warning",
            "El saldo global ERP corresponde al día auditado. El stock de Point ALMACÉN se capturó en otra fecha.",
            "Inventarios",
        )
    if inventory["untraceable_count"] > 0:
        add(
            "Registros de existencias ERP sin traza suficiente", "warning",
            f"Hay {inventory['untraceable_count']} registros de insumo por ubicación con apertura y saldo por acreditar. "
            "La procedencia automática registrada no equivale a un conteo físico ni a un faltante de insumos.",
            "Inventarios",
        )
    if production["plan_count"] == 0:
        add(
            "Sin plan de producción", "warning",
            "Revisa si correspondía registrar un plan de producción para la fecha auditada.",
            "Planeación de Producción",
        )
    if not alerts:
        alerts.append(
            _status_row(
                label="Sin alertas detectadas",
                tone="success",
                detail="La revisión automática del día no generó alertas.",
            )
        )
    return alerts


def _overall_status(*, alerts: list[dict[str, str]]) -> dict[str, str]:
    if any(alert["tone"] == "danger" for alert in alerts):
        return _status_row(
            label="Publicación pendiente",
            tone="danger",
            detail="Hay información del día auditado en Point que aún no se refleja en el ERP.",
        )
    if any(alert["tone"] == "warning" for alert in alerts):
        return _status_row(
            label="Requiere revisión",
            tone="warning",
            detail="El día auditado tiene observaciones de inventario o planeación que requieren revisión.",
        )
    return _status_row(
        label="Sin alertas detectadas",
        tone="success",
        detail="La revisión automática del día no generó alertas.",
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
    inventory_references = _inventory_references(target_date)

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
        "inventory_references": inventory_references,
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
                "detail": "FactInventarioDiario resume movimientos globales del ERP. Point ALMACÉN aporta una lectura viva de una ubicación. Ambas referencias conservan la unidad base de cada insumo.",
            },
        ],
    }
