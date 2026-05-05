from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.db import connection, transaction
from django.db.models import Max, Sum
from django.utils import timezone

from core.cache_versions import bump_cache_scopes
from compras.models import OrdenCompra
from inventario.models import MovimientoInventario
from maestros.models import CostoInsumo
from pos_bridge.historical_freeze import assert_not_frozen
from pos_bridge.models import PointDailySale, PointProductionLine, PointSalesDailyProductFact, PointTransferLine, PointWasteLine
from recetas.utils.derived_product_presentations import get_total_cost_map
from reportes.models import (
    AnalyticAuditLog,
    AnalyticRefreshWindow,
    DashboardFullSnapshot,
    FactInventarioDiario,
    FactProduccionDiaria,
    FactVentaDiaria,
    ForecastInput,
    ProductoCostoOperativoMensual,
)
from reportes.dashboard_full_dataset import (
    ALLOWED_MONTH_WINDOWS,
    build_dashboard_full_payload,
    normalize_dashboard_months_window,
    serialize_dashboard_full_payload,
)
from reportes.forecast_calibration_service import rebuild_forecast_calibration_profiles
from ventas.models import VentaAutoritativaPoint


ZERO = Decimal("0")
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
LEGACY_POINT_SOURCE = "/Report/VentasCategorias"
DAILY_OPS_MV_NAME = "mv_dashboard_daily_ops"
DASHBOARD_FULL_MV_NAME = "mv_dashboard_full"


def _bump_sales_dashboard_cache_scopes() -> dict[str, int]:
    # Sales-facing executive datasets compose both scopes, so refreshes must
    # invalidate the sales snapshot and the broader dashboard shell together.
    return bump_cache_scopes("ventas", "dashboard")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _month_start(day: date) -> date:
    return date(day.year, day.month, 1)


def _month_end(day: date) -> date:
    return date(day.year, day.month, monthrange(day.year, day.month)[1])


def _month_sequence(start_date: date, end_date: date) -> list[date]:
    cursor = _month_start(start_date)
    limit = _month_start(end_date)
    months: list[date] = []
    while cursor <= limit:
        months.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


@dataclass(slots=True)
class RefreshSummary:
    sales_rows: int = 0
    inventory_rows: int = 0
    production_rows: int = 0
    forecast_rows: int = 0
    calibration_rows: int = 0


def mark_analytics_dirty(
    *,
    dataset: str,
    date_from: date,
    date_to: date,
    reason: str,
    metadata: dict | None = None,
) -> AnalyticRefreshWindow:
    original_from = date_from
    original_to = date_to
    date_from = min(original_from, original_to)
    date_to = max(original_from, original_to)
    pending_qs = AnalyticRefreshWindow.objects.filter(
        dataset=dataset,
        status__in=[AnalyticRefreshWindow.STATUS_PENDING, AnalyticRefreshWindow.STATUS_ERROR],
        date_from__lte=date_to,
        date_to__gte=date_from,
    ).order_by("date_from", "id")
    existing = pending_qs.first()
    if existing is None:
        return AnalyticRefreshWindow.objects.create(
            dataset=dataset,
            date_from=date_from,
            date_to=date_to,
            reason=reason[:160],
            metadata=metadata or {},
        )

    merged_from = min(existing.date_from, date_from)
    merged_to = max(existing.date_to, date_to)
    merged_reason = " | ".join([value for value in [existing.reason, reason] if value])[:160]
    payload = dict(existing.metadata or {})
    if metadata:
        payload.update(metadata)
    existing.date_from = merged_from
    existing.date_to = merged_to
    existing.reason = merged_reason
    existing.metadata = payload
    existing.status = AnalyticRefreshWindow.STATUS_PENDING
    existing.last_error = ""
    existing.save(update_fields=["date_from", "date_to", "reason", "metadata", "status", "last_error", "updated_at"])
    pending_qs.exclude(pk=existing.pk).delete()
    return existing


def mark_analytics_dirty_for_range(
    *,
    start_date: date,
    end_date: date,
    include_sales: bool = False,
    include_inventory: bool = False,
    include_production: bool = False,
    include_forecast: bool = False,
    reason: str,
) -> None:
    datasets: list[str] = []
    if include_sales:
        datasets.append(AnalyticRefreshWindow.DATASET_SALES)
    if include_inventory:
        datasets.append(AnalyticRefreshWindow.DATASET_INVENTORY)
    if include_production:
        datasets.append(AnalyticRefreshWindow.DATASET_PRODUCTION)
        datasets.append(AnalyticRefreshWindow.DATASET_SNAPSHOT_LEDGER)
        datasets.append(AnalyticRefreshWindow.DATASET_SNAPSHOT_FLOW)
    if include_forecast:
        datasets.append(AnalyticRefreshWindow.DATASET_FORECAST)
    for dataset in datasets:
        mark_analytics_dirty(dataset=dataset, date_from=start_date, date_to=end_date, reason=reason)


def _selected_sales_source_by_branch_day(start_date: date, end_date: date) -> dict[tuple[date, int], str]:
    authoritative_pairs = {
        (sale_date, int(branch_id))
        for sale_date, branch_id in VentaAutoritativaPoint.objects.filter(
            sale_date__range=(start_date, end_date)
        ).values_list("sale_date", "branch_id")
        if branch_id is not None
    }
    v2_pairs = {
        (sale_date, int(branch_id))
        for sale_date, branch_id in PointSalesDailyProductFact.objects.filter(
            sale_date__range=(start_date, end_date),
            branch__erp_branch_id__isnull=False,
        ).values_list("sale_date", "branch__erp_branch_id")
        if branch_id is not None
    }
    legacy_pairs = {
        (sale_date, int(branch_id))
        for sale_date, branch_id in PointDailySale.objects.filter(
            sale_date__range=(start_date, end_date),
            branch__erp_branch_id__isnull=False,
        ).values_list("sale_date", "branch__erp_branch_id")
        if branch_id is not None
    }
    result: dict[tuple[date, int], str] = {}
    for pair in sorted(authoritative_pairs | v2_pairs | legacy_pairs):
        if pair in authoritative_pairs:
            result[pair] = FactVentaDiaria.SOURCE_AUTHORITATIVE
        elif pair in v2_pairs:
            result[pair] = FactVentaDiaria.SOURCE_V2
        else:
            result[pair] = FactVentaDiaria.SOURCE_LEGACY
    return result


def _preferred_legacy_endpoint_by_branch_day(
    *,
    start_date: date,
    end_date: date,
) -> dict[tuple[date, int], str]:
    preferred: dict[tuple[date, int], str] = {}
    pairs = PointDailySale.objects.filter(
        sale_date__range=(start_date, end_date),
        branch__erp_branch_id__isnull=False,
    ).values_list("sale_date", "branch__erp_branch_id", "source_endpoint")
    for sale_date, branch_id, source_endpoint in pairs:
        if branch_id is None:
            continue
        key = (sale_date, int(branch_id))
        endpoint = str(source_endpoint or "")
        current = preferred.get(key)
        if endpoint == OFFICIAL_POINT_SOURCE:
            preferred[key] = OFFICIAL_POINT_SOURCE
        elif current is None and endpoint == LEGACY_POINT_SOURCE:
            preferred[key] = LEGACY_POINT_SOURCE
        elif current is None:
            preferred[key] = endpoint
    return preferred


def _latest_recipe_unit_cost_map(recipe_ids: set[int]) -> dict[int, Decimal]:
    if not recipe_ids:
        return {}
    latest_rows = (
        ProductoCostoOperativoMensual.objects.filter(receta_id__in=recipe_ids)
        .order_by("receta_id", "-periodo")
        .distinct("receta_id")
        .values_list("receta_id", "costo_fabricacion_unit")
    )
    cost_map = {int(recipe_id): _to_decimal(unit_cost) for recipe_id, unit_cost in latest_rows}
    missing_ids = sorted(set(int(recipe_id) for recipe_id in recipe_ids) - set(cost_map))
    if missing_ids:
        fallback_map = get_total_cost_map(missing_ids)
        for recipe_id, fallback_cost in fallback_map.items():
            cost_map[int(recipe_id)] = _to_decimal(fallback_cost)
    return cost_map


def rebuild_sales_facts(*, start_date: date, end_date: date) -> int:
    for target_date in _daterange(start_date, end_date):
        assert_not_frozen(target_date, caller="analytics_service")
    source_by_branch_day = _selected_sales_source_by_branch_day(start_date, end_date)
    if not source_by_branch_day:
        FactVentaDiaria.objects.filter(fecha__range=(start_date, end_date)).delete()
        return 0

    recipe_ids: set[int] = set()
    branch_day_keys_by_source: dict[str, set[tuple[date, int]]] = defaultdict(set)
    for pair, source_kind in source_by_branch_day.items():
        branch_day_keys_by_source[source_kind].add(pair)
    preferred_legacy_endpoint_by_branch_day = _preferred_legacy_endpoint_by_branch_day(
        start_date=start_date,
        end_date=end_date,
    )

    fact_rows: list[FactVentaDiaria] = []

    authoritative_pairs = branch_day_keys_by_source.get(FactVentaDiaria.SOURCE_AUTHORITATIVE, set())
    if authoritative_pairs:
        rows = (
            VentaAutoritativaPoint.objects.filter(sale_date__range=(start_date, end_date))
            .values(
                "sale_date",
                "branch_id",
                "product_id",
                "product_code",
                "point_name",
                "category",
            )
            .annotate(
                cantidad=Sum("quantity"),
                venta_bruta=Sum("gross_amount"),
                descuento=Sum("discount_amount"),
                venta_total=Sum("total_amount"),
                venta_neta=Sum("net_amount"),
            )
        )
        for row in rows:
            branch_id = row.get("branch_id")
            if branch_id is None:
                continue
            key = (row["sale_date"], int(branch_id))
            if key not in authoritative_pairs:
                continue
            recipe_id = row.get("product_id")
            if recipe_id:
                recipe_ids.add(int(recipe_id))
            product_code = str(row.get("product_code") or "").strip() or f"recipe:{recipe_id or 'none'}"
            fact_rows.append(
                FactVentaDiaria(
                    fecha=row["sale_date"],
                    sucursal_id=branch_id,
                    receta_id=recipe_id,
                    producto_clave=product_code[:160],
                    producto_nombre=str(row.get("point_name") or product_code)[:255],
                    categoria=str(row.get("category") or "")[:200],
                    cantidad=_to_decimal(row.get("cantidad")),
                    venta_bruta=_to_decimal(row.get("venta_bruta")),
                    descuento=_to_decimal(row.get("descuento")),
                    venta_total=_to_decimal(row.get("venta_total")),
                    venta_neta=_to_decimal(row.get("venta_neta")),
                    source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
                    metadata={"origin": "ventas_autoritativas_point"},
                )
            )

    v2_pairs = branch_day_keys_by_source.get(FactVentaDiaria.SOURCE_V2, set())
    if v2_pairs:
        rows = (
            PointSalesDailyProductFact.objects.filter(
                sale_date__range=(start_date, end_date),
                branch__erp_branch_id__isnull=False,
            )
            .values(
                "sale_date",
                "branch__erp_branch_id",
                "receta_id",
                "point_product_id",
                "producto_nombre_historico",
                "categoria",
            )
            .annotate(
                cantidad=Sum("total_cantidad"),
                venta_bruta=Sum("total_venta"),
                descuento=Sum("total_descuento"),
                venta_total=Sum("total_venta"),
                venta_neta=Sum("total_venta_neta"),
            )
        )
        for row in rows:
            branch_id = row.get("branch__erp_branch_id")
            if branch_id is None:
                continue
            key = (row["sale_date"], int(branch_id))
            if key not in v2_pairs:
                continue
            recipe_id = row.get("receta_id")
            if recipe_id:
                recipe_ids.add(int(recipe_id))
            point_product_id = row.get("point_product_id")
            product_name = str(row.get("producto_nombre_historico") or "").strip()
            product_key = (
                f"receta:{recipe_id}"
                if recipe_id
                else f"point:{point_product_id}"
                if point_product_id
                else product_name[:150] or "sin-clave"
            )
            fact_rows.append(
                FactVentaDiaria(
                    fecha=row["sale_date"],
                    sucursal_id=branch_id,
                    receta_id=recipe_id,
                    point_product_id=point_product_id,
                    producto_clave=product_key[:160],
                    producto_nombre=product_name[:255],
                    categoria=str(row.get("categoria") or "")[:200],
                    cantidad=_to_decimal(row.get("cantidad")),
                    venta_bruta=_to_decimal(row.get("venta_bruta")),
                    descuento=_to_decimal(row.get("descuento")),
                    venta_total=_to_decimal(row.get("venta_total")),
                    venta_neta=_to_decimal(row.get("venta_neta")),
                    source_kind=FactVentaDiaria.SOURCE_V2,
                    metadata={"origin": "point_sales_daily_product_fact"},
                )
            )

    legacy_pairs = branch_day_keys_by_source.get(FactVentaDiaria.SOURCE_LEGACY, set())
    if legacy_pairs:
        rows = (
            PointDailySale.objects.filter(
                sale_date__range=(start_date, end_date),
                branch__erp_branch_id__isnull=False,
            )
            .values(
                "sale_date",
                "branch__erp_branch_id",
                "receta_id",
                "product_id",
                "product__name",
                "source_endpoint",
            ).annotate(
                cantidad=Sum("quantity"),
                tickets=Sum("tickets"),
                venta_bruta=Sum("gross_amount"),
                descuento=Sum("discount_amount"),
                venta_total=Sum("total_amount"),
                venta_neta=Sum("net_amount"),
            )
        )
        for row in rows:
            branch_id = row.get("branch__erp_branch_id")
            if branch_id is None:
                continue
            key = (row["sale_date"], int(branch_id))
            if key not in legacy_pairs:
                continue
            preferred_endpoint = preferred_legacy_endpoint_by_branch_day.get(key)
            row_endpoint = str(row.get("source_endpoint") or "")
            if preferred_endpoint and row_endpoint != preferred_endpoint:
                continue
            recipe_id = row.get("receta_id")
            if recipe_id:
                recipe_ids.add(int(recipe_id))
            point_product_id = row.get("product_id")
            product_name = str(row.get("product__name") or "").strip()
            product_key = (
                f"receta:{recipe_id}"
                if recipe_id
                else f"point:{point_product_id}"
                if point_product_id
                else product_name[:150] or "sin-clave"
            )
            fact_rows.append(
                FactVentaDiaria(
                    fecha=row["sale_date"],
                    sucursal_id=branch_id,
                    receta_id=recipe_id,
                    point_product_id=point_product_id,
                    producto_clave=product_key[:160],
                    producto_nombre=product_name[:255],
                    cantidad=_to_decimal(row.get("cantidad")),
                    tickets=int(row.get("tickets") or 0),
                    venta_bruta=_to_decimal(row.get("venta_bruta")),
                    descuento=_to_decimal(row.get("descuento")),
                    venta_total=_to_decimal(row.get("venta_total")),
                    venta_neta=_to_decimal(row.get("venta_neta")),
                    source_kind=FactVentaDiaria.SOURCE_LEGACY,
                    metadata={
                        "origin": "point_daily_sale",
                        "source_detail": (
                            "point_daily_sale_official"
                            if row.get("source_endpoint") == OFFICIAL_POINT_SOURCE
                            else "point_daily_sale_legacy"
                            if row.get("source_endpoint") == LEGACY_POINT_SOURCE
                            else "point_daily_sale_unknown"
                        ),
                    },
                )
            )

    cost_map = _latest_recipe_unit_cost_map(recipe_ids)
    for row in fact_rows:
        unit_cost = cost_map.get(int(row.receta_id)) if row.receta_id else None
        if unit_cost is not None and unit_cost > 0:
            estimated_cost = (_to_decimal(row.cantidad) * _to_decimal(unit_cost)).quantize(Decimal("0.01"))
            row.costo_estimado = estimated_cost
            row.margen = (_to_decimal(row.venta_neta) - estimated_cost).quantize(Decimal("0.01"))
        else:
            row.costo_estimado = ZERO
            row.margen = _to_decimal(row.venta_neta).quantize(Decimal("0.01"))

    merged_rows: dict[tuple[date, int | None, str, str], FactVentaDiaria] = {}
    for row in fact_rows:
        key = (row.fecha, row.sucursal_id, row.producto_clave, row.source_kind)
        existing = merged_rows.get(key)
        if existing is None:
            merged_rows[key] = row
            continue
        existing.cantidad = _to_decimal(existing.cantidad) + _to_decimal(row.cantidad)
        existing.tickets = int(existing.tickets or 0) + int(row.tickets or 0)
        existing.venta_bruta = _to_decimal(existing.venta_bruta) + _to_decimal(row.venta_bruta)
        existing.descuento = _to_decimal(existing.descuento) + _to_decimal(row.descuento)
        existing.venta_total = _to_decimal(existing.venta_total) + _to_decimal(row.venta_total)
        existing.venta_neta = _to_decimal(existing.venta_neta) + _to_decimal(row.venta_neta)
        existing.costo_estimado = _to_decimal(existing.costo_estimado) + _to_decimal(row.costo_estimado)
        existing.margen = _to_decimal(existing.margen) + _to_decimal(row.margen)
        if not existing.producto_nombre and row.producto_nombre:
            existing.producto_nombre = row.producto_nombre
        if not existing.categoria and row.categoria:
            existing.categoria = row.categoria
        if existing.receta_id is None and row.receta_id is not None:
            existing.receta_id = row.receta_id
        if existing.point_product_id is None and row.point_product_id is not None:
            existing.point_product_id = row.point_product_id

    with transaction.atomic():
        FactVentaDiaria.objects.filter(fecha__range=(start_date, end_date)).delete()
        if merged_rows:
            FactVentaDiaria.objects.bulk_create(list(merged_rows.values()), batch_size=500)
    return len(merged_rows)


def rebuild_inventory_facts(*, start_date: date, end_date: date) -> int:
    movement_rows = (
        MovimientoInventario.objects.filter(fecha__date__range=(start_date, end_date))
        .values("fecha__date", "insumo_id", "tipo")
        .annotate(total=Sum("cantidad"))
        .order_by("fecha__date", "insumo_id")
    )
    historical_opening_rows = (
        MovimientoInventario.objects.filter(fecha__date__lt=start_date)
        .values("insumo_id", "tipo")
        .annotate(total=Sum("cantidad"))
    )
    opening_by_insumo: dict[int, Decimal] = defaultdict(lambda: ZERO)
    for row in historical_opening_rows:
        insumo_id = int(row["insumo_id"])
        total = _to_decimal(row.get("total"))
        movement_type = str(row.get("tipo") or "")
        if movement_type == MovimientoInventario.TIPO_ENTRADA:
            opening_by_insumo[insumo_id] += total
        elif movement_type in {MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO}:
            opening_by_insumo[insumo_id] -= total
        else:
            opening_by_insumo[insumo_id] += total

    day_maps: dict[tuple[date, int], dict[str, Decimal]] = defaultdict(lambda: {"entradas": ZERO, "salidas": ZERO})
    for row in movement_rows:
        day = row["fecha__date"]
        insumo_id = int(row["insumo_id"])
        total = _to_decimal(row.get("total"))
        movement_type = str(row.get("tipo") or "")
        bucket = day_maps[(day, insumo_id)]
        if movement_type == MovimientoInventario.TIPO_ENTRADA:
            bucket["entradas"] += total
        elif movement_type in {MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO}:
            bucket["salidas"] += total
        else:
            if total >= 0:
                bucket["entradas"] += total
            else:
                bucket["salidas"] += abs(total)

    latest_cost_map = {
        int(insumo_id): _to_decimal(costo)
        for insumo_id, costo in CostoInsumo.objects.order_by("insumo_id", "-fecha", "-id").distinct("insumo_id").values_list(
            "insumo_id",
            "costo_unitario",
        )
    }

    fact_rows: list[FactInventarioDiario] = []
    grouped_keys = defaultdict(list)
    for (day, insumo_id), payload in day_maps.items():
        grouped_keys[insumo_id].append((day, payload))
    for insumo_id, payloads in grouped_keys.items():
        running_stock = opening_by_insumo.get(insumo_id, ZERO)
        for day, payload in sorted(payloads, key=lambda item: item[0]):
            entradas = _to_decimal(payload["entradas"])
            salidas = _to_decimal(payload["salidas"])
            stock_inicial = running_stock
            stock_final = stock_inicial + entradas - salidas
            running_stock = stock_final
            fact_rows.append(
                FactInventarioDiario(
                    fecha=day,
                    insumo_id=insumo_id,
                    stock_inicial=stock_inicial,
                    entradas=entradas,
                    salidas=salidas,
                    stock_final=stock_final,
                    costo=(stock_final * latest_cost_map.get(insumo_id, ZERO)).quantize(Decimal("0.01")),
                    metadata={"scope": "GLOBAL_ERP"},
                )
            )

    with transaction.atomic():
        FactInventarioDiario.objects.filter(fecha__range=(start_date, end_date)).delete()
        if fact_rows:
            FactInventarioDiario.objects.bulk_create(fact_rows, batch_size=500)
    return len(fact_rows)


def rebuild_production_facts(*, start_date: date, end_date: date) -> int:
    produced_rows = (
        PointProductionLine.objects.filter(
            production_date__range=(start_date, end_date),
            is_insumo=False,
            receta_id__isnull=False,
        )
        .values("production_date", "branch__erp_branch_id", "receta_id")
        .annotate(total=Sum("produced_quantity"))
    )
    sold_rows = (
        FactVentaDiaria.objects.filter(
            fecha__range=(start_date, end_date),
            receta_id__isnull=False,
        )
        .values("fecha", "sucursal_id", "receta_id")
        .annotate(total=Sum("cantidad"))
    )
    waste_rows = (
        PointWasteLine.objects.filter(
            movement_at__date__range=(start_date, end_date),
            receta_id__isnull=False,
        )
        .values("movement_at__date", "erp_branch_id", "receta_id")
        .annotate(total=Sum("quantity"))
    )
    transfer_rows = (
        PointTransferLine.objects.filter(
            received_at__date__range=(start_date, end_date),
            receta_id__isnull=False,
            is_insumo=False,
            is_cancelled=False,
        )
        .values("received_at__date", "erp_destination_branch_id", "receta_id")
        .annotate(total=Sum("received_quantity"))
    )

    rows_map: dict[tuple[date, int | None, int | None], dict[str, Decimal]] = defaultdict(
        lambda: {"producido": ZERO, "vendido": ZERO, "merma": ZERO, "transferido": ZERO}
    )
    for row in produced_rows:
        rows_map[(row["production_date"], row.get("branch__erp_branch_id"), row.get("receta_id"))]["producido"] += _to_decimal(row.get("total"))
    for row in sold_rows:
        rows_map[(row["fecha"], row.get("sucursal_id"), row.get("receta_id"))]["vendido"] += _to_decimal(row.get("total"))
    for row in waste_rows:
        rows_map[(row["movement_at__date"], row.get("erp_branch_id"), row.get("receta_id"))]["merma"] += _to_decimal(row.get("total"))
    for row in transfer_rows:
        rows_map[(row["received_at__date"], row.get("erp_destination_branch_id"), row.get("receta_id"))]["transferido"] += _to_decimal(row.get("total"))

    fact_rows = [
        FactProduccionDiaria(
            fecha=day,
            sucursal_id=sucursal_id,
            receta_id=receta_id,
            producido=payload["producido"],
            vendido=payload["vendido"],
            merma=payload["merma"],
            transferido=payload["transferido"],
        )
        for (day, sucursal_id, receta_id), payload in rows_map.items()
    ]

    with transaction.atomic():
        FactProduccionDiaria.objects.filter(fecha__range=(start_date, end_date)).delete()
        if fact_rows:
            FactProduccionDiaria.objects.bulk_create(fact_rows, batch_size=500)
    return len(fact_rows)


def rebuild_forecast_inputs(*, start_date: date, end_date: date) -> int:
    fact_rows = list(
        FactVentaDiaria.objects.filter(
            fecha__range=(start_date - timedelta(days=35), end_date),
            receta_id__isnull=False,
        )
        .values("fecha", "receta_id")
        .annotate(total=Sum("cantidad"))
        .order_by("fecha", "receta_id")
    )
    series_by_recipe: dict[int, list[tuple[date, Decimal]]] = defaultdict(list)
    for row in fact_rows:
        series_by_recipe[int(row["receta_id"])].append((row["fecha"], _to_decimal(row.get("total"))))

    forecast_rows: list[ForecastInput] = []
    for receta_id, values in series_by_recipe.items():
        daily_map = {day: qty for day, qty in values}
        for target_day in _daterange(start_date, end_date):
            current_qty = daily_map.get(target_day, ZERO)
            trailing_7 = [daily_map.get(target_day - timedelta(days=offset), ZERO) for offset in range(1, 8)]
            trailing_28 = [daily_map.get(target_day - timedelta(days=offset), ZERO) for offset in range(1, 29)]
            avg_7 = (sum(trailing_7, ZERO) / Decimal("7")) if trailing_7 else ZERO
            avg_28 = (sum(trailing_28, ZERO) / Decimal("28")) if trailing_28 else ZERO
            variance = ZERO
            for value in trailing_28:
                variance += (value - avg_28) ** 2
            stddev_28 = (variance / Decimal("28")).sqrt() if trailing_28 else ZERO
            seasonality = (current_qty / avg_28) if avg_28 > 0 else ZERO
            growth = ((avg_7 - avg_28) / avg_28) if avg_28 > 0 else ZERO
            forecast_rows.append(
                ForecastInput(
                    fecha=target_day,
                    receta_id=receta_id,
                    ventas_historicas=current_qty,
                    estacionalidad=seasonality,
                    tendencia=avg_7 - avg_28,
                    moving_avg_7=avg_7,
                    moving_avg_28=avg_28,
                    stddev_28=stddev_28,
                    crecimiento_28=growth,
                )
            )

    with transaction.atomic():
        ForecastInput.objects.filter(fecha__range=(start_date, end_date)).delete()
        if forecast_rows:
            ForecastInput.objects.bulk_create(forecast_rows, batch_size=500)
    return len(forecast_rows)


def get_sales_fact_range_summary(*, start_date: date, end_date: date) -> dict[str, object] | None:
    queryset = FactVentaDiaria.objects.filter(fecha__range=(start_date, end_date))
    rows = queryset.count()
    if rows <= 0:
        return None
    totals = queryset.aggregate(
        cantidad=Sum("cantidad"),
        venta_total=Sum("venta_total"),
        venta_neta=Sum("venta_neta"),
        costo_estimado=Sum("costo_estimado"),
        margen=Sum("margen"),
        coverage_days=Max("fecha"),
    )
    return {
        "cantidad": _to_decimal(totals.get("cantidad")),
        "monto": _to_decimal(totals.get("venta_total")),
        "venta_neta": _to_decimal(totals.get("venta_neta")),
        "costo_estimado": _to_decimal(totals.get("costo_estimado")),
        "margen": _to_decimal(totals.get("margen")),
        "rows": rows,
        "coverage_days": queryset.values("fecha").distinct().count(),
        "coverage_branches": queryset.exclude(sucursal_id__isnull=True).values("sucursal_id").distinct().count(),
    }


def audit_sales_fact_consistency(*, start_date: date, end_date: date) -> AnalyticAuditLog:
    source_by_branch_day = _selected_sales_source_by_branch_day(start_date, end_date)
    preferred_legacy_endpoint_by_branch_day = _preferred_legacy_endpoint_by_branch_day(
        start_date=start_date,
        end_date=end_date,
    )
    discrepancies: list[dict[str, object]] = []
    fact_totals = {
        (row["fecha"], int(row["sucursal_id"])): _to_decimal(row.get("total"))
        for row in FactVentaDiaria.objects.filter(
            fecha__range=(start_date, end_date),
            sucursal_id__isnull=False,
        ).values("fecha", "sucursal_id").annotate(total=Sum("venta_total"))
    }
    authoritative_totals = {
        (row["sale_date"], int(row["branch_id"])): _to_decimal(row.get("total"))
        for row in VentaAutoritativaPoint.objects.filter(
            sale_date__range=(start_date, end_date)
        ).values("sale_date", "branch_id").annotate(total=Sum("total_amount"))
        if row.get("branch_id") is not None
    }
    v2_totals = {
        (row["sale_date"], int(row["branch__erp_branch_id"])): _to_decimal(row.get("total"))
        for row in PointSalesDailyProductFact.objects.filter(
            sale_date__range=(start_date, end_date),
            branch__erp_branch_id__isnull=False,
        ).values("sale_date", "branch__erp_branch_id").annotate(total=Sum("total_venta"))
        if row.get("branch__erp_branch_id") is not None
    }
    legacy_totals = {
        (row["sale_date"], int(row["branch__erp_branch_id"])): _to_decimal(row.get("total"))
        for row in PointDailySale.objects.filter(
            sale_date__range=(start_date, end_date),
            branch__erp_branch_id__isnull=False,
        ).values("sale_date", "branch__erp_branch_id", "source_endpoint").annotate(total=Sum("total_amount"))
        if row.get("branch__erp_branch_id") is not None
        and (
            preferred_legacy_endpoint_by_branch_day.get((row["sale_date"], int(row["branch__erp_branch_id"])))
            == str(row.get("source_endpoint") or "")
        )
    }
    all_pairs = sorted(set(source_by_branch_day) | set(fact_totals))
    for target_day, branch_id in all_pairs:
        source_kind = source_by_branch_day.get((target_day, branch_id), "NONE")
        fact_total = fact_totals.get((target_day, branch_id), ZERO)
        if source_kind == FactVentaDiaria.SOURCE_AUTHORITATIVE:
            source_total = authoritative_totals.get((target_day, branch_id), ZERO)
        elif source_kind == FactVentaDiaria.SOURCE_V2:
            source_total = v2_totals.get((target_day, branch_id), ZERO)
        elif source_kind == FactVentaDiaria.SOURCE_LEGACY:
            source_total = legacy_totals.get((target_day, branch_id), ZERO)
        else:
            source_total = ZERO
        if _to_decimal(fact_total).quantize(Decimal("0.01")) != _to_decimal(source_total).quantize(Decimal("0.01")):
            discrepancies.append(
                {
                    "fecha": target_day.isoformat(),
                    "sucursal_id": branch_id,
                    "source_kind": source_kind,
                    "fact_total": str(_to_decimal(fact_total)),
                    "source_total": str(_to_decimal(source_total)),
                }
            )

    status = AnalyticAuditLog.STATUS_OK if not discrepancies else AnalyticAuditLog.STATUS_WARNING
    message = "Consistencia de ventas OK" if not discrepancies else "Se detectaron discrepancias en fact_ventas_diarias"
    return AnalyticAuditLog.objects.create(
        audit_type="FACT_VENTAS_VS_SOURCE",
        status=status,
        date_from=start_date,
        date_to=end_date,
        discrepancy_count=len(discrepancies),
        message=message,
        payload={"discrepancies": discrepancies[:50]},
    )


def refresh_dashboard_daily_ops_materialized_view(*, concurrently: bool = True) -> None:
    statement = (
        f"REFRESH MATERIALIZED VIEW CONCURRENTLY {DAILY_OPS_MV_NAME}"
        if concurrently and not connection.in_atomic_block
        else f"REFRESH MATERIALIZED VIEW {DAILY_OPS_MV_NAME}"
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)
    _bump_sales_dashboard_cache_scopes()


def refresh_dashboard_full_materialized_view(
    *,
    months_windows: tuple[int, ...] = ALLOWED_MONTH_WINDOWS,
    concurrently: bool = True,
) -> int:
    normalized_windows = tuple(
        dict.fromkeys(normalize_dashboard_months_window(value) for value in months_windows)
    ) or ALLOWED_MONTH_WINDOWS
    persisted_rows: list[DashboardFullSnapshot] = []
    generated_at = timezone.now()
    for months_window in normalized_windows:
        payload = build_dashboard_full_payload(months_window=months_window)
        persisted_rows.append(
            DashboardFullSnapshot(
                months_window=months_window,
                payload=serialize_dashboard_full_payload(payload),
                metadata={
                    "months_window": months_window,
                    "latest_cutoff_date": str((payload.get("executive_panels") or {}).get("latest_cutoff_date") or ""),
                },
                generated_at=generated_at,
            )
        )
    DashboardFullSnapshot.objects.bulk_create(
        persisted_rows,
        update_conflicts=True,
        unique_fields=["months_window"],
        update_fields=["payload", "metadata", "generated_at", "updated_at"],
    )
    statement = (
        f"REFRESH MATERIALIZED VIEW CONCURRENTLY {DASHBOARD_FULL_MV_NAME}"
        if concurrently and not connection.in_atomic_block
        else f"REFRESH MATERIALIZED VIEW {DASHBOARD_FULL_MV_NAME}"
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)
    _bump_sales_dashboard_cache_scopes()
    return len(persisted_rows)


def refresh_incremental(*, reference_date: date | None = None, lookback_days: int = 3) -> RefreshSummary:
    reference_date = reference_date or timezone.localdate()
    start_date = reference_date - timedelta(days=max(lookback_days, 1))
    summary = RefreshSummary()
    summary.sales_rows = rebuild_sales_facts(start_date=start_date, end_date=reference_date)
    summary.inventory_rows = rebuild_inventory_facts(start_date=start_date, end_date=reference_date)
    summary.production_rows = rebuild_production_facts(start_date=start_date, end_date=reference_date)
    summary.forecast_rows = rebuild_forecast_inputs(start_date=start_date, end_date=reference_date)
    summary.calibration_rows = int(
        rebuild_forecast_calibration_profiles(reference_date=reference_date).get("segments") or 0
    )
    audit_sales_fact_consistency(start_date=start_date, end_date=reference_date)
    refresh_dashboard_daily_ops_materialized_view()
    refresh_dashboard_full_materialized_view()
    AnalyticRefreshWindow.objects.filter(
        status__in=[AnalyticRefreshWindow.STATUS_PENDING, AnalyticRefreshWindow.STATUS_ERROR],
        date_from__lte=reference_date,
        date_to__gte=start_date,
    ).update(status=AnalyticRefreshWindow.STATUS_DONE, last_error="")
    return summary


def full_rebuild(*, start_date: date | None = None, end_date: date | None = None) -> RefreshSummary:
    today = timezone.localdate()
    sales_min = VentaAutoritativaPoint.objects.values_list("sale_date", flat=True).order_by("sale_date").first()
    legacy_min = PointDailySale.objects.values_list("sale_date", flat=True).order_by("sale_date").first()
    v2_min = PointSalesDailyProductFact.objects.values_list("sale_date", flat=True).order_by("sale_date").first()
    movement_min = MovimientoInventario.objects.values_list("fecha__date", flat=True).order_by("fecha__date").first()
    production_min = PointProductionLine.objects.values_list("production_date", flat=True).order_by("production_date").first()
    candidates = [value for value in [sales_min, legacy_min, v2_min, movement_min, production_min] if value]
    start_date = start_date or (min(candidates) if candidates else today)
    end_date = end_date or today
    summary = RefreshSummary()
    summary.sales_rows = rebuild_sales_facts(start_date=start_date, end_date=end_date)
    summary.inventory_rows = rebuild_inventory_facts(start_date=start_date, end_date=end_date)
    summary.production_rows = rebuild_production_facts(start_date=start_date, end_date=end_date)
    summary.forecast_rows = rebuild_forecast_inputs(start_date=start_date, end_date=end_date)
    summary.calibration_rows = int(
        rebuild_forecast_calibration_profiles(reference_date=end_date).get("segments") or 0
    )
    audit_sales_fact_consistency(start_date=start_date, end_date=end_date)
    refresh_dashboard_daily_ops_materialized_view()
    refresh_dashboard_full_materialized_view()
    AnalyticRefreshWindow.objects.filter(
        status__in=[AnalyticRefreshWindow.STATUS_PENDING, AnalyticRefreshWindow.STATUS_ERROR],
        date_from__lte=end_date,
        date_to__gte=start_date,
    ).update(status=AnalyticRefreshWindow.STATUS_DONE, last_error="")
    return summary
