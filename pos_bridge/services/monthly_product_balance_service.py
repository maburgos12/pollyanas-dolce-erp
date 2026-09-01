from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from django.apps import apps
from django.conf import settings
from django.db import connection
from django.utils import timezone

from pos_bridge.models import (
    PointConversionLine,
    PointDailySale,
    PointInventorySnapshot,
    PointProductionLine,
    PointWasteLine,
)
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta, RecetaEquivalencia, VentaHistorica

ZERO = Decimal("0")
DIFFERENCE_TOLERANCE = Decimal("0.01")

ORIGIN_POINT = "POINT"
ORIGIN_CONFIGURED_EQUIVALENCE = "EQUIVALENCIA_CONFIGURADA"
ORIGIN_UNRESOLVED = "UNRESOLVED"
ORIGIN_MIXED = "MIXED"
ISSUE_CONVERSION_ORIGIN_UNRESOLVED = "CONVERSION_ORIGIN_UNRESOLVED"
ISSUE_POINT_SOURCE_UNRESOLVED = "POINT_CONVERSION_SOURCE_UNRESOLVED"
ISSUE_SOURCE_FACTOR_MISMATCH = "CONVERSION_SOURCE_FACTOR_MISMATCH"
ISSUE_FACTOR_MISSING = "CONVERSION_FACTOR_MISSING"
ISSUE_FACTOR_INVALID = "CONVERSION_FACTOR_INVALID"
ISSUE_DESTINATION_UNRESOLVED = "CONVERSION_DESTINATION_UNRESOLVED"
ISSUE_SNAPSHOT_UNRESOLVED = "SNAPSHOT_RECIPE_UNRESOLVED"
ISSUE_SALE_UNRESOLVED = "OFFICIAL_SALE_RECIPE_UNRESOLVED"
ISSUE_OPENING_MISSING = "OPENING_SNAPSHOT_MISSING"
ISSUE_CLOSING_MISSING = "CLOSING_SNAPSHOT_MISSING"

OFFICIAL_CATEGORY_REPORT_SOURCE = "POINT_OFFICIAL_MONTHLY_CATEGORY_REPORT"
OFFICIAL_POINT_DAILY_SOURCE = "/Report/PrintReportes?idreporte=3"
POINT_BRIDGE_SALES_SOURCE = "POINT_BRIDGE_SALES"


def _empty_counts() -> Mapping[str, int]:
    return MappingProxyType({})


def _empty_mapping() -> Mapping[str, object]:
    return MappingProxyType({})


def _freeze_mapping(value: Mapping) -> Mapping:
    return MappingProxyType(
        {
            key: _freeze_mapping(item) if isinstance(item, Mapping) else tuple(item) if isinstance(item, list) else item
            for key, item in value.items()
        }
    )


@dataclass(frozen=True, slots=True)
class MonthlyPointBalanceRow:
    receta_id: int
    opening_point: Decimal | None = None
    production: Decimal = ZERO
    sales: Decimal = ZERO
    waste: Decimal = ZERO
    conversion_in: Decimal = ZERO
    conversion_out: Decimal = ZERO
    calculated_closing: Decimal | None = None
    closing_point: Decimal | None = None
    difference_point: Decimal | None = None
    status: str = "REVISAR_FUENTE"
    conversion_origin: str = ""
    issues: tuple[str, ...] = ()
    source_counts: Mapping[str, int] = field(default_factory=_empty_counts)


def _empty_rows() -> Mapping[int, MonthlyPointBalanceRow]:
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class MonthlyPointUnresolvedMovement:
    source: str
    movement_id: str
    item_code: str
    item_name: str
    quantity: Decimal
    issue: str
    source_hash: str = ""


@dataclass(frozen=True, slots=True)
class MonthlyPointUnresolvedConversion:
    movement_external_id: str
    source_hash: str
    item_code: str
    item_name: str
    quantity: Decimal
    issue: str = ISSUE_DESTINATION_UNRESOLVED


@dataclass(frozen=True, slots=True)
class MonthlyPointBalance:
    month_start: date
    month_end: date
    rows: Mapping[int, MonthlyPointBalanceRow] = field(default_factory=_empty_rows)
    unresolved_movements: tuple[MonthlyPointUnresolvedMovement, ...] = ()
    unresolved_conversions: tuple[MonthlyPointUnresolvedConversion, ...] = ()
    issues: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    sources: Mapping[str, object] = field(default_factory=_empty_mapping)
    effective_snapshot_dates: Mapping[str, date | None] = field(default_factory=_empty_mapping)
    source_counts: Mapping[str, int] = field(default_factory=_empty_counts)


@dataclass(slots=True)
class _MutableBalanceRow:
    opening_point: Decimal | None = None
    production: Decimal = ZERO
    sales: Decimal = ZERO
    waste: Decimal = ZERO
    conversion_in: Decimal = ZERO
    conversion_out: Decimal = ZERO
    closing_point: Decimal | None = None
    origins: set[str] = field(default_factory=set)
    issues: set[str] = field(default_factory=set)
    counts: dict[str, int] = field(default_factory=dict)

    def add(self, field_name: str, quantity: Decimal, *, count_name: str) -> None:
        setattr(self, field_name, getattr(self, field_name) + quantity)
        self.counts[count_name] = self.counts.get(count_name, 0) + 1

    def record_origin(self, origin: str) -> None:
        self.origins.add(origin)

    def freeze(self, receta_id: int) -> MonthlyPointBalanceRow:
        if len(self.origins) == 1:
            conversion_origin = next(iter(self.origins))
        elif self.origins:
            conversion_origin = ORIGIN_MIXED
        else:
            conversion_origin = ""

        calculated_closing = None
        difference_point = None
        status = "REVISAR_FUENTE"
        issues = set(self.issues)
        if self.opening_point is None:
            issues.add(ISSUE_OPENING_MISSING)
        else:
            calculated_closing = (
                self.opening_point
                + self.production
                + self.conversion_in
                - self.sales
                - self.waste
                - self.conversion_out
            )
        if self.closing_point is None:
            issues.add(ISSUE_CLOSING_MISSING)
        if calculated_closing is not None and self.closing_point is not None:
            difference_point = self.closing_point - calculated_closing
            if abs(difference_point) <= DIFFERENCE_TOLERANCE:
                status = "COINCIDE"
            elif difference_point > ZERO:
                status = "POINT_MAYOR"
            else:
                status = "POINT_MENOR"

        default_counts = {
            "opening_snapshot_rows": 0,
            "production_rows": 0,
            "sales_rows": 0,
            "waste_rows": 0,
            "conversion_in_rows": 0,
            "conversion_out_rows": 0,
            "closing_snapshot_rows": 0,
        }
        default_counts.update(self.counts)
        return MonthlyPointBalanceRow(
            receta_id=receta_id,
            opening_point=self.opening_point,
            production=self.production,
            sales=self.sales,
            waste=self.waste,
            conversion_in=self.conversion_in,
            conversion_out=self.conversion_out,
            calculated_closing=calculated_closing,
            closing_point=self.closing_point,
            difference_point=difference_point,
            status=status,
            conversion_origin=conversion_origin,
            issues=tuple(sorted(issues)),
            source_counts=MappingProxyType(default_counts),
        )


class MonthlyPointProductBalanceService:
    """Build an exact-recipe monthly Point ledger without parent-equivalence collapsing."""

    DEFAULT_SNAPSHOT_TOLERANCE_DAYS = 3

    def __init__(
        self,
        identity_service: PointRecipeIdentityService | None = None,
        matcher: PointSalesMatchingService | None = None,
        official_sales_report_service: PointSalesCategoryReportService | None = None,
    ):
        self.identity_service = identity_service or PointRecipeIdentityService()
        self.matcher = matcher or PointSalesMatchingService()
        self.official_sales_report_service = official_sales_report_service or PointSalesCategoryReportService()

    def build(self, month: str | date) -> MonthlyPointBalance:
        month_start = self._parse_month(month)
        month_end = date(month_start.year, month_start.month, monthrange(month_start.year, month_start.month)[1])
        opening_target = month_start - timedelta(days=1)

        opening, opening_meta, opening_unresolved = self._load_snapshot(snapshot_date=opening_target, source="opening_snapshot")
        closing, closing_meta, closing_unresolved = self._load_snapshot(snapshot_date=month_end, source="closing_snapshot")
        # Preserve the selected day for diagnostics, but do not make a snapshot
        # outside the closure tolerance authoritative in the ledger formula.
        if not opening_meta.get("within_tolerance"):
            opening = {}
        if not closing_meta.get("within_tolerance"):
            closing = {}
        production, production_meta = self._load_production(month_start=month_start, month_end=month_end)
        sales, sales_meta, sales_unresolved = self._load_sales(month_start=month_start, month_end=month_end)
        waste, waste_meta = self._load_waste(month_start=month_start, month_end=month_end)
        conversion_rows, unresolved_conversions, conversion_unresolved, conversion_counts = self._load_conversions(
            month_start=month_start
        )

        receta_ids = set(opening) | set(closing) | set(production) | set(sales) | set(waste) | set(conversion_rows)
        rows: dict[int, _MutableBalanceRow] = {
            receta_id: conversion_rows.get(receta_id, _MutableBalanceRow()) for receta_id in receta_ids
        }
        self._merge_quantities(rows, opening, field_name="opening_point", count_name="opening_snapshot_rows", optional=True)
        self._merge_quantities(rows, production, field_name="production", count_name="production_rows")
        self._merge_quantities(rows, sales, field_name="sales", count_name="sales_rows")
        self._merge_quantities(rows, waste, field_name="waste", count_name="waste_rows")
        self._merge_quantities(rows, closing, field_name="closing_point", count_name="closing_snapshot_rows", optional=True)

        frozen_rows = MappingProxyType({receta_id: rows[receta_id].freeze(receta_id) for receta_id in sorted(rows)})
        unresolved_movements = tuple(opening_unresolved + closing_unresolved + sales_unresolved + conversion_unresolved)
        warnings = self._snapshot_warnings(opening_meta, label="inventario inicial")
        warnings.extend(self._snapshot_warnings(closing_meta, label="inventario final"))
        missing_opening_rows = sum(row.opening_point is None for row in frozen_rows.values())
        missing_closing_rows = sum(row.closing_point is None for row in frozen_rows.values())
        if missing_opening_rows and opening_meta.get("within_tolerance"):
            warnings.append(
                f"{missing_opening_rows} receta(s) no tienen inventario inicial en el snapshot seleccionado."
            )
        if missing_closing_rows and closing_meta.get("within_tolerance"):
            warnings.append(
                f"{missing_closing_rows} receta(s) no tienen inventario final en el snapshot seleccionado."
            )
        warnings.extend(sales_meta.get("warnings") or [])
        issues = tuple(
            sorted(
                {issue for row in frozen_rows.values() for issue in row.issues}
                | {movement.issue for movement in unresolved_movements}
                | {conversion.issue for conversion in unresolved_conversions}
            )
        )
        sources = _freeze_mapping(
            {
                "opening_snapshot": opening_meta,
                "closing_snapshot": closing_meta,
                "production": production_meta,
                "sales": sales_meta,
                "waste": waste_meta,
                "conversions": {"source": "PointConversionLine"},
            }
        )
        source_counts = {
            "opening_snapshot_rows": int(opening_meta.get("snapshot_rows") or 0),
            "opening_snapshot_unresolved": len(opening_unresolved),
            "closing_snapshot_rows": int(closing_meta.get("snapshot_rows") or 0),
            "closing_snapshot_unresolved": len(closing_unresolved),
            "production_rows": sum(value[1] for value in production.values()),
            "sales_rows": sum(value[1] for value in sales.values()),
            "official_sales_unresolved": len(sales_unresolved),
            "waste_rows": sum(value[1] for value in waste.values()),
            **conversion_counts,
        }
        return MonthlyPointBalance(
            month_start=month_start,
            month_end=month_end,
            rows=frozen_rows,
            unresolved_movements=unresolved_movements,
            unresolved_conversions=tuple(unresolved_conversions),
            issues=issues,
            warnings=tuple(dict.fromkeys(warnings)),
            sources=sources,
            effective_snapshot_dates=MappingProxyType(
                {"opening": opening_meta.get("effective_date"), "closing": closing_meta.get("effective_date")}
            ),
            source_counts=MappingProxyType(source_counts),
        )

    @staticmethod
    def _merge_quantities(rows, values, *, field_name: str, count_name: str, optional: bool = False) -> None:
        for receta_id, (quantity, count) in values.items():
            row = rows.setdefault(receta_id, _MutableBalanceRow())
            if optional:
                setattr(row, field_name, quantity)
                row.counts[count_name] = count
            else:
                setattr(row, field_name, getattr(row, field_name) + quantity)
                row.counts[count_name] = row.counts.get(count_name, 0) + count

    def _load_snapshot(self, *, snapshot_date: date, source: str):
        tolerance_days = int(
            getattr(settings, "PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS", self.DEFAULT_SNAPSHOT_TOLERANCE_DAYS)
        )
        current_timezone = timezone.get_current_timezone()
        target_start = timezone.make_aware(datetime.combine(snapshot_date, time.min), current_timezone)
        target_end = timezone.make_aware(datetime.combine(snapshot_date + timedelta(days=1), time.min), current_timezone)
        before_at = (
            PointInventorySnapshot.objects.filter(captured_at__lt=target_end)
            .order_by("-captured_at", "-id")
            .values_list("captured_at", flat=True)
            .first()
        )
        after_at = (
            PointInventorySnapshot.objects.filter(captured_at__gte=target_start)
            .order_by("captured_at", "id")
            .values_list("captured_at", flat=True)
            .first()
        )
        candidates = [candidate for candidate in (before_at, after_at) if candidate is not None]
        if not candidates:
            return {}, self._empty_snapshot_meta(snapshot_date, tolerance_days), []

        selected_at = min(candidates, key=lambda value: abs(value - target_start))
        effective_date = timezone.localtime(selected_at, current_timezone).date()
        day_start = timezone.make_aware(datetime.combine(effective_date, time.min), current_timezone)
        day_end = timezone.make_aware(datetime.combine(effective_date + timedelta(days=1), time.min), current_timezone)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (branch_id, product_id) id
                FROM pos_bridge_inventory_snapshots
                WHERE captured_at >= %s AND captured_at < %s
                ORDER BY branch_id, product_id, captured_at DESC, id DESC
                """,
                [day_start, day_end],
            )
            snapshot_ids = [row[0] for row in cursor.fetchall()]

        snapshots = list(
            PointInventorySnapshot.objects.filter(id__in=snapshot_ids)
            .select_related("product")
            .only("id", "product_id", "product__sku", "product__name", "stock")
            .order_by("id")
        )
        values: dict[int, tuple[Decimal, int]] = {}
        unresolved: list[MonthlyPointUnresolvedMovement] = []
        identity_cache: dict[int, Receta | None] = {}
        for snapshot in snapshots:
            if snapshot.product_id not in identity_cache:
                identity_cache[snapshot.product_id] = self.matcher.resolve_receta(
                    codigo_point=snapshot.product.sku,
                    point_name=snapshot.product.name,
                )
            receta = identity_cache[snapshot.product_id]
            quantity = Decimal(snapshot.stock)
            if receta is None:
                unresolved.append(
                    MonthlyPointUnresolvedMovement(
                        source=source,
                        movement_id=str(snapshot.id),
                        item_code=snapshot.product.sku,
                        item_name=snapshot.product.name,
                        quantity=quantity,
                        issue=ISSUE_SNAPSHOT_UNRESOLVED,
                    )
                )
                continue
            current, count = values.get(receta.id, (ZERO, 0))
            values[receta.id] = (current + quantity, count + 1)

        days_from_target = abs((effective_date - snapshot_date).days)
        return values, {
            "source": "PointInventorySnapshot",
            "target_date": snapshot_date,
            "effective_date": effective_date,
            "tolerance_days": tolerance_days,
            "fallback_used": effective_date != snapshot_date,
            "within_tolerance": days_from_target <= tolerance_days,
            "days_from_target": days_from_target,
            "snapshot_rows": len(snapshots),
            "matched_recipe_count": len(values),
            "unresolved_rows": len(unresolved),
        }, unresolved

    @staticmethod
    def _empty_snapshot_meta(snapshot_date: date, tolerance_days: int) -> dict[str, object]:
        return {
            "source": "PointInventorySnapshot",
            "target_date": snapshot_date,
            "effective_date": None,
            "tolerance_days": tolerance_days,
            "fallback_used": False,
            "within_tolerance": False,
            "days_from_target": None,
            "snapshot_rows": 0,
            "matched_recipe_count": 0,
            "unresolved_rows": 0,
        }

    @staticmethod
    def _snapshot_warnings(meta: Mapping[str, object], *, label: str) -> list[str]:
        if not meta.get("effective_date"):
            return [f"No existe snapshot Point para {label}; el balance no es autoritativo."]
        if meta.get("fallback_used"):
            warning = f"Se uso fecha alternativa {meta['effective_date']} para {label} (objetivo {meta['target_date']})."
            if not meta.get("within_tolerance"):
                warning += " La fecha queda fuera de la tolerancia configurada."
            return [warning]
        return []

    def _load_production(self, *, month_start: date, month_end: date):
        facts, facts_present = self._load_fact_values(
            month_start=month_start,
            month_end=month_end,
            field_name="producido",
        )
        if facts_present:
            return facts, {"source": "FactProduccionDiaria", "priority": "primary"}
        rows = PointProductionLine.objects.filter(
            production_date__gte=month_start,
            production_date__lte=month_end,
            receta_id__isnull=False,
            is_insumo=False,
        ).only("id", "receta_id", "produced_quantity").order_by("id")
        return self._aggregate_rows(rows, "produced_quantity"), {
            "source": "PointProductionLine",
            "priority": "fallback",
        }

    def _load_waste(self, *, month_start: date, month_end: date):
        facts, facts_present = self._load_fact_values(
            month_start=month_start,
            month_end=month_end,
            field_name="merma",
        )
        if facts_present:
            return facts, {"source": "FactProduccionDiaria", "priority": "primary"}
        lower_bound, upper_bound = self._month_datetime_bounds(month_start)
        point_rows = list(
            PointWasteLine.objects.filter(
                movement_at__gte=lower_bound,
                movement_at__lt=upper_bound,
                receta_id__isnull=False,
            ).only("id", "receta_id", "quantity").order_by("id")
        )
        if point_rows:
            return self._aggregate_rows(point_rows, "quantity"), {
                "source": "PointWasteLine",
                "priority": "fallback",
            }

        monthly_waste_model = apps.get_model("control", "MermaMensualSucursal")
        monthly_rows = monthly_waste_model.objects.filter(
            periodo=month_start,
            receta_id__isnull=False,
        ).only("id", "receta_id", "unidades_merma").order_by("id")
        return self._aggregate_rows(monthly_rows, "unidades_merma"), {
            "source": "MermaMensualSucursal",
            "priority": "monthly_fallback",
        }

    def _load_fact_values(self, *, month_start: date, month_end: date, field_name: str):
        fact_model = apps.get_model("reportes", "FactProduccionDiaria")
        rows = list(
            fact_model.objects.filter(
                fecha__gte=month_start,
                fecha__lte=month_end,
                receta_id__isnull=False,
            )
            .only("id", "receta_id", field_name)
            .order_by("id")
        )
        return self._aggregate_rows(rows, field_name), bool(rows)

    @staticmethod
    def _aggregate_rows(rows, field_name: str):
        values: dict[int, tuple[Decimal, int]] = {}
        for row in rows:
            quantity = Decimal(getattr(row, field_name) or ZERO)
            current, count = values.get(row.receta_id, (ZERO, 0))
            values[row.receta_id] = (current + quantity, count + 1)
        return values

    def _load_sales(self, *, month_start: date, month_end: date):
        source_mode = str(getattr(settings, "PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE", "AUTO")).strip().upper() or "AUTO"
        prefer_official = source_mode in {"AUTO", "OFFICIAL_MONTHLY_REPORT"}
        official_error: Exception | None = None
        if prefer_official:
            try:
                return self._load_official_sales(month_start=month_start, month_end=month_end)
            except Exception as exc:  # noqa: BLE001
                if source_mode == "OFFICIAL_MONTHLY_REPORT":
                    raise
                official_error = exc

            daily = self._load_daily_sales(month_start=month_start, month_end=month_end)
            if daily:
                return daily, {
                    "source": OFFICIAL_POINT_DAILY_SOURCE,
                    "mode": "official_point_daily_sales",
                    "warnings": ["No se pudo usar el reporte oficial mensual; se uso PointDailySale oficial."],
                    "fallback_reason": str(official_error),
                }, []

        facts, facts_present = self._load_fact_values(
            month_start=month_start,
            month_end=month_end,
            field_name="vendido",
        )
        if facts_present:
            return facts, {"source": "FactProduccionDiaria", "mode": "production_facts"}, []

        rows = VentaHistorica.objects.filter(
            fecha__gte=month_start,
            fecha__lte=month_end,
            fuente=POINT_BRIDGE_SALES_SOURCE,
            receta_id__isnull=False,
        ).only("id", "receta_id", "cantidad").order_by("id")
        values = self._aggregate_rows(rows, "cantidad")
        meta = {"source": POINT_BRIDGE_SALES_SOURCE, "mode": "bridge_history"}
        if official_error is not None:
            meta.update(
                {
                    "warnings": ["No se pudo usar el reporte oficial mensual; se uso VentaHistorica."],
                    "fallback_reason": str(official_error),
                }
            )
        return values, meta, []

    def _load_official_sales(self, *, month_start: date, month_end: date):
        report = self.official_sales_report_service.fetch_report(
            start_date=month_start,
            end_date=month_end,
            branch_external_id=None,
            branch_display_name=None,
            credito=None,
        )
        parsed = self.official_sales_report_service.parse_report(report_path=report.report_path)
        values: dict[int, tuple[Decimal, int]] = {}
        unresolved: list[MonthlyPointUnresolvedMovement] = []
        identity_cache: dict[tuple[str, str], Receta | None] = {}
        for index, source_row in enumerate(parsed.rows, start=1):
            code = str(source_row.get("Codigo") or "").strip()
            name = str(source_row.get("Nombre") or "").strip()
            quantity = Decimal(str(source_row.get("Cantidad") or ZERO))
            key = (code.casefold(), name.casefold())
            if key not in identity_cache:
                identity_cache[key] = self.matcher.resolve_receta(codigo_point=code, point_name=name)
            receta = identity_cache[key]
            if receta is None:
                unresolved.append(
                    MonthlyPointUnresolvedMovement(
                        source="official_sales",
                        movement_id=f"official-row-{index}",
                        item_code=code,
                        item_name=name,
                        quantity=quantity,
                        issue=ISSUE_SALE_UNRESOLVED,
                    )
                )
                continue
            current, count = values.get(receta.id, (ZERO, 0))
            values[receta.id] = (current + quantity, count + 1)
        return values, {
            "source": OFFICIAL_CATEGORY_REPORT_SOURCE,
            "mode": "official_monthly_report",
            "report_path": report.report_path,
            "request_url": report.request_url,
            "row_count": len(parsed.rows),
            "unresolved_rows": len(unresolved),
            "summary": {key: str(value) for key, value in parsed.summary.items()},
        }, unresolved

    def _load_daily_sales(self, *, month_start: date, month_end: date):
        rows = PointDailySale.objects.filter(
            sale_date__gte=month_start,
            sale_date__lte=month_end,
            receta_id__isnull=False,
            source_endpoint=OFFICIAL_POINT_DAILY_SOURCE,
        ).only("id", "receta_id", "quantity").order_by("id")
        return self._aggregate_rows(rows, "quantity")

    def _load_conversions(self, *, month_start: date):
        lower_bound, upper_bound = self._month_datetime_bounds(month_start)
        conversions = list(
            PointConversionLine.objects.filter(movement_at__gte=lower_bound, movement_at__lt=upper_bound)
            .only(
                "id",
                "receta_id",
                "movement_external_id",
                "source_hash",
                "movement_at",
                "item_code",
                "item_name",
                "quantity",
                "source_item_code",
                "source_item_name",
            )
            .order_by("movement_at", "id")
        )
        destination_ids = {row.receta_id for row in conversions if row.receta_id is not None}
        equivalences = {
            equivalence.receta_porcion_id: equivalence
            for equivalence in RecetaEquivalencia.objects.filter(
                receta_porcion_id__in=destination_ids,
                tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
                activo=True,
            ).only("receta_porcion_id", "receta_padre_id", "factor_conversion")
        }

        result: dict[int, _MutableBalanceRow] = {}
        unresolved_conversions: list[MonthlyPointUnresolvedConversion] = []
        unresolved_movements: list[MonthlyPointUnresolvedMovement] = []
        source_counts = {"conversion_rows_read": len(conversions), "conversion_destination_rows_applied": 0}
        identity_cache: dict[tuple[str, str], Receta | None] = {}
        for conversion in conversions:
            quantity = Decimal(conversion.quantity)
            if conversion.receta_id is None:
                unresolved_conversions.append(
                    MonthlyPointUnresolvedConversion(
                        movement_external_id=conversion.movement_external_id,
                        source_hash=conversion.source_hash,
                        item_code=conversion.item_code,
                        item_name=conversion.item_name,
                        quantity=quantity,
                    )
                )
                unresolved_movements.append(
                    MonthlyPointUnresolvedMovement(
                        source="conversion_destination",
                        movement_id=conversion.movement_external_id,
                        source_hash=conversion.source_hash,
                        item_code=conversion.item_code,
                        item_name=conversion.item_name,
                        quantity=quantity,
                        issue=ISSUE_DESTINATION_UNRESOLVED,
                    )
                )
                continue
            if quantity == ZERO:
                continue

            source_counts["conversion_destination_rows_applied"] += 1
            destination = result.setdefault(conversion.receta_id, _MutableBalanceRow())
            destination.add("conversion_in", quantity, count_name="conversion_in_rows")
            source_recipe_id, factor, origin, issue = self._resolve_source(
                conversion,
                equivalences.get(conversion.receta_id),
                identity_cache,
            )
            destination.record_origin(origin)
            if issue:
                destination.issues.add(issue)
                unresolved_movements.append(
                    MonthlyPointUnresolvedMovement(
                        source="conversion_source",
                        movement_id=conversion.movement_external_id,
                        source_hash=conversion.source_hash,
                        item_code=conversion.source_item_code,
                        item_name=conversion.source_item_name,
                        quantity=quantity,
                        issue=issue,
                    )
                )
            if source_recipe_id is None or factor is None:
                continue
            source = result.setdefault(source_recipe_id, _MutableBalanceRow())
            source.add("conversion_out", quantity / factor, count_name="conversion_out_rows")
            source.record_origin(origin)
        return result, unresolved_conversions, unresolved_movements, source_counts

    def _resolve_source(self, conversion, equivalence, identity_cache):
        point_code = (conversion.source_item_code or "").strip()
        point_name = (conversion.source_item_name or "").strip()
        if point_code or point_name:
            identity_key = (point_code.casefold(), point_name.casefold())
            if identity_key not in identity_cache:
                identity_cache[identity_key] = self.identity_service.resolve_recipe(
                    point_code=point_code,
                    point_name=point_name,
                )
            point_source = identity_cache[identity_key]
            if point_source is None:
                return None, None, ORIGIN_POINT, ISSUE_POINT_SOURCE_UNRESOLVED
            if equivalence is None:
                return point_source.id, None, ORIGIN_POINT, ISSUE_FACTOR_MISSING
            if equivalence.receta_padre_id != point_source.id:
                return point_source.id, None, ORIGIN_POINT, ISSUE_SOURCE_FACTOR_MISMATCH
            factor = Decimal(equivalence.factor_conversion)
            if factor <= ZERO:
                return point_source.id, None, ORIGIN_POINT, ISSUE_FACTOR_INVALID
            return point_source.id, factor, ORIGIN_POINT, ""

        if equivalence is not None:
            factor = Decimal(equivalence.factor_conversion)
            if factor <= ZERO:
                return equivalence.receta_padre_id, None, ORIGIN_CONFIGURED_EQUIVALENCE, ISSUE_FACTOR_INVALID
            return equivalence.receta_padre_id, factor, ORIGIN_CONFIGURED_EQUIVALENCE, ""
        return None, None, ORIGIN_UNRESOLVED, ISSUE_CONVERSION_ORIGIN_UNRESOLVED

    @staticmethod
    def _month_datetime_bounds(month_start: date):
        next_month = date(month_start.year + 1, 1, 1) if month_start.month == 12 else date(month_start.year, month_start.month + 1, 1)
        current_timezone = timezone.get_current_timezone()
        return (
            timezone.make_aware(datetime.combine(month_start, time.min), current_timezone),
            timezone.make_aware(datetime.combine(next_month, time.min), current_timezone),
        )

    @staticmethod
    def _parse_month(month: str | date) -> date:
        if isinstance(month, datetime):
            raise ValueError("datetime month values are not supported; use a date or YYYY-MM string")
        if isinstance(month, date):
            return month.replace(day=1)
        try:
            parsed = datetime.strptime(month, "%Y-%m").date()
        except (TypeError, ValueError) as exc:
            raise ValueError("month must use YYYY-MM format or be a date") from exc
        return parsed.replace(day=1)
