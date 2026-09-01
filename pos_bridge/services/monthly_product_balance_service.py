from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from django.apps import apps
from django.conf import settings
from django.utils import timezone

from pos_bridge.models import (
    PointConversionLine,
    PointDailySale,
    PointExtractionLog,
    PointInventorySnapshot,
    PointProductionLine,
    PointSyncJob,
    PointWasteLine,
)
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.utils.dates import iter_business_dates
from recetas.models import Receta, RecetaEquivalencia, VentaHistorica
from recetas.utils.normalizacion import normalizar_nombre

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
ISSUE_DAILY_SALE_UNRESOLVED = "SALES_DESTINATION_UNRESOLVED"
ISSUE_PRODUCTION_UNRESOLVED = "PRODUCTION_RECIPE_UNRESOLVED"
ISSUE_WASTE_UNRESOLVED = "WASTE_RECIPE_UNRESOLVED"
ISSUE_FACT_UNRESOLVED = "FACT_RECIPE_UNRESOLVED"
ISSUE_OFFICIAL_REPORT_EMPTY = "OFFICIAL_SALES_REPORT_EMPTY"
ISSUE_OFFICIAL_REPORT_INVALID = "OFFICIAL_SALES_REPORT_INVALID"
ISSUE_MONTH_SOURCE_INCOMPLETE = "MONTH_SOURCE_INCOMPLETE"
ISSUE_SNAPSHOT_BRANCH_COVERAGE_INCOMPLETE = "SNAPSHOT_BRANCH_COVERAGE_INCOMPLETE"
ISSUE_SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE = "SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE"
ISSUE_OFFICIAL_SALES_REFRESH_REQUIRED = "OFFICIAL_SALES_REFRESH_REQUIRED"
ISSUE_SALES_SOURCE_REQUIRES_REVIEW = "SALES_SOURCE_REQUIRES_REVIEW"
ISSUE_SALES_SYNC_JOB_MISSING = "SALES_SYNC_JOB_MISSING"
ISSUE_SALES_SYNC_JOB_PARTIAL = "SALES_SYNC_JOB_PARTIAL"
ISSUE_SALES_SYNC_JOB_FAILED = "SALES_SYNC_JOB_FAILED"
ISSUE_SALES_SOURCE_MIXED = "SALES_SOURCE_MIXED"
ISSUE_SALES_SYNC_JOB_MIXED = "SALES_SYNC_JOB_MIXED"
ISSUE_SALES_SYNC_JOB_RESTRICTED = "SALES_SYNC_JOB_RESTRICTED"
ISSUE_SALES_SYNC_COVERAGE_UNPROVEN = "SALES_SYNC_COVERAGE_UNPROVEN"
ISSUE_BRIDGE_UNRESOLVED = "BRIDGE_UNRESOLVED"
ISSUE_OPENING_MISSING = "OPENING_SNAPSHOT_MISSING"
ISSUE_CLOSING_MISSING = "CLOSING_SNAPSHOT_MISSING"

OFFICIAL_CATEGORY_REPORT_SOURCE = "POINT_OFFICIAL_MONTHLY_CATEGORY_REPORT"
OFFICIAL_POINT_DAILY_SOURCE = "/Report/PrintReportes?idreporte=3"
POINT_BRIDGE_SALES_SOURCE = "POINT_BRIDGE_SALES"


def _empty_counts() -> Mapping[str, int]:
    return MappingProxyType({})


def _empty_mapping() -> Mapping[str, object]:
    return MappingProxyType({})


def _freeze_value(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze_value(item) for item in value)
    return value


def _freeze_mapping(value: Mapping) -> Mapping:
    return _freeze_value(value)


class _OfficialSalesReportError(RuntimeError):
    def __init__(self, issue: str, message: str):
        super().__init__(message)
        self.issue = issue


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
    closing_point_cedis: Decimal | None = None
    closing_point_sucursales: Decimal | None = None
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
    branch_external_id: str = ""
    branch_name: str = ""
    movement_date: date | None = None


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
    closing_point_cedis: Decimal | None = None
    closing_point_sucursales: Decimal | None = None
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
        status = self._status(difference=difference_point, issues=issues)

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
            closing_point_cedis=self.closing_point_cedis,
            closing_point_sucursales=self.closing_point_sucursales,
            difference_point=difference_point,
            status=status,
            conversion_origin=conversion_origin,
            issues=tuple(sorted(issues)),
            source_counts=MappingProxyType(default_counts),
        )

    @staticmethod
    def _status(*, difference: Decimal | None, issues: set[str]) -> str:
        if issues or difference is None:
            return "REVISAR_FUENTE"
        if abs(difference) <= DIFFERENCE_TOLERANCE:
            return "COINCIDE"
        if difference > ZERO:
            return "POINT_MAYOR"
        return "POINT_MENOR"


class MonthlyPointProductBalanceService:
    """Build an exact-recipe monthly Point ledger without parent-equivalence collapsing."""

    DEFAULT_SNAPSHOT_TOLERANCE_DAYS = 3

    def __init__(
        self,
        identity_service: PointRecipeIdentityService | None = None,
        matcher: PointSalesMatchingService | None = None,
        official_sales_report_service: PointSalesCategoryReportService | None = None,
        refresh_official_sales: bool = False,
    ):
        self.identity_service = identity_service or PointRecipeIdentityService()
        self.matcher = matcher or PointSalesMatchingService()
        self.official_sales_report_service = official_sales_report_service or PointSalesCategoryReportService()
        self.refresh_official_sales = bool(refresh_official_sales)

    def build(
        self,
        month: str | date,
        *,
        refresh_official_sales: bool | None = None,
    ) -> MonthlyPointBalance:
        self._build_match_cache: dict[tuple[str, str], Receta | None] = {}
        self._build_conversion_cache: dict[tuple[str, str], Receta | None] = {}
        month_start = self._parse_month(month)
        month_end = date(month_start.year, month_start.month, monthrange(month_start.year, month_start.month)[1])
        opening_target = month_start - timedelta(days=1)

        opening, opening_meta, opening_unresolved = self._load_snapshot(snapshot_date=opening_target, source="opening_snapshot")
        closing, closing_meta, closing_unresolved = self._load_snapshot(snapshot_date=month_end, source="closing_snapshot")
        coverage_issues = self._compare_snapshot_coverage(opening_meta=opening_meta, closing_meta=closing_meta)
        production, production_meta, production_unresolved = self._load_production(
            month_start=month_start,
            month_end=month_end,
        )
        sales, sales_meta, sales_unresolved = self._load_sales(
            month_start=month_start,
            month_end=month_end,
            refresh_official_sales=(
                self.refresh_official_sales
                if refresh_official_sales is None
                else bool(refresh_official_sales)
            ),
        )
        waste, waste_meta, waste_unresolved = self._load_waste(
            month_start=month_start,
            month_end=month_end,
        )
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
        self._apply_closing_scopes(rows, closing_meta)

        unresolved_movements = tuple(
            opening_unresolved
            + closing_unresolved
            + production_unresolved
            + sales_unresolved
            + waste_unresolved
            + conversion_unresolved
        )
        month_source_incomplete = bool(unresolved_movements or unresolved_conversions)
        month_source_incomplete = month_source_incomplete or not bool(opening_meta.get("authoritative"))
        month_source_incomplete = month_source_incomplete or not bool(closing_meta.get("authoritative"))
        month_source_incomplete = month_source_incomplete or bool(coverage_issues)
        month_source_incomplete = month_source_incomplete or not bool(sales_meta.get("authoritative", True))
        if month_source_incomplete:
            for row in rows.values():
                row.issues.add(ISSUE_MONTH_SOURCE_INCOMPLETE)

        frozen_rows = MappingProxyType({receta_id: rows[receta_id].freeze(receta_id) for receta_id in sorted(rows)})
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
                | coverage_issues
                | ({ISSUE_MONTH_SOURCE_INCOMPLETE} if month_source_incomplete else set())
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
            "production_unresolved": len(production_unresolved),
            "sales_rows": sum(value[1] for value in sales.values()),
            "sales_unresolved": len(sales_unresolved),
            "official_sales_unresolved": sum(movement.source == "official_sales" for movement in sales_unresolved),
            "official_daily_sales_unresolved": sum(
                movement.source == "official_daily_sales" for movement in sales_unresolved
            ),
            "official_sales_report_unresolved": sum(
                movement.source == "official_sales_report" for movement in sales_unresolved
            ),
            "official_sales_mode_unresolved": sum(
                movement.source == "official_sales_mode" for movement in sales_unresolved
            ),
            "fact_sales_unresolved": sum(movement.source == "fact_sales" for movement in sales_unresolved),
            "waste_rows": sum(value[1] for value in waste.values()),
            "waste_unresolved": len(waste_unresolved),
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

    @staticmethod
    def _compare_snapshot_coverage(*, opening_meta: dict[str, object], closing_meta: dict[str, object]) -> set[str]:
        opening_branch_ids = set(opening_meta.get("applied_branch_ids") or ())
        closing_branch_ids = set(closing_meta.get("applied_branch_ids") or ())
        opening_keys = set(opening_meta.get("applied_coverage_keys") or ())
        closing_keys = set(closing_meta.get("applied_coverage_keys") or ())
        opening_meta["missing_in_closing_branch_ids"] = tuple(sorted(opening_branch_ids - closing_branch_ids))
        closing_meta["missing_in_opening_branch_ids"] = tuple(sorted(closing_branch_ids - opening_branch_ids))
        opening_meta["missing_in_closing_coverage_keys"] = tuple(sorted(opening_keys - closing_keys))
        closing_meta["missing_in_opening_coverage_keys"] = tuple(sorted(closing_keys - opening_keys))

        issues: set[str] = set()
        if opening_branch_ids != closing_branch_ids:
            issues.add(ISSUE_SNAPSHOT_BRANCH_COVERAGE_INCOMPLETE)
        if opening_branch_ids == closing_branch_ids and opening_keys != closing_keys:
            issues.add(ISSUE_SNAPSHOT_PRODUCT_COVERAGE_INCOMPLETE)
        return issues

    @staticmethod
    def _apply_closing_scopes(rows: dict[int, _MutableBalanceRow], closing_meta: Mapping[str, object]) -> None:
        for receta_id, scopes in (closing_meta.get("recipe_scope_totals") or {}).items():
            row = rows.get(receta_id)
            if row is None or row.closing_point is None:
                continue
            row.closing_point_cedis = Decimal(scopes["cedis"])
            row.closing_point_sucursales = Decimal(scopes["sucursales"])

    def _load_snapshot(self, *, snapshot_date: date, source: str):
        tolerance_days = int(
            getattr(settings, "PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS", self.DEFAULT_SNAPSHOT_TOLERANCE_DAYS)
        )
        current_timezone = timezone.get_current_timezone()
        window_start = timezone.make_aware(
            datetime.combine(snapshot_date - timedelta(days=tolerance_days), time.min),
            current_timezone,
        )
        window_end = timezone.make_aware(
            datetime.combine(snapshot_date + timedelta(days=tolerance_days + 1), time.min),
            current_timezone,
        )
        candidates = list(
            PointInventorySnapshot.objects.filter(captured_at__gte=window_start, captured_at__lt=window_end)
            .select_related("product", "branch", "branch__erp_branch")
            .only(
                "id",
                "product_id",
                "product__sku",
                "product__name",
                "branch_id",
                "branch__external_id",
                "branch__name",
                "branch__erp_branch_id",
                "branch__erp_branch__codigo",
                "branch__erp_branch__activa",
                "stock",
                "captured_at",
            )
            .order_by("branch_id", "product_id", "captured_at", "id")
        )
        selected_by_key: dict[tuple[int, int], PointInventorySnapshot] = {}
        for candidate in candidates:
            key = (candidate.branch_id, candidate.product_id)
            candidate_date = timezone.localtime(candidate.captured_at, current_timezone).date()
            current = selected_by_key.get(key)
            if current is None:
                selected_by_key[key] = candidate
                continue
            current_date = timezone.localtime(current.captured_at, current_timezone).date()
            candidate_rank = (
                candidate_date != snapshot_date,
                abs((candidate_date - snapshot_date).days),
                candidate_date > snapshot_date,
                -candidate.captured_at.timestamp(),
                -candidate.id,
            )
            current_rank = (
                current_date != snapshot_date,
                abs((current_date - snapshot_date).days),
                current_date > snapshot_date,
                -current.captured_at.timestamp(),
                -current.id,
            )
            if candidate_rank < current_rank:
                selected_by_key[key] = candidate
        snapshots = list(selected_by_key.values())
        if not snapshots:
            return {}, self._empty_snapshot_meta(snapshot_date, tolerance_days), []

        values: dict[int, tuple[Decimal, int]] = {}
        unresolved: list[MonthlyPointUnresolvedMovement] = []
        selected_branch_ids = {snapshot.branch_id for snapshot in snapshots}
        selected_branch_codes = {snapshot.branch.external_id for snapshot in snapshots}
        applied_branch_ids: set[int] = set()
        selected_coverage_keys: set[tuple[int, int]] = set()
        applied_coverage_keys: set[tuple[int, int]] = set()
        selected_mapped_recipe_keys: set[tuple[int, int, int]] = set()
        applied_mapped_recipe_keys: set[tuple[int, int, int]] = set()
        recipe_scope_totals: dict[int, dict[str, Decimal]] = {}
        cedis_scope_rows = 0
        sucursales_scope_rows = 0
        selected_dates = {
            timezone.localtime(snapshot.captured_at, current_timezone).date()
            for snapshot in snapshots
        }
        for snapshot in snapshots:
            receta = self._match_recipe(
                code=snapshot.product.sku,
                name=snapshot.product.name,
            )
            quantity = Decimal(snapshot.stock)
            coverage_key = (snapshot.branch_id, snapshot.product_id)
            selected_coverage_keys.add(coverage_key)
            if receta is None:
                unresolved.append(
                    MonthlyPointUnresolvedMovement(
                        source=source,
                        movement_id=str(snapshot.id),
                        item_code=snapshot.product.sku,
                        item_name=snapshot.product.name,
                        quantity=quantity,
                        issue=ISSUE_SNAPSHOT_UNRESOLVED,
                        branch_external_id=snapshot.branch.external_id,
                        branch_name=snapshot.branch.name,
                        movement_date=timezone.localtime(snapshot.captured_at, current_timezone).date(),
                    )
                )
                continue
            mapped_recipe_key = (snapshot.branch_id, snapshot.product_id, receta.id)
            selected_mapped_recipe_keys.add(mapped_recipe_key)
            current, count = values.get(receta.id, (ZERO, 0))
            values[receta.id] = (current + quantity, count + 1)
            applied_branch_ids.add(snapshot.branch_id)
            applied_coverage_keys.add(coverage_key)
            applied_mapped_recipe_keys.add(mapped_recipe_key)
            if source == "closing_snapshot":
                scopes = recipe_scope_totals.setdefault(receta.id, {"cedis": ZERO, "sucursales": ZERO})
                if self._is_cedis_inventory_scope(snapshot):
                    scopes["cedis"] += quantity
                    cedis_scope_rows += 1
                else:
                    scopes["sucursales"] += quantity
                    sucursales_scope_rows += 1

        fallback_used = any(selected_date != snapshot_date for selected_date in selected_dates)
        effective_date = next(iter(selected_dates)) if len(selected_dates) == 1 else None
        return values, {
            "source": "PointInventorySnapshot",
            "target_date": snapshot_date,
            "effective_date": effective_date,
            "tolerance_days": tolerance_days,
            "selected_dates": tuple(sorted(selected_dates)),
            "fallback_used": fallback_used,
            "within_tolerance": True,
            "authoritative": bool(snapshots),
            "days_from_target": max(abs((selected_date - snapshot_date).days) for selected_date in selected_dates),
            "snapshot_rows": len(snapshots),
            "selected_rows": len(snapshots),
            "selected_branch_count": len(selected_branch_ids),
            "selected_branch_ids": tuple(sorted(selected_branch_ids)),
            "selected_branch_codes": tuple(sorted(selected_branch_codes)),
            "selected_coverage_key_count": len(selected_coverage_keys),
            "selected_coverage_keys": tuple(sorted(selected_coverage_keys)),
            "selected_mapped_recipe_keys": tuple(sorted(selected_mapped_recipe_keys)),
            "applied_rows": sum(count for _quantity, count in values.values()),
            "applied_branch_count": len(applied_branch_ids),
            "applied_branch_ids": tuple(sorted(applied_branch_ids)),
            "applied_coverage_key_count": len(applied_coverage_keys),
            "applied_coverage_keys": tuple(sorted(applied_coverage_keys)),
            "applied_mapped_recipe_keys": tuple(sorted(applied_mapped_recipe_keys)),
            "out_of_tolerance_key_count": 0,
            "recipe_scope_totals": recipe_scope_totals,
            "cedis_scope_rows": cedis_scope_rows,
            "sucursales_scope_rows": sucursales_scope_rows,
            "cedis_scope_total": sum((scopes["cedis"] for scopes in recipe_scope_totals.values()), ZERO),
            "sucursales_scope_total": sum((scopes["sucursales"] for scopes in recipe_scope_totals.values()), ZERO),
            "matched_recipe_count": len(values),
            "unresolved_rows": len(unresolved),
        }, unresolved

    @staticmethod
    def _empty_snapshot_meta(snapshot_date: date, tolerance_days: int) -> dict[str, object]:
        return {
            "source": "PointInventorySnapshot",
            "target_date": snapshot_date,
            "effective_date": None,
            "selected_dates": (),
            "tolerance_days": tolerance_days,
            "fallback_used": False,
            "within_tolerance": False,
            "authoritative": False,
            "days_from_target": None,
            "snapshot_rows": 0,
            "selected_rows": 0,
            "selected_branch_count": 0,
            "selected_branch_ids": (),
            "selected_branch_codes": (),
            "selected_coverage_key_count": 0,
            "selected_coverage_keys": (),
            "selected_mapped_recipe_keys": (),
            "applied_rows": 0,
            "applied_branch_count": 0,
            "applied_branch_ids": (),
            "applied_coverage_key_count": 0,
            "applied_coverage_keys": (),
            "applied_mapped_recipe_keys": (),
            "out_of_tolerance_key_count": 0,
            "recipe_scope_totals": {},
            "cedis_scope_rows": 0,
            "sucursales_scope_rows": 0,
            "cedis_scope_total": ZERO,
            "sucursales_scope_total": ZERO,
            "matched_recipe_count": 0,
            "unresolved_rows": 0,
        }

    @staticmethod
    def _is_cedis_inventory_scope(snapshot: PointInventorySnapshot) -> bool:
        branch = snapshot.branch
        erp_branch = getattr(branch, "erp_branch", None)
        code = str(getattr(erp_branch, "codigo", "") or "").strip().upper()
        name = normalizar_nombre(getattr(branch, "name", "") or "")
        if code in {"CEDIS", "DEVOLUCIONES", "ALMACEN"}:
            return True
        if "cedis" in name or "produccion" in name or "devolucion" in name or "almacen" in name:
            return True
        return erp_branch is not None and not bool(getattr(erp_branch, "activa", True))

    @staticmethod
    def _snapshot_warnings(meta: Mapping[str, object], *, label: str) -> list[str]:
        selected_dates = tuple(meta.get("selected_dates") or ())
        if not selected_dates:
            return [f"No existe snapshot Point para {label}; el balance no es autoritativo."]
        if meta.get("fallback_used"):
            selected_label = ", ".join(str(selected_date) for selected_date in selected_dates)
            warning = f"Se usaron fechas alternativas {selected_label} para {label} (objetivo {meta['target_date']})."
            if not meta.get("within_tolerance"):
                warning += " La fecha queda fuera de la tolerancia configurada."
            return [warning]
        return []

    def _load_production(self, *, month_start: date, month_end: date):
        facts, facts_present, fact_unresolved, fact_rows_read = self._load_fact_values(
            month_start=month_start,
            month_end=month_end,
            field_name="producido",
            source="fact_production",
        )
        if facts_present:
            return facts, {
                "source": "FactProduccionDiaria",
                "priority": "primary",
                "source_present": True,
                "rows_read": fact_rows_read,
                "unresolved_rows": len(fact_unresolved),
            }, fact_unresolved
        rows = list(
            PointProductionLine.objects.filter(
                production_date__gte=month_start,
                production_date__lte=month_end,
                is_insumo=False,
            )
            .select_related("branch")
            .only(
                "id",
                "receta_id",
                "produced_quantity",
                "production_date",
                "production_external_id",
                "item_code",
                "item_name",
                "branch_id",
                "branch__external_id",
                "branch__name",
            )
            .order_by("id")
        )
        matched = [row for row in rows if row.receta_id is not None]
        unresolved = [
            MonthlyPointUnresolvedMovement(
                source="point_production",
                movement_id=row.production_external_id or str(row.id),
                item_code=row.item_code,
                item_name=row.item_name,
                quantity=Decimal(row.produced_quantity),
                issue=ISSUE_PRODUCTION_UNRESOLVED,
                branch_external_id=row.branch.external_id,
                branch_name=row.branch.name,
                movement_date=row.production_date,
            )
            for row in rows
            if row.receta_id is None
        ]
        return self._aggregate_rows(matched, "produced_quantity"), {
            "source": "PointProductionLine",
            "priority": "fallback",
            "source_present": bool(rows),
            "rows_read": len(rows),
            "unresolved_rows": len(unresolved),
        }, unresolved

    def _load_waste(self, *, month_start: date, month_end: date):
        facts, facts_present, fact_unresolved, fact_rows_read = self._load_fact_values(
            month_start=month_start,
            month_end=month_end,
            field_name="merma",
            source="fact_waste",
        )
        if facts_present:
            return facts, {
                "source": "FactProduccionDiaria",
                "priority": "primary",
                "source_present": True,
                "rows_read": fact_rows_read,
                "unresolved_rows": len(fact_unresolved),
            }, fact_unresolved
        lower_bound, upper_bound = self._month_datetime_bounds(month_start)
        point_rows = list(
            PointWasteLine.objects.filter(
                movement_at__gte=lower_bound,
                movement_at__lt=upper_bound,
            )
            .select_related("branch")
            .only(
                "id",
                "receta_id",
                "quantity",
                "movement_at",
                "movement_external_id",
                "source_hash",
                "item_code",
                "item_name",
                "branch_id",
                "branch__external_id",
                "branch__name",
            )
            .order_by("id")
        )
        if point_rows:
            matched = [row for row in point_rows if row.receta_id is not None]
            unresolved = [
                MonthlyPointUnresolvedMovement(
                    source="point_waste",
                    movement_id=row.movement_external_id or str(row.id),
                    source_hash=row.source_hash,
                    item_code=row.item_code,
                    item_name=row.item_name,
                    quantity=Decimal(row.quantity),
                    issue=ISSUE_WASTE_UNRESOLVED,
                    branch_external_id=row.branch.external_id,
                    branch_name=row.branch.name,
                    movement_date=timezone.localtime(row.movement_at).date(),
                )
                for row in point_rows
                if row.receta_id is None
            ]
            return self._aggregate_rows(matched, "quantity"), {
                "source": "PointWasteLine",
                "priority": "fallback",
                "source_present": True,
                "rows_read": len(point_rows),
                "unresolved_rows": len(unresolved),
            }, unresolved

        monthly_waste_model = apps.get_model("control", "MermaMensualSucursal")
        monthly_rows = list(
            monthly_waste_model.objects.filter(periodo=month_start)
            .select_related("sucursal")
            .only(
                "id",
                "receta_id",
                "unidades_merma",
                "periodo",
                "nombre_producto",
                "sucursal_id",
                "sucursal__codigo",
                "sucursal__nombre",
            )
            .order_by("id")
        )
        matched = [row for row in monthly_rows if row.receta_id is not None]
        unresolved = [
            MonthlyPointUnresolvedMovement(
                source="monthly_waste",
                movement_id=str(row.id),
                item_code="",
                item_name=row.nombre_producto,
                quantity=Decimal(row.unidades_merma),
                issue=ISSUE_WASTE_UNRESOLVED,
                branch_external_id=row.sucursal.codigo if row.sucursal_id else "",
                branch_name=row.sucursal.nombre if row.sucursal_id else "",
                movement_date=row.periodo,
            )
            for row in monthly_rows
            if row.receta_id is None
        ]
        return self._aggregate_rows(matched, "unidades_merma"), {
            "source": "MermaMensualSucursal",
            "priority": "monthly_fallback",
            "source_present": bool(monthly_rows),
            "rows_read": len(monthly_rows),
            "unresolved_rows": len(unresolved),
        }, unresolved

    def _load_fact_values(self, *, month_start: date, month_end: date, field_name: str, source: str):
        fact_model = apps.get_model("reportes", "FactProduccionDiaria")
        rows = list(
            fact_model.objects.filter(fecha__gte=month_start, fecha__lte=month_end)
            .select_related("sucursal")
            .only(
                "id",
                "receta_id",
                "fecha",
                "sucursal_id",
                "sucursal__codigo",
                "sucursal__nombre",
                "metadata",
                field_name,
            )
            .order_by("id")
        )
        matched = [row for row in rows if row.receta_id is not None]
        unresolved = []
        has_unmapped_nonzero = False
        for row in rows:
            quantity = Decimal(getattr(row, field_name) or ZERO)
            if row.receta_id is not None or quantity == ZERO:
                continue
            has_unmapped_nonzero = True
            metadata = row.metadata or {}
            unresolved.append(
                MonthlyPointUnresolvedMovement(
                    source=source,
                    movement_id=str(row.id),
                    item_code=str(metadata.get("item_code") or metadata.get("codigo_point") or ""),
                    item_name=str(metadata.get("item_name") or metadata.get("nombre") or ""),
                    quantity=quantity,
                    issue=ISSUE_FACT_UNRESOLVED,
                    branch_external_id=row.sucursal.codigo if row.sucursal_id else "",
                    branch_name=row.sucursal.nombre if row.sucursal_id else "",
                    movement_date=row.fecha,
                )
            )
        return self._aggregate_rows(matched, field_name), bool(matched) or has_unmapped_nonzero, unresolved, len(rows)

    @staticmethod
    def _aggregate_rows(rows, field_name: str):
        values: dict[int, tuple[Decimal, int]] = {}
        for row in rows:
            quantity = Decimal(getattr(row, field_name) or ZERO)
            current, count = values.get(row.receta_id, (ZERO, 0))
            values[row.receta_id] = (current + quantity, count + 1)
        return values

    def _load_sales(
        self,
        *,
        month_start: date,
        month_end: date,
        refresh_official_sales: bool,
    ):
        source_mode = str(getattr(settings, "PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE", "AUTO")).strip().upper() or "AUTO"
        if source_mode not in {"AUTO", "OFFICIAL_MONTHLY_REPORT", "BRIDGE_HISTORY"}:
            source_mode = "AUTO"

        if source_mode == "BRIDGE_HISTORY":
            return self._load_persisted_sales(
                month_start=month_start,
                month_end=month_end,
                include_daily=False,
                configured_source_mode=source_mode,
                fallback_chain=("FactProduccionDiaria", POINT_BRIDGE_SALES_SOURCE),
                remote_refresh_requested=refresh_official_sales,
            )

        if refresh_official_sales:
            try:
                values, meta, unresolved = self._load_official_sales(month_start=month_start, month_end=month_end)
                return values, self._sales_meta(
                    meta,
                    configured_source_mode=source_mode,
                    fallback_chain=(OFFICIAL_CATEGORY_REPORT_SOURCE,),
                    selection_reason="remote_monthly_report",
                    remote_refresh_requested=True,
                    authoritative=True,
                ), unresolved
            except Exception as exc:  # noqa: BLE001
                issue = getattr(exc, "issue", ISSUE_OFFICIAL_REPORT_INVALID)
                remote_unresolved = [
                    MonthlyPointUnresolvedMovement(
                        source="official_sales_report",
                        movement_id=f"{month_start:%Y-%m}",
                        item_code="",
                        item_name=str(exc),
                        quantity=ZERO,
                        issue=issue,
                        movement_date=month_start,
                    )
                ]
                values, meta, unresolved = self._load_persisted_sales(
                    month_start=month_start,
                    month_end=month_end,
                    include_daily=True,
                    configured_source_mode=source_mode,
                    fallback_chain=(OFFICIAL_CATEGORY_REPORT_SOURCE, OFFICIAL_POINT_DAILY_SOURCE, "FactProduccionDiaria", POINT_BRIDGE_SALES_SOURCE),
                    remote_refresh_requested=True,
                    selection_reason="remote_monthly_report_failed",
                    authoritative=False,
                )
                mutable_meta = dict(meta)
                mutable_meta.update(
                    {
                        "warnings": ["No se pudo usar el reporte oficial mensual; se uso una fuente persistida no autoritativa."],
                        "fallback_reason": str(exc),
                    }
                )
                return values, mutable_meta, remote_unresolved + unresolved

        values, meta, unresolved = self._load_persisted_sales(
            month_start=month_start,
            month_end=month_end,
            include_daily=True,
            configured_source_mode=source_mode,
            fallback_chain=(OFFICIAL_POINT_DAILY_SOURCE, "FactProduccionDiaria", POINT_BRIDGE_SALES_SOURCE),
            remote_refresh_requested=False,
        )
        if source_mode == "OFFICIAL_MONTHLY_REPORT":
            required = MonthlyPointUnresolvedMovement(
                source="official_sales_mode",
                movement_id=f"{month_start:%Y-%m}",
                item_code="",
                item_name="El modo OFFICIAL_MONTHLY_REPORT requiere refresh_official_sales=True.",
                quantity=ZERO,
                issue=ISSUE_OFFICIAL_SALES_REFRESH_REQUIRED,
                movement_date=month_start,
            )
            mutable_meta = dict(meta)
            mutable_meta.update(
                {
                    "authoritative": False,
                    "selection_reason": "official_monthly_refresh_required",
                    "warnings": ["La fuente persistida se muestra solo como referencia hasta refrescar el reporte oficial mensual."],
                }
            )
            return values, mutable_meta, [required] + unresolved
        return values, meta, unresolved

    def _load_persisted_sales(
        self,
        *,
        month_start: date,
        month_end: date,
        include_daily: bool,
        configured_source_mode: str,
        fallback_chain: tuple[str, ...],
        remote_refresh_requested: bool,
        selection_reason: str = "",
        authoritative: bool = True,
    ):
        if include_daily:
            daily, daily_unresolved, daily_rows_read, daily_rows = self._load_daily_sales(
                month_start=month_start,
                month_end=month_end,
            )
            if daily_rows_read:
                daily_authoritative, daily_evidence, daily_authority_unresolved = self._validate_official_daily_sales_authority(
                    month_start=month_start,
                    month_end=month_end,
                    official_daily_row_count=daily_rows_read,
                    daily_rows=daily_rows,
                )
                return daily, self._sales_meta(
                    {
                        "source": OFFICIAL_POINT_DAILY_SOURCE,
                        "mode": "official_point_daily_sales",
                        "source_present": True,
                        "row_count": daily_rows_read,
                        "unresolved_rows": len(daily_unresolved),
                        **daily_evidence,
                    },
                    configured_source_mode=configured_source_mode,
                    fallback_chain=fallback_chain,
                    selection_reason=selection_reason or "persisted_official_daily_sales",
                    remote_refresh_requested=remote_refresh_requested,
                    authoritative=authoritative and daily_authoritative,
                ), daily_unresolved + daily_authority_unresolved

        facts, facts_present, fact_unresolved, fact_rows_read = self._load_fact_values(
            month_start=month_start,
            month_end=month_end,
            field_name="vendido",
            source="fact_sales",
        )
        if facts_present:
            return facts, self._sales_meta(
                {
                    "source": "FactProduccionDiaria",
                    "mode": "production_facts",
                    "source_present": True,
                    "row_count": fact_rows_read,
                    "unresolved_rows": len(fact_unresolved),
                },
                configured_source_mode=configured_source_mode,
                fallback_chain=fallback_chain,
                selection_reason=selection_reason or "persisted_production_facts",
                remote_refresh_requested=remote_refresh_requested,
                authoritative=authoritative,
            ), fact_unresolved

        rows = list(
            VentaHistorica.objects.filter(
            fecha__gte=month_start,
            fecha__lte=month_end,
            fuente=POINT_BRIDGE_SALES_SOURCE,
            )
            .select_related("sucursal")
            .only("id", "receta_id", "sucursal_id", "sucursal__codigo", "sucursal__nombre", "fecha", "cantidad")
            .order_by("id")
        )
        matched_rows = [row for row in rows if row.receta_id is not None]
        unresolved = [
            MonthlyPointUnresolvedMovement(
                source="bridge_sales",
                movement_id=str(row.id),
                item_code="",
                item_name="VentaHistorica POINT_BRIDGE_SALES sin receta homologada.",
                quantity=Decimal(row.cantidad),
                issue=ISSUE_BRIDGE_UNRESOLVED,
                branch_external_id=row.sucursal.codigo if row.sucursal_id else "",
                branch_name=row.sucursal.nombre if row.sucursal_id else "",
                movement_date=row.fecha,
            )
            for row in rows
            if row.receta_id is None
        ]
        values = self._aggregate_rows(matched_rows, "cantidad")
        bridge_review = MonthlyPointUnresolvedMovement(
            source="sales_authority",
            movement_id=f"{month_start:%Y-%m}",
            item_code="",
            item_name="VentaHistorica POINT_BRIDGE_SALES requiere revisión manual antes de cierre.",
            quantity=ZERO,
            issue=ISSUE_SALES_SOURCE_REQUIRES_REVIEW,
            movement_date=month_start,
        )
        bridge_present = bool(rows)
        return values, self._sales_meta(
            {
                "source": POINT_BRIDGE_SALES_SOURCE,
                "mode": "bridge_history",
                "source_present": bridge_present,
                "row_count": len(matched_rows),
                "raw_row_count": len(rows),
                "unresolved_rows": len(unresolved),
            },
            configured_source_mode=configured_source_mode,
            fallback_chain=fallback_chain,
            selection_reason=(selection_reason or "bridge_history_requires_manual_review") if bridge_present else "no_persisted_sales",
            remote_refresh_requested=remote_refresh_requested,
            authoritative=authoritative if not bridge_present else False,
        ), ([bridge_review] if bridge_present else []) + unresolved

    @staticmethod
    def _expected_official_sales_branch_days(*, month_start: date, month_end: date) -> set[tuple[str, date]]:
        excluded = {
            item.strip().lower()
            for item in load_point_bridge_settings().sales_excluded_branches
            if item
        }
        branches = [
            branch
            for branch in PointSalesBranchIndicatorService.canonical_branches()
            if branch.name.strip().lower() not in excluded
        ]
        expected: set[tuple[str, date]] = set()
        for sale_date in iter_business_dates(month_start, month_end):
            for branch in branches:
                if branch.erp_branch_id and not branch.erp_branch.esta_operativa(sale_date):
                    continue
                expected.add((branch.external_id, sale_date))
        return expected

    @staticmethod
    def _official_sales_job_is_unrestricted(job: PointSyncJob, *, month_start: date, month_end: date) -> bool:
        parameters = dict(job.parameters or {})
        return (
            MonthlyPointProductBalanceService._official_sales_job_has_month_contract(
                job, month_start=month_start, month_end=month_end
            )
            and not str(parameters.get("branch_filter") or "").strip()
            and not parameters.get("excluded_ranges")
            and parameters.get("max_days") is None
            and [str(scope) for scope in (parameters.get("credito_scopes") or [])] == ["null"]
        )

    @staticmethod
    def _official_sales_job_has_month_contract(job: PointSyncJob, *, month_start: date, month_end: date) -> bool:
        parameters = dict(job.parameters or {})
        return (
            job.job_type == PointSyncJob.JOB_TYPE_SALES
            and parameters.get("source") == "POINT_OFFICIAL_REPORT"
            and str(parameters.get("start_date") or "") == month_start.isoformat()
            and str(parameters.get("end_date") or "") == month_end.isoformat()
        )

    def _validate_official_daily_sales_authority(
        self,
        *,
        month_start: date,
        month_end: date,
        official_daily_row_count: int,
        daily_rows: list[PointDailySale],
    ) -> tuple[bool, dict[str, object], list[MonthlyPointUnresolvedMovement]]:
        legacy_daily_row_count = PointDailySale.objects.filter(
            sale_date__gte=month_start,
            sale_date__lte=month_end,
            source_endpoint="/Report/VentasCategorias",
        ).count()
        bridge_rows = list(
            VentaHistorica.objects.filter(
            fecha__gte=month_start,
            fecha__lte=month_end,
            fuente=POINT_BRIDGE_SALES_SOURCE,
            )
            .only("id", "receta_id", "sucursal_id", "fecha", "cantidad")
            .order_by("id")
        )
        legacy_bridge_row_count = len(bridge_rows)
        selected_row_job_ids = tuple(sorted({row.sync_job_id for row in daily_rows if row.sync_job_id is not None}))
        jobs_by_id = PointSyncJob.objects.filter(id__in=selected_row_job_ids).only(
            "id", "job_type", "status", "parameters", "result_summary"
        ).in_bulk()
        rejected_provenance: list[dict[str, object]] = []
        issues: list[str] = []
        expected_branch_days = self._expected_official_sales_branch_days(
            month_start=month_start,
            month_end=month_end,
        )
        log_contexts = PointExtractionLog.objects.filter(sync_job_id__in=selected_row_job_ids).values_list(
            "sync_job_id", "level", "message", "context"
        )
        logged_branch_days_by_job: dict[int, set[tuple[str, date]]] = {}
        no_aplica_branch_days_by_job: dict[int, set[tuple[str, date]]] = {}
        for job_id, level, message, context in log_contexts:
            context = context or {}
            branch_external_id = str(context.get("branch_external_id") or "").strip()
            sale_date_text = str(context.get("sale_date") or "").strip()
            if not branch_external_id or not sale_date_text:
                continue
            try:
                logged_date = date.fromisoformat(sale_date_text)
            except ValueError:
                continue
            branch_day = (branch_external_id, logged_date)
            if (
                level == PointExtractionLog.LEVEL_INFO
                and context.get("status") == "NO_APLICA_POR_APERTURA"
                and message == f"Backfill oficial no aplica por apertura para {branch_external_id} {logged_date.isoformat()}."
            ):
                no_aplica_branch_days_by_job.setdefault(job_id, set()).add(branch_day)
                continue
            if (
                level == PointExtractionLog.LEVEL_INFO
                and message == f"Backfill oficial {branch_external_id} {logged_date.isoformat()}"
            ):
                logged_branch_days_by_job.setdefault(job_id, set()).add(branch_day)
        if any(row.sync_job_id is None for row in daily_rows):
            issues.append(ISSUE_SALES_SYNC_JOB_MISSING)
            rejected_provenance.append({"job_id": None, "reason": "missing_sync_job"})
        if len(selected_row_job_ids) > 1:
            issues.append(ISSUE_SALES_SYNC_JOB_MIXED)
            rejected_provenance.append({"job_ids": selected_row_job_ids, "reason": "mixed_sync_jobs"})
        job = jobs_by_id.get(selected_row_job_ids[0]) if len(selected_row_job_ids) == 1 else None
        if job is None and selected_row_job_ids:
            issues.append(ISSUE_SALES_SYNC_JOB_MISSING)
            rejected_provenance.append({"job_ids": selected_row_job_ids, "reason": "unavailable_sync_job"})
        elif job is not None:
            if not self._official_sales_job_has_month_contract(job, month_start=month_start, month_end=month_end):
                issues.append(ISSUE_SALES_SYNC_JOB_MISSING)
                rejected_provenance.append({"job_id": job.id, "reason": "job_contract_mismatch"})
            elif not self._official_sales_job_is_unrestricted(job, month_start=month_start, month_end=month_end):
                issues.append(ISSUE_SALES_SYNC_JOB_RESTRICTED)
                rejected_provenance.append({"job_id": job.id, "reason": "job_contract_restricted_or_mismatched"})
            elif job.status == PointSyncJob.STATUS_PARTIAL:
                issues.append(ISSUE_SALES_SYNC_JOB_PARTIAL)
                rejected_provenance.append({"job_id": job.id, "reason": "job_partial"})
            elif job.status != PointSyncJob.STATUS_SUCCESS:
                issues.append(ISSUE_SALES_SYNC_JOB_FAILED)
                rejected_provenance.append({"job_id": job.id, "reason": "job_not_successful"})
            else:
                summary = dict(job.result_summary or {})
                logged_branch_days = logged_branch_days_by_job.get(job.id, set())
                summary_branch_days = int(summary.get("branch_days_processed") or 0)
                failed_branch_days = int(summary.get("failed_branch_days") or 0)
                if (
                    failed_branch_days
                    or summary_branch_days != len(expected_branch_days)
                    or logged_branch_days != expected_branch_days
                ):
                    issues.append(ISSUE_SALES_SYNC_COVERAGE_UNPROVEN)
                    rejected_provenance.append(
                        {
                            "job_id": job.id,
                            "reason": "coverage_manifest_incomplete",
                            "expected_branch_days": len(expected_branch_days),
                            "logged_branch_days": len(logged_branch_days),
                            "summary_branch_days": summary_branch_days,
                            "failed_branch_days": failed_branch_days,
                        }
                    )
        materialized_bridge_reconciled = False
        bridge_unresolved_count = sum(row.receta_id is None for row in bridge_rows)
        if bridge_unresolved_count:
            issues.append(ISSUE_BRIDGE_UNRESOLVED)
            rejected_provenance.append({"reason": "bridge_rows_without_recipe", "count": bridge_unresolved_count})
        elif bridge_rows:
            daily_totals: dict[tuple[int, int | None, date], Decimal] = {}
            for row in daily_rows:
                if row.receta_id is None:
                    continue
                key = (row.receta_id, getattr(row.branch, "erp_branch_id", None), row.sale_date)
                daily_totals[key] = daily_totals.get(key, ZERO) + Decimal(row.quantity)
            bridge_totals: dict[tuple[int, int | None, date], Decimal] = {}
            for row in bridge_rows:
                key = (row.receta_id, row.sucursal_id, row.fecha)
                bridge_totals[key] = bridge_totals.get(key, ZERO) + Decimal(row.cantidad)
            materialized_bridge_reconciled = daily_totals == bridge_totals
            if not materialized_bridge_reconciled:
                issues.append(ISSUE_SALES_SOURCE_MIXED)
                rejected_provenance.append({"reason": "bridge_rows_diverge_from_selected_daily_sales"})
        if legacy_daily_row_count or legacy_bridge_row_count:
            if legacy_daily_row_count:
                issues.append(ISSUE_SALES_SOURCE_MIXED)
                rejected_provenance.append({"reason": "legacy_daily_sales_present", "count": legacy_daily_row_count})
        unresolved = [
            MonthlyPointUnresolvedMovement(
                source="sales_authority",
                movement_id=str(job.id) if job is not None else f"{month_start:%Y-%m}",
                item_code="",
                item_name="La evidencia persistida de ventas Point requiere revisión.",
                quantity=ZERO,
                issue=issue,
                movement_date=month_start,
            )
            for issue in issues
        ]
        return not issues, {
            "job_id": job.id if job is not None and not rejected_provenance else None,
            "job_status": job.status if job is not None else "",
            "job_coverage_start": month_start,
            "job_coverage_end": month_end,
            "official_daily_row_count": official_daily_row_count,
            "legacy_daily_row_count": legacy_daily_row_count,
            "legacy_bridge_row_count": legacy_bridge_row_count,
            "selected_row_job_ids": selected_row_job_ids,
            "coverage_expected_branch_days": len(expected_branch_days),
            "coverage_logged_branch_days": len(logged_branch_days_by_job.get(job.id, set())) if job is not None else 0,
            "coverage_summary_branch_days": int((job.result_summary or {}).get("branch_days_processed") or 0) if job is not None else 0,
            "coverage_no_aplica_branch_days": tuple(
                sorted(
                    f"{branch_external_id}:{sale_date.isoformat()}"
                    for branch_external_id, sale_date in no_aplica_branch_days_by_job.get(job.id, set())
                )
            ) if job is not None else (),
            "coverage_missing_branch_days": tuple(
                sorted(
                    f"{branch_external_id}:{sale_date.isoformat()}"
                    for branch_external_id, sale_date in (
                        expected_branch_days - logged_branch_days_by_job.get(job.id, set()) if job is not None else expected_branch_days
                    )
                )
            ),
            "materialized_bridge_reconciled": materialized_bridge_reconciled,
            "bridge_unresolved_row_count": bridge_unresolved_count,
            "rejected_provenance": tuple(rejected_provenance),
        }, unresolved

    @staticmethod
    def _sales_meta(
        meta: Mapping[str, object],
        *,
        configured_source_mode: str,
        fallback_chain: tuple[str, ...],
        selection_reason: str,
        remote_refresh_requested: bool,
        authoritative: bool,
    ) -> dict[str, object]:
        return {
            **meta,
            "configured_source_mode": configured_source_mode,
            "selected_source": meta.get("mode", ""),
            "fallback_chain_attempted": fallback_chain,
            "selection_reason": selection_reason,
            "remote_refresh_requested": remote_refresh_requested,
            "authoritative": authoritative,
        }

    def _load_official_sales(self, *, month_start: date, month_end: date):
        report = self.official_sales_report_service.fetch_report(
            start_date=month_start,
            end_date=month_end,
            branch_external_id=None,
            branch_display_name=None,
            credito=None,
        )
        parsed = self.official_sales_report_service.parse_report(report_path=report.report_path)
        if not hasattr(parsed, "rows") or not isinstance(parsed.rows, list):
            raise _OfficialSalesReportError(
                ISSUE_OFFICIAL_REPORT_INVALID,
                "El reporte oficial mensual no contiene una estructura de filas valida.",
            )
        if not parsed.rows:
            raise _OfficialSalesReportError(
                ISSUE_OFFICIAL_REPORT_EMPTY,
                "El reporte oficial mensual no contiene filas y no puede asumirse como venta cero.",
            )
        values: dict[int, tuple[Decimal, int]] = {}
        unresolved: list[MonthlyPointUnresolvedMovement] = []
        for index, source_row in enumerate(parsed.rows, start=1):
            if not isinstance(source_row, Mapping):
                raise _OfficialSalesReportError(
                    ISSUE_OFFICIAL_REPORT_INVALID,
                    f"La fila oficial {index} no tiene estructura valida.",
                )
            code = str(source_row.get("Codigo") or "").strip()
            name = str(source_row.get("Nombre") or "").strip()
            quantity = Decimal(str(source_row.get("Cantidad") or ZERO))
            receta = self._match_recipe(code=code, name=name)
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
            "source_present": True,
            "remote_refresh_requested": True,
            "unresolved_rows": len(unresolved),
            "summary": {key: str(value) for key, value in parsed.summary.items()},
        }, unresolved

    def _load_daily_sales(self, *, month_start: date, month_end: date):
        rows = list(
            PointDailySale.objects.filter(
                sale_date__gte=month_start,
                sale_date__lte=month_end,
                source_endpoint=OFFICIAL_POINT_DAILY_SOURCE,
            )
            .select_related("product", "branch")
            .only(
                "id",
                "receta_id",
                "quantity",
                "sale_date",
                "product_id",
                "product__sku",
                "product__name",
                "branch_id",
                "branch__external_id",
                "branch__name",
                "branch__erp_branch_id",
                "sync_job_id",
            )
            .order_by("id")
        )
        matched_rows = [row for row in rows if row.receta_id is not None]
        unresolved = [
            MonthlyPointUnresolvedMovement(
                source="official_daily_sales",
                movement_id=str(row.id),
                item_code=row.product.sku,
                item_name=row.product.name,
                quantity=Decimal(row.quantity),
                issue=ISSUE_DAILY_SALE_UNRESOLVED,
                branch_external_id=row.branch.external_id,
                branch_name=row.branch.name,
                movement_date=row.sale_date,
            )
            for row in rows
            if row.receta_id is None
        ]
        return self._aggregate_rows(matched_rows, "quantity"), unresolved, len(rows), rows

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
                self._build_conversion_cache,
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

    def _match_recipe(self, *, code: str, name: str) -> Receta | None:
        key = ((code or "").strip().casefold(), (name or "").strip().casefold())
        if key not in self._build_match_cache:
            self._build_match_cache[key] = self.matcher.resolve_receta(
                codigo_point=code,
                point_name=name,
            )
        return self._build_match_cache[key]

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
