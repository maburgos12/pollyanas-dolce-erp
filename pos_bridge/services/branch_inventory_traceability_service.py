from __future__ import annotations

from calendar import monthrange
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import MappingProxyType

from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
    PointProductionLine,
    PointWasteLine,
)
from pos_bridge.models.product import _normalize_name
from ventas.services.sales_canonical_source import official_point_sales_rows_for_range

ZERO = Decimal("0")
TRACE_SOURCE_NAMES = (
    "opening",
    "closing",
    "sales",
    "production",
    "waste",
    "transfers",
    "conversions",
    "adjustments",
)


@dataclass(frozen=True)
class TraceSourceIssue:
    code: str
    message: str
    branch_id: int | None = None
    product_id: int | None = None
    source_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class BranchProductBalance:
    branch: PointBranch
    product: PointProduct
    opening: Decimal
    production: Decimal
    sales: Decimal
    waste: Decimal
    transfer_in: Decimal
    transfer_out: Decimal
    conversion_in: Decimal
    conversion_out: Decimal
    identified_adjustment: Decimal
    expected_closing: Decimal
    point_closing: Decimal
    difference: Decimal
    source_trace: Mapping[str, tuple[int, ...]]
    issues: tuple[TraceSourceIssue, ...]


@dataclass(frozen=True)
class BranchInventoryTraceability:
    month: date
    lines: tuple[BranchProductBalance, ...]
    global_issues: tuple[TraceSourceIssue, ...]
    company_difference: Decimal
    exception_count: int
    source_complete: bool


class BranchInventoryTraceabilityService:
    def build(self, month: date) -> BranchInventoryTraceability:
        month_start = month.replace(day=1)
        opening_date = month_start - timedelta(days=1)
        closing_date = date(
            month_start.year,
            month_start.month,
            monthrange(month_start.year, month_start.month)[1],
        )

        opening_closing = self._select_closing(opening_date)
        point_closing = self._select_closing(closing_date)
        selected_closings = {
            opening_date: opening_closing,
            closing_date: point_closing,
        }
        missing_dates = [
            required_date
            for required_date, selected in selected_closings.items()
            if selected is None
        ]
        if missing_dates:
            issues = tuple(
                TraceSourceIssue(
                    code="SOURCE_INCOMPLETE",
                    message=f"Falta cierre Point verificado para {missing_date.isoformat()}.",
                )
                for missing_date in missing_dates
            )
            return BranchInventoryTraceability(
                month=month_start,
                lines=(),
                global_issues=issues,
                company_difference=ZERO,
                exception_count=0,
                source_complete=False,
            )

        opening = self._load_closing(opening_closing)
        closing = self._load_closing(point_closing)
        incomplete_manifests = [
            (required_date, selected, balances)
            for required_date, selected, balances in (
                (opening_date, opening_closing, opening),
                (closing_date, point_closing, closing),
            )
            if not self._coverage_complete(selected, balances)
        ]
        if incomplete_manifests:
            return BranchInventoryTraceability(
                month=month_start,
                lines=(),
                global_issues=tuple(
                    TraceSourceIssue(
                        code="SOURCE_INCOMPLETE",
                        message=(
                            "El cierre Point verificado tiene cobertura incompleta para "
                            f"{required_date.isoformat()}."
                        ),
                        source_ids=(selected.id,),
                    )
                    for required_date, selected, _balances in incomplete_manifests
                ),
                company_difference=ZERO,
                exception_count=0,
                source_complete=False,
            )

        products = list(PointProduct.objects.all().order_by("id"))
        product_indexes = self._build_product_indexes(products)
        sales, production, waste, movement_issues = self._load_direct_movements(
            month_start=month_start,
            closing_date=closing_date,
            product_indexes=product_indexes,
        )
        keys = sorted(
            opening.keys()
            | closing.keys()
            | sales.keys()
            | production.keys()
            | waste.keys()
        )
        branches = PointBranch.objects.in_bulk({branch_id for branch_id, _ in keys})
        products_by_id = {product.id: product for product in products}

        global_issues = []
        issues_by_key: dict[tuple[int, int], list[TraceSourceIssue]] = {}
        for issue in movement_issues:
            if issue.branch_id is not None and issue.product_id is not None:
                issues_by_key.setdefault(
                    (issue.branch_id, issue.product_id), []
                ).append(issue)
            else:
                global_issues.append(issue)

        lines = []
        for branch_id, product_id in keys:
            opening_stock, opening_ids = opening.get(
                (branch_id, product_id), (ZERO, ())
            )
            point_closing, closing_ids = closing.get(
                (branch_id, product_id), (ZERO, ())
            )
            production_quantity, production_ids = production.get(
                (branch_id, product_id), (ZERO, ())
            )
            sales_quantity, sales_ids = sales.get((branch_id, product_id), (ZERO, ()))
            waste_quantity, waste_ids = waste.get((branch_id, product_id), (ZERO, ()))
            expected_closing = (
                opening_stock + production_quantity - sales_quantity - waste_quantity
            )
            difference = point_closing - expected_closing
            trace_values = {
                "opening": opening_ids,
                "closing": closing_ids,
                "sales": sales_ids,
                "production": production_ids,
                "waste": waste_ids,
                "transfers": (),
                "conversions": (),
                "adjustments": (),
            }
            lines.append(
                BranchProductBalance(
                    branch=branches[branch_id],
                    product=products_by_id[product_id],
                    opening=opening_stock,
                    production=production_quantity,
                    sales=sales_quantity,
                    waste=waste_quantity,
                    transfer_in=ZERO,
                    transfer_out=ZERO,
                    conversion_in=ZERO,
                    conversion_out=ZERO,
                    identified_adjustment=ZERO,
                    expected_closing=expected_closing,
                    point_closing=point_closing,
                    difference=difference,
                    source_trace=MappingProxyType(
                        {
                            source_name: tuple(trace_values[source_name])
                            for source_name in TRACE_SOURCE_NAMES
                        }
                    ),
                    issues=tuple(issues_by_key.get((branch_id, product_id), ())),
                )
            )

        frozen_lines = tuple(lines)
        return BranchInventoryTraceability(
            month=month_start,
            lines=frozen_lines,
            global_issues=tuple(global_issues),
            company_difference=sum((line.difference for line in frozen_lines), ZERO),
            exception_count=sum(line.difference != ZERO for line in frozen_lines),
            source_complete=True,
        )

    @staticmethod
    def _build_product_indexes(
        products: list[PointProduct],
    ) -> Mapping[str, Mapping[object, tuple[int, ...] | int]]:
        by_id = {product.id: product.id for product in products}
        by_external_id = {
            product.external_id.strip(): product.id
            for product in products
            if product.external_id.strip()
        }
        sku_candidates: dict[str, list[int]] = {}
        name_candidates: dict[str, list[int]] = {}
        for product in products:
            sku = product.sku.strip()
            if sku:
                sku_candidates.setdefault(sku, []).append(product.id)
            normalized_name = product.normalized_name.strip()
            if normalized_name:
                name_candidates.setdefault(normalized_name, []).append(product.id)
        return MappingProxyType(
            {
                "id": MappingProxyType(by_id),
                "external_id": MappingProxyType(by_external_id),
                "sku": MappingProxyType(
                    {key: tuple(value) for key, value in sku_candidates.items()}
                ),
                "normalized_name": MappingProxyType(
                    {key: tuple(value) for key, value in name_candidates.items()}
                ),
            }
        )

    def _load_direct_movements(self, *, month_start, closing_date, product_indexes):
        lower_bound, upper_bound = self._month_datetime_bounds(month_start)
        sales_rows = list(
            official_point_sales_rows_for_range(
                start_date=month_start, end_date=closing_date
            )
            .select_related("branch", "product")
            .only("id", "branch_id", "product_id", "quantity")
            .order_by("id")
        )
        production_rows = list(
            PointProductionLine.objects.filter(
                production_date__gte=month_start,
                production_date__lte=closing_date,
            )
            .select_related("branch")
            .only(
                "id",
                "branch_id",
                "item_code",
                "item_name",
                "produced_quantity",
            )
            .order_by("id")
        )
        waste_rows = list(
            PointWasteLine.objects.filter(
                movement_at__gte=lower_bound,
                movement_at__lt=upper_bound,
            )
            .select_related("branch")
            .only("id", "branch_id", "item_code", "item_name", "quantity")
            .order_by("id")
        )

        sales: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]] = {}
        production: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]] = {}
        waste: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]] = {}
        issues: list[TraceSourceIssue] = []

        for row in sales_rows:
            product_id, issue_code = self._resolve_product(row, product_indexes)
            self._record_direct_row(
                balances=sales,
                issues=issues,
                source_name="sales",
                row=row,
                branch_id=row.branch_id,
                product_id=product_id,
                issue_code=issue_code,
                quantity=row.quantity,
            )
        for row in production_rows:
            product_id, issue_code = self._resolve_product(row, product_indexes)
            self._record_direct_row(
                balances=production,
                issues=issues,
                source_name="production",
                row=row,
                branch_id=row.branch_id,
                product_id=product_id,
                issue_code=issue_code,
                quantity=row.produced_quantity,
            )
        for row in waste_rows:
            product_id, issue_code = self._resolve_product(row, product_indexes)
            self._record_direct_row(
                balances=waste,
                issues=issues,
                source_name="waste",
                row=row,
                branch_id=row.branch_id,
                product_id=product_id,
                issue_code=issue_code,
                quantity=row.quantity,
            )
        return sales, production, waste, tuple(issues)

    @staticmethod
    def _resolve_product(row, indexes) -> tuple[int | None, str | None]:
        direct_product_id = getattr(row, "product_id", None)
        if direct_product_id in indexes["id"]:
            return int(direct_product_id), None

        item_code = str(getattr(row, "item_code", "") or "").strip()
        if item_code:
            external_match = indexes["external_id"].get(item_code)
            if external_match is not None:
                return int(external_match), None
            sku_matches = indexes["sku"].get(item_code, ())
            if len(sku_matches) == 1:
                return int(sku_matches[0]), "PRODUCT_RESOLVED_BY_SKU"
            if len(sku_matches) > 1:
                return None, "AMBIGUOUS_PRODUCT"

        normalized_name = _normalize_name(str(getattr(row, "item_name", "") or ""))
        name_matches = indexes["normalized_name"].get(normalized_name, ())
        if len(name_matches) == 1:
            return int(name_matches[0]), "PRODUCT_RESOLVED_BY_NAME"
        if len(name_matches) > 1:
            return None, "AMBIGUOUS_PRODUCT"
        return None, "UNRESOLVED_PRODUCT"

    @staticmethod
    def _record_direct_row(
        *,
        balances,
        issues,
        source_name,
        row,
        branch_id,
        product_id,
        issue_code,
        quantity,
    ) -> None:
        if product_id is None:
            issues.append(
                TraceSourceIssue(
                    code=issue_code or "UNRESOLVED_PRODUCT",
                    message=(
                        f"No fue posible asignar la fila {row.id} de {source_name} "
                        "a un único producto Point."
                    ),
                    branch_id=branch_id,
                    source_ids=(row.id,),
                )
            )
            return
        key = (branch_id, product_id)
        current_quantity, source_ids = balances.get(key, (ZERO, ()))
        balances[key] = (current_quantity + Decimal(quantity), (*source_ids, row.id))
        if issue_code:
            issues.append(
                TraceSourceIssue(
                    code=issue_code,
                    message=(
                        f"La fila {row.id} de {source_name} se asignó por una "
                        "coincidencia secundaria que requiere auditoría."
                    ),
                    branch_id=branch_id,
                    product_id=product_id,
                    source_ids=(row.id,),
                )
            )

    @staticmethod
    def _month_datetime_bounds(month_start: date):
        next_month = (
            date(month_start.year + 1, 1, 1)
            if month_start.month == 12
            else date(month_start.year, month_start.month + 1, 1)
        )
        current_timezone = timezone.get_current_timezone()
        return (
            timezone.make_aware(
                datetime.combine(month_start, time.min),
                current_timezone,
            ),
            timezone.make_aware(
                datetime.combine(next_month, time.min),
                current_timezone,
            ),
        )

    @staticmethod
    def _coverage_complete(
        closing: PointHistoricalInventoryClosing,
        balances: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]],
    ) -> bool:
        expected_branch_ids = {
            int(value) for value in (closing.expected_branch_ids or [])
        }
        expected_product_ids = {
            int(value) for value in (closing.expected_product_ids or [])
        }
        expected_keys = {
            (branch_id, product_id)
            for branch_id in expected_branch_ids
            for product_id in expected_product_ids
        }
        return bool(
            expected_branch_ids
            and expected_product_ids
            and balances.keys() == expected_keys
        )

    @staticmethod
    def _select_closing(
        operational_date: date,
    ) -> PointHistoricalInventoryClosing | None:
        return (
            PointHistoricalInventoryClosing.objects.filter(
                operational_date=operational_date,
                status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
            )
            .order_by("-id")
            .first()
        )

    @staticmethod
    def _load_closing(
        closing: PointHistoricalInventoryClosing,
    ) -> dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]]:
        balances: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]] = {}
        lines = PointHistoricalInventoryClosingLine.objects.filter(
            closing=closing
        ).values_list("id", "branch_id", "product_id", "stock")
        for line_id, branch_id, product_id, line_stock in lines:
            key = (branch_id, product_id)
            stock, source_ids = balances.get(key, (ZERO, ()))
            balances[key] = (stock + line_stock, (*source_ids, line_id))
        return balances
