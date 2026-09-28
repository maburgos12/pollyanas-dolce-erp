from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
)


ZERO = Decimal("0")


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
        keys = sorted(opening.keys() | closing.keys())
        branches = PointBranch.objects.in_bulk({branch_id for branch_id, _ in keys})
        products = PointProduct.objects.in_bulk({product_id for _, product_id in keys})

        lines = []
        for branch_id, product_id in keys:
            opening_stock, opening_ids = opening.get((branch_id, product_id), (ZERO, ()))
            point_closing, closing_ids = closing.get((branch_id, product_id), (ZERO, ()))
            expected_closing = opening_stock
            difference = point_closing - expected_closing
            lines.append(
                BranchProductBalance(
                    branch=branches[branch_id],
                    product=products[product_id],
                    opening=opening_stock,
                    production=ZERO,
                    sales=ZERO,
                    waste=ZERO,
                    transfer_in=ZERO,
                    transfer_out=ZERO,
                    conversion_in=ZERO,
                    conversion_out=ZERO,
                    identified_adjustment=ZERO,
                    expected_closing=expected_closing,
                    point_closing=point_closing,
                    difference=difference,
                    source_trace=MappingProxyType(
                        {"opening": opening_ids, "closing": closing_ids}
                    ),
                    issues=(),
                )
            )

        frozen_lines = tuple(lines)
        return BranchInventoryTraceability(
            month=month_start,
            lines=frozen_lines,
            global_issues=(),
            company_difference=sum((line.difference for line in frozen_lines), ZERO),
            exception_count=sum(line.difference != ZERO for line in frozen_lines),
            source_complete=True,
        )

    @staticmethod
    def _coverage_complete(
        closing: PointHistoricalInventoryClosing,
        balances: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]],
    ) -> bool:
        expected_branch_ids = {int(value) for value in (closing.expected_branch_ids or [])}
        expected_product_ids = {int(value) for value in (closing.expected_product_ids or [])}
        expected_keys = {
            (branch_id, product_id)
            for branch_id in expected_branch_ids
            for product_id in expected_product_ids
        }
        return bool(expected_branch_ids and expected_product_ids and balances.keys() == expected_keys)

    @staticmethod
    def _select_closing(operational_date: date) -> PointHistoricalInventoryClosing | None:
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
        lines = PointHistoricalInventoryClosingLine.objects.filter(closing=closing).values_list(
            "id", "branch_id", "product_id", "stock"
        )
        for line_id, branch_id, product_id, line_stock in lines:
            key = (branch_id, product_id)
            stock, source_ids = balances.get(key, (ZERO, ()))
            balances[key] = (stock + line_stock, (*source_ids, line_id))
        return balances
