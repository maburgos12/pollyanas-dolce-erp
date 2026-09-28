from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

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
    source_trace: dict[str, tuple[int, ...]]
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

        missing_dates = [
            required_date
            for required_date in (opening_date, closing_date)
            if not PointHistoricalInventoryClosing.objects.filter(
                operational_date=required_date,
                status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
            ).exists()
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

        opening = self._load_closing(opening_date)
        closing = self._load_closing(closing_date)
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
                    source_trace={"opening": opening_ids, "closing": closing_ids},
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
    def _load_closing(
        operational_date: date,
    ) -> dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]]:
        balances: dict[tuple[int, int], tuple[Decimal, tuple[int, ...]]] = {}
        lines = PointHistoricalInventoryClosingLine.objects.filter(
            closing__operational_date=operational_date,
            closing__status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
        ).select_related("branch", "product")
        for line in lines:
            key = (line.branch_id, line.product_id)
            stock, source_ids = balances.get(key, (ZERO, ()))
            balances[key] = (stock + line.stock, (*source_ids, line.id))
        return balances
