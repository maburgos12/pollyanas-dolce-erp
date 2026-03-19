from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from django.db.models import Count, Q, Sum
from django.db.models.functions import Coalesce
from django.utils import timezone
from django.utils.text import slugify

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.models import PointDailySale, PointSyncJob


ZERO_DECIMAL = Decimal("0")


@dataclass(slots=True)
class SalesCutoffSnapshot:
    sale_date: date
    branch_filter: str
    row_count: int
    branch_count: int
    total_quantity: Decimal
    total_tickets: int
    gross_amount: Decimal
    discount_amount: Decimal
    total_amount: Decimal
    tax_amount: Decimal
    net_amount: Decimal
    branches: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "sale_date": self.sale_date.isoformat(),
            "branch_filter": self.branch_filter,
            "row_count": self.row_count,
            "branch_count": self.branch_count,
            "total_quantity": str(self.total_quantity),
            "total_tickets": self.total_tickets,
            "gross_amount": str(self.gross_amount),
            "discount_amount": str(self.discount_amount),
            "total_amount": str(self.total_amount),
            "tax_amount": str(self.tax_amount),
            "net_amount": str(self.net_amount),
            "branches": self.branches,
        }


def _decimal_delta(current: str | Decimal | int | float, previous: str | Decimal | int | float) -> str:
    return str(Decimal(str(current)) - Decimal(str(previous)))


def build_probe_delta(previous_snapshot: dict[str, Any] | None, current_snapshot: dict[str, Any]) -> dict[str, Any]:
    if previous_snapshot is None:
        return {
            "status": "BASELINE",
            "is_unchanged": False,
            "changed_fields": [],
            "delta": {},
        }

    delta = {
        "row_count": int(current_snapshot["row_count"]) - int(previous_snapshot["row_count"]),
        "branch_count": int(current_snapshot["branch_count"]) - int(previous_snapshot["branch_count"]),
        "total_quantity": _decimal_delta(current_snapshot["total_quantity"], previous_snapshot["total_quantity"]),
        "total_tickets": int(current_snapshot["total_tickets"]) - int(previous_snapshot["total_tickets"]),
        "gross_amount": _decimal_delta(current_snapshot["gross_amount"], previous_snapshot["gross_amount"]),
        "discount_amount": _decimal_delta(current_snapshot["discount_amount"], previous_snapshot["discount_amount"]),
        "total_amount": _decimal_delta(current_snapshot["total_amount"], previous_snapshot["total_amount"]),
        "tax_amount": _decimal_delta(current_snapshot["tax_amount"], previous_snapshot["tax_amount"]),
        "net_amount": _decimal_delta(current_snapshot["net_amount"], previous_snapshot["net_amount"]),
    }
    changed_fields = [
        field
        for field, value in delta.items()
        if (Decimal(value) != ZERO_DECIMAL if isinstance(value, str) else value != 0)
    ]
    return {
        "status": "UNCHANGED" if not changed_fields else "CHANGED",
        "is_unchanged": not changed_fields,
        "changed_fields": changed_fields,
        "delta": delta,
    }


def summarize_probe_series(probes: list[dict[str, Any]], *, stable_after: int = 2) -> dict[str, Any]:
    stable_after = max(int(stable_after or 2), 1)
    trailing_unchanged = 0
    latest_probe = probes[-1] if probes else None
    latest_status = ((latest_probe or {}).get("comparison") or {}).get("status") or "NO_PROBES"

    for probe in reversed(probes):
        status = (probe.get("comparison") or {}).get("status") or "BASELINE"
        if status == "UNCHANGED":
            trailing_unchanged += 1
            continue
        break

    return {
        "probe_count": len(probes),
        "stable_after_unchanged_probes": stable_after,
        "trailing_unchanged_probes": trailing_unchanged,
        "latest_status": latest_status,
        "is_stable": trailing_unchanged >= stable_after,
        "latest_probe_at": latest_probe.get("captured_at_local") if latest_probe else "",
    }


class PointSalesCutoffService:
    def __init__(self, settings: PointBridgeSettings | None = None):
        self.settings = settings or load_point_bridge_settings()

    @property
    def reports_dir(self) -> Path:
        path = self.settings.storage_root / "reports"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def build_report_path(self, *, sale_date: date, branch_filter: str = "") -> Path:
        branch_suffix = slugify(branch_filter) if branch_filter else "all"
        return self.reports_dir / f"sales_close_validation_{sale_date.isoformat()}_{branch_suffix}.json"

    def summarize_sales(self, *, sale_date: date, branch_filter: str = "") -> SalesCutoffSnapshot:
        queryset = PointDailySale.objects.filter(sale_date=sale_date).select_related("branch")
        branch_filter = branch_filter.strip()
        if branch_filter:
            queryset = queryset.filter(
                Q(branch__external_id__iexact=branch_filter) | Q(branch__name__icontains=branch_filter)
            )

        totals = queryset.aggregate(
            row_count=Count("id"),
            branch_count=Count("branch", distinct=True),
            total_quantity=Coalesce(Sum("quantity"), ZERO_DECIMAL),
            total_tickets=Coalesce(Sum("tickets"), 0),
            gross_amount=Coalesce(Sum("gross_amount"), ZERO_DECIMAL),
            discount_amount=Coalesce(Sum("discount_amount"), ZERO_DECIMAL),
            total_amount=Coalesce(Sum("total_amount"), ZERO_DECIMAL),
            tax_amount=Coalesce(Sum("tax_amount"), ZERO_DECIMAL),
            net_amount=Coalesce(Sum("net_amount"), ZERO_DECIMAL),
        )

        branch_rows = (
            queryset.values("branch__external_id", "branch__name")
            .annotate(
                row_count=Count("id"),
                total_quantity=Coalesce(Sum("quantity"), ZERO_DECIMAL),
                total_tickets=Coalesce(Sum("tickets"), 0),
                total_amount=Coalesce(Sum("total_amount"), ZERO_DECIMAL),
                net_amount=Coalesce(Sum("net_amount"), ZERO_DECIMAL),
            )
            .order_by("branch__name", "branch__external_id")
        )
        branches = [
            {
                "external_id": row["branch__external_id"],
                "name": row["branch__name"],
                "row_count": int(row["row_count"] or 0),
                "total_quantity": str(row["total_quantity"] or ZERO_DECIMAL),
                "total_tickets": int(row["total_tickets"] or 0),
                "total_amount": str(row["total_amount"] or ZERO_DECIMAL),
                "net_amount": str(row["net_amount"] or ZERO_DECIMAL),
            }
            for row in branch_rows
        ]

        return SalesCutoffSnapshot(
            sale_date=sale_date,
            branch_filter=branch_filter,
            row_count=int(totals["row_count"] or 0),
            branch_count=int(totals["branch_count"] or 0),
            total_quantity=totals["total_quantity"] or ZERO_DECIMAL,
            total_tickets=int(totals["total_tickets"] or 0),
            gross_amount=totals["gross_amount"] or ZERO_DECIMAL,
            discount_amount=totals["discount_amount"] or ZERO_DECIMAL,
            total_amount=totals["total_amount"] or ZERO_DECIMAL,
            tax_amount=totals["tax_amount"] or ZERO_DECIMAL,
            net_amount=totals["net_amount"] or ZERO_DECIMAL,
            branches=branches,
        )

    def load_report(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {"probes": [], "analysis": {}}
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save_report(self, path: Path, report: dict[str, Any]) -> None:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)

    def append_probe(
        self,
        *,
        report: dict[str, Any],
        sale_date: date,
        branch_filter: str = "",
        snapshot: SalesCutoffSnapshot,
        sync_job: PointSyncJob | None,
        stable_after: int = 2,
    ) -> dict[str, Any]:
        probes = list(report.get("probes") or [])
        previous_snapshot = (probes[-1] or {}).get("snapshot") if probes else None
        probe = {
            "captured_at_utc": timezone.now().isoformat(),
            "captured_at_local": timezone.localtime(timezone.now()).isoformat(),
            "sync_job_id": sync_job.id if sync_job else None,
            "sync_job_status": sync_job.status if sync_job else "",
            "sync_job_summary": sync_job.result_summary if sync_job else {},
            "snapshot": snapshot.to_dict(),
            "comparison": build_probe_delta(previous_snapshot, snapshot.to_dict()),
        }
        probes.append(probe)
        return {
            "sale_date": sale_date.isoformat(),
            "branch_filter": branch_filter,
            "probes": probes,
            "analysis": summarize_probe_series(probes, stable_after=stable_after),
        }
