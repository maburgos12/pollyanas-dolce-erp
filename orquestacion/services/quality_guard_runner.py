from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from orquestacion.services.pointdailysale_guard import GuardScanResult, scan_pointdailysale_usage
from orquestacion.services.protected_sales_reader_guard import (
    ProtectedSalesReaderScanResult,
    scan_protected_sales_reader_usage,
)
from orquestacion.services.quality_findings import (
    sync_pointdailysale_guard_findings,
    sync_protected_sales_reader_findings,
    sync_sales_publication_gap_finding,
)
from orquestacion.services.sales_publication_guard import SalesPublicationGapScanResult, scan_sales_publication_gap


@dataclass(frozen=True)
class QualityGuardRunResult:
    base_dir: Path
    point_scan: GuardScanResult
    protected_scan: ProtectedSalesReaderScanResult
    publication_gap_scan: SalesPublicationGapScanResult

    @property
    def blocking_violations(self) -> int:
        return len(self.point_scan.violations) + len(self.protected_scan.violations)

    @property
    def has_blocking_violations(self) -> bool:
        return self.blocking_violations > 0


def run_quality_guards(
    *,
    base_dir: str | Path,
    reference_date: date | None = None,
) -> QualityGuardRunResult:
    root = Path(base_dir).resolve()
    return QualityGuardRunResult(
        base_dir=root,
        point_scan=scan_pointdailysale_usage(base_dir=root),
        protected_scan=scan_protected_sales_reader_usage(base_dir=root),
        publication_gap_scan=scan_sales_publication_gap(reference_date=reference_date),
    )


def sync_quality_guards(run_result: QualityGuardRunResult) -> dict[str, object]:
    point_summary = sync_pointdailysale_guard_findings(scan_result=run_result.point_scan)
    protected_summary = sync_protected_sales_reader_findings(scan_result=run_result.protected_scan)
    publication_summary = sync_sales_publication_gap_finding(gap_result=run_result.publication_gap_scan)
    return {
        "pointdailysale": point_summary,
        "protected_sales_reader": protected_summary,
        "sales_publication_gap": publication_summary,
        "blocking_violations": run_result.blocking_violations,
        "has_blocking_violations": run_result.has_blocking_violations,
    }
