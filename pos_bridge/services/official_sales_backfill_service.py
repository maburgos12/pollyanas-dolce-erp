from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from pos_bridge.models import PointBranch, PointDailySale, PointExtractionLog, PointProduct, PointSyncJob
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.services.sales_materialization_repair_service import BridgeSalesMaterializationRepairService
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.helpers import deterministic_id
from pos_bridge.utils.logger import get_job_logger


@dataclass
class OfficialSalesBackfillResult:
    branch_days_processed: int
    rows_imported: int
    rows_deleted: int
    reports_downloaded: int
    bridge_history_deleted: int
    bridge_history_created: int
    recipe_rows_updated: int
    recipe_rows_cleared: int
    unresolved_rows: int
    non_recipe_rows: int
    raw_exports: list[str]


class OfficialSalesBackfillService:
    OFFICIAL_SOURCE_ENDPOINT = "/Report/PrintReportes?idreporte=3"

    def __init__(
        self,
        report_service: PointSalesCategoryReportService | None = None,
        sync_service: PointSyncService | None = None,
        matcher: PointSalesMatchingService | None = None,
        repair_service: BridgeSalesMaterializationRepairService | None = None,
        indicator_service: PointSalesBranchIndicatorService | None = None,
    ):
        self.report_service = report_service or PointSalesCategoryReportService()
        self.sync_service = sync_service or PointSyncService()
        self.matcher = matcher or PointSalesMatchingService()
        self.repair_service = repair_service or BridgeSalesMaterializationRepairService(self.matcher)
        self.indicator_service = indicator_service or PointSalesBranchIndicatorService()

    def _iter_dates(self, *, start_date: date, end_date: date):
        current = start_date
        while current <= end_date:
            yield current
            current += timedelta(days=1)

    def _sales_branches(self, *, branch_filter: str | None = None) -> list[PointBranch]:
        excluded = {item.strip().lower() for item in self.sync_service.settings.sales_excluded_branches if item}
        branches = []
        for branch in self.indicator_service.canonical_branches(branch_filter=branch_filter):
            if branch.name.strip().lower() in excluded:
                continue
            branches.append(branch)
        return branches

    def _resolve_product(self, *, sku: str, name: str, category: str) -> PointProduct:
        product = (
            PointProduct.objects.filter(sku__iexact=sku, name__iexact=name).order_by("id").first()
            or PointProduct.objects.filter(sku__iexact=sku).order_by("id").first()
        )
        external_id = product.external_id if product is not None else f"official:{sku or deterministic_id(name, category)}"
        defaults = {
            "sku": sku,
            "name": name,
            "category": category,
            "active": True,
            "metadata": {"official_report": True},
        }
        product, _ = PointProduct.objects.update_or_create(external_id=external_id, defaults=defaults)
        return product

    def _aggregate_report_rows(self, parsed_reports: list[tuple[str, list[dict]]]) -> dict[tuple[str, str, str], dict]:
        aggregated: dict[tuple[str, str, str], dict] = {}
        for credito_scope, rows in parsed_reports:
            for row in rows:
                sku = str(row.get("Codigo") or "").strip()
                name = str(row.get("Nombre") or sku).strip()
                category = str(row.get("Categoria") or "").strip()
                key = (sku, name, category)
                bucket = aggregated.setdefault(
                    key,
                    {
                        "sku": sku,
                        "name": name,
                        "category": category,
                        "quantity": Decimal("0"),
                        "gross_amount": Decimal("0"),
                        "discount_amount": Decimal("0"),
                        "total_amount": Decimal("0"),
                        "tax_amount": Decimal("0"),
                        "net_amount": Decimal("0"),
                        "scopes": set(),
                    },
                )
                bucket["quantity"] += Decimal(str(row.get("Cantidad") or 0))
                bucket["gross_amount"] += Decimal(str(row.get("Bruto") or 0))
                bucket["discount_amount"] += Decimal(str(row.get("Descuento") or 0))
                bucket["total_amount"] += Decimal(str(row.get("Venta") or 0))
                bucket["tax_amount"] += Decimal(str(row.get("IVA") or 0))
                bucket["net_amount"] += Decimal(str(row.get("Venta_neta") or 0))
                bucket["scopes"].add(credito_scope)
        return aggregated

    @transaction.atomic
    def _replace_branch_day_sales(
        self,
        *,
        branch: PointBranch,
        sale_date: date,
        aggregated_rows: dict[tuple[str, str, str], dict],
    ) -> tuple[int, int]:
        deleted, _ = PointDailySale.objects.filter(branch=branch, sale_date=sale_date).delete()
        rows_to_create: list[PointDailySale] = []

        for payload in aggregated_rows.values():
            product = self._resolve_product(sku=payload["sku"], name=payload["name"], category=payload["category"])
            raw_payload = {
                "source": "POINT_OFFICIAL_REPORT",
                "credito_scopes": sorted(payload["scopes"]),
                "sku": payload["sku"],
                "name": payload["name"],
                "category": payload["category"],
            }
            match_payload = {
                "sku": payload["sku"],
                "name": payload["name"],
                "category": payload["category"],
                "family": "",
            }
            receta = None
            if not self.matcher.is_non_recipe_sale_row(match_payload):
                receta = self.matcher.resolve_receta(codigo_point=payload["sku"], point_name=payload["name"])

            rows_to_create.append(
                PointDailySale(
                    branch=branch,
                    product=product,
                    receta=receta,
                    sale_date=sale_date,
                    quantity=payload["quantity"],
                    tickets=0,
                    gross_amount=payload["gross_amount"],
                    discount_amount=payload["discount_amount"],
                    total_amount=payload["total_amount"],
                    tax_amount=payload["tax_amount"],
                    net_amount=payload["net_amount"],
                    source_endpoint=self.OFFICIAL_SOURCE_ENDPOINT,
                    raw_payload=raw_payload,
                )
            )

        if rows_to_create:
            PointDailySale.objects.bulk_create(rows_to_create, batch_size=500)
        return deleted, len(rows_to_create)

    def run(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None = None,
        credito_scopes: list[str] | None = None,
        triggered_by=None,
    ) -> PointSyncJob:
        credito_scopes = credito_scopes or ["null"]
        parameters = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "credito_scopes": credito_scopes,
            "source": "POINT_OFFICIAL_REPORT",
        }
        sync_job = self.sync_service.create_job(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            triggered_by=triggered_by,
            parameters=parameters,
            attempt_count=1,
        )
        self.sync_service.record_log(
            sync_job,
            PointExtractionLog.LEVEL_INFO,
            "Inicio de backfill oficial de ventas Point por categoría.",
            context=parameters,
        )

        try:
            branches = self._sales_branches(branch_filter=branch_filter)
            summary = {
                "branch_days_processed": 0,
                "rows_imported": 0,
                "rows_deleted": 0,
                "indicator_rows_created": 0,
                "indicator_rows_updated": 0,
                "reports_downloaded": 0,
                "raw_exports": [],
            }
            for sale_date in self._iter_dates(start_date=start_date, end_date=end_date):
                for branch in branches:
                    parsed_reports: list[tuple[str, list[dict]]] = []
                    raw_paths: list[str] = []
                    for credito in credito_scopes:
                        report = self.report_service.fetch_report(
                            start_date=sale_date,
                            end_date=sale_date,
                            branch_external_id=branch.external_id,
                            branch_display_name=branch.name,
                            credito=None if credito == "null" else credito,
                        )
                        parsed = self.report_service.parse_report(report_path=report.report_path)
                        parsed_reports.append((credito, parsed.rows))
                        raw_paths.append(report.report_path)
                    aggregated = self._aggregate_report_rows(parsed_reports)
                    deleted, imported = self._replace_branch_day_sales(branch=branch, sale_date=sale_date, aggregated_rows=aggregated)
                    indicator_payload = None
                    indicator_created = False
                    try:
                        indicator_payload = self.indicator_service.fetch_branch_day(branch=branch, indicator_date=sale_date)
                        _, indicator_created = self.indicator_service.persist_branch_day(indicator_payload=indicator_payload, sync_job=sync_job)
                    except Exception as exc:
                        self.sync_service.record_log(
                            sync_job,
                            PointExtractionLog.LEVEL_WARNING,
                            f"Indicador diario no disponible para {branch.external_id} {sale_date.isoformat()}; se conserva venta oficial sin tickets.",
                            context={
                                "branch": branch.name,
                                "branch_external_id": branch.external_id,
                                "sale_date": sale_date.isoformat(),
                                "error": str(exc),
                            },
                        )
                    summary["branch_days_processed"] += 1
                    summary["rows_imported"] += imported
                    summary["rows_deleted"] += deleted
                    summary["indicator_rows_created"] += 1 if indicator_created else 0
                    summary["indicator_rows_updated"] += 0 if indicator_created else 1
                    summary["reports_downloaded"] += len(raw_paths)
                    summary["raw_exports"].extend(raw_paths)
                    self.sync_service.record_log(
                        sync_job,
                        PointExtractionLog.LEVEL_INFO,
                        f"Backfill oficial {branch.external_id} {sale_date.isoformat()}",
                        context={
                            "branch": branch.name,
                            "branch_external_id": branch.external_id,
                            "sale_date": sale_date.isoformat(),
                            "rows_imported": imported,
                            "rows_deleted": deleted,
                            "indicator_tickets": indicator_payload.total_tickets if indicator_payload is not None else None,
                            "reports_downloaded": len(raw_paths),
                        },
                    )

            repair = self.repair_service.repair(start_date=start_date, end_date=end_date)
            summary.update(
                {
                    "bridge_history_deleted": repair.bridge_history_deleted,
                    "bridge_history_created": repair.bridge_history_created,
                    "recipe_rows_updated": repair.recipe_rows_updated,
                    "recipe_rows_cleared": repair.recipe_rows_cleared,
                    "unresolved_rows": repair.unresolved_rows,
                    "non_recipe_rows": repair.non_recipe_rows,
                }
            )
            return self.sync_service.mark_success(sync_job, summary)
        except Exception as exc:
            return self.sync_service.mark_failure(sync_job, exc)
