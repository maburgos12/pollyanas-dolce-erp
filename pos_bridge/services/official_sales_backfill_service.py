from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import time

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from core.cache_versions import bump_cache_scopes
from pos_bridge.models import (
    PointBranch,
    PointDailySale,
    PointExtractionLog,
    PointProduct,
    PointSalesQualityAlert,
    PointSyncJob,
)
from reportes.analytics_service import mark_analytics_dirty_for_range
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.services.sales_materialization_repair_service import BridgeSalesMaterializationRepairService
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.exceptions import ExtractionError
from pos_bridge.utils.dates import iter_business_dates
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

    @staticmethod
    def _is_no_aplica_por_apertura(branch: PointBranch, sale_date: date) -> bool:
        erp_branch = getattr(branch, "erp_branch", None)
        if erp_branch is None:
            return False
        return not erp_branch.esta_operativa(sale_date)

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

    def _fetch_branch_day_reports_with_retry(
        self,
        *,
        branch: PointBranch,
        sale_date: date,
        credito_scopes: list[str],
        sync_job: PointSyncJob,
        session_cache: dict[str, object],
    ) -> tuple[list[tuple[str, list[dict]]], list[str]]:
        attempts = max(int(getattr(self.sync_service.settings, "retry_attempts", 1) or 1), 1)
        last_exc: Exception | None = None

        def _create_session_with_fallback():
            primary_exc: Exception | None = None
            try:
                return self.report_service.http_session_service.create(
                    branch_external_id=branch.external_id,
                    branch_display_name=branch.name,
                )
            except Exception as exc:  # noqa: BLE001
                primary_exc = exc
                self.sync_service.record_log(
                    sync_job,
                    PointExtractionLog.LEVEL_WARNING,
                    f"Autenticación Point por sucursal falló para {branch.external_id} {sale_date.isoformat()}, se reintenta con sesión genérica.",
                    context={
                        "branch": branch.name,
                        "branch_external_id": branch.external_id,
                        "sale_date": sale_date.isoformat(),
                        "error": str(exc),
                    },
                )
            try:
                return self.report_service.http_session_service.create(
                    branch_external_id=None,
                    branch_display_name=branch.name,
                )
            except Exception as fallback_exc:  # noqa: BLE001
                if primary_exc is not None:
                    raise primary_exc
                raise fallback_exc

        for attempt in range(1, attempts + 1):
            try:
                parsed_reports: list[tuple[str, list[dict]]] = []
                raw_paths: list[str] = []
                auth_session = session_cache.get(branch.external_id)
                if auth_session is None:
                    auth_session = _create_session_with_fallback()
                    session_cache[branch.external_id] = auth_session
                list_available_branches = getattr(self.report_service, "list_available_branches_with_session", None)
                if callable(list_available_branches):
                    report_branch_catalog_key = f"__report_branch_catalog__::{branch.external_id}"
                    available_report_branches = session_cache.get(report_branch_catalog_key)
                    if available_report_branches is None:
                        available_report_branches = {
                            str(item.get("external_id") or "").strip()
                            for item in list_available_branches(auth_session=auth_session)
                            if str(item.get("external_id") or "").strip()
                        }
                        session_cache[report_branch_catalog_key] = available_report_branches
                    if branch.external_id not in available_report_branches:
                        raise ExtractionError(
                            f"Point no expone la sucursal {branch.external_id} ({branch.name}) en Report/Get_Sucursales.",
                            context={
                                "branch": branch.name,
                                "branch_external_id": branch.external_id,
                                "sale_date": sale_date.isoformat(),
                                "available_branch_ids": sorted(available_report_branches),
                            },
                        )
                for credito in credito_scopes:
                    report = self.report_service.fetch_report_with_session(
                        auth_session=auth_session,
                        start_date=sale_date,
                        end_date=sale_date,
                        branch_external_id=branch.external_id,
                        branch_display_name=branch.name,
                        credito=None if credito == "null" else credito,
                    )
                    parsed = self.report_service.parse_report(report_path=report.report_path)
                    parsed_reports.append((credito, parsed.rows))
                    raw_paths.append(report.report_path)
                return parsed_reports, raw_paths
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                stale_session = session_cache.pop(branch.external_id, None)
                try:
                    if stale_session is not None:
                        stale_session.session.close()
                except Exception:  # noqa: BLE001
                    pass
                if attempt >= attempts:
                    break
                self.sync_service.record_log(
                    sync_job,
                    PointExtractionLog.LEVEL_WARNING,
                    f"Retry backfill oficial {branch.external_id} {sale_date.isoformat()} intento {attempt}/{attempts}.",
                    context={
                        "branch": branch.name,
                        "branch_external_id": branch.external_id,
                        "sale_date": sale_date.isoformat(),
                        "attempt": attempt,
                        "max_attempts": attempts,
                        "error": str(exc),
                    },
                )
                time.sleep(min(attempt, 3))

        if last_exc is None:
            raise RuntimeError("No se pudo obtener reporte oficial y no se generó excepción explícita.")
        raise last_exc

    @transaction.atomic
    def _replace_branch_day_sales(
        self,
        *,
        branch: PointBranch,
        sale_date: date,
        sync_job: PointSyncJob,
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
                    sync_job=sync_job,
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
            bump_cache_scopes("ventas", "dashboard")
            mark_analytics_dirty_for_range(
                start_date=min(row.sale_date for row in rows_to_create),
                end_date=max(row.sale_date for row in rows_to_create),
                include_sales=True,
                include_production=True,
                include_forecast=True,
                reason="official_sales_backfill_service",
            )
        return deleted, len(rows_to_create)

    def run(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None = None,
        credito_scopes: list[str] | None = None,
        excluded_ranges: list[tuple[date, date]] | None = None,
        max_days: int | None = None,
        triggered_by=None,
    ) -> PointSyncJob:
        credito_scopes = credito_scopes or ["null"]
        parameters = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "credito_scopes": credito_scopes,
            "source": "POINT_OFFICIAL_REPORT",
            "excluded_ranges": [(start.isoformat(), end.isoformat()) for start, end in (excluded_ranges or [])],
            "max_days": max_days,
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
            session_cache: dict[str, object] = {}
            summary = {
                "branch_days_processed": 0,
                "failed_branch_days": 0,
                "rows_imported": 0,
                "rows_deleted": 0,
                "indicator_rows_created": 0,
                "indicator_rows_updated": 0,
                "reports_downloaded": 0,
                "raw_exports": [],
                "failures": [],
            }
            sale_dates = iter_business_dates(start_date, end_date, excluded_ranges=excluded_ranges)
            if max_days is not None:
                sale_dates = sale_dates[: max(int(max_days), 0)]
            for sale_date in sale_dates:
                for branch in branches:
                    if self._is_no_aplica_por_apertura(branch, sale_date):
                        summary.setdefault("no_aplica_por_apertura_branch_days", 0)
                        summary["no_aplica_por_apertura_branch_days"] += 1
                        self.sync_service.record_log(
                            sync_job,
                            PointExtractionLog.LEVEL_INFO,
                            f"Backfill oficial no aplica por apertura para {branch.external_id} {sale_date.isoformat()}.",
                            context={
                                "branch": branch.name,
                                "branch_external_id": branch.external_id,
                                "sale_date": sale_date.isoformat(),
                                "status": "NO_APLICA_POR_APERTURA",
                                "reason": "Sucursal todavía no operativa en esta fecha.",
                            },
                        )
                        continue
                    try:
                        parsed_reports, raw_paths = self._fetch_branch_day_reports_with_retry(
                            branch=branch,
                            sale_date=sale_date,
                            credito_scopes=credito_scopes,
                            sync_job=sync_job,
                            session_cache=session_cache,
                        )
                        aggregated = self._aggregate_report_rows(parsed_reports)
                        deleted, imported = self._replace_branch_day_sales(
                            branch=branch,
                            sale_date=sale_date,
                            sync_job=sync_job,
                            aggregated_rows=aggregated,
                        )
                        indicator_payload = None
                        indicator_created = False
                        try:
                            indicator_payload = self.indicator_service.fetch_branch_day(branch=branch, indicator_date=sale_date)
                            _, indicator_created = self.indicator_service.persist_branch_day(
                                indicator_payload=indicator_payload,
                                sync_job=sync_job,
                            )
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
                        summary["indicator_rows_updated"] += 1 if indicator_payload is not None and not indicator_created else 0
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
                    except Exception as exc:
                        PointSalesQualityAlert.objects.create(
                            sync_job=sync_job,
                            branch=branch,
                            alert_type="OFFICIAL_BACKFILL_EXTRACTION_ERROR",
                            severity=PointSalesQualityAlert.SEVERITY_CRITICAL,
                            sucursal=branch.name,
                            fecha=sale_date,
                            detalle=str(exc),
                            payload_json={
                                "branch_external_id": branch.external_id,
                                "source": "POINT_OFFICIAL_REPORT",
                                "credito_scopes": credito_scopes,
                            },
                        )
                        self.sync_service.record_log(
                            sync_job,
                            PointExtractionLog.LEVEL_WARNING,
                            f"Backfill oficial omitido para {branch.external_id} {sale_date.isoformat()} por error de extracción.",
                            context={
                                "branch": branch.name,
                                "branch_external_id": branch.external_id,
                                "sale_date": sale_date.isoformat(),
                                "error": str(exc),
                            },
                        )
                        summary["failed_branch_days"] += 1
                        if len(summary["failures"]) < 50:
                            summary["failures"].append(
                                {
                                    "branch_external_id": branch.external_id,
                                    "branch": branch.name,
                                    "sale_date": sale_date.isoformat(),
                                    "error": str(exc),
                                }
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
            if summary["branch_days_processed"] == 0 and summary["failed_branch_days"] > 0:
                raise RuntimeError("No se pudo importar ningún branch-day desde Point oficial.")
            if summary["failed_branch_days"] > 0:
                return self.sync_service.mark_partial(
                    sync_job,
                    summary,
                    warning_message=(
                        f"Backfill oficial completado con {summary['failed_branch_days']} branch-day(s) omitidos por error."
                    ),
                )
            return self.sync_service.mark_success(sync_job, summary)
        except Exception as exc:
            return self.sync_service.mark_failure(sync_job, exc)
        finally:
            for auth_session in locals().get("session_cache", {}).values():
                try:
                    auth_session.session.close()
                except Exception:  # noqa: BLE001
                    pass
