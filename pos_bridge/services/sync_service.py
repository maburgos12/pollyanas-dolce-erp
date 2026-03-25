from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from core.models import Sucursal
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import (
    PointBranch,
    PointDailyBranchIndicator,
    PointDailySale,
    PointExtractionLog,
    PointInventorySnapshot,
    PointProduct,
    PointSyncJob,
)
from pos_bridge.services.alert_service import PointAlertService
from pos_bridge.services.inventory_extractor import PointInventoryExtractor
from pos_bridge.services.product_recipe_sync_service import PointProductRecipeSyncService
from pos_bridge.services.recipe_gap_audit_service import PointRecipeGapAuditService
from pos_bridge.services.sales_branch_indicator_service import PointSalesBranchIndicatorService
from pos_bridge.services.sales_extractor import PointSalesExtractor
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.utils.exceptions import PersistenceError, PosBridgeError
from pos_bridge.utils.helpers import normalize_text, sanitize_sensitive_data
from pos_bridge.utils.logger import get_job_logger, get_pos_bridge_logger
from recetas.models import VentaHistorica


LOG_LEVELS = {
    PointExtractionLog.LEVEL_DEBUG: logging.DEBUG,
    PointExtractionLog.LEVEL_INFO: logging.INFO,
    PointExtractionLog.LEVEL_WARNING: logging.WARNING,
    PointExtractionLog.LEVEL_ERROR: logging.ERROR,
}


class PointSyncService:
    SALES_HISTORY_SOURCE = "POINT_BRIDGE_SALES"

    def __init__(
        self,
        extractor: PointInventoryExtractor | None = None,
        sales_extractor: PointSalesExtractor | None = None,
        sales_matcher: PointSalesMatchingService | None = None,
        recipe_sync_service: PointProductRecipeSyncService | None = None,
        recipe_gap_audit_service: PointRecipeGapAuditService | None = None,
    ):
        self.settings = load_point_bridge_settings()
        self.extractor = extractor or PointInventoryExtractor(self.settings)
        self.sales_extractor = sales_extractor or PointSalesExtractor(self.settings)
        self.sales_matcher = sales_matcher or PointSalesMatchingService()
        self.recipe_sync_service = recipe_sync_service or PointProductRecipeSyncService(self.settings)
        self.recipe_gap_audit_service = recipe_gap_audit_service or PointRecipeGapAuditService(self.settings)
        self.sales_indicator_service = PointSalesBranchIndicatorService(self.settings)
        self.logger = get_pos_bridge_logger()
        self.alert_service = PointAlertService()

    def _resolve_erp_branch(self, branch_payload: dict):
        external_id = str(branch_payload.get("external_id") or "").strip()
        name = str(branch_payload.get("name") or "").strip()
        match = None
        if external_id:
            match = Sucursal.objects.filter(codigo__iexact=external_id).first()
        if match is None and name:
            match = Sucursal.objects.filter(nombre__iexact=name).first()
        if match is None and name:
            normalized = normalize_text(name)
            for branch in Sucursal.objects.all().only("id", "nombre"):
                if normalize_text(branch.nombre) == normalized:
                    match = branch
                    break
        return match

    def record_log(self, sync_job: PointSyncJob, level: str, message: str, *, context: dict | None = None) -> None:
        context = sanitize_sensitive_data(context or {})
        PointExtractionLog.objects.create(sync_job=sync_job, level=level, message=message, context=context)
        logger = get_job_logger(sync_job.id)
        logger.log(LOG_LEVELS.get(level, logging.INFO), "%s | %s", message, context)

    def create_job(
        self,
        *,
        job_type: str = PointSyncJob.JOB_TYPE_INVENTORY,
        triggered_by=None,
        parameters: dict | None = None,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        return PointSyncJob.objects.create(
            job_type=job_type,
            status=PointSyncJob.STATUS_RUNNING,
            started_at=timezone.now(),
            triggered_by=triggered_by,
            parameters=parameters or {},
            attempt_count=attempt_count,
        )

    def _upsert_branch(self, payload: dict) -> PointBranch:
        defaults = {
            "name": payload["name"],
            "status": payload["status"],
            "metadata": payload.get("metadata") or {},
            "erp_branch": self._resolve_erp_branch(payload),
            "last_seen_at": timezone.now(),
        }
        branch, _ = PointBranch.objects.update_or_create(
            external_id=payload["external_id"],
            defaults=defaults,
        )
        return branch

    def _upsert_product(self, payload: dict) -> PointProduct:
        defaults = {
            "sku": payload["sku"],
            "name": payload["name"],
            "category": payload["category"],
            "active": True,
            "metadata": payload.get("metadata") or {},
        }
        product, _ = PointProduct.objects.update_or_create(
            external_id=payload["external_id"],
            defaults=defaults,
        )
        return product

    @transaction.atomic
    def persist_branch_inventory(self, sync_job: PointSyncJob, branch_result) -> dict:
        branch = self._upsert_branch(branch_result.branch)
        snapshots_to_create = []
        products_seen = 0

        for row in branch_result.inventory_rows:
            product = self._upsert_product(row)
            products_seen += 1
            snapshots_to_create.append(
                PointInventorySnapshot(
                    branch=branch,
                    product=product,
                    stock=row["stock"],
                    min_stock=row["min_stock"],
                    max_stock=row["max_stock"],
                    captured_at=branch_result.captured_at,
                    sync_job=sync_job,
                    raw_payload=row["raw_payload"],
                )
            )

        PointInventorySnapshot.objects.bulk_create(snapshots_to_create, batch_size=500)
        return {
            "branch_id": branch.id,
            "branch_external_id": branch.external_id,
            "products_seen": products_seen,
            "snapshots_created": len(snapshots_to_create),
        }

    def _upsert_sales_product(self, payload: dict) -> PointProduct:
        metadata = payload.get("metadata") or {}
        family = str(payload.get("family") or "").strip()
        if family:
            metadata = {**metadata, "family": family}
        return self._upsert_product(
            {
                "external_id": payload["external_id"],
                "sku": payload["sku"],
                "name": payload["name"],
                "category": payload["category"],
                "metadata": metadata,
            }
        )

    def _upsert_sales_materialization(
        self,
        *,
        receta,
        sucursal,
        sale_date: date,
        quantity,
        tickets: int,
        total_amount,
    ) -> tuple[int, int]:
        existing_qs = VentaHistorica.objects.filter(
            receta=receta,
            fecha=sale_date,
            fuente=self.SALES_HISTORY_SOURCE,
        )
        if sucursal is not None:
            existing_qs = existing_qs.filter(sucursal=sucursal)
        else:
            existing_qs = existing_qs.filter(sucursal__isnull=True)

        existing = existing_qs.order_by("id").first()
        duplicates = existing_qs.exclude(id=existing.id).count() if existing is not None else 0
        if duplicates:
            existing_qs.exclude(id=existing.id).delete()

        payload = {
            "cantidad": quantity,
            "tickets": max(0, int(tickets or 0)),
            "monto_total": total_amount,
            "fuente": self.SALES_HISTORY_SOURCE,
        }
        if existing is None:
            VentaHistorica.objects.create(receta=receta, sucursal=sucursal, fecha=sale_date, **payload)
            return 1, 0

        for field, value in payload.items():
            setattr(existing, field, value)
        existing.save(update_fields=["cantidad", "tickets", "monto_total", "fuente", "actualizado_en"])
        return 0, 1

    @transaction.atomic
    def persist_daily_sales(self, sync_job: PointSyncJob, sales_result) -> dict:
        branch = self._upsert_branch(sales_result.branch)
        created_sales = 0
        updated_sales = 0
        created_history = 0
        updated_history = 0
        unresolved_recipe_rows = 0
        unresolved_branch_rows = 0
        point_identity_synced = 0
        history_accumulator: dict[tuple[int, int | None, date], dict[str, object]] = {}

        for row in sales_result.sales_rows:
            product = self._upsert_sales_product(row)
            receta = None
            if not self.sales_matcher.is_non_recipe_sale_row(row):
                receta = self.sales_matcher.resolve_receta(codigo_point=row["sku"], point_name=row["name"])
                if receta is not None:
                    point_identity_synced += self.sales_matcher.sync_point_identity(
                        receta=receta,
                        codigo_point=row["sku"],
                        nombre_point=row["name"],
                    )
                else:
                    unresolved_recipe_rows += 1

            defaults = {
                "receta": receta,
                "sync_job": sync_job,
                "quantity": row["quantity"],
                "tickets": max(0, int(row.get("tickets") or 0)),
                "gross_amount": row["gross_amount"],
                "discount_amount": row["discount_amount"],
                "total_amount": row["total_amount"],
                "tax_amount": row["tax_amount"],
                "net_amount": row["net_amount"],
                "source_endpoint": row.get("source_endpoint") or "/Report/VentasCategorias",
                "raw_payload": row["raw_payload"],
            }
            _, created = PointDailySale.objects.update_or_create(
                sale_date=sales_result.sale_date,
                branch=branch,
                product=product,
                defaults=defaults,
            )
            if created:
                created_sales += 1
            else:
                updated_sales += 1

            if receta is None:
                continue
            if branch.erp_branch is None:
                unresolved_branch_rows += 1
                continue
            key = (receta.id, branch.erp_branch.id if branch.erp_branch_id else None, sales_result.sale_date)
            bucket = history_accumulator.setdefault(
                key,
                {
                    "receta": receta,
                    "sucursal": branch.erp_branch,
                    "sale_date": sales_result.sale_date,
                    "quantity": Decimal("0"),
                    "tickets": 0,
                    "total_amount": Decimal("0"),
                },
            )
            bucket["quantity"] = Decimal(str(bucket["quantity"])) + Decimal(str(row["quantity"] or 0))
            bucket["tickets"] = int(bucket["tickets"] or 0) + max(0, int(row.get("tickets") or 0))
            bucket["total_amount"] = Decimal(str(bucket["total_amount"])) + Decimal(
                str(row["total_amount"] or row["gross_amount"] or row["net_amount"] or 0)
            )

        for payload in history_accumulator.values():
            vh_created, vh_updated = self._upsert_sales_materialization(
                receta=payload["receta"],
                sucursal=payload["sucursal"],
                sale_date=payload["sale_date"],
                quantity=payload["quantity"],
                tickets=payload["tickets"],
                total_amount=payload["total_amount"],
            )
            created_history += vh_created
            updated_history += vh_updated

        return {
            "branch_id": branch.id,
            "branch_external_id": branch.external_id,
            "sale_date": sales_result.sale_date.isoformat(),
            "sales_rows": len(sales_result.sales_rows),
            "daily_sales_created": created_sales,
            "daily_sales_updated": updated_sales,
            "historical_sales_created": created_history,
            "historical_sales_updated": updated_history,
            "unresolved_recipe_rows": unresolved_recipe_rows,
            "unresolved_branch_rows": unresolved_branch_rows,
            "point_identity_synced": point_identity_synced,
        }

    def persist_daily_branch_indicator(self, *, sync_job: PointSyncJob, branch: PointBranch, sale_date: date) -> dict:
        try:
            indicator_payload = self.sales_indicator_service.fetch_branch_day(branch=branch, indicator_date=sale_date)
        except PointDailyBranchIndicator.DoesNotExist as exc:
            self.record_log(
                sync_job,
                PointExtractionLog.LEVEL_WARNING,
                "Indicador diario Point no disponible; ventas quedan cargadas sin tickets para este branch-day.",
                context={
                    "branch_external_id": branch.external_id,
                    "branch_name": branch.name,
                    "sale_date": sale_date.isoformat(),
                    "error": str(exc),
                },
            )
            return {
                "indicator_created": 0,
                "indicator_updated": 0,
                "indicator_missing": 1,
                "total_tickets": 0,
                "total_amount": "0",
            }

        _, created = self.sales_indicator_service.persist_branch_day(indicator_payload=indicator_payload, sync_job=sync_job)
        return {
            "indicator_created": 1 if created else 0,
            "indicator_updated": 0 if created else 1,
            "indicator_missing": 0,
            "total_tickets": indicator_payload.total_tickets,
            "total_amount": indicator_payload.total_amount,
        }

    def mark_success(self, sync_job: PointSyncJob, summary: dict) -> PointSyncJob:
        summary = sanitize_sensitive_data(summary)
        sync_job.status = PointSyncJob.STATUS_SUCCESS
        sync_job.finished_at = timezone.now()
        sync_job.result_summary = summary
        sync_job.error_message = ""
        sync_job.save(update_fields=["status", "finished_at", "result_summary", "error_message", "updated_at"])
        log_event(sync_job.triggered_by, "POS_BRIDGE_SYNC_SUCCESS", "pos_bridge.PointSyncJob", str(sync_job.id), payload=summary)
        return sync_job

    def mark_failure(self, sync_job: PointSyncJob, exc: Exception) -> PointSyncJob:
        context = sanitize_sensitive_data(getattr(exc, "context", {}) or {})
        sync_job.status = PointSyncJob.STATUS_FAILED
        sync_job.finished_at = timezone.now()
        sync_job.error_message = str(exc)
        sync_job.artifacts = {**sync_job.artifacts, **context}
        sync_job.save(update_fields=["status", "finished_at", "error_message", "artifacts", "updated_at"])
        self.record_log(sync_job, PointExtractionLog.LEVEL_ERROR, str(exc), context=context)
        self.alert_service.emit_failure(job_id=sync_job.id, message=str(exc), context=context)
        log_event(
            sync_job.triggered_by,
            "POS_BRIDGE_SYNC_FAILED",
            "pos_bridge.PointSyncJob",
            str(sync_job.id),
            payload={"error": str(exc), "context": context},
        )
        return sync_job

    def run_inventory_sync(
        self,
        *,
        triggered_by=None,
        branch_filter: str | None = None,
        limit_branches: int | None = None,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        parameters = {
            "branch_filter": branch_filter or "",
            "limit_branches": limit_branches,
            "settings": self.settings.safe_dict(),
        }
        sync_job = self.create_job(triggered_by=triggered_by, parameters=parameters, attempt_count=attempt_count)
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronización Point inventario.", context=parameters)

        try:
            branch_results = self.extractor.extract(
                branch_filter=branch_filter,
                limit_branches=limit_branches,
            )
            if not branch_results:
                raise PersistenceError("La extracción no devolvió sucursales ni inventario. Ajusta selectores/configuración.")

            summary = {
                "branches_processed": 0,
                "products_seen": 0,
                "snapshots_created": 0,
                "raw_exports": [],
            }
            for branch_result in branch_results:
                branch_summary = self.persist_branch_inventory(sync_job, branch_result)
                summary["branches_processed"] += 1
                summary["products_seen"] += branch_summary["products_seen"]
                summary["snapshots_created"] += branch_summary["snapshots_created"]
                summary["raw_exports"].append(branch_result.raw_export_path)
                self.record_log(
                    sync_job,
                    PointExtractionLog.LEVEL_INFO,
                    f"Sucursal procesada {branch_result.branch['external_id']}.",
                    context=branch_summary,
                )
            return self.mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self.mark_failure(sync_job, exc)
        except Exception as exc:
            wrapped = PersistenceError(f"Error no controlado en sync Point: {exc}")
            return self.mark_failure(sync_job, wrapped)

    def retry_failed_jobs(self, *, limit: int = 5, max_attempts: int | None = None, triggered_by=None) -> list[PointSyncJob]:
        max_attempts = max_attempts or self.settings.retry_attempts
        failed_jobs = list(
            PointSyncJob.objects.filter(status=PointSyncJob.STATUS_FAILED, attempt_count__lt=max_attempts)
            .order_by("started_at")[:limit]
        )
        retried_jobs: list[PointSyncJob] = []
        for failed_job in failed_jobs:
            parameters = failed_job.parameters or {}
            retried_jobs.append(
                self.run_inventory_sync(
                    triggered_by=triggered_by or failed_job.triggered_by,
                    branch_filter=parameters.get("branch_filter") or None,
                    limit_branches=parameters.get("limit_branches"),
                    attempt_count=failed_job.attempt_count + 1,
                )
            )
        return retried_jobs

    def run_sales_sync(
        self,
        *,
        start_date: date,
        end_date: date,
        excluded_ranges: list[tuple[date, date]] | None = None,
        triggered_by=None,
        branch_filter: str | None = None,
        max_days: int | None = None,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        parameters = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "excluded_ranges": [(start.isoformat(), end.isoformat()) for start, end in (excluded_ranges or [])],
            "branch_filter": branch_filter or "",
            "max_days": max_days,
            "settings": self.settings.safe_dict(),
        }
        sync_job = self.create_job(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            triggered_by=triggered_by,
            parameters=parameters,
            attempt_count=attempt_count,
        )
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronización Point ventas.", context=parameters)

        try:
            sales_results = self.sales_extractor.extract(
                start_date=start_date,
                end_date=end_date,
                branch_filter=branch_filter,
                excluded_ranges=excluded_ranges,
                max_days=max_days,
            )
            if not sales_results:
                raise PersistenceError("La extracción de ventas no devolvió filas ni ventanas procesadas.")

            summary = {
                "days_processed": 0,
                "branches_processed": 0,
                "sales_rows_seen": 0,
                "daily_sales_created": 0,
                "daily_sales_updated": 0,
                "daily_indicators_created": 0,
                "daily_indicators_updated": 0,
                "historical_sales_created": 0,
                "historical_sales_updated": 0,
                "unresolved_recipe_rows": 0,
                "unresolved_branch_rows": 0,
                "point_identity_synced": 0,
                "raw_exports": [],
            }
            seen_branch_days: set[tuple[str, str]] = set()
            for sales_result in sales_results:
                sales_summary = self.persist_daily_sales(sync_job, sales_result)
                branch = PointBranch.objects.get(external_id=sales_result.branch["external_id"])
                indicator_summary = self.persist_daily_branch_indicator(
                    sync_job=sync_job,
                    branch=branch,
                    sale_date=sales_result.sale_date,
                )
                branch_day_key = (sales_result.branch["external_id"], sales_result.sale_date.isoformat())
                seen_branch_days.add(branch_day_key)
                summary["sales_rows_seen"] += sales_summary["sales_rows"]
                summary["daily_sales_created"] += sales_summary["daily_sales_created"]
                summary["daily_sales_updated"] += sales_summary["daily_sales_updated"]
                summary["daily_indicators_created"] += indicator_summary["indicator_created"]
                summary["daily_indicators_updated"] += indicator_summary["indicator_updated"]
                summary["daily_indicators_missing"] = summary.get("daily_indicators_missing", 0) + indicator_summary.get("indicator_missing", 0)
                summary["historical_sales_created"] += sales_summary["historical_sales_created"]
                summary["historical_sales_updated"] += sales_summary["historical_sales_updated"]
                summary["unresolved_recipe_rows"] += sales_summary["unresolved_recipe_rows"]
                summary["unresolved_branch_rows"] += sales_summary["unresolved_branch_rows"]
                summary["point_identity_synced"] += sales_summary["point_identity_synced"]
                summary["raw_exports"].append(sales_result.raw_export_path)
                self.record_log(
                    sync_job,
                    PointExtractionLog.LEVEL_INFO,
                    f"Ventas procesadas {sales_result.branch['external_id']} {sales_result.sale_date.isoformat()}.",
                    context={**sales_summary, **indicator_summary},
                )
            summary["branches_processed"] = len({branch for branch, _ in seen_branch_days})
            summary["days_processed"] = len({day for _, day in seen_branch_days})
            return self.mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self.mark_failure(sync_job, exc)
        except Exception as exc:
            wrapped = PersistenceError(f"Error no controlado en sync histórico de ventas Point: {exc}")
            return self.mark_failure(sync_job, wrapped)

    def run_product_recipe_sync(
        self,
        *,
        triggered_by=None,
        branch_hint: str | None = None,
        product_codes: list[str] | None = None,
        limit: int | None = None,
        include_without_recipe: bool = False,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        parameters = {
            "branch_hint": branch_hint or "",
            "product_codes": product_codes or [],
            "limit": limit,
            "include_without_recipe": include_without_recipe,
            "settings": self.settings.safe_dict(),
        }
        sync_job = self.create_job(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            triggered_by=triggered_by,
            parameters=parameters,
            attempt_count=attempt_count,
        )
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronización Point recetas de productos.", context=parameters)

        try:
            result = self.recipe_sync_service.sync(
                branch_hint=branch_hint,
                product_codes=product_codes,
                limit=limit,
                include_without_recipe=include_without_recipe,
                sync_job=sync_job,
            )
            summary = {
                **result.summary,
                "raw_exports": [result.raw_export_path],
            }
            self.record_log(
                sync_job,
                PointExtractionLog.LEVEL_INFO,
                "Sincronización de recetas Point completada.",
                context=summary,
            )
            return self.mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self.mark_failure(sync_job, exc)
        except Exception as exc:
            wrapped = PersistenceError(f"Error no controlado en sync de recetas Point: {exc}")
            return self.mark_failure(sync_job, wrapped)

    def run_recipe_gap_audit(
        self,
        *,
        triggered_by=None,
        branch_hint: str | None = None,
        product_codes: list[str] | None = None,
        limit: int | None = None,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        parameters = {
            "mode": "recipe_gap_audit",
            "branch_hint": branch_hint or "",
            "product_codes": product_codes or [],
            "limit": limit,
            "settings": self.settings.safe_dict(),
        }
        sync_job = self.create_job(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            triggered_by=triggered_by,
            parameters=parameters,
            attempt_count=attempt_count,
        )
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de auditoría Point de recetas faltantes.", context=parameters)

        try:
            result = self.recipe_gap_audit_service.audit(
                branch_hint=branch_hint,
                product_codes=product_codes,
                limit=limit,
            )
            summary = {
                **result.summary,
                "report_path": result.report_path,
                "raw_exports": [result.raw_export_path],
            }
            self.record_log(
                sync_job,
                PointExtractionLog.LEVEL_INFO,
                "Auditoría Point de recetas faltantes completada.",
                context=summary,
            )
            return self.mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self.mark_failure(sync_job, exc)
        except Exception as exc:
            wrapped = PersistenceError(f"Error no controlado en auditoría Point de recetas faltantes: {exc}")
            return self.mark_failure(sync_job, wrapped)
