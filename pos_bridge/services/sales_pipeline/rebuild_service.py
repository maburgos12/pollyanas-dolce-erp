from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path

from django.db import transaction
from django.db.models import Count, Sum
from django.utils import timezone

from core.cache_versions import bump_cache_scopes
from pos_bridge.models import (
    PointDailySale,
    PointExtractionLog,
    PointProduct,
    PointSalesDailyCategoryFact,
    PointSalesDailyProductFact,
    PointSalesExtractionTask,
    PointSalesNormalized,
    PointSalesQualityAlert,
    PointSalesRawStaging,
    PointSyncJob,
)
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.services.sales_pipeline.queue_service import PointSalesTaskQueueService
from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService
from pos_bridge.services.sync_service import PointSyncService
from pos_bridge.utils.helpers import decimal_from_value, deterministic_id, sanitize_sensitive_data
from reportes.analytics_service import mark_analytics_dirty_for_range
from recetas.models import RecetaCodigoPointAlias, normalizar_codigo_point
from ventas.models import VentaAutoritativaPoint


class PointSalesRebuildService:
    PIPELINE_CODE = PointSalesTaskQueueService.PIPELINE_CODE

    def __init__(
        self,
        report_service: PointSalesCategoryReportService | None = None,
        sync_service: PointSyncService | None = None,
        matcher: PointSalesMatchingService | None = None,
        queue_service: PointSalesTaskQueueService | None = None,
    ):
        self.report_service = report_service or PointSalesCategoryReportService()
        self.sync_service = sync_service or PointSyncService()
        self.matcher = matcher or PointSalesMatchingService()
        self.queue_service = queue_service or PointSalesTaskQueueService()

    def create_backfill_job(
        self,
        *,
        start_date,
        end_date,
        branch_filter: str | None = None,
        credito_scope: str = "null",
        triggered_by=None,
    ) -> PointSyncJob:
        self.queue_service.ensure_single_running_job()
        parameters = {
            "pipeline_code": self.PIPELINE_CODE,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "credito_scope": credito_scope,
            "source_mode": PointSalesExtractionTask.SOURCE_MODE_OFFICIAL,
        }
        sync_job = self.sync_service.create_job(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            triggered_by=triggered_by,
            parameters=parameters,
            attempt_count=1,
        )
        planned = self.queue_service.plan_tasks(
            sync_job=sync_job,
            start_date=start_date,
            end_date=end_date,
            branch_filter=branch_filter,
            credito_scope=credito_scope,
        )
        self.sync_service.record_log(
            sync_job,
            PointExtractionLog.LEVEL_INFO,
            "Job de reconstrucción Point v2 planeado.",
            context={**parameters, "tasks_planned": planned},
        )
        return sync_job

    def get_job(self, *, job_id: int) -> PointSyncJob:
        return PointSyncJob.objects.get(pk=job_id)

    def _create_session_with_fallback(self, *, task: PointSalesExtractionTask):
        primary_exc: Exception | None = None
        try:
            return self.report_service.http_session_service.create(
                branch_external_id=task.branch.external_id if task.branch_id else None,
                branch_display_name=task.branch.name if task.branch_id else "",
            )
        except Exception as exc:  # noqa: BLE001
            primary_exc = exc
            self.sync_service.record_log(
                task.sync_job,
                PointExtractionLog.LEVEL_WARNING,
                "Autenticación Point por sucursal falló; se reintenta con sesión genérica.",
                context={
                    "task_id": task.id,
                    "branch": task.branch.name if task.branch_id else "",
                    "branch_external_id": task.branch.external_id if task.branch_id else "",
                    "sale_date": task.sale_date.isoformat(),
                    "error": str(exc),
                },
            )
        try:
            return self.report_service.http_session_service.create(
                branch_external_id=None,
                branch_display_name=task.branch.name if task.branch_id else "",
            )
        except Exception as fallback_exc:  # noqa: BLE001
            if primary_exc is not None:
                raise primary_exc
            raise fallback_exc

    @staticmethod
    def _sha256_file(path: str) -> str:
        digest = hashlib.sha256()
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _row_hash(*, task: PointSalesExtractionTask, row_number: int, row: dict) -> str:
        serialized = json.dumps(
            {
                "task_id": task.id,
                "sale_date": task.sale_date.isoformat(),
                "branch_external_id": task.branch.external_id if task.branch_id else "",
                "row_number": row_number,
                "row": row,
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _resolve_point_product(self, *, sku: str, name: str, category: str) -> PointProduct:
        product = (
            PointProduct.objects.filter(sku__iexact=sku, name__iexact=name).order_by("id").first()
            or PointProduct.objects.filter(sku__iexact=sku).order_by("id").first()
        )
        external_id = product.external_id if product is not None else f"official:{sku or deterministic_id(name, category)}"
        defaults = {
            "sku": sku,
            "name": name or sku,
            "category": category,
            "active": True,
            "metadata": {"official_report_v2": True},
        }
        product, _ = PointProduct.objects.update_or_create(external_id=external_id, defaults=defaults)
        return product

    def _resolve_match_status(self, *, receta, sku: str, point_name: str, payload: dict) -> str:
        if self.matcher.is_non_recipe_sale_row(payload):
            return PointSalesNormalized.MATCH_NON_RECIPE
        if receta is None:
            return PointSalesNormalized.MATCH_SIN_CATALOGO
        if sku and (receta.codigo_point or "").strip().lower() == sku.strip().lower():
            return PointSalesNormalized.MATCH_EXACT_CODE
        code_norm = normalizar_codigo_point(sku or "")
        if code_norm and RecetaCodigoPointAlias.objects.filter(receta=receta, codigo_point_normalizado=code_norm, activo=True).exists():
            return PointSalesNormalized.MATCH_ALIAS
        return PointSalesNormalized.MATCH_NAME

    def _fetch_report_with_retry(
        self,
        *,
        task: PointSalesExtractionTask,
        session_cache: dict[str, object],
    ):
        attempts = max(int(getattr(self.sync_service.settings, "retry_attempts", 1) or 1), 1)
        last_exc: Exception | None = None
        task_key = task.branch.external_id if task.branch_id else "generic"
        for attempt in range(1, attempts + 1):
            try:
                auth_session = session_cache.get(task_key)
                if auth_session is None:
                    auth_session = self._create_session_with_fallback(task=task)
                    session_cache[task_key] = auth_session
                start_fetch = time.perf_counter()
                report = self.report_service.fetch_report_with_session(
                    auth_session=auth_session,
                    start_date=task.sale_date,
                    end_date=task.sale_date,
                    branch_external_id=task.branch.external_id if task.branch_id else None,
                    branch_display_name=task.branch.name if task.branch_id else None,
                    credito=None if task.credito_scope == "null" else task.credito_scope,
                )
                fetch_ms = int((time.perf_counter() - start_fetch) * 1000)
                return report, fetch_ms
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                stale_session = session_cache.pop(task_key, None)
                try:
                    if stale_session is not None:
                        stale_session.session.close()
                except Exception:  # noqa: BLE001
                    pass
                if attempt >= attempts:
                    break
                self.sync_service.record_log(
                    task.sync_job,
                    PointExtractionLog.LEVEL_WARNING,
                    "Retry de extracción oficial Point v2.",
                    context={
                        "task_id": task.id,
                        "attempt": attempt,
                        "max_attempts": attempts,
                        "branch": task.branch.name if task.branch_id else "",
                        "branch_external_id": task.branch.external_id if task.branch_id else "",
                        "sale_date": task.sale_date.isoformat(),
                        "error": str(exc),
                    },
                )
                time.sleep(min(attempt, 3))
        if last_exc is None:
            raise RuntimeError("La extracción oficial Point v2 falló sin excepción explícita.")
        raise last_exc

    def _replace_task_rows(
        self,
        *,
        task: PointSalesExtractionTask,
        parsed_rows: list[dict],
        source_file: str,
        source_hash: str,
        extracted_at,
    ) -> tuple[list[PointSalesRawStaging], list[PointSalesNormalized]]:
        PointSalesNormalized.objects.filter(task=task).delete()
        PointSalesRawStaging.objects.filter(task=task).delete()

        raw_objects: list[PointSalesRawStaging] = []
        for row_number, row in enumerate(parsed_rows, start=1):
            payload = {
                "Categoria": str(row.get("Categoria") or "").strip(),
                "Codigo": str(row.get("Codigo") or "").strip(),
                "Nombre": str(row.get("Nombre") or "").strip(),
                "Cantidad": str(row.get("Cantidad") or ""),
                "Bruto": str(row.get("Bruto") or ""),
                "Descuento": str(row.get("Descuento") or ""),
                "Venta": str(row.get("Venta") or ""),
                "IVA": str(row.get("IVA") or ""),
                "Venta_neta": str(row.get("Venta_neta") or ""),
            }
            raw_objects.append(
                PointSalesRawStaging(
                    sync_job=task.sync_job,
                    task=task,
                    row_number=row_number,
                    source_mode=task.source_mode,
                    source_endpoint=task.source_endpoint,
                    source_file=source_file,
                    source_hash=source_hash,
                    fecha_extraccion=extracted_at,
                    credito_scope=task.credito_scope,
                    sucursal_raw=task.branch.name if task.branch_id else "",
                    fecha_raw=task.sale_date.isoformat(),
                    categoria_raw=payload["Categoria"],
                    codigo_raw=payload["Codigo"],
                    producto_raw=payload["Nombre"],
                    total_cantidad_raw=payload["Cantidad"],
                    total_descuento_raw=payload["Descuento"],
                    total_venta_raw=payload["Venta"],
                    total_impuestos_raw=payload["IVA"],
                    total_venta_neta_raw=payload["Venta_neta"],
                    payload_original_json=payload,
                    row_hash=self._row_hash(task=task, row_number=row_number, row=payload),
                )
            )
        if raw_objects:
            PointSalesRawStaging.objects.bulk_create(raw_objects, batch_size=1000)
        raw_rows = list(PointSalesRawStaging.objects.filter(task=task).order_by("row_number", "id"))

        normalized_objects: list[PointSalesNormalized] = []
        for raw_row in raw_rows:
            payload = raw_row.payload_original_json or {}
            sku = str(raw_row.codigo_raw or "").strip()
            point_name = str(raw_row.producto_raw or sku).strip()
            category = str(raw_row.categoria_raw or "").strip()
            point_product = self._resolve_point_product(sku=sku, name=point_name, category=category)
            receta = None
            matching_payload = {
                "sku": sku,
                "name": point_name,
                "category": category,
                "Codigo": sku,
                "Nombre": point_name,
                "Categoria": category,
            }
            if not self.matcher.is_non_recipe_sale_row(matching_payload):
                receta = self.matcher.resolve_receta(codigo_point=sku, point_name=point_name)
            match_status = self._resolve_match_status(
                receta=receta,
                sku=sku,
                point_name=point_name,
                payload=matching_payload,
            )
            normalized_objects.append(
                PointSalesNormalized(
                    sync_job=task.sync_job,
                    task=task,
                    raw_row=raw_row,
                    branch=task.branch,
                    sucursal_nombre=task.branch.name if task.branch_id else raw_row.sucursal_raw,
                    fecha=task.sale_date,
                    categoria=category,
                    producto_nombre_historico=point_name,
                    point_product=point_product,
                    receta=receta,
                    match_catalogo_status=match_status,
                    total_cantidad=decimal_from_value(payload.get("Cantidad")),
                    total_descuento=decimal_from_value(payload.get("Descuento")),
                    total_venta=decimal_from_value(payload.get("Venta")),
                    total_impuestos=decimal_from_value(payload.get("IVA")),
                    total_venta_neta=decimal_from_value(payload.get("Venta_neta")),
                    source_hash=source_hash,
                    source_file=source_file,
                    credito_scope=task.credito_scope,
                    extracted_at=extracted_at,
                    normalized_at=timezone.now(),
                    payload_normalized_json={
                        "sku": sku,
                        "point_name": point_name,
                        "category": category,
                        "match_status": match_status,
                        "receta_id": receta.id if receta is not None else None,
                        "point_product_id": point_product.id if point_product is not None else None,
                    },
                )
            )
        if normalized_objects:
            PointSalesNormalized.objects.bulk_create(normalized_objects, batch_size=1000)
        normalized_rows = list(
            PointSalesNormalized.objects.filter(task=task)
            .select_related("receta", "point_product", "raw_row")
            .order_by("id")
        )
        return raw_rows, normalized_rows

    def _replace_day_facts(self, *, task: PointSalesExtractionTask, normalized_rows: list[PointSalesNormalized]) -> tuple[int, int]:
        PointSalesDailyCategoryFact.objects.filter(branch=task.branch, sale_date=task.sale_date).delete()
        PointSalesDailyProductFact.objects.filter(branch=task.branch, sale_date=task.sale_date).delete()

        if not normalized_rows:
            return 0, 0

        product_facts = [
            PointSalesDailyProductFact(
                branch=row.branch,
                sync_job=task.sync_job,
                sale_date=row.fecha,
                sucursal_nombre=row.sucursal_nombre,
                categoria=row.categoria,
                producto_nombre_historico=row.producto_nombre_historico,
                point_product=row.point_product,
                receta=row.receta,
                match_catalogo_status=row.match_catalogo_status,
                total_cantidad=row.total_cantidad,
                total_descuento=row.total_descuento,
                total_venta=row.total_venta,
                total_impuestos=row.total_impuestos,
                total_venta_neta=row.total_venta_neta,
                source_hash=row.source_hash,
                source_file=row.source_file,
                extracted_at=row.extracted_at,
                normalized_at=row.normalized_at,
            )
            for row in normalized_rows
        ]
        PointSalesDailyProductFact.objects.bulk_create(product_facts, batch_size=1000)

        category_totals: dict[str, dict] = defaultdict(
            lambda: {
                "cantidad": decimal_from_value(0),
                "descuento": decimal_from_value(0),
                "venta": decimal_from_value(0),
                "impuestos": decimal_from_value(0),
                "venta_neta": decimal_from_value(0),
                "detail_rows": 0,
                "source_hash": "",
                "first_extracted_at": None,
                "last_extracted_at": None,
            }
        )
        for row in normalized_rows:
            bucket = category_totals[row.categoria]
            bucket["cantidad"] += row.total_cantidad
            bucket["descuento"] += row.total_descuento
            bucket["venta"] += row.total_venta
            bucket["impuestos"] += row.total_impuestos
            bucket["venta_neta"] += row.total_venta_neta
            bucket["detail_rows"] += 1
            bucket["source_hash"] = row.source_hash
            bucket["first_extracted_at"] = min(filter(None, [bucket["first_extracted_at"], row.extracted_at]), default=row.extracted_at)
            bucket["last_extracted_at"] = max(filter(None, [bucket["last_extracted_at"], row.extracted_at]), default=row.extracted_at)

        category_facts = [
            PointSalesDailyCategoryFact(
                branch=task.branch,
                sync_job=task.sync_job,
                sale_date=task.sale_date,
                sucursal_nombre=task.branch.name if task.branch_id else "",
                categoria=category,
                total_cantidad=payload["cantidad"],
                total_descuento=payload["descuento"],
                total_venta=payload["venta"],
                total_impuestos=payload["impuestos"],
                total_venta_neta=payload["venta_neta"],
                detail_row_count=payload["detail_rows"],
                last_source_hash=payload["source_hash"],
                first_extracted_at=payload["first_extracted_at"],
                last_extracted_at=payload["last_extracted_at"],
            )
            for category, payload in sorted(category_totals.items())
        ]
        PointSalesDailyCategoryFact.objects.bulk_create(category_facts, batch_size=500)
        if product_facts or category_facts:
            bump_cache_scopes("ventas", "dashboard")
            affected_days = [row.sale_date for row in product_facts] + [row.sale_date for row in category_facts]
            if affected_days:
                mark_analytics_dirty_for_range(
                    start_date=min(affected_days),
                    end_date=max(affected_days),
                    include_sales=True,
                    include_production=True,
                    include_forecast=True,
                    reason="sales_pipeline_fact_rebuild",
                )
        return len(category_facts), len(product_facts)

    def _replace_authoritative_rows(self, *, task: PointSalesExtractionTask, normalized_rows: list[PointSalesNormalized]) -> int:
        erp_branch = getattr(task.branch, "erp_branch", None)
        if erp_branch is None:
            return 0
        VentaAutoritativaPoint.objects.filter(branch=erp_branch, sale_date=task.sale_date).delete()
        if not normalized_rows:
            return 0
        rows = [
            VentaAutoritativaPoint(
                branch=erp_branch,
                product=row.receta,
                sale_date=row.fecha,
                product_code=row.raw_row.codigo_raw,
                point_name=row.producto_nombre_historico,
                category=row.categoria,
                quantity=row.total_cantidad,
                gross_amount=decimal_from_value((row.raw_row.payload_original_json or {}).get("Bruto"), default=str(row.total_venta + row.total_descuento)),
                discount_amount=row.total_descuento,
                total_amount=row.total_venta,
                tax_amount=row.total_impuestos,
                net_amount=row.total_venta_neta,
                source_file=row.source_file,
                source_sheet="category_report",
                raw_payload={
                    "source_hash": row.source_hash,
                    "match_catalogo_status": row.match_catalogo_status,
                    "task_id": task.id,
                    "sync_job_id": task.sync_job_id,
                },
                imported_at=timezone.now(),
            )
            for row in normalized_rows
        ]
        VentaAutoritativaPoint.objects.bulk_create(rows, batch_size=1000)
        bump_cache_scopes("ventas", "dashboard")
        if rows:
            mark_analytics_dirty_for_range(
                start_date=min(row.sale_date for row in rows),
                end_date=max(row.sale_date for row in rows),
                include_sales=True,
                include_production=True,
                include_forecast=True,
                reason="sales_pipeline_authoritative_rebuild",
            )
        return len(rows)

    @transaction.atomic
    def _persist_successful_task(
        self,
        *,
        task: PointSalesExtractionTask,
        parsed_rows: list[dict],
        source_file: str,
        source_hash: str,
        extracted_at,
        timings_ms: dict,
        summary_json: dict,
        promote_authoritative: bool,
    ) -> dict:
        raw_rows, normalized_rows = self._replace_task_rows(
            task=task,
            parsed_rows=parsed_rows,
            source_file=source_file,
            source_hash=source_hash,
            extracted_at=extracted_at,
        )
        PointSalesQualityAlert.objects.filter(task=task, alert_type="EXTRACTION_ERROR").delete()
        category_fact_count, product_fact_count = self._replace_day_facts(task=task, normalized_rows=normalized_rows)
        authoritative_rows = self._replace_authoritative_rows(task=task, normalized_rows=normalized_rows) if promote_authoritative else 0
        task.status = PointSalesExtractionTask.STATUS_SUCCESS
        task.finished_at = timezone.now()
        task.extracted_at = extracted_at
        task.source_file = source_file
        task.source_hash = source_hash
        task.row_count = len(raw_rows)
        task.timings_ms = timings_ms
        task.summary_json = {
            **sanitize_sensitive_data(summary_json),
            "raw_rows": len(raw_rows),
            "normalized_rows": len(normalized_rows),
            "category_fact_count": category_fact_count,
            "product_fact_count": product_fact_count,
            "authoritative_rows": authoritative_rows,
            "unmatched_rows": sum(1 for row in normalized_rows if row.match_catalogo_status == PointSalesNormalized.MATCH_SIN_CATALOGO),
        }
        task.save(
            update_fields=[
                "status",
                "finished_at",
                "extracted_at",
                "source_file",
                "source_hash",
                "row_count",
                "timings_ms",
                "summary_json",
                "updated_at",
            ]
        )
        return task.summary_json

    def process_task(
        self,
        *,
        task: PointSalesExtractionTask,
        session_cache: dict[str, object],
        promote_authoritative: bool = True,
    ) -> dict:
        started = time.perf_counter()
        timings_ms: dict[str, int] = {}
        try:
            report, fetch_ms = self._fetch_report_with_retry(task=task, session_cache=session_cache)
            timings_ms["download"] = fetch_ms
            if not Path(report.report_path).exists():
                raise RuntimeError(f"Point devolvió una ruta inexistente: {report.report_path}")
            file_size = Path(report.report_path).stat().st_size
            if file_size <= 0:
                raise RuntimeError(f"Descarga incompleta o vacía: {report.report_path}")

            hash_started = time.perf_counter()
            source_hash = self._sha256_file(report.report_path)
            timings_ms["checksum"] = int((time.perf_counter() - hash_started) * 1000)

            parse_started = time.perf_counter()
            parsed = self.report_service.parse_report(report_path=report.report_path)
            timings_ms["parse"] = int((time.perf_counter() - parse_started) * 1000)

            extracted_at = timezone.now()
            persist_started = time.perf_counter()
            summary = self._persist_successful_task(
                task=task,
                parsed_rows=parsed.rows,
                source_file=report.report_path,
                source_hash=source_hash,
                extracted_at=extracted_at,
                timings_ms=timings_ms,
                summary_json={
                    "report_path": report.report_path,
                    "request_url": report.request_url,
                    "report_summary": parsed.summary,
                    "file_size_bytes": file_size,
                },
                promote_authoritative=promote_authoritative,
            )
            timings_ms["persist"] = int((time.perf_counter() - persist_started) * 1000)
            timings_ms["total"] = int((time.perf_counter() - started) * 1000)
            task.timings_ms = timings_ms
            task.save(update_fields=["timings_ms", "updated_at"])
            self.sync_service.record_log(
                task.sync_job,
                PointExtractionLog.LEVEL_INFO,
                "Task Point v2 procesada correctamente.",
                context={
                    "task_id": task.id,
                    "branch": task.branch.name if task.branch_id else "",
                    "branch_external_id": task.branch.external_id if task.branch_id else "",
                    "sale_date": task.sale_date.isoformat(),
                    **summary,
                    "timings_ms": timings_ms,
                },
            )
            return summary
        except Exception as exc:  # noqa: BLE001
            timings_ms["total"] = int((time.perf_counter() - started) * 1000)
            task.status = PointSalesExtractionTask.STATUS_FAILED
            task.finished_at = timezone.now()
            task.last_error = str(exc)
            task.timings_ms = timings_ms
            task.save(update_fields=["status", "finished_at", "last_error", "timings_ms", "updated_at"])
            PointSalesQualityAlert.objects.create(
                sync_job=task.sync_job,
                task=task,
                branch=task.branch,
                alert_type="EXTRACTION_ERROR",
                severity=PointSalesQualityAlert.SEVERITY_CRITICAL,
                sucursal=task.branch.name if task.branch_id else "",
                fecha=task.sale_date,
                detalle=str(exc),
                payload_json={"task_id": task.id, "timings_ms": timings_ms},
            )
            self.sync_service.record_log(
                task.sync_job,
                PointExtractionLog.LEVEL_ERROR,
                "Task Point v2 falló.",
                context={
                    "task_id": task.id,
                    "branch": task.branch.name if task.branch_id else "",
                    "branch_external_id": task.branch.external_id if task.branch_id else "",
                    "sale_date": task.sale_date.isoformat(),
                    "error": str(exc),
                    "timings_ms": timings_ms,
                },
            )
            raise

    def build_job_summary(self, *, sync_job: PointSyncJob) -> dict:
        task_counts = {
            item["status"]: item["count"]
            for item in PointSalesExtractionTask.objects.filter(sync_job=sync_job)
            .values("status")
            .annotate(count=Count("id"))
        }
        total_tasks = sum(task_counts.values())
        category_rows = PointSalesDailyCategoryFact.objects.filter(sync_job=sync_job).count()
        product_rows = PointSalesDailyProductFact.objects.filter(sync_job=sync_job).count()
        unmatched_rows = PointSalesNormalized.objects.filter(
            sync_job=sync_job,
            match_catalogo_status=PointSalesNormalized.MATCH_SIN_CATALOGO,
        ).count()
        duplicate_raw_rows = (
            PointSalesRawStaging.objects.filter(sync_job=sync_job)
            .values("task_id", "row_hash")
            .annotate(count=Count("id"))
            .filter(count__gt=1)
            .count()
        )
        return {
            "pipeline_code": self.PIPELINE_CODE,
            "total_tasks": total_tasks,
            "tasks_pending": task_counts.get(PointSalesExtractionTask.STATUS_PENDING, 0),
            "tasks_running": task_counts.get(PointSalesExtractionTask.STATUS_RUNNING, 0),
            "tasks_success": task_counts.get(PointSalesExtractionTask.STATUS_SUCCESS, 0),
            "tasks_failed": task_counts.get(PointSalesExtractionTask.STATUS_FAILED, 0),
            "tasks_skipped": task_counts.get(PointSalesExtractionTask.STATUS_SKIPPED, 0),
            "category_fact_rows": category_rows,
            "product_fact_rows": product_rows,
            "unmatched_rows": unmatched_rows,
            "duplicate_raw_rows": duplicate_raw_rows,
            "quality_alerts": PointSalesQualityAlert.objects.filter(sync_job=sync_job).count(),
        }

    def _emit_unmatched_catalog_alert(self, *, sync_job: PointSyncJob) -> None:
        PointSalesQualityAlert.objects.filter(sync_job=sync_job, alert_type="UNMATCHED_CATALOG_ROWS").delete()
        unmatched_qs = PointSalesNormalized.objects.filter(
            sync_job=sync_job,
            match_catalogo_status=PointSalesNormalized.MATCH_SIN_CATALOGO,
        )
        unmatched_rows = unmatched_qs.count()
        if unmatched_rows <= 0:
            return

        total_rows = PointSalesNormalized.objects.filter(sync_job=sync_job).count()
        unmatched_total = decimal_from_value(unmatched_qs.aggregate(total=Sum("total_venta")).get("total"))
        pct_unmatched = round((unmatched_rows / total_rows * 100), 2) if total_rows else 0
        top_unmatched = list(
            unmatched_qs.values("point_product__sku", "producto_nombre_historico", "categoria")
            .annotate(rows=Count("id"), total_venta=Sum("total_venta"))
            .order_by("-total_venta", "producto_nombre_historico")[:10]
        )
        branch_ids = list(
            unmatched_qs.exclude(branch__isnull=True).values_list("branch_id", flat=True).distinct()[:2]
        )
        branch = None
        sucursal = ""
        if len(branch_ids) == 1:
            sample_task = (
                PointSalesExtractionTask.objects.filter(sync_job=sync_job, branch_id=branch_ids[0])
                .select_related("branch")
                .first()
            )
            if sample_task and sample_task.branch_id:
                branch = sample_task.branch
                sucursal = sample_task.branch.name

        severity = (
            PointSalesQualityAlert.SEVERITY_CRITICAL
            if pct_unmatched >= 5
            else PointSalesQualityAlert.SEVERITY_WARNING
        )
        PointSalesQualityAlert.objects.create(
            sync_job=sync_job,
            branch=branch,
            alert_type="UNMATCHED_CATALOG_ROWS",
            severity=severity,
            sucursal=sucursal,
            detalle=(
                f"Se detectaron {unmatched_rows} renglones sin match de catálogo "
                f"({pct_unmatched}% del detalle reconstruido, venta={unmatched_total})."
            ),
            payload_json={
                "unmatched_rows": unmatched_rows,
                "total_normalized_rows": total_rows,
                "percentage_unmatched": pct_unmatched,
                "unmatched_total_venta": str(unmatched_total),
                "top_unmatched_products": [
                    {
                        **row,
                        "total_venta": str(decimal_from_value(row.get("total_venta"))),
                    }
                    for row in top_unmatched
                ],
            },
        )
        self.sync_service.record_log(
            sync_job,
            PointExtractionLog.LEVEL_WARNING if severity == PointSalesQualityAlert.SEVERITY_WARNING else PointExtractionLog.LEVEL_ERROR,
            "Reconstrucción Point v2 detectó renglones sin match de catálogo.",
            context={
                "unmatched_rows": unmatched_rows,
                "total_normalized_rows": total_rows,
                "percentage_unmatched": pct_unmatched,
                "unmatched_total_venta": str(unmatched_total),
            },
        )

    def finalize_job_if_complete(self, *, sync_job: PointSyncJob) -> PointSyncJob:
        task_counts = {
            item["status"]: item["count"]
            for item in PointSalesExtractionTask.objects.filter(sync_job=sync_job)
            .values("status")
            .annotate(count=Count("id"))
        }
        if task_counts.get(PointSalesExtractionTask.STATUS_PENDING, 0) > 0 or task_counts.get(PointSalesExtractionTask.STATUS_RUNNING, 0) > 0:
            summary = self.build_job_summary(sync_job=sync_job)
            sync_job.result_summary = summary
            sync_job.save(update_fields=["result_summary", "updated_at"])
            return sync_job
        self._emit_unmatched_catalog_alert(sync_job=sync_job)
        summary = self.build_job_summary(sync_job=sync_job)
        if summary["tasks_success"] == 0 and summary["tasks_failed"] > 0:
            exc = RuntimeError("La reconstrucción Point v2 terminó sin tasks exitosas.")
            return self.sync_service.mark_failure(sync_job, exc)
        if summary["tasks_failed"] > 0:
            return self.sync_service.mark_partial(
                sync_job,
                summary,
                warning_message=f"Reconstrucción Point v2 completada con {summary['tasks_failed']} task(s) fallidas.",
            )
        return self.sync_service.mark_success(sync_job, summary)

    def run_worker(
        self,
        *,
        sync_job: PointSyncJob,
        worker_name: str,
        batch_size: int = 10,
        max_tasks: int | None = None,
        promote_authoritative: bool = True,
        stale_after_minutes: int = 60,
    ) -> PointSyncJob:
        self.queue_service.requeue_stale_tasks(sync_job=sync_job, stale_after_minutes=stale_after_minutes)
        processed = 0
        session_cache: dict[str, object] = {}
        try:
            while True:
                if max_tasks is not None and processed >= max(int(max_tasks), 0):
                    break
                remaining = None if max_tasks is None else max(int(max_tasks) - processed, 0)
                claim_limit = batch_size if remaining is None else min(batch_size, remaining)
                if claim_limit <= 0:
                    break
                tasks = self.queue_service.claim_tasks(sync_job=sync_job, worker_name=worker_name, limit=claim_limit)
                if not tasks:
                    break
                for task in tasks:
                    try:
                        self.process_task(task=task, session_cache=session_cache, promote_authoritative=promote_authoritative)
                    except Exception:  # noqa: BLE001
                        pass
                    finally:
                        processed += 1
        finally:
            for auth_session in session_cache.values():
                try:
                    auth_session.session.close()
                except Exception:  # noqa: BLE001
                    pass
        return self.finalize_job_if_complete(sync_job=sync_job)

    def promote_detail_to_legacy_history(
        self,
        *,
        sync_job: PointSyncJob,
        source_label: str = "POINT_BRIDGE_SALES_V2",
    ) -> dict:
        from recetas.models import VentaHistorica

        detail_rows = (
            PointSalesDailyProductFact.objects.filter(sync_job=sync_job, receta__isnull=False, branch__erp_branch__isnull=False)
            .select_related("receta", "branch__erp_branch")
            .order_by("sale_date", "branch_id", "receta_id")
        )
        grouped: dict[tuple[int, int, object], dict] = defaultdict(
            lambda: {
                "cantidad": decimal_from_value(0),
                "monto_total": decimal_from_value(0),
            }
        )
        for row in detail_rows:
            key = (row.receta_id, row.branch.erp_branch_id, row.sale_date)
            grouped[key]["cantidad"] += row.total_cantidad
            grouped[key]["monto_total"] += row.total_venta

        created = 0
        updated = 0
        for (receta_id, sucursal_id, sale_date), payload in grouped.items():
            obj, was_created = VentaHistorica.objects.update_or_create(
                receta_id=receta_id,
                sucursal_id=sucursal_id,
                fecha=sale_date,
                fuente=source_label,
                defaults={
                    "cantidad": payload["cantidad"],
                    "monto_total": payload["monto_total"],
                    "tickets": 0,
                },
            )
            if was_created:
                created += 1
            else:
                updated += 1
        return {"created": created, "updated": updated, "source_label": source_label}

    def compare_with_legacy_point_daily_sales(self, *, start_date, end_date) -> list[dict]:
        legacy_rows = (
            PointDailySale.objects.filter(sale_date__range=(start_date, end_date))
            .values("branch__name", "sale_date")
            .annotate(total_venta_neta=Sum("net_amount"))
            .order_by("sale_date", "branch__name")
        )
        rebuilt_rows = (
            PointSalesDailyCategoryFact.objects.filter(sale_date__range=(start_date, end_date))
            .values("sucursal_nombre", "sale_date")
            .annotate(total_venta_neta=Sum("total_venta_neta"))
            .order_by("sale_date", "sucursal_nombre")
        )
        rebuilt_map = {(row["sucursal_nombre"], row["sale_date"]): row["total_venta_neta"] for row in rebuilt_rows}
        differences: list[dict] = []
        for row in legacy_rows:
            key = (row["branch__name"], row["sale_date"])
            rebuilt_total = rebuilt_map.get(key, decimal_from_value(0))
            legacy_total = decimal_from_value(row["total_venta_neta"])
            delta = rebuilt_total - legacy_total
            if delta != decimal_from_value(0):
                differences.append(
                    {
                        "sucursal": row["branch__name"],
                        "fecha": row["sale_date"].isoformat(),
                        "legacy_total_venta_neta": str(legacy_total),
                        "rebuilt_total_venta_neta": str(rebuilt_total),
                        "delta": str(delta),
                    }
                )
        return differences
