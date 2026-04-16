from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from django.db.models import Count, Max, Min, Sum

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import (
    PointExtractionLog,
    PointSalesDailyCategoryFact,
    PointSalesDailyProductFact,
    PointSalesExtractionTask,
    PointSalesNormalized,
    PointSalesQualityAlert,
    PointSalesRawStaging,
    PointSyncJob,
)
from pos_bridge.utils.helpers import decimal_from_value, write_json_file
from recetas.models import VentaHistorica


class PointSalesValidationService:
    LEGACY_SOURCE = "POINT_BRIDGE_SALES"

    def __init__(self):
        self.settings = load_point_bridge_settings()

    def report_dir(self, *, sync_job: PointSyncJob) -> Path:
        return self.settings.storage_root / "reports" / f"point_sales_rebuild_job_{sync_job.id}"

    def _counts_by_branch_year(self, *, sync_job: PointSyncJob) -> list[dict]:
        rows = (
            PointSalesDailyCategoryFact.objects.filter(sync_job=sync_job)
            .values("sucursal_nombre", "sale_date__year")
            .annotate(record_count=Count("id"))
            .order_by("sucursal_nombre", "sale_date__year")
        )
        return [
            {
                "sucursal": row["sucursal_nombre"],
                "year": row["sale_date__year"],
                "record_count": row["record_count"],
            }
            for row in rows
        ]

    def _neta_by_branch_month(self, *, sync_job: PointSyncJob) -> list[dict]:
        rows = (
            PointSalesDailyCategoryFact.objects.filter(sync_job=sync_job)
            .values("sucursal_nombre", "sale_date__year", "sale_date__month")
            .annotate(total_venta_neta=Sum("total_venta_neta"))
            .order_by("sucursal_nombre", "sale_date__year", "sale_date__month")
        )
        return [
            {
                "sucursal": row["sucursal_nombre"],
                "year": row["sale_date__year"],
                "month": row["sale_date__month"],
                "total_venta_neta": str(decimal_from_value(row["total_venta_neta"])),
            }
            for row in rows
        ]

    def _legacy_vs_rebuilt(self, *, sync_job: PointSyncJob) -> list[dict]:
        parameters = sync_job.parameters or {}
        start_date = parameters.get("start_date")
        end_date = parameters.get("end_date")
        branch_ids = list(
            PointSalesExtractionTask.objects.filter(sync_job=sync_job, branch__erp_branch__isnull=False)
            .values_list("branch__erp_branch_id", flat=True)
            .distinct()
        )
        rebuilt_rows = (
            PointSalesDailyProductFact.objects.filter(sync_job=sync_job, receta__isnull=False, branch__erp_branch__isnull=False)
            .values("receta_id", "branch__erp_branch_id", "sale_date")
            .annotate(
                rebuilt_cantidad=Sum("total_cantidad"),
                rebuilt_total_venta=Sum("total_venta"),
            )
        )
        rebuilt_map = {
            (row["receta_id"], row["branch__erp_branch_id"], row["sale_date"]): {
                "cantidad": decimal_from_value(row["rebuilt_cantidad"]),
                "monto": decimal_from_value(row["rebuilt_total_venta"]),
            }
            for row in rebuilt_rows
        }

        legacy_qs = VentaHistorica.objects.filter(fuente=self.LEGACY_SOURCE)
        if start_date:
            legacy_qs = legacy_qs.filter(fecha__gte=start_date)
        if end_date:
            legacy_qs = legacy_qs.filter(fecha__lte=end_date)
        if branch_ids:
            legacy_qs = legacy_qs.filter(sucursal_id__in=branch_ids)
        legacy_rows = legacy_qs.values("receta_id", "sucursal_id", "fecha").annotate(
            legacy_cantidad=Sum("cantidad"),
            legacy_total_venta=Sum("monto_total"),
        )
        differences: list[dict] = []
        seen_keys = set()
        for row in legacy_rows:
            key = (row["receta_id"], row["sucursal_id"], row["fecha"])
            seen_keys.add(key)
            rebuilt = rebuilt_map.get(key, {"cantidad": decimal_from_value(0), "monto": decimal_from_value(0)})
            legacy_qty = decimal_from_value(row["legacy_cantidad"])
            legacy_amt = decimal_from_value(row["legacy_total_venta"])
            if rebuilt["cantidad"] != legacy_qty or rebuilt["monto"] != legacy_amt:
                differences.append(
                    {
                        "receta_id": row["receta_id"],
                        "sucursal_id": row["sucursal_id"],
                        "fecha": row["fecha"].isoformat(),
                        "legacy_cantidad": str(legacy_qty),
                        "legacy_total_venta": str(legacy_amt),
                        "rebuilt_cantidad": str(rebuilt["cantidad"]),
                        "rebuilt_total_venta": str(rebuilt["monto"]),
                    }
                )
        for key, rebuilt in rebuilt_map.items():
            if key in seen_keys:
                continue
            receta_id, sucursal_id, sale_date = key
            differences.append(
                {
                    "receta_id": receta_id,
                    "sucursal_id": sucursal_id,
                    "fecha": sale_date.isoformat(),
                    "legacy_cantidad": "0",
                    "legacy_total_venta": "0",
                    "rebuilt_cantidad": str(rebuilt["cantidad"]),
                    "rebuilt_total_venta": str(rebuilt["monto"]),
                }
            )
        differences.sort(key=lambda item: (item["fecha"], item["sucursal_id"] or 0, item["receta_id"] or 0))
        return differences

    def _exact_duplicates(self, *, sync_job: PointSyncJob) -> list[dict]:
        rows = (
            PointSalesDailyProductFact.objects.filter(sync_job=sync_job)
            .values("sucursal_nombre", "sale_date", "categoria", "producto_nombre_historico")
            .annotate(duplicate_count=Count("id"))
            .filter(duplicate_count__gt=1)
            .order_by("sale_date", "sucursal_nombre", "categoria", "producto_nombre_historico")
        )
        return [
            {
                "sucursal": row["sucursal_nombre"],
                "fecha": row["sale_date"].isoformat(),
                "categoria": row["categoria"],
                "producto": row["producto_nombre_historico"],
                "duplicate_count": row["duplicate_count"],
            }
            for row in rows
        ]

    def _partial_duplicates(self, *, sync_job: PointSyncJob) -> list[dict]:
        rows = (
            PointSalesRawStaging.objects.filter(sync_job=sync_job)
            .values("sucursal_raw", "fecha_raw", "categoria_raw", "producto_raw")
            .annotate(row_count=Count("id"), file_count=Count("source_hash", distinct=True))
            .filter(row_count__gt=1)
            .order_by("fecha_raw", "sucursal_raw", "categoria_raw", "producto_raw")
        )
        return [
            {
                "sucursal_raw": row["sucursal_raw"],
                "fecha_raw": row["fecha_raw"],
                "categoria_raw": row["categoria_raw"],
                "producto_raw": row["producto_raw"],
                "row_count": row["row_count"],
                "source_file_variants": row["file_count"],
            }
            for row in rows
        ]

    def _coverage_by_branch(self, *, sync_job: PointSyncJob) -> list[dict]:
        success_tasks = list(
            PointSalesExtractionTask.objects.filter(sync_job=sync_job, status=PointSalesExtractionTask.STATUS_SUCCESS)
            .select_related("branch")
            .order_by("branch__name", "sale_date")
        )
        grouped: dict[str, list[PointSalesExtractionTask]] = defaultdict(list)
        for task in success_tasks:
            grouped[task.branch.name if task.branch_id else "SIN_SUCURSAL"].append(task)

        coverage: list[dict] = []
        for branch_name, tasks in grouped.items():
            dates_with_sales = [task.sale_date for task in tasks if task.row_count > 0]
            first_operational = min(dates_with_sales) if dates_with_sales else None
            last_operational = max(dates_with_sales) if dates_with_sales else None
            plausible_missing = sum(1 for task in tasks if task.row_count == 0)
            suspicious_missing = 0
            if first_operational and last_operational:
                success_dates = {task.sale_date for task in tasks}
                cursor = first_operational
                while cursor <= last_operational:
                    if cursor not in success_dates:
                        suspicious_missing += 1
                    cursor += timedelta(days=1)
            coverage.append(
                {
                    "sucursal": branch_name,
                    "first_operational_date": first_operational.isoformat() if first_operational else None,
                    "last_operational_date": last_operational.isoformat() if last_operational else None,
                    "successful_tasks": len(tasks),
                    "days_with_sales": len(dates_with_sales),
                    "days_without_sales_but_successful": plausible_missing,
                    "suspicious_missing_days_inside_coverage": suspicious_missing,
                }
            )
        coverage.sort(key=lambda item: (item["sucursal"], item["first_operational_date"] or ""))
        return coverage

    def _missing_days_report(self, *, sync_job: PointSyncJob) -> dict:
        coverage = self._coverage_by_branch(sync_job=sync_job)
        plausible = []
        suspicious = []
        tasks = list(
            PointSalesExtractionTask.objects.filter(sync_job=sync_job)
            .select_related("branch")
            .order_by("sale_date", "branch__name")
        )
        for task in tasks:
            if task.status == PointSalesExtractionTask.STATUS_SUCCESS and task.row_count == 0:
                plausible.append(
                    {
                        "sucursal": task.branch.name if task.branch_id else "",
                        "fecha": task.sale_date.isoformat(),
                        "motivo": "reporte_exitoso_sin_renglones",
                    }
                )
            elif task.status == PointSalesExtractionTask.STATUS_FAILED:
                suspicious.append(
                    {
                        "sucursal": task.branch.name if task.branch_id else "",
                        "fecha": task.sale_date.isoformat(),
                        "motivo": "task_failed",
                        "error": task.last_error,
                    }
                )
        return {"plausible": plausible, "suspicious": suspicious, "coverage": coverage}

    def _run_deltas(self, *, sync_job: PointSyncJob) -> list[dict]:
        current_params = sync_job.parameters or {}
        previous_job = (
            PointSyncJob.objects.exclude(id=sync_job.id)
            .filter(job_type=PointSyncJob.JOB_TYPE_SALES)
            .order_by("-finished_at", "-id")
        )
        previous_job = next(
            (job for job in previous_job if (job.parameters or {}).get("pipeline_code") == current_params.get("pipeline_code")),
            None,
        )
        if previous_job is None:
            return []
        current_rows = (
            PointSalesDailyCategoryFact.objects.filter(sync_job=sync_job)
            .values("sucursal_nombre", "sale_date")
            .annotate(total_venta_neta=Sum("total_venta_neta"))
        )
        previous_rows = (
            PointSalesDailyCategoryFact.objects.filter(sync_job=previous_job)
            .values("sucursal_nombre", "sale_date")
            .annotate(total_venta_neta=Sum("total_venta_neta"))
        )
        previous_map = {(row["sucursal_nombre"], row["sale_date"]): decimal_from_value(row["total_venta_neta"]) for row in previous_rows}
        deltas = []
        for row in current_rows:
            key = (row["sucursal_nombre"], row["sale_date"])
            current_total = decimal_from_value(row["total_venta_neta"])
            previous_total = previous_map.get(key)
            if previous_total is None or previous_total == current_total:
                continue
            deltas.append(
                {
                    "sucursal": row["sucursal_nombre"],
                    "fecha": row["sale_date"].isoformat(),
                    "previous_total_venta_neta": str(previous_total),
                    "current_total_venta_neta": str(current_total),
                    "delta": str(current_total - previous_total),
                    "previous_job_id": previous_job.id,
                }
            )
        return deltas

    def _unmatched_catalog_stats(self, *, sync_job: PointSyncJob) -> dict:
        total = PointSalesNormalized.objects.filter(sync_job=sync_job).count()
        unmatched = PointSalesNormalized.objects.filter(
            sync_job=sync_job,
            match_catalogo_status=PointSalesNormalized.MATCH_SIN_CATALOGO,
        ).count()
        percentage = (unmatched / total * 100) if total else 0
        return {
            "total_normalized_rows": total,
            "unmatched_rows": unmatched,
            "percentage_unmatched": round(percentage, 2),
        }

    def _extraction_errors(self, *, sync_job: PointSyncJob) -> list[dict]:
        task_errors = list(
            PointSalesExtractionTask.objects.filter(sync_job=sync_job, status=PointSalesExtractionTask.STATUS_FAILED)
            .select_related("branch")
            .order_by("sale_date", "branch__name")
            .values("id", "branch__name", "sale_date", "last_error", "attempts")
        )
        log_errors = list(
            PointExtractionLog.objects.filter(sync_job=sync_job, level=PointExtractionLog.LEVEL_ERROR)
            .order_by("created_at", "id")
            .values("created_at", "message", "context")
        )
        return [
            {
                "task_id": row["id"],
                "sucursal": row["branch__name"] or "",
                "fecha": row["sale_date"].isoformat(),
                "attempts": row["attempts"],
                "error": row["last_error"],
            }
            for row in task_errors
        ] + [
            {
                "timestamp": row["created_at"].isoformat(),
                "error": row["message"],
                "context": row["context"],
            }
            for row in log_errors
        ]

    def _reconciliation_summary(self, *, sync_job: PointSyncJob, legacy_differences: list[dict], missing_report: dict) -> dict:
        summary = sync_job.result_summary or {}
        coverage = missing_report["coverage"]
        return {
            "records_recovered": summary.get("product_fact_rows", 0),
            "records_new_or_corrected_vs_legacy": len(legacy_differences),
            "records_duplicated_exact": len(self._exact_duplicates(sync_job=sync_job)),
            "records_duplicated_partial": len(self._partial_duplicates(sync_job=sync_job)),
            "days_with_plausible_missing": len(missing_report["plausible"]),
            "days_with_suspicious_missing": len(missing_report["suspicious"]),
            "branches_with_detected_opening": [
                item["sucursal"]
                for item in coverage
                if item["first_operational_date"] is not None
            ],
            "differences_monetary_against_legacy_rows": len(legacy_differences),
            "quality_alerts": PointSalesQualityAlert.objects.filter(sync_job=sync_job).count(),
        }

    def build_report(self, *, sync_job: PointSyncJob) -> dict:
        report = {
            "job_id": sync_job.id,
            "parameters": sync_job.parameters or {},
            "summary": sync_job.result_summary or {},
            "counts_by_branch_year": self._counts_by_branch_year(sync_job=sync_job),
            "net_sales_by_branch_month": self._neta_by_branch_month(sync_job=sync_job),
        }
        report["legacy_vs_rebuilt"] = self._legacy_vs_rebuilt(sync_job=sync_job)
        report["exact_duplicates"] = self._exact_duplicates(sync_job=sync_job)
        report["partial_duplicates"] = self._partial_duplicates(sync_job=sync_job)
        report["missing_days"] = self._missing_days_report(sync_job=sync_job)
        report["run_deltas"] = self._run_deltas(sync_job=sync_job)
        report["unmatched_catalog"] = self._unmatched_catalog_stats(sync_job=sync_job)
        report["coverage_by_branch"] = report["missing_days"]["coverage"]
        report["extraction_errors"] = self._extraction_errors(sync_job=sync_job)
        report["reconciliation_summary"] = self._reconciliation_summary(
            sync_job=sync_job,
            legacy_differences=report["legacy_vs_rebuilt"],
            missing_report=report["missing_days"],
        )
        self._write_report_files(sync_job=sync_job, report=report)
        return report

    def _write_report_files(self, *, sync_job: PointSyncJob, report: dict) -> None:
        output_dir = self.report_dir(sync_job=sync_job)
        write_json_file(output_dir / "reconciliation_report.json", report)

        lines = [
            f"# Point Sales Rebuild Report · job {sync_job.id}",
            "",
            "## Resumen",
            f"- pipeline_code: `{report['summary'].get('pipeline_code', '')}`",
            f"- total_tasks: `{report['summary'].get('total_tasks', 0)}`",
            f"- tasks_success: `{report['summary'].get('tasks_success', 0)}`",
            f"- tasks_failed: `{report['summary'].get('tasks_failed', 0)}`",
            f"- category_fact_rows: `{report['summary'].get('category_fact_rows', 0)}`",
            f"- product_fact_rows: `{report['summary'].get('product_fact_rows', 0)}`",
            "",
            "## Conciliación",
            f"- diferencias_vs_legacy: `{len(report['legacy_vs_rebuilt'])}`",
            f"- duplicados_exactos: `{len(report['exact_duplicates'])}`",
            f"- duplicados_parciales: `{len(report['partial_duplicates'])}`",
            f"- faltantes_plausibles: `{len(report['missing_days']['plausible'])}`",
            f"- faltantes_sospechosos: `{len(report['missing_days']['suspicious'])}`",
            f"- porcentaje_sin_match_catalogo: `{report['unmatched_catalog']['percentage_unmatched']}%`",
            "",
            "## Primeras diferencias vs legacy",
        ]
        preview = report["legacy_vs_rebuilt"][:20]
        if not preview:
            lines.append("- Sin diferencias monetarias ni de cantidad contra `VentaHistorica` fuente `POINT_BRIDGE_SALES`.")
        else:
            for item in preview:
                lines.append(
                    "- fecha={fecha} sucursal_id={sucursal_id} receta_id={receta_id} "
                    "legacy_cantidad={legacy_cantidad} rebuilt_cantidad={rebuilt_cantidad} "
                    "legacy_total_venta={legacy_total_venta} rebuilt_total_venta={rebuilt_total_venta}".format(**item)
                )
        (output_dir / "reconciliation_report.md").write_text("\n".join(lines), encoding="utf-8")
