from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from reportes.models import CargaGastoOperativoArchivo
from reportes.services_branch_admin_expense_import import (
    BranchAdminExpenseImportService,
    BranchAdminExpenseImportSummary,
)


logger = logging.getLogger(__name__)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class HistoricalBranchExpenseImportResult:
    upload: CargaGastoOperativoArchivo
    summary: BranchAdminExpenseImportSummary | None
    classification: str


class HistoricalBranchExpenseImportService:
    CLASSIFICATION_HISTORICAL_REAL = "historical_real"
    IMPORT_SCOPE_BRANCH_SALES = "branch_sales_workbook"

    def _build_summary_payload(
        self,
        *,
        classification: str,
        summary: BranchAdminExpenseImportSummary | None,
    ) -> dict[str, object]:
        return {
            "classification": classification,
            "processed_rows": summary.processed_rows if summary else 0,
            "loaded_rows": (summary.created + summary.updated) if summary else 0,
            "created_rows": summary.created if summary else 0,
            "updated_rows": summary.updated if summary else 0,
            "deleted_rows": summary.deleted if summary else 0,
            "affected_branches": sorted(summary.affected_branches) if summary else [],
            "covered_periods": sorted(summary.periods) if summary else [],
            "skipped_concepts": {
                key: sorted(values)
                for key, values in (summary.skipped_concepts.items() if summary else [])
                if values
            },
            "flagged_outliers": summary.flagged_outliers if summary else [],
        }

    def import_sales_history_workbook(
        self,
        workbook_path: str | Path,
        *,
        target_year: int = 2025,
        uploaded_by=None,
        source_channel: str = CargaGastoOperativoArchivo.SOURCE_COMMAND,
    ) -> HistoricalBranchExpenseImportResult:
        workbook = Path(workbook_path).expanduser().resolve()
        if not workbook.exists():
            raise FileNotFoundError(workbook)
        if workbook.suffix.lower() != ".xlsx":
            raise ValueError("Sólo se aceptan archivos .xlsx para histórico de gastos por sucursal.")

        classification = self.CLASSIFICATION_HISTORICAL_REAL
        file_hash = _file_sha256(workbook)
        metadata = {
            "classification": classification,
            "import_scope": self.IMPORT_SCOPE_BRANCH_SALES,
            "target_dataset": "historical_2025",
            "column_used": "REAL",
            "file_kind": "budget_workbook_with_actual_columns",
        }
        duplicate_of = (
            CargaGastoOperativoArchivo.objects.filter(file_hash=file_hash)
            .order_by("uploaded_at", "id")
            .first()
        )
        upload = CargaGastoOperativoArchivo.objects.create(
            original_filename=workbook.name,
            stored_file_path=str(workbook),
            file_hash=file_hash,
            file_size_bytes=workbook.stat().st_size,
            source_channel=source_channel,
            status=CargaGastoOperativoArchivo.STATUS_PENDING,
            target_year=target_year,
            uploaded_by=uploaded_by if getattr(uploaded_by, "is_authenticated", False) else None,
            metadata=metadata,
        )

        if duplicate_of is not None:
            upload.status = CargaGastoOperativoArchivo.STATUS_DUPLICATE
            upload.processed_at = timezone.now()
            upload.error_log = [{"message": "Archivo ya importado previamente con el mismo contenido."}]
            upload.metadata = {
                **metadata,
                "duplicate_of_upload_id": duplicate_of.pk,
            }
            upload.summary = self._build_summary_payload(classification=classification, summary=None)
            upload.save(update_fields=["status", "processed_at", "error_log", "metadata", "summary", "updated_at"])
            return HistoricalBranchExpenseImportResult(upload=upload, summary=None, classification=classification)

        import_service = BranchAdminExpenseImportService(
            target_year=target_year,
            branch_tipo_dato="REAL",
            admin_tipo_dato="REAL",
            external_prefix="OPEX_HIST",
        )

        try:
            with transaction.atomic():
                summary = import_service.import_sales_workbook(workbook)
        except Exception as exc:
            upload.status = CargaGastoOperativoArchivo.STATUS_ERROR
            upload.processed_at = timezone.now()
            upload.error_log = [{"message": str(exc) or exc.__class__.__name__}]
            upload.summary = self._build_summary_payload(classification=classification, summary=None)
            upload.save(update_fields=["status", "processed_at", "error_log", "summary", "updated_at"])
            logger.exception("historical_branch_expense_import_failed file=%s", workbook)
            raise

        upload.status = CargaGastoOperativoArchivo.STATUS_SUCCESS
        upload.processed_at = timezone.now()
        upload.processed_rows = summary.processed_rows
        upload.loaded_rows = summary.created + summary.updated
        upload.created_rows = summary.created
        upload.updated_rows = summary.updated
        upload.skipped_rows = 0
        upload.project_refresh_count = 0
        upload.affected_branches = sorted(summary.affected_branches)
        upload.covered_periods = sorted(summary.periods)
        upload.error_log = []
        upload.summary = self._build_summary_payload(classification=classification, summary=summary)
        upload.save(
            update_fields=[
                "status",
                "processed_at",
                "processed_rows",
                "loaded_rows",
                "created_rows",
                "updated_rows",
                "skipped_rows",
                "project_refresh_count",
                "affected_branches",
                "covered_periods",
                "error_log",
                "summary",
                "updated_at",
            ]
        )
        log_event(
            uploaded_by,
            "IMPORT",
            "reportes.CargaGastoOperativoArchivo",
            upload.pk,
            payload=upload.summary,
        )
        return HistoricalBranchExpenseImportResult(upload=upload, summary=summary, classification=classification)
