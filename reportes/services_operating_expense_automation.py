from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

from django.conf import settings
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.audit import log_event
from reportes.models import CargaGastoOperativoArchivo
from reportes.services_branch_real_operating_expense_import import (
    BranchRealOperatingExpenseImportService,
    BranchRealOperatingExpenseImportValidationError,
)
from reportes.utils.notifier import notify_duplicate, notify_error, notify_validation_issue


logger = logging.getLogger(__name__)


def _safe_filename(name: str) -> str:
    original = Path(name or "gastos_operativos.xlsx").name
    keep = [ch if ch.isalnum() or ch in {".", "-", "_"} else "_" for ch in original]
    cleaned = "".join(keep).strip("._")
    return cleaned or "gastos_operativos.xlsx"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class OperatingExpenseInboxSummary:
    processed_files: int = 0
    success_files: int = 0
    error_files: int = 0
    duplicate_files: int = 0
    run_ids: list[int] | None = None


class OperatingExpenseImportAutomationService:
    def __init__(self) -> None:
        self.storage_root = Path(settings.BASE_DIR) / "storage" / "uploads" / "gastos"
        self.import_service = BranchRealOperatingExpenseImportService()

    def _ensure_dir(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _default_web_inbox_dir(self) -> Path:
        stamp = timezone.localtime().strftime("%Y/%m")
        return self._ensure_dir(self.storage_root / "web" / stamp)

    def _lock_dir(self) -> Path:
        return self._ensure_dir(self.storage_root / ".locks")

    def _archive_target(self, *, status: str, original_name: str, archive_root: Path | None = None) -> Path:
        root = self._ensure_dir(archive_root or self.storage_root)
        folder_map = {
            CargaGastoOperativoArchivo.STATUS_SUCCESS: "processed",
            CargaGastoOperativoArchivo.STATUS_ERROR: "failed",
            CargaGastoOperativoArchivo.STATUS_DUPLICATE: "duplicate",
        }
        destination_dir = self._ensure_dir(root / folder_map.get(status, "processed"))
        candidate = destination_dir / _safe_filename(original_name)
        if not candidate.exists():
            return candidate
        return destination_dir / f"{candidate.stem}_{timezone.localtime().strftime('%Y%m%d%H%M%S')}{candidate.suffix}"

    def _build_run_summary(self, run: CargaGastoOperativoArchivo) -> dict[str, object]:
        return {
            "status": run.status,
            "processed_rows": run.processed_rows,
            "loaded_rows": run.loaded_rows,
            "created_rows": run.created_rows,
            "updated_rows": run.updated_rows,
            "skipped_rows": run.skipped_rows,
            "project_refresh_count": run.project_refresh_count,
            "affected_branches": run.affected_branches,
            "covered_periods": run.covered_periods,
            "error_log": run.error_log,
        }

    def _log_processing_event(self, event_name: str, *, run: CargaGastoOperativoArchivo | None = None, **extra) -> None:
        payload = {
            "event": event_name,
            "upload_id": getattr(run, "pk", None),
            "filename": getattr(run, "original_filename", ""),
            "file_hash": getattr(run, "file_hash", ""),
            "branches": list(getattr(run, "affected_branches", []) or []),
            "periods": list(getattr(run, "covered_periods", []) or []),
        }
        payload.update({key: value for key, value in extra.items() if value is not None})
        logger.info("operating_expense_pipeline %s", json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True))

    @contextmanager
    def _hash_processing_guard(self, file_hash: str):
        lock_path = self._lock_dir() / f"{file_hash}.lock"
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            yield False
            return
        try:
            os.write(fd, str(os.getpid()).encode("utf-8"))
            yield True
        finally:
            os.close(fd)
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("operating_expense_pipeline could_not_remove_lock path=%s", lock_path)

    def _prepare_duplicate_run(
        self,
        run: CargaGastoOperativoArchivo,
        *,
        message: str,
        duplicate_of_id: int | None = None,
    ) -> CargaGastoOperativoArchivo:
        metadata = dict(run.metadata or {})
        if duplicate_of_id is not None:
            metadata["duplicate_of_upload_id"] = duplicate_of_id
        run.status = CargaGastoOperativoArchivo.STATUS_DUPLICATE
        run.processed_at = timezone.now()
        run.error_log = [{"message": message}]
        run.metadata = metadata
        return run

    def _prepare_error_run(
        self,
        run: CargaGastoOperativoArchivo,
        *,
        message: str,
        processed_rows: int = 0,
        error_log: list[dict[str, object]] | None = None,
    ) -> CargaGastoOperativoArchivo:
        run.status = CargaGastoOperativoArchivo.STATUS_ERROR
        run.processed_at = timezone.now()
        run.processed_rows = processed_rows
        run.loaded_rows = 0
        run.created_rows = 0
        run.updated_rows = 0
        run.skipped_rows = 0
        run.project_refresh_count = 0
        run.affected_branches = []
        run.covered_periods = []
        run.error_log = error_log or [{"message": message}]
        return run

    def _finalize_run(
        self,
        run: CargaGastoOperativoArchivo,
        *,
        archive_root: str | Path | None = None,
        user=None,
    ) -> CargaGastoOperativoArchivo:
        target = self._archive_target(
            status=run.status,
            original_name=run.original_filename,
            archive_root=Path(archive_root) if archive_root else None,
        )
        current = Path(run.stored_file_path)
        if current.exists() and current != target:
            self._ensure_dir(target.parent)
            shutil.move(str(current), str(target))
            run.stored_file_path = str(target)
        run.summary = self._build_run_summary(run)
        run.save(
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
                "stored_file_path",
                "summary",
                "metadata",
                "updated_at",
            ]
        )
        self._log_processing_event("file_archived", run=run, archive_path=run.stored_file_path, status=run.status)
        log_event(
            user or run.uploaded_by,
            "IMPORT",
            "reportes.CargaGastoOperativoArchivo",
            run.pk,
            payload=run.summary,
        )
        return run

    @transaction.atomic
    def register_path(
        self,
        file_path: str | Path,
        *,
        source_channel: str,
        uploaded_by=None,
        target_year: int = 2026,
        metadata: dict[str, object] | None = None,
        precomputed_hash: str | None = None,
        precomputed_size: int | None = None,
    ) -> CargaGastoOperativoArchivo:
        path = Path(file_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)
        stat = path.stat()
        file_hash = precomputed_hash or _file_sha256(path)
        file_size_bytes = precomputed_size if precomputed_size is not None else stat.st_size
        duplicate_of = (
            CargaGastoOperativoArchivo.objects.filter(file_hash=file_hash)
            .order_by("uploaded_at", "id")
            .first()
        )
        try:
            run = CargaGastoOperativoArchivo.objects.create(
                original_filename=path.name,
                stored_file_path=str(path),
                file_hash=file_hash,
                file_size_bytes=file_size_bytes,
                source_channel=source_channel,
                target_year=target_year,
                uploaded_by=uploaded_by if getattr(uploaded_by, "is_authenticated", False) else None,
                metadata=metadata or {},
            )
        except IntegrityError as exc:
            raise ValueError(f"No se pudo registrar la carga para {path.name}") from exc
        self._log_processing_event(
            "hash_computed",
            run=run,
            source_channel=source_channel,
            file_size_bytes=file_size_bytes,
        )
        if duplicate_of is not None:
            self._prepare_duplicate_run(
                run,
                message="Archivo ya registrado previamente con el mismo contenido.",
                duplicate_of_id=duplicate_of.pk,
            )
            self._log_processing_event("duplicate_detected", run=run, duplicate_of_upload_id=duplicate_of.pk)
        return run

    def save_uploaded_file(self, uploaded_file, *, uploaded_by=None, target_year: int = 2026) -> CargaGastoOperativoArchivo:
        inbox_dir = self._default_web_inbox_dir()
        filename = _safe_filename(uploaded_file.name)
        stored_path = inbox_dir / f"{timezone.localtime().strftime('%Y%m%d%H%M%S')}_{uuid4().hex[:8]}_{filename}"
        with stored_path.open("wb") as handle:
            for chunk in uploaded_file.chunks():
                handle.write(chunk)
        self._log_processing_event("file_saved", filename=stored_path.name, file_path=str(stored_path), target_year=target_year)
        return self.register_path(
            stored_path,
            source_channel=CargaGastoOperativoArchivo.SOURCE_WEB,
            uploaded_by=uploaded_by,
            target_year=target_year,
            metadata={"original_upload_name": uploaded_file.name},
        )

    @transaction.atomic
    def process_run(
        self,
        run: CargaGastoOperativoArchivo,
        *,
        refresh_projects: bool = True,
        refresh_until: date | None = None,
        non_real_policy: str = "ignore",
        user=None,
        archive_root: str | Path | None = None,
    ) -> CargaGastoOperativoArchivo:
        if run.status == CargaGastoOperativoArchivo.STATUS_DUPLICATE:
            notify_duplicate(run)
            return self._finalize_run(run, archive_root=archive_root, user=user)

        self._log_processing_event("start_processing", run=run, refresh_projects=refresh_projects)
        run.status = CargaGastoOperativoArchivo.STATUS_PROCESSING
        run.error_log = []
        run.save(update_fields=["status", "error_log", "updated_at"])

        if Path(run.stored_file_path).suffix.lower() != ".xlsx":
            self._prepare_error_run(run, message="Formato no soportado. Sólo se aceptan archivos .xlsx.")
            notify_validation_issue(run)
            self._log_processing_event("validation_failed", run=run, reason="invalid_extension")
            return self._finalize_run(run, archive_root=archive_root, user=user)

        if run.file_size_bytes <= 0:
            self._prepare_error_run(run, message="Archivo vacío.")
            notify_validation_issue(run)
            self._log_processing_event("validation_failed", run=run, reason="empty_file")
            return self._finalize_run(run, archive_root=archive_root, user=user)

        try:
            self._log_processing_event("validation_started", run=run)
            self._log_processing_event("upsert_started", run=run)
            if refresh_projects:
                self._log_processing_event("projects_refresh_started", run=run)
            summary = self.import_service.import_workbook(
                run.stored_file_path,
                target_year=run.target_year,
                non_real_policy=non_real_policy,
                refresh_projects=refresh_projects,
                refresh_until=refresh_until,
                user=user or run.uploaded_by,
                allow_open_month_real=(
                    run.source_channel in {
                        CargaGastoOperativoArchivo.SOURCE_WEB,
                        CargaGastoOperativoArchivo.SOURCE_COMMAND,
                    }
                ),
            )
        except BranchRealOperatingExpenseImportValidationError as exc:
            self._prepare_error_run(
                run,
                message="Validación de negocio falló.",
                processed_rows=exc.summary.processed_rows,
                error_log=[
                    {
                        "row_number": item.row_number,
                        "field": item.field,
                        "message": item.message,
                    }
                    for item in exc.summary.errors
                ],
            )
            notify_validation_issue(run)
            self._log_processing_event("validation_failed", run=run, error_count=len(run.error_log))
        except Exception as exc:
            self._prepare_error_run(run, message=str(exc) or exc.__class__.__name__)
            notify_error(run, exc)
            self._log_processing_event("processing_failed", run=run, error_type=exc.__class__.__name__, error_message=str(exc))
        else:
            run.status = CargaGastoOperativoArchivo.STATUS_SUCCESS
            run.processed_at = timezone.now()
            run.processed_rows = summary.processed_rows
            run.loaded_rows = summary.loaded_rows
            run.created_rows = summary.created
            run.updated_rows = summary.updated
            run.skipped_rows = summary.skipped_non_real
            run.project_refresh_count = summary.project_refresh_count
            run.affected_branches = summary.affected_branches
            run.covered_periods = summary.periods
            run.error_log = []
            self._log_processing_event("upsert_finished", run=run, loaded_rows=run.loaded_rows)
            if refresh_projects:
                self._log_processing_event(
                    "projects_refresh_finished",
                    run=run,
                    refresh_count=run.project_refresh_count,
                )

        return self._finalize_run(run, archive_root=archive_root, user=user)

    def _process_path(
        self,
        file_path: str | Path,
        *,
        source_channel: str,
        uploaded_by=None,
        target_year: int = 2026,
        refresh_projects: bool = True,
        refresh_until: date | None = None,
        user=None,
        archive_root: str | Path | None = None,
        metadata: dict[str, object] | None = None,
    ) -> CargaGastoOperativoArchivo:
        path = Path(file_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)
        self._log_processing_event("start_processing", filename=path.name, file_path=str(path), source_channel=source_channel)
        stat = path.stat()
        file_hash = _file_sha256(path)
        self._log_processing_event("hash_computed", filename=path.name, file_hash=file_hash)
        with self._hash_processing_guard(file_hash) as guard_acquired:
            if not guard_acquired:
                run = self.register_path(
                    path,
                    source_channel=source_channel,
                    uploaded_by=uploaded_by,
                    target_year=target_year,
                    metadata=metadata,
                    precomputed_hash=file_hash,
                    precomputed_size=stat.st_size,
                )
                self._prepare_duplicate_run(
                    run,
                    message="Otro proceso ya está trabajando este mismo archivo.",
                )
                self._log_processing_event("duplicate_detected", run=run, reason="hash_lock_active")
                notify_duplicate(run)
                return self._finalize_run(run, archive_root=archive_root, user=user)

            run = self.register_path(
                path,
                source_channel=source_channel,
                uploaded_by=uploaded_by,
                target_year=target_year,
                metadata=metadata,
                precomputed_hash=file_hash,
                precomputed_size=stat.st_size,
            )
            return self.process_run(
                run,
                refresh_projects=refresh_projects,
                refresh_until=refresh_until,
                user=user,
                archive_root=archive_root,
            )

    def process_uploaded_file(
        self,
        uploaded_file,
        *,
        uploaded_by=None,
        target_year: int = 2026,
        refresh_until: date | None = None,
    ) -> CargaGastoOperativoArchivo:
        inbox_dir = self._default_web_inbox_dir()
        filename = _safe_filename(uploaded_file.name)
        stored_path = inbox_dir / f"{timezone.localtime().strftime('%Y%m%d%H%M%S')}_{uuid4().hex[:8]}_{filename}"
        with stored_path.open("wb") as handle:
            for chunk in uploaded_file.chunks():
                handle.write(chunk)
        self._log_processing_event("file_saved", filename=stored_path.name, file_path=str(stored_path), target_year=target_year)
        return self._process_path(
            stored_path,
            source_channel=CargaGastoOperativoArchivo.SOURCE_WEB,
            uploaded_by=uploaded_by,
            target_year=target_year,
            refresh_until=refresh_until,
            user=uploaded_by,
            metadata={"original_upload_name": uploaded_file.name},
        )

    def process_directory(
        self,
        directory: str | Path,
        *,
        target_year: int = 2026,
        refresh_projects: bool = True,
        refresh_until: date | None = None,
        user=None,
        source_channel: str = CargaGastoOperativoArchivo.SOURCE_DROPBOX,
    ) -> OperatingExpenseInboxSummary:
        folder = Path(directory).expanduser().resolve()
        if not folder.exists():
            raise FileNotFoundError(folder)
        summary = OperatingExpenseInboxSummary(run_ids=[])
        skipped_names = {"processed", "failed", "duplicate", ".locks"}
        for file_path in sorted(folder.iterdir()):
            if file_path.is_dir():
                continue
            if file_path.name in skipped_names:
                continue
            try:
                processed = self._process_path(
                    file_path,
                    source_channel=source_channel,
                    uploaded_by=user,
                    target_year=target_year,
                    refresh_projects=refresh_projects,
                    refresh_until=refresh_until,
                    user=user,
                    archive_root=folder,
                )
            except Exception as exc:
                self._log_processing_event(
                    "processing_failed",
                    filename=file_path.name,
                    file_path=str(file_path),
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                )
                if file_path.exists():
                    failed_target = self._archive_target(
                        status=CargaGastoOperativoArchivo.STATUS_ERROR,
                        original_name=file_path.name,
                        archive_root=folder,
                    )
                    self._ensure_dir(failed_target.parent)
                    shutil.move(str(file_path), str(failed_target))
                summary.processed_files += 1
                summary.error_files += 1
                continue
            summary.processed_files += 1
            summary.run_ids.append(processed.pk)
            if processed.status == CargaGastoOperativoArchivo.STATUS_SUCCESS:
                summary.success_files += 1
            elif processed.status == CargaGastoOperativoArchivo.STATUS_DUPLICATE:
                summary.duplicate_files += 1
            else:
                summary.error_files += 1
        return summary
