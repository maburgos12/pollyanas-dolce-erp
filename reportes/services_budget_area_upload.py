from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from core.models import AuditLog
from reportes.models import PresupuestoImport
from reportes.services_budget_detail_import import TrustedBudgetDetailImportService
from reportes.services_budget_import import GeneralBudgetImportService, _sha256
from reportes.services_budget_monitoring import BudgetMonitoringSnapshotService


@dataclass(frozen=True)
class BudgetAreaDefinition:
    key: str
    label: str
    expected_filename: str
    expected_sheets: tuple[str, ...]
    import_mode: str
    detail_kind: str
    description: str


@dataclass
class BudgetAreaUploadResult:
    status: str
    area_key: str
    area_label: str
    original_filename: str
    canonical_filename: str
    file_hash: str
    periods: list[str]
    sheets_imported: list[str]
    imports_created: int
    imports_updated: int
    lines_created: int
    lines_updated: int
    snapshot_rows_created: int
    snapshot_rows_updated: int
    summary_message: str


class BudgetAreaUploadService:
    STATUS_SUCCESS = "SUCCESS"
    STATUS_DUPLICATE = "DUPLICATE"

    GENERAL_AREA_KEY = "general"
    DETAIL_AREA_DEFINITIONS = (
        BudgetAreaDefinition(
            key="admin",
            label="Administración",
            expected_filename="PRESUPUESTO ADMINISTRACIÓN 2026.xlsx",
            expected_sheets=("ADMON",),
            import_mode="detail",
            detail_kind="admin_recurrente",
            description="Egresos administrativos recurrentes desde la hoja ADMON.",
        ),
        BudgetAreaDefinition(
            key="branch_sales",
            label="Ventas por sucursal",
            expected_filename="PRESUPUESTO DE GASTOS VENTAS 2026 AUTORIZADO.xlsx",
            expected_sheets=("GUAMUCHIL", "EL TUNEL", "PLAZA NIO", "CRUCERO", "COLOSIO", "GLORIAS", "PAYAN", "LEYVA", "MATRIZ"),
            import_mode="detail",
            detail_kind="branch_sales",
            description="Gasto comercial por sucursal con una hoja por punto de venta.",
        ),
        BudgetAreaDefinition(
            key="payroll_area",
            label="Nómina por área",
            expected_filename="PRESUPUESTO NOMINA 2026 AUTORIZADO.xlsx",
            expected_sheets=("VENTAS", "PRODUCCCION", "LOGISTICA", "ADMINISTRACION"),
            import_mode="detail",
            detail_kind="payroll_area",
            description="Nómina desglosada por área operativa y administrativa.",
        ),
        BudgetAreaDefinition(
            key="production_budget",
            label="Producción",
            expected_filename="PRESUPUESTO PRODUCCIÓN 2026 AUTORIZADO.xlsx",
            expected_sheets=("PRESUPUESTO PRODUCCIÓN",),
            import_mode="detail",
            detail_kind="production_budget",
            description="Costo y presupuesto operativo de producción.",
        ),
        BudgetAreaDefinition(
            key="logistics_budget",
            label="Logística",
            expected_filename="PRESUPUESTO LOGISTICA 2026.xlsx",
            expected_sheets=("LOGÍSTICA",),
            import_mode="detail",
            detail_kind="logistics_budget",
            description="Gasto logístico mensual con hoja dedicada de logística.",
        ),
    )

    AREA_DEFINITIONS = {
        GENERAL_AREA_KEY: BudgetAreaDefinition(
            key=GENERAL_AREA_KEY,
            label="Dirección / Finanzas",
            expected_filename="PRESUPUESTO GENERAL 2026.xlsx",
            expected_sheets=("GENERAL",),
            import_mode="general",
            detail_kind="general_budget",
            description="Workbook consolidado con hoja GENERAL para conciliación ejecutiva.",
        ),
        **{definition.key: definition for definition in DETAIL_AREA_DEFINITIONS},
    }

    def __init__(self) -> None:
        self.storage_root = Path(settings.BASE_DIR) / "storage" / "uploads" / "presupuestos"
        self.general_import_service = GeneralBudgetImportService()
        self.detail_import_service = TrustedBudgetDetailImportService()
        self.snapshot_service = BudgetMonitoringSnapshotService()

    @classmethod
    def get_area_definition(cls, area_key: str) -> BudgetAreaDefinition:
        try:
            return cls.AREA_DEFINITIONS[area_key]
        except KeyError as exc:
            raise ValueError("Área de presupuesto no soportada.") from exc

    @classmethod
    def list_area_definitions(cls) -> list[BudgetAreaDefinition]:
        ordered_keys = [
            cls.GENERAL_AREA_KEY,
            "admin",
            "branch_sales",
            "payroll_area",
            "production_budget",
            "logistics_budget",
        ]
        return [cls.AREA_DEFINITIONS[key] for key in ordered_keys]

    def _ensure_dir(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _build_upload_dir(self, *, area_key: str) -> Path:
        stamp = timezone.localtime().strftime("%Y/%m")
        return self._ensure_dir(self.storage_root / "web" / stamp / area_key / uuid4().hex)

    def _save_uploaded_file(self, uploaded_file, *, area: BudgetAreaDefinition) -> Path:
        if not uploaded_file or not getattr(uploaded_file, "name", ""):
            raise ValueError("Selecciona un archivo XLSX para continuar.")
        if not uploaded_file.name.lower().endswith(".xlsx"):
            raise ValueError("Formato no soportado. Sólo se aceptan archivos .xlsx.")

        target_dir = self._build_upload_dir(area_key=area.key)
        target_path = target_dir / area.expected_filename
        with target_path.open("wb") as handle:
            for chunk in uploaded_file.chunks():
                handle.write(chunk)
        return target_path

    def _existing_duplicate(self, *, file_hash: str) -> PresupuestoImport | None:
        return (
            PresupuestoImport.objects.filter(archivo_hash=file_hash)
            .order_by("-updated_at", "-created_at", "-id")
            .first()
        )

    def _audit_payload(
        self,
        *,
        area: BudgetAreaDefinition,
        original_filename: str,
        canonical_path: Path,
        file_hash: str,
        periods: list[str] | None = None,
        sheets_imported: list[str] | None = None,
        imports_created: int = 0,
        imports_updated: int = 0,
        lines_created: int = 0,
        lines_updated: int = 0,
        snapshot_rows_created: int = 0,
        snapshot_rows_updated: int = 0,
        status: str,
        detail: str = "",
        duplicate_of_import_id: int | None = None,
        user=None,
    ) -> dict[str, Any]:
        target_year = None
        if periods:
            try:
                target_year = date.fromisoformat(periods[0]).year
            except ValueError:
                target_year = None
        return {
            "area_key": area.key,
            "area_label": area.label,
            "expected_filename": area.expected_filename,
            "expected_sheets": list(area.expected_sheets),
            "original_filename": original_filename,
            "canonical_filename": canonical_path.name,
            "stored_file_path": str(canonical_path),
            "file_hash": file_hash,
            "periods": list(periods or []),
            "sheets_imported": list(sheets_imported or []),
            "imports_created": imports_created,
            "imports_updated": imports_updated,
            "lines_created": lines_created,
            "lines_updated": lines_updated,
            "snapshot_rows_created": snapshot_rows_created,
            "snapshot_rows_updated": snapshot_rows_updated,
            "status": status,
            "detail": detail,
            "target_year": target_year,
            "duplicate_of_import_id": duplicate_of_import_id,
            "user_id": getattr(user, "id", None),
            "user_label": getattr(user, "get_full_name", lambda: "")() or getattr(user, "username", "") or "Sistema",
        }

    def _touch_import_metadata(
        self,
        *,
        area: BudgetAreaDefinition,
        canonical_filename: str,
        original_filename: str,
        file_hash: str,
        periods: list[str],
        sheets_imported: list[str],
        user,
    ) -> list[PresupuestoImport]:
        if area.import_mode == "general":
            queryset = PresupuestoImport.objects.filter(
                tipo=PresupuestoImport.TIPO_GENERAL,
                fuente_nombre=canonical_filename,
                sheet_name="GENERAL",
            )
        else:
            queryset = PresupuestoImport.objects.filter(
                tipo=PresupuestoImport.TIPO_DETALLE,
                fuente_nombre=canonical_filename,
                sheet_name__in=list(area.expected_sheets),
            )

        touched = []
        stamp = timezone.now().isoformat()
        target_year = None
        if periods:
            try:
                target_year = date.fromisoformat(periods[0]).year
            except ValueError:
                target_year = None
        for import_obj in queryset:
            metadata = dict(import_obj.metadata or {})
            metadata.update(
                {
                    "upload_area_key": area.key,
                    "upload_area_label": area.label,
                    "expected_filename": area.expected_filename,
                    "expected_sheets": list(area.expected_sheets),
                    "original_filename": original_filename,
                    "uploaded_by_id": getattr(user, "id", None),
                    "uploaded_by_label": getattr(user, "get_full_name", lambda: "")() or getattr(user, "username", "") or "Sistema",
                    "uploaded_via": "ui",
                    "uploaded_at": stamp,
                    "target_year": target_year,
                    "periods": periods,
                    "sheets_imported": sheets_imported,
                    "detail_kind": area.detail_kind,
                }
            )
            import_obj.archivo_hash = file_hash
            import_obj.metadata = metadata
            import_obj.save(update_fields=["archivo_hash", "metadata", "updated_at"])
            touched.append(import_obj)
        return touched

    def _build_snapshots(self, periods: list[str]) -> tuple[int, int]:
        rows_created = 0
        rows_updated = 0
        for raw_period in periods:
            period_start = date.fromisoformat(raw_period)
            snapshot = self.snapshot_service.build_snapshot(period_start=period_start)
            rows_created += snapshot.rows_created
            rows_updated += snapshot.rows_updated
        return rows_created, rows_updated

    def process_uploaded_file(self, *, area_key: str, uploaded_file, uploaded_by=None) -> BudgetAreaUploadResult:
        area = self.get_area_definition(area_key)
        original_filename = getattr(uploaded_file, "name", area.expected_filename)
        canonical_path = self._save_uploaded_file(uploaded_file, area=area)
        file_hash = _sha256(canonical_path)

        duplicate = self._existing_duplicate(file_hash=file_hash)
        if duplicate is not None:
            payload = self._audit_payload(
                area=area,
                original_filename=original_filename,
                canonical_path=canonical_path,
                file_hash=file_hash,
                status=self.STATUS_DUPLICATE,
                detail="El archivo ya había sido importado anteriormente; no se reprocesó.",
                duplicate_of_import_id=duplicate.pk,
                user=uploaded_by,
            )
            log_event(uploaded_by, "BUDGET_UPLOAD_DUPLICATE", "reportes.PresupuestoImport", str(duplicate.pk), payload=payload)
            return BudgetAreaUploadResult(
                status=self.STATUS_DUPLICATE,
                area_key=area.key,
                area_label=area.label,
                original_filename=original_filename,
                canonical_filename=canonical_path.name,
                file_hash=file_hash,
                periods=[],
                sheets_imported=[],
                imports_created=0,
                imports_updated=0,
                lines_created=0,
                lines_updated=0,
                snapshot_rows_created=0,
                snapshot_rows_updated=0,
                summary_message="El archivo ya existe en histórico y se registró como duplicado.",
            )

        try:
            with transaction.atomic():
                if area.import_mode == "general":
                    summary = self.general_import_service.import_workbook(canonical_path)
                    sheets_imported = [f"{canonical_path.name}::GENERAL"]
                else:
                    summary = self.detail_import_service.import_workbook(canonical_path)
                    sheets_imported = list(summary.sheets_imported)

                periods = list(summary.periods)
                snapshot_rows_created, snapshot_rows_updated = self._build_snapshots(periods)
                touched_imports = self._touch_import_metadata(
                    area=area,
                    canonical_filename=canonical_path.name,
                    original_filename=original_filename,
                    file_hash=file_hash,
                    periods=periods,
                    sheets_imported=sheets_imported,
                    user=uploaded_by,
                )
                batch_object_id = ",".join(str(row.pk) for row in touched_imports) or area.key
                payload = self._audit_payload(
                    area=area,
                    original_filename=original_filename,
                    canonical_path=canonical_path,
                    file_hash=file_hash,
                    periods=periods,
                    sheets_imported=sheets_imported,
                    imports_created=summary.imports_created,
                    imports_updated=summary.imports_updated,
                    lines_created=summary.lines_created,
                    lines_updated=summary.lines_updated,
                    snapshot_rows_created=snapshot_rows_created,
                    snapshot_rows_updated=snapshot_rows_updated,
                    status=self.STATUS_SUCCESS,
                    detail="Importación de presupuesto procesada correctamente.",
                    user=uploaded_by,
                )
                log_event(uploaded_by, "BUDGET_UPLOAD_SUCCESS", "reportes.PresupuestoImport", batch_object_id, payload=payload)
        except Exception as exc:
            payload = self._audit_payload(
                area=area,
                original_filename=original_filename,
                canonical_path=canonical_path,
                file_hash=file_hash,
                status="ERROR",
                detail=str(exc),
                user=uploaded_by,
            )
            log_event(uploaded_by, "BUDGET_UPLOAD_FAILED", "reportes.PresupuestoImport", area.key, payload=payload)
            raise

        return BudgetAreaUploadResult(
            status=self.STATUS_SUCCESS,
            area_key=area.key,
            area_label=area.label,
            original_filename=original_filename,
            canonical_filename=canonical_path.name,
            file_hash=file_hash,
            periods=periods,
            sheets_imported=sheets_imported,
            imports_created=summary.imports_created,
            imports_updated=summary.imports_updated,
            lines_created=summary.lines_created,
            lines_updated=summary.lines_updated,
            snapshot_rows_created=snapshot_rows_created,
            snapshot_rows_updated=snapshot_rows_updated,
            summary_message="Carga procesada y snapshot presupuestal actualizado.",
        )

    @classmethod
    def history_queryset(cls):
        return AuditLog.objects.select_related("user").filter(
            action__in=["BUDGET_UPLOAD_SUCCESS", "BUDGET_UPLOAD_FAILED", "BUDGET_UPLOAD_DUPLICATE"],
            model="reportes.PresupuestoImport",
        )
