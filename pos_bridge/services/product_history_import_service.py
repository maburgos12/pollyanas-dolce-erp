from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointProduct,
    PointProductCostReconciliation,
    PointProductHistoryImport,
    PointProductHistoryRow,
)
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from pos_bridge.utils.exceptions import ExtractionError
from recetas.models import RecetaCostoSemanal
from recetas.utils.normalizacion import normalizar_nombre


@dataclass
class ParsedProductHistoryRow:
    row_number: int
    movement_at: object
    movement_type: str
    previous_existence: Decimal
    quantity: Decimal
    new_existence: Decimal
    total_cost: Decimal
    unit_cost: Decimal
    cancelled: bool
    raw_payload: dict


@dataclass
class ParsedProductHistoryReport:
    file_hash: str
    source_filename: str
    report_path: str
    report_title: str
    product_name: str
    branch_name: str
    report_date: date | None
    rows: list[ParsedProductHistoryRow]
    metadata: dict


class PointProductHistoryImportService:
    TITLE_PREFIX = "HISTORIAL DE MOVIMIENTOS DE "

    def __init__(self, identity_service: PointRecipeIdentityService | None = None):
        self.identity_service = identity_service or PointRecipeIdentityService()

    @staticmethod
    def _decimal(value) -> Decimal:
        text = str(value or "").strip().replace(",", "")
        if not text:
            return Decimal("0")
        try:
            return Decimal(text)
        except Exception:
            return Decimal("0")

    def _file_hash(self, path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _extract_title_cell(self, dataframe: pd.DataFrame) -> str:
        for _, row in dataframe.head(8).iterrows():
            for value in row.tolist():
                text = str(value or "").strip()
                if self.TITLE_PREFIX in text.upper():
                    return text
        raise ExtractionError("No se encontró el encabezado del historial de producto Point.")

    def _parse_title_metadata(self, title_cell: str) -> tuple[str, str, date | None]:
        parts = [part.strip() for part in str(title_cell or "").splitlines() if str(part or "").strip()]
        if not parts:
            raise ExtractionError("El encabezado del historial Point está vacío.")
        title_line = parts[0]
        title_upper = title_line.upper()
        if self.TITLE_PREFIX not in title_upper:
            raise ExtractionError("El encabezado del historial Point no tiene el prefijo esperado.")
        product_name = title_line[title_upper.index(self.TITLE_PREFIX) + len(self.TITLE_PREFIX) :].strip().title()
        branch_name = parts[1].strip() if len(parts) > 1 else ""
        report_date = None
        if len(parts) > 2:
            parsed_date = pd.to_datetime(parts[2], dayfirst=True, errors="coerce")
            if pd.notna(parsed_date):
                report_date = parsed_date.date()
        return product_name, branch_name, report_date

    def _find_header_index(self, dataframe: pd.DataFrame) -> int:
        for index, row in dataframe.iterrows():
            values = [str(value or "").strip().upper() for value in row.tolist()]
            if values and "FECHA" in values and "MOVIMIENTO" in values and "COSTO" in values:
                return int(index)
        raise ExtractionError("No se encontró la fila de encabezados del historial Point.")

    def _parse_rows(self, dataframe: pd.DataFrame, *, start_index: int) -> list[ParsedProductHistoryRow]:
        rows: list[ParsedProductHistoryRow] = []
        for row_index in range(start_index + 1, len(dataframe.index)):
            row = dataframe.iloc[row_index].tolist()
            movement_at_raw = row[0] if len(row) > 0 else None
            movement_type = str(row[3] if len(row) > 3 else "").strip()
            if pd.isna(movement_at_raw) or not movement_type:
                continue
            movement_at = pd.to_datetime(movement_at_raw, errors="coerce")
            if pd.isna(movement_at):
                continue
            movement_dt = movement_at.to_pydatetime()
            if timezone.is_naive(movement_dt):
                movement_dt = timezone.make_aware(movement_dt, timezone.get_current_timezone())
            quantity = self._decimal(row[5] if len(row) > 5 else 0)
            total_cost = self._decimal(row[7] if len(row) > 7 else 0)
            unit_cost = Decimal("0")
            if quantity and quantity != 0:
                unit_cost = (total_cost.copy_abs() / quantity.copy_abs()).quantize(Decimal("0.000001"))
            cancelled_text = str(row[8] if len(row) > 8 else "").strip().upper()
            parsed = ParsedProductHistoryRow(
                row_number=row_index + 1,
                movement_at=movement_dt,
                movement_type=movement_type,
                previous_existence=self._decimal(row[4] if len(row) > 4 else 0),
                quantity=quantity,
                new_existence=self._decimal(row[6] if len(row) > 6 else 0),
                total_cost=total_cost,
                unit_cost=unit_cost,
                cancelled=cancelled_text == "SI",
                raw_payload={
                    "fecha": str(movement_at_raw),
                    "movimiento": movement_type,
                    "existencia_anterior": str(row[4] if len(row) > 4 else ""),
                    "cantidad": str(row[5] if len(row) > 5 else ""),
                    "existencia_nueva": str(row[6] if len(row) > 6 else ""),
                    "costo": str(row[7] if len(row) > 7 else ""),
                    "cancelado": cancelled_text,
                },
            )
            rows.append(parsed)
        if not rows:
            raise ExtractionError("El historial Point no contiene movimientos utilizables.")
        return rows

    def parse_report(self, *, report_path: str) -> ParsedProductHistoryReport:
        path = Path(report_path)
        dataframe = pd.read_excel(path, sheet_name=0, header=None)
        title_cell = self._extract_title_cell(dataframe)
        product_name, branch_name, report_date = self._parse_title_metadata(title_cell)
        header_index = self._find_header_index(dataframe)
        rows = self._parse_rows(dataframe, start_index=header_index)
        return ParsedProductHistoryReport(
            file_hash=self._file_hash(path),
            source_filename=path.name,
            report_path=str(path),
            report_title=title_cell,
            product_name=product_name,
            branch_name=branch_name,
            report_date=report_date,
            rows=rows,
            metadata={"header_row_index": header_index + 1},
        )

    def _resolve_branch(self, branch_name: str) -> PointBranch | None:
        normalized = normalizar_nombre(branch_name)
        if not normalized:
            return None
        branch = PointBranch.objects.filter(normalized_name=normalized).order_by("id").first()
        if branch is not None:
            return branch
        return PointBranch.objects.filter(name__icontains=branch_name.strip()).order_by("id").first()

    def _resolve_point_product(self, product_name: str):
        normalized = normalizar_nombre(product_name)
        if not normalized:
            return None
        product = PointProduct.objects.filter(normalized_name=normalized).order_by("id").first()
        if product is not None:
            return product
        return PointProduct.objects.filter(name__icontains=product_name.strip()).order_by("id").first()

    def _latest_point_cost(self, rows: list[ParsedProductHistoryRow]) -> Decimal:
        for row in sorted(rows, key=lambda item: item.movement_at, reverse=True):
            if row.cancelled:
                continue
            if row.unit_cost > 0:
                return row.unit_cost
        return Decimal("0")

    def _build_reconciliation_status(self, *, receta, point_unit_cost: Decimal, erp_unit_cost: Decimal) -> tuple[str, str]:
        if receta is None:
            return PointProductCostReconciliation.STATUS_MISSING_RECIPE, "No se encontró receta ERP para el producto del reporte."
        if point_unit_cost <= 0:
            return PointProductCostReconciliation.STATUS_MISSING_POINT_COST, "El historial no devolvió un costo unitario Point utilizable."
        if erp_unit_cost <= 0:
            return PointProductCostReconciliation.STATUS_MISSING_ERP_COST, "La receta ERP aún no devuelve un costo unitario mayor a cero."
        if point_unit_cost == erp_unit_cost:
            return PointProductCostReconciliation.STATUS_MATCH, "El costo Point y el costo ERP coinciden exactamente."
        return PointProductCostReconciliation.STATUS_DELTA, "Existe diferencia entre el costo Point importado y el costo ERP vigente."

    def _resolve_erp_unit_cost(self, receta) -> Decimal:
        if receta is None:
            return Decimal("0")
        weekly_snapshot = (
            RecetaCostoSemanal.objects.filter(
                receta=receta,
                scope_type=RecetaCostoSemanal.SCOPE_RECIPE,
            )
            .order_by("-week_start", "-id")
            .first()
        )
        if weekly_snapshot is not None and weekly_snapshot.costo_total:
            return Decimal(weekly_snapshot.costo_total)
        return Decimal(receta.costo_total_estimado_decimal or 0)

    def import_report(self, *, report_path: str, allow_reimport: bool = False) -> tuple[PointProductHistoryImport, bool]:
        parsed = self.parse_report(report_path=report_path)
        existing = PointProductHistoryImport.objects.filter(file_hash=parsed.file_hash).first()
        if existing is not None and not allow_reimport:
            return existing, False
        if existing is not None and allow_reimport:
            existing.rows.all().delete()
            import_record = existing
        else:
            import_record = PointProductHistoryImport(file_hash=parsed.file_hash)

        point_branch = self._resolve_branch(parsed.branch_name)
        point_product = self._resolve_point_product(parsed.product_name)
        receta = self.identity_service.resolve_recipe(
            point_code=getattr(point_product, "sku", "") or "",
            point_name=parsed.product_name,
        )
        latest_point_cost = self._latest_point_cost(parsed.rows)

        import_record.source_filename = parsed.source_filename
        import_record.report_path = parsed.report_path
        import_record.report_title = parsed.report_title[:300]
        import_record.product_name = parsed.product_name[:255]
        import_record.branch_name = parsed.branch_name[:200]
        import_record.report_date = parsed.report_date
        import_record.point_branch = point_branch
        import_record.point_product = point_product
        import_record.receta = receta
        import_record.row_count = len(parsed.rows)
        import_record.latest_movement_at = max(row.movement_at for row in parsed.rows)
        import_record.latest_unit_cost = latest_point_cost
        import_record.raw_metadata = parsed.metadata
        import_record.save()

        PointProductHistoryRow.objects.bulk_create(
            [
                PointProductHistoryRow(
                    import_record=import_record,
                    row_number=row.row_number,
                    movement_at=row.movement_at,
                    movement_type=row.movement_type[:160],
                    previous_existence=row.previous_existence,
                    quantity=row.quantity,
                    new_existence=row.new_existence,
                    total_cost=row.total_cost,
                    unit_cost=row.unit_cost,
                    cancelled=row.cancelled,
                    raw_payload=row.raw_payload,
                )
                for row in parsed.rows
            ],
            batch_size=500,
        )

        erp_unit_cost = self._resolve_erp_unit_cost(receta)
        variance_amount = latest_point_cost - erp_unit_cost
        variance_pct = Decimal("0")
        if erp_unit_cost > 0:
            variance_pct = variance_amount / erp_unit_cost
        status, notes = self._build_reconciliation_status(
            receta=receta,
            point_unit_cost=latest_point_cost,
            erp_unit_cost=erp_unit_cost,
        )
        PointProductCostReconciliation.objects.update_or_create(
            import_record=import_record,
            defaults={
                "point_branch": point_branch,
                "receta": receta,
                "point_unit_cost": latest_point_cost,
                "erp_unit_cost": erp_unit_cost,
                "variance_amount": variance_amount,
                "variance_pct": variance_pct,
                "status": status,
                "notes": notes,
                "raw_payload": {
                    "source_filename": parsed.source_filename,
                    "report_date": parsed.report_date.isoformat() if parsed.report_date else "",
                    "row_count": len(parsed.rows),
                },
            },
        )
        return import_record, True
