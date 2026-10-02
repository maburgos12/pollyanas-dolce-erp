from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db import transaction
from django.db.models import Prefetch
from django.utils import timezone

from pos_bridge.models import (
    PointProductHistoryImport,
    PointProductHistoryRow,
)
from pos_bridge.services.historical_inventory_capture import _movement_datetime


HISTORY_LIMIT = 500
SOURCE_NAME = "POINT_STOCK_HISTORY_API"


class AuditStockHistoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class PointHistoryReconciliation:
    coverage_status: str
    production: Decimal = Decimal("0")
    sales: Decimal = Decimal("0")
    waste: Decimal = Decimal("0")
    transfer_in: Decimal = Decimal("0")
    transfer_out: Decimal = Decimal("0")
    conversion_in: Decimal = Decimal("0")
    conversion_out: Decimal = Decimal("0")
    identified_adjustment: Decimal = Decimal("0")
    movement_ids: tuple[int, ...] = ()
    movement_ids_by_category: dict[str, tuple[int, ...]] | None = None
    unknown_movement_ids: tuple[int, ...] = ()

    def expected_closing(self, opening: Decimal) -> Decimal:
        return (
            opening
            + self.production
            + self.transfer_in
            + self.conversion_in
            + self.identified_adjustment
            - self.sales
            - self.waste
            - self.transfer_out
            - self.conversion_out
        )

    def unexplained_remainder(
        self,
        opening: Decimal,
        point_closing: Decimal,
    ) -> Decimal:
        return point_closing - self.expected_closing(opening)

    def as_dict(self, *, opening: Decimal, point_closing: Decimal) -> dict[str, object]:
        expected = self.expected_closing(opening)
        return {
            "coverage_status": self.coverage_status,
            "production": str(self.production),
            "sales": str(self.sales),
            "waste": str(self.waste),
            "transfer_in": str(self.transfer_in),
            "transfer_out": str(self.transfer_out),
            "conversion_in": str(self.conversion_in),
            "conversion_out": str(self.conversion_out),
            "identified_adjustment": str(self.identified_adjustment),
            "expected_closing": str(expected),
            "point_closing": str(point_closing),
            "unexplained_remainder": str(point_closing - expected),
            "movement_ids": list(self.movement_ids),
            "movement_ids_by_category": {
                key: list(value)
                for key, value in (self.movement_ids_by_category or {}).items()
            },
            "unknown_movement_ids": list(self.unknown_movement_ids),
        }


def _decimal(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise AuditStockHistoryError(f"Cantidad inválida en historial Point: {value!r}") from exc


def _normalized(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(
        "".join(char for char in decomposed if not unicodedata.combining(char))
        .upper()
        .split()
    )


def _boolean(value) -> bool:
    if isinstance(value, bool):
        return value
    return _normalized(value) in {"1", "TRUE", "SI", "YES"}


def _month_bounds(month: date) -> tuple[datetime, datetime]:
    month = month.replace(day=1)
    next_month = (
        month.replace(year=month.year + 1, month=1)
        if month.month == 12
        else month.replace(month=month.month + 1)
    )
    local_tz = ZoneInfo(settings.TIME_ZONE)
    return (
        datetime.combine(month, time.min, tzinfo=local_tz),
        datetime.combine(next_month, time.min, tzinfo=local_tz),
    )


class AuditStockHistoryService:
    def __init__(self, *, client=None):
        self.client = client

    @staticmethod
    def _file_hash(branch, product) -> str:
        identity = f"point-stock-history:{branch.external_id}:{product.external_id}"
        return hashlib.sha256(identity.encode()).hexdigest()

    def _canonical_import(self, branch, product) -> PointProductHistoryImport:
        record, _ = PointProductHistoryImport.objects.get_or_create(
            file_hash=self._file_hash(branch, product),
            defaults={
                "source_filename": "point-api-stock-history",
                "report_path": "/Stock/GetHistorial",
                "report_title": "Historial transaccional Point",
                "product_name": product.name,
                "branch_name": branch.name,
                "point_branch": branch,
                "point_product": product,
                "raw_metadata": {"source": SOURCE_NAME},
            },
        )
        return record

    def _existing_import(self, branch, product):
        return PointProductHistoryImport.objects.filter(
            file_hash=self._file_hash(branch, product),
            raw_metadata__source=SOURCE_NAME,
        ).first()

    @staticmethod
    def _covers_month(record, month: date) -> bool:
        if record is None:
            return False
        metadata = record.raw_metadata or {}
        if "fetched_rows" not in metadata:
            return False
        _, month_end = _month_bounds(month)
        try:
            fetched_at = datetime.fromisoformat(str(metadata.get("fetched_at") or ""))
        except ValueError:
            return False
        if timezone.is_naive(fetched_at) or fetched_at < month_end:
            return False
        fetched_rows = int(metadata.get("fetched_rows") or 0)
        history_limit = int(metadata.get("history_limit") or HISTORY_LIMIT)
        if fetched_rows < history_limit:
            return True
        month_start, _ = _month_bounds(month)
        earliest = str(metadata.get("earliest_movement_at") or "")
        if not earliest:
            return False
        try:
            return datetime.fromisoformat(earliest) <= month_start
        except ValueError:
            return False

    def capture(self, branch, product, month: date, *, force: bool = False):
        record = self._existing_import(branch, product)
        if not force and self._covers_month(record, month):
            return self.reconcile(branch, product, month)
        if self.client is None:
            raise AuditStockHistoryError("No hay cliente Point para completar el historial.")

        rows = self.client.get_stock_history(
            product.external_id,
            branch.external_id,
            movements=HISTORY_LIMIT,
        )
        record = self._canonical_import(branch, product)
        parsed_rows = [self._parse_row(row) for row in rows]
        with transaction.atomic():
            for movement_id, defaults in parsed_rows:
                PointProductHistoryRow.objects.update_or_create(
                    import_record=record,
                    row_number=movement_id,
                    defaults=defaults,
                )
            all_rows = record.rows.order_by("movement_at", "row_number")
            last = all_rows.last()
            record.source_filename = "point-api-stock-history"
            record.report_path = "/Stock/GetHistorial"
            record.report_title = "Historial transaccional Point"
            record.product_name = product.name
            record.branch_name = branch.name
            record.report_date = timezone.localdate()
            record.point_branch = branch
            record.point_product = product
            record.row_count = all_rows.count()
            record.latest_movement_at = last.movement_at if last else None
            record.latest_unit_cost = last.unit_cost if last else Decimal("0")
            record.raw_metadata = {
                "source": SOURCE_NAME,
                "history_limit": HISTORY_LIMIT,
                "fetched_rows": len(rows),
                "earliest_movement_at": min(
                    (values["movement_at"] for _, values in parsed_rows), default=None,
                ).isoformat() if parsed_rows else "",
                "fetched_movement_ids": [movement_id for movement_id, _ in parsed_rows],
                "latest_movement_at": last.movement_at.isoformat() if last else "",
                "fetched_at": timezone.now().isoformat(),
            }
            record.save()
        return self.reconcile(branch, product, month)

    @staticmethod
    def _parse_row(row: dict) -> tuple[int, dict[str, object]]:
        try:
            movement_id = int(row.get("FK_Movimiento"))
        except (TypeError, ValueError) as exc:
            raise AuditStockHistoryError("Movimiento Point sin FK_Movimiento válido.") from exc
        stamp = _movement_datetime(row)
        if timezone.is_naive(stamp):
            stamp = timezone.make_aware(stamp, ZoneInfo(settings.TIME_ZONE))
        return movement_id, {
            "movement_at": stamp,
            "movement_type": str(row.get("Movimiento") or "")[:160],
            "previous_existence": _decimal(row.get("Existencia_anterior")),
            "quantity": _decimal(row.get("Cantidad")),
            "new_existence": _decimal(row.get("Existencia_nueva")),
            "total_cost": _decimal(row.get("Costo_Total") or row.get("Costo total")),
            "unit_cost": _decimal(row.get("Costo_Unitario") or row.get("Costo unitario")),
            "cancelled": _boolean(row.get("Cancelado")),
            "raw_payload": row,
        }

    def reconcile(self, branch, product, month: date) -> PointHistoryReconciliation:
        record = self._existing_import(branch, product)
        if record is None:
            return PointHistoryReconciliation(coverage_status="MISSING")

        month_start, month_end = _month_bounds(month)
        rows = list(
            record.rows.filter(
                movement_at__gte=month_start,
                movement_at__lt=month_end,
                cancelled=False,
            ).order_by("movement_at", "row_number")
        )
        return self._reconcile_record(record, month, rows)

    def reconcile_many(self, lines, month: date, *, include_zero_difference=False) -> dict[tuple[int, int], PointHistoryReconciliation]:
        keys = {
            (line.branch.id, line.product.id)
            for line in lines
            if include_zero_difference or Decimal(line.difference) != 0
        }
        if not keys:
            return {}
        month_start, month_end = _month_bounds(month)
        month_rows = PointProductHistoryRow.objects.filter(
            movement_at__gte=month_start,
            movement_at__lt=month_end,
            cancelled=False,
        ).order_by("movement_at", "row_number")
        records = PointProductHistoryImport.objects.filter(
            point_branch_id__in={key[0] for key in keys},
            point_product_id__in={key[1] for key in keys},
            raw_metadata__source=SOURCE_NAME,
        ).prefetch_related(
            Prefetch("rows", queryset=month_rows, to_attr="audit_month_rows")
        )
        return {
            (record.point_branch_id, record.point_product_id): self._reconcile_record(
                record,
                month,
                record.audit_month_rows,
            )
            for record in records
            if (record.point_branch_id, record.point_product_id) in keys
        }

    def _reconcile_record(
        self,
        record,
        month: date,
        rows,
    ) -> PointHistoryReconciliation:
        coverage_status = "COMPLETE" if self._covers_month(record, month) else "INCOMPLETE"
        totals = {
            "production": Decimal("0"),
            "sales": Decimal("0"),
            "waste": Decimal("0"),
            "transfer_in": Decimal("0"),
            "transfer_out": Decimal("0"),
            "conversion_in": Decimal("0"),
            "conversion_out": Decimal("0"),
            "identified_adjustment": Decimal("0"),
        }
        ids_by_category = {key: [] for key in totals}
        unknown_ids = []
        movement_ids = []
        for row in rows:
            movement_ids.append(row.row_number)
            category = self._category(row.movement_type, row.quantity)
            if category is None:
                if row.quantity:
                    unknown_ids.append(row.row_number)
                continue
            amount = row.quantity
            if _normalized(row.movement_type) == "CANCELACION VENTA":
                if row.new_existence - row.previous_existence != abs(amount):
                    unknown_ids.append(row.row_number)
                    continue
                amount = -abs(amount)
            elif category != "identified_adjustment":
                amount = abs(amount)
            totals[category] += amount
            ids_by_category[category].append(row.row_number)

        return PointHistoryReconciliation(
            coverage_status=coverage_status,
            **totals,
            movement_ids=tuple(movement_ids),
            movement_ids_by_category={
                key: tuple(value) for key, value in ids_by_category.items() if value
            },
            unknown_movement_ids=tuple(unknown_ids),
        )

    @staticmethod
    def _category(movement_type: str, quantity: Decimal) -> str | None:
        movement = _normalized(movement_type)
        words = set(movement.split())
        if "CANCELACION" in words and movement != "CANCELACION VENTA":
            return None
        if "PRODUCCION" in words:
            return "production"
        if "VENTA" in words:
            return "sales"
        if "MERMA" in words:
            return "waste"
        if "TRANSFERENCIA" in words:
            if "RETORNO" in words or "ENTRADA" in words:
                return "transfer_in"
            if "SALIDA" in words:
                return "transfer_out"
            return "transfer_in" if quantity > 0 else "transfer_out"
        if "CONVERSION" in words:
            if "ENTRADA" in words:
                return "conversion_in"
            if "SALIDA" in words:
                return "conversion_out"
            return "conversion_in" if quantity > 0 else "conversion_out"
        if "AJUSTE" in words or "INVENTARIO" in words:
            return "identified_adjustment"
        return None
