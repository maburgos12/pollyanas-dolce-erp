from __future__ import annotations

import base64
import hashlib
import json
import unicodedata
import zlib
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db import transaction
from django.db.models import Prefetch, Q, prefetch_related_objects
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosingLine,
    PointProductHistoryImport,
    PointProductHistoryRow,
    PointProduct,
)
from pos_bridge.services.historical_inventory_capture import (
    HistoricalInventoryCaptureError,
    _movement_datetime,
    point_stock_history_instant,
)
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources


HISTORY_LIMIT = 500
ORIGINAL_HISTORY_LIMITS = frozenset({5, 10, 15, 50, 100, 300, 500})
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
    documentary_opening: Decimal | None = None
    documentary_closing: Decimal | None = None
    documentary_boundary_movement_ids: tuple[int, ...] = ()

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
            "documentary_opening": str(self.documentary_opening) if self.documentary_opening is not None else None,
            "documentary_closing": str(self.documentary_closing) if self.documentary_closing is not None else None,
            "documentary_boundary_movement_ids": list(self.documentary_boundary_movement_ids),
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
    def _covers_month(record, month: date, *, boundary_rows=None) -> bool:
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
        try:
            fetched_rows = int(metadata.get("fetched_rows") or 0)
            history_limit = int(metadata.get("history_limit", HISTORY_LIMIT))
        except (TypeError, ValueError):
            return False
        if fetched_rows < 0 or history_limit <= 0 or fetched_rows > history_limit:
            return False
        fetched_ids = metadata.get("fetched_movement_ids")
        if "fetched_movement_ids" in metadata and (
            not isinstance(fetched_ids, list)
            or any(type(value) is not int or value <= 0 for value in fetched_ids)
            or len(fetched_ids) != fetched_rows
            or len(set(fetched_ids)) != len(fetched_ids)
        ):
            return False
        if fetched_rows < history_limit:
            return True
        month_start, _ = _month_bounds(month)
        earliest = AuditStockHistoryService._boundary_stamp(record)
        if earliest is None:
            return False
        if boundary_rows is None:
            boundary_rows = list(record.rows.filter(movement_at=earliest))
        matches = [row for row in boundary_rows if row.movement_at == earliest
                   and (not fetched_ids or row.row_number in fetched_ids)]
        if not matches:
            return False
        try:
            # The boundary belongs to the latest capture, not older retained rows.
            return all(point_stock_history_instant(row.raw_payload) <= month_start for row in matches)
        except HistoricalInventoryCaptureError:
            return False

    @staticmethod
    def _boundary_stamp(record):
        try:
            stamp = datetime.fromisoformat(str((record.raw_metadata or {}).get("earliest_movement_at") or ""))
        except (TypeError, ValueError):
            return None
        return stamp if timezone.is_aware(stamp) else None

    @staticmethod
    def _candidate_filter(records, month):
        month_start, month_end = _month_bounds(month)
        # Legacy naive dates were persisted seven hours late. Explicit-zone rows
        # remain correct; the raw reader performs the exact cut for both kinds.
        query = Q(movement_at__gte=month_start, movement_at__lt=month_end + timedelta(hours=7), cancelled=False)
        for record in records:
            boundary = AuditStockHistoryService._boundary_stamp(record)
            if boundary is not None:
                query |= Q(import_record_id=record.pk, movement_at=boundary)
        return query

    def _reconcile_candidates(self, record, month, candidates):
        month_start, month_end = _month_bounds(month)
        selected, invalid = [], []
        for row in candidates:
            if row.cancelled:
                continue
            try:
                instant = point_stock_history_instant(row.raw_payload)
            except HistoricalInventoryCaptureError:
                invalid.append(row.row_number)
                continue
            if month_start <= instant < month_end:
                selected.append((instant, row.row_number, row))
        rows = [entry[2] for entry in sorted(selected, key=lambda entry: entry[:2])]
        return self._reconcile_record(record, month, rows, boundary_rows=candidates, invalid_ids=invalid)

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
        fetched_at = timezone.now().isoformat()
        self._persist_response(
            branch, product, month, rows, fetched_at=fetched_at,
            history_limit=HISTORY_LIMIT,
        )
        return self.reconcile(branch, product, month)

    @staticmethod
    def _aware_receipt(value):
        try:
            stamp = datetime.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise AuditStockHistoryError("Fecha original de consulta inválida.") from exc
        if timezone.is_naive(stamp):
            raise AuditStockHistoryError("La consulta original requiere zona horaria.")
        return stamp

    def ingest_original_response(self, branch, product, month: date, rows, *, evidence):
        """Ingest a complete preserved Point response, without contacting Point.

        The caller supplies the original request and locator, not a reconstructed
        boundary pair. Receipt time remains distinct from this database write.
        """
        rows, evidence = deepcopy(rows), deepcopy(evidence)
        if not isinstance(branch, PointBranch) or not isinstance(product, PointProduct):
            raise AuditStockHistoryError("La identidad requiere sucursal y producto Point canónicos.")
        if not isinstance(rows, list) or not isinstance(evidence, dict):
            raise AuditStockHistoryError("Se requiere respuesta original íntegra y evidencia.")
        expected_keys = {"source", "domain", "response_complete", "branch_id", "product_id", "request",
                         "retrieved_at", "history_limit", "fetched_rows", "raw_sha256", "original_locator", "request_provenance"}
        if set(evidence) != expected_keys:
            raise AuditStockHistoryError("El contrato de evidencia original tiene campos faltantes o ajenos.")
        limit, count = evidence.get("history_limit"), evidence.get("fetched_rows")
        if (type(limit) is not int or type(count) is not int or
                limit not in ORIGINAL_HISTORY_LIMITS or count != len(rows) or count > limit):
            raise AuditStockHistoryError("Conteo/límite no acredita la respuesta original completa.")
        request = {"path": "/Stock/GetHistorial", "params": {
            "tipo": "false", "almacen": str(branch.external_id),
            "pkproducto": str(product.external_id), "movimientos": str(limit), "tipoMovimiento": "",
        }}
        if (evidence.get("source") != SOURCE_NAME or evidence.get("domain") != "PRODUCT" or
                evidence.get("response_complete") is not True or
                type(evidence.get("branch_id")) is not int or evidence["branch_id"] != branch.pk or
                type(evidence.get("product_id")) is not int or evidence["product_id"] != product.pk or
                evidence.get("request") != request or not branch.external_id or not product.external_id):
            raise AuditStockHistoryError("Identidad, dominio o petición original Point no coincide.")
        locator = evidence.get("original_locator")
        if (not isinstance(locator, dict) or set(locator) != {"source_file", "source_line"} or not isinstance(locator.get("source_file"), str) or
                not locator["source_file"].strip() or type(locator.get("source_line")) is not int or
                locator["source_line"] <= 0):
            raise AuditStockHistoryError("Falta localizador de la evidencia original.")
        request_proof = evidence["request_provenance"]
        if (not isinstance(request_proof, dict) or set(request_proof) != {
                "kind", "source_file", "source_code", "source_sha256", "client_contract"} or
                request_proof.get("kind") != "DERIVED_FROM_ACQUISITION_SCRIPT" or
                request_proof.get("client_contract") != "PointHttpSessionClient.get_stock_history" or
                not isinstance(request_proof.get("source_file"), str) or not request_proof["source_file"].strip() or
                not isinstance(request_proof.get("source_code"), str) or not request_proof["source_code"].strip() or
                hashlib.sha256(request_proof["source_code"].encode()).hexdigest() != request_proof.get("source_sha256")):
            raise AuditStockHistoryError("Procedencia del script de adquisición original inválida.")
        receipt = self._aware_receipt(evidence.get("retrieved_at"))
        if receipt > timezone.now():
            raise AuditStockHistoryError("La fecha original de consulta está en el futuro.")
        try:
            raw_json = json.dumps(rows, sort_keys=True, default=str).encode()
        except (TypeError, ValueError) as exc:
            raise AuditStockHistoryError("Respuesta original no serializable.") from exc
        if hashlib.sha256(raw_json).hexdigest() != evidence.get("raw_sha256"):
            raise AuditStockHistoryError("SHA de respuesta original no coincide.")
        ids = set()
        for raw in rows:
            if not isinstance(raw, dict):
                raise AuditStockHistoryError("Movimiento original inválido.")
            movement_id = raw.get("FK_Movimiento")
            if type(movement_id) is not int or movement_id <= 0 or movement_id in ids:
                raise AuditStockHistoryError("FK_Movimiento inválido o duplicado dentro de la respuesta.")
            ids.add(movement_id)
            for field in ("Cantidad", "Existencia_anterior", "Existencia_nueva"):
                if raw.get(field) in (None, "") or not _decimal(raw[field]).is_finite():
                    raise AuditStockHistoryError("Cantidad o existencia original incompleta.")
            cancelled = raw.get("Cancelado")
            if not (type(cancelled) is bool or (type(cancelled) is str and cancelled in {"true", "false", "True", "False"})):
                raise AuditStockHistoryError("Estado de cancelación original inválido.")
            try:
                instant = point_stock_history_instant(raw)
            except HistoricalInventoryCaptureError as exc:
                raise AuditStockHistoryError("Fecha original de movimiento inválida.") from exc
            if instant > receipt:
                raise AuditStockHistoryError("Movimiento posterior a su consulta original.")
        self._persist_response(
            branch, product, month, rows, fetched_at=evidence["retrieved_at"],
            history_limit=limit, evidence=evidence, raw_json=raw_json,
        )
        return self.reconcile(branch, product, month)

    def _persist_response(self, branch, product, month, rows, *, fetched_at, history_limit,
                          evidence=None, raw_json=None):
        receipt = self._aware_receipt(fetched_at)
        rows = deepcopy(rows)
        parsed_rows = [self._parse_row(row) for row in rows]
        with transaction.atomic():
            # Unique canonical identity serializes creation too. A losing creator
            # rereads the committed import before discovering all affected months.
            record = self._canonical_import(branch, product)
            record = PointProductHistoryImport.objects.select_for_update().get(pk=record.pk)
            if (record.point_branch_id != branch.pk or record.point_product_id != product.pk or
                    (record.raw_metadata or {}).get("source") != SOURCE_NAME):
                raise AuditStockHistoryError("Identidad canónica ocupada por otra fuente o claves incompatibles.")
            metadata = deepcopy(record.raw_metadata or {})
            legacy_membership = "response_provenance" not in metadata
            proof = deepcopy(evidence) if evidence is not None else {
                "source": SOURCE_NAME, "domain": "PRODUCT", "response_complete": True,
                "retrieved_at": fetched_at, "history_limit": history_limit, "fetched_rows": len(rows),
                "raw_sha256": hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
                "request": {"path": "/Stock/GetHistorial", "params": {
                    "tipo": "false", "almacen": str(branch.external_id), "pkproducto": str(product.external_id),
                    "movimientos": str(history_limit), "tipoMovimiento": "",
                }},
                "request_provenance": {"kind": "LIVE_HTTP", "client_contract": "PointHttpSessionClient.get_stock_history"},
            }
            fingerprint = hashlib.sha256(json.dumps(proof, sort_keys=True, default=str).encode()).hexdigest()
            proofs = metadata.setdefault("response_provenance", [])
            if any(item.get("fingerprint") == fingerprint for item in proofs):
                if evidence is not None:
                    archive = metadata.get("original_responses", {}).get(fingerprint, {})
                    try:
                        archived_json = zlib.decompress(base64.b64decode(archive["raw_zlib_base64"], validate=True))
                    except (KeyError, TypeError, ValueError, zlib.error) as exc:
                        raise AuditStockHistoryError("Archivo original canónico ausente o corrupto.") from exc
                    if (archived_json != raw_json or archive.get("fingerprint") != fingerprint or
                            archive.get("encoding") != "zlib-base64-json" or
                            any(archive.get(key) != value for key, value in evidence.items())):
                        raise AuditStockHistoryError("Procedencia del archivo original canónico no coincide.")
                return
            previous_rows = list(record.rows.all())
            previous_by_id = {row.row_number: row for row in previous_rows}
            versions = metadata.setdefault("movement_fetched_at", {})
            try:
                latest_receipt = self._aware_receipt(metadata.get("fetched_at"))
            except AuditStockHistoryError:
                latest_receipt = None
            legacy_ids = metadata.get("fetched_movement_ids")
            if (legacy_membership and latest_receipt is not None and isinstance(legacy_ids, list) and
                    type(metadata.get("fetched_rows")) is int and len(legacy_ids) == metadata["fetched_rows"] and
                    all(type(value) is int and value > 0 and value in previous_by_id for value in legacy_ids) and
                    len(set(legacy_ids)) == len(legacy_ids)):
                # Only the exact last-capture membership has a known legacy age.
                # Unselected retained rows must never inherit that receipt time.
                for movement_id in legacy_ids:
                    versions.setdefault(str(movement_id), metadata["fetched_at"])
            writes = []
            for movement_id, defaults in parsed_rows:
                old = previous_by_id.get(movement_id)
                same = old is not None and old.raw_payload == defaults["raw_payload"]
                known = versions.get(str(movement_id))
                try:
                    row_receipt = self._aware_receipt(known)
                except AuditStockHistoryError:
                    row_receipt = None
                if old is not None and not same:
                    if evidence is not None and (row_receipt is None or receipt <= row_receipt):
                        raise AuditStockHistoryError("Conflicto de movimiento sin procedencia anterior verificable.")
                    if row_receipt is not None and receipt <= row_receipt:
                        raise AuditStockHistoryError("La respuesta no puede sobrescribir un movimiento más reciente.")
                if not same:
                    writes.append((movement_id, defaults))
                if (old is None or evidence is None or row_receipt is not None) and (row_receipt is None or receipt > row_receipt):
                    versions[str(movement_id)] = fetched_at
            local_tz = ZoneInfo(settings.TIME_ZONE)
            months = {month.replace(day=1), timezone.localdate().replace(day=1)}
            for _, values in parsed_rows:
                months.add(timezone.localtime(values["movement_at"], local_tz).date().replace(day=1))
                months.add(timezone.localtime(point_stock_history_instant(values["raw_payload"]), local_tz).date().replace(day=1))
            for old in previous_rows:
                months.add(timezone.localtime(old.movement_at, local_tz).date().replace(day=1))
                months.add(timezone.localtime(point_stock_history_instant(old.raw_payload), local_tz).date().replace(day=1))
            # A metadata replacement also changes coverage of months with no
            # movements. Existing audits and closures may read those months;
            # closures are company-wide and have no branch/product key.
            from recetas.models import ProductoMonthClosure
            from reportes.models import ProductInventoryAuditCase
            months.update(ProductoMonthClosure.objects.values_list("month_start", flat=True))
            months.update(ProductInventoryAuditCase.objects.filter(
                branch=branch, product=product,
            ).values_list("month", flat=True))
            months.update(day.replace(day=1) for day in
                          PointHistoricalInventoryClosingLine.objects.filter(
                              branch=branch, product=product,
                          ).values_list("closing__operational_date", flat=True))
            oldest, newest = min(months), max(months)
            cursor = oldest
            while cursor <= newest:
                months.add(cursor)
                cursor = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)
            # A historical closing is the next month's documentary opening.
            months.add(cursor)
            lock_product_month_sources(sorted(months))
            for movement_id, defaults in writes:
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
            promote = latest_receipt is None or receipt > latest_receipt
            if promote:
                record.report_date = timezone.localdate(receipt)
            record.point_branch = branch
            record.point_product = product
            record.row_count = all_rows.count()
            record.latest_movement_at = last.movement_at if last else None
            record.latest_unit_cost = last.unit_cost if last else Decimal("0")
            if promote:
                metadata.update({
                "source": SOURCE_NAME,
                "history_limit": history_limit,
                "fetched_rows": len(rows),
                "earliest_movement_at": min(
                    (values["movement_at"] for _, values in parsed_rows), default=None,
                ).isoformat() if parsed_rows else "",
                "fetched_movement_ids": [movement_id for movement_id, _ in parsed_rows],
                "latest_movement_at": last.movement_at.isoformat() if last else "",
                "fetched_at": fetched_at,
                })
            proof.update({"fingerprint": fingerprint, "ingested_at": timezone.now().isoformat()})
            proofs.append(proof)
            if evidence is not None:
                metadata.setdefault("original_responses", {})[fingerprint] = {
                    **proof, "encoding": "zlib-base64-json",
                    "raw_zlib_base64": base64.b64encode(zlib.compress(raw_json)).decode("ascii"),
                }
            record.raw_metadata = metadata
            record.save()

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

        rows = list(
            record.rows.filter(self._candidate_filter([record], month)).order_by("movement_at", "row_number")
        )
        return self._reconcile_candidates(record, month, rows)

    def reconcile_many(self, lines, month: date, *, include_zero_difference=False) -> dict[tuple[int, int], PointHistoryReconciliation]:
        keys = {
            (line.branch.id, line.product.id)
            for line in lines
            if include_zero_difference or Decimal(line.difference) != 0
        }
        if not keys:
            return {}
        records = list(PointProductHistoryImport.objects.filter(
            point_branch_id__in={key[0] for key in keys},
            point_product_id__in={key[1] for key in keys},
            raw_metadata__source=SOURCE_NAME,
        ))
        month_rows = PointProductHistoryRow.objects.filter(
            self._candidate_filter(records, month),
        ).order_by("movement_at", "row_number")
        prefetch_related_objects(records,
            Prefetch("rows", queryset=month_rows, to_attr="audit_month_rows")
        )
        return {
            (record.point_branch_id, record.point_product_id): self._reconcile_candidates(
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
        *,
        boundary_rows=None,
        invalid_ids=(),
    ) -> PointHistoryReconciliation:
        coverage_status = "COMPLETE" if not invalid_ids and self._covers_month(record, month, boundary_rows=boundary_rows) else "INCOMPLETE"
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
        unknown_ids = list(invalid_ids)
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
            elif category == "identified_adjustment":
                amount = row.new_existence - row.previous_existence
                words = set(_normalized(row.movement_type).split())
                if (
                    abs(amount) != abs(row.quantity)
                    or ("SALIDA" in words and amount > 0)
                    or ("ENTRADA" in words and amount < 0)
                ):
                    unknown_ids.append(row.row_number)
                    continue
            else:
                amount = abs(amount)
            totals[category] += amount
            ids_by_category[category].append(row.row_number)

        result = PointHistoryReconciliation(
            coverage_status=coverage_status,
            **totals,
            movement_ids=tuple(movement_ids),
            movement_ids_by_category={
                key: tuple(value) for key, value in ids_by_category.items() if value
            },
            unknown_movement_ids=tuple(unknown_ids),
        )
        if coverage_status == "COMPLETE" and rows and not unknown_ids:
            opening, closing = rows[0].previous_existence, rows[-1].new_existence
            continuous = all(
                current.previous_existence == previous.new_existence
                for previous, current in zip(rows, rows[1:])
            )
            if continuous and result.expected_closing(opening) == closing:
                return replace(
                    result,
                    documentary_opening=opening,
                    documentary_closing=closing,
                    documentary_boundary_movement_ids=(rows[0].row_number, rows[-1].row_number),
                )
        return result

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
