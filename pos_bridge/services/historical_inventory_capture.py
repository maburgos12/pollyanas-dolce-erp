from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone as datetime_timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Iterable

from django.db import transaction
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
)
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources, POINT_BUSINESS_TIMEZONE
from recetas.models import ProductoMonthClosure


HISTORY_LIMIT = 500


class HistoricalInventoryCaptureError(ValueError):
    pass


@dataclass(frozen=True)
class HistoricalStockResolution:
    stock: Decimal
    evidence: dict


@dataclass(frozen=True)
class HistoricalInventoryCaptureResult:
    closing: PointHistoricalInventoryClosing
    resolved_count: int
    unresolved_count: int


def _movement_datetime(row: dict) -> datetime:
    value = str(row.get("Fecha") or "").strip()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HistoricalInventoryCaptureError(f"Fecha inválida en historial Point: {value or '(vacía)'}") from exc


def point_stock_history_instant(raw_payload: dict) -> datetime:
    """Stock Fecha uses moment.utc in Point; legacy persisted dates stay intact.

    This contract is exclusive to Stock history, not commercial note timestamps.
    Invalid evidence must fail closed rather than fall back to a derived date.
    """
    if not isinstance(raw_payload, dict):
        raise HistoricalInventoryCaptureError("Historial Stock sin payload documental.")
    stamp = _movement_datetime(raw_payload)
    if timezone.is_naive(stamp):
        stamp = stamp.replace(tzinfo=datetime_timezone.utc)
    return stamp.astimezone(datetime_timezone.utc)


def _decimal(value, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalInventoryCaptureError(f"{field} inválida en historial Point: {value!r}") from exc


@dataclass(frozen=True)
class HistoricalWasteEffect:
    status: str = "NO_SPECIAL"
    direction: int = 0
    signed_waste: Decimal = Decimal("0")


def historical_waste_effect(raw: dict) -> HistoricalWasteEffect:
    """Original debit and reversal are independent events, never an inferred pair."""
    if not isinstance(raw, dict):
        return HistoricalWasteEffect()
    name = " ".join("".join(char for char in unicodedata.normalize(
        "NFKD", str(raw.get("Movimiento") or "")) if not unicodedata.combining(char)).upper().split())
    def flag(value):
        if type(value) is bool:
            return value
        if type(value) is str and value.casefold() in {"true", "false"}:
            return value.casefold() == "true"
        return None
    cancelled = flag(raw.get("Cancelado"))
    kind = raw.get("FK_Tipo_Movimiento")
    if type(kind) is int and kind == 5 and name == "MERMA" and cancelled is False:
        return HistoricalWasteEffect()
    if not (name == "CANCELACION DE MERMA" or kind == 15
            or cancelled is not False and (name == "MERMA" or kind == 5)):
        return HistoricalWasteEffect()
    invalid = HistoricalWasteEffect("INVALID")
    raw_id = raw.get("FK_Movimiento")
    if not ((type(raw_id) is int and raw_id > 0)
            or type(raw_id) is str and re.fullmatch(r"[0-9]+", raw_id) and int(raw_id) > 0):
        return invalid
    if "isInsumo" in raw and flag(raw["isInsumo"]) is not False:
        return invalid
    if type(kind) is not int:
        return invalid
    direction = -1 if kind == 5 and name == "MERMA" and cancelled is True else (
        1 if kind == 15 and name == "CANCELACION DE MERMA" and cancelled is False else 0)
    if not direction or "isCargo" in raw and flag(raw["isCargo"]) is not (direction == -1):
        return invalid
    try:
        values = [raw.get(key) for key in ("Existencia_anterior", "Existencia_nueva", "Cantidad")]
        if any(value is None or value == "" or type(value) is bool for value in values):
            return invalid
        previous, new, quantity = (Decimal(str(value)) for value in values)
        if not all(value.is_finite() for value in (previous, new, quantity)) or quantity <= 0:
            return invalid
        if Fraction(new) - Fraction(previous) != direction * Fraction(quantity):
            return invalid
    except (InvalidOperation, TypeError, ValueError):
        return invalid
    return HistoricalWasteEffect("VALID", direction, -direction * quantity)


def _movement_evidence(row: dict, *, method: str, history_rows: int) -> dict:
    return {
        "method": method,
        "movement_id": row.get("FK_Movimiento"),
        "movement_type_id": row.get("FK_Tipo_Movimiento"),
        "movement": row.get("Movimiento") or "",
        "movement_date": row.get("Fecha"),
        "history_rows": history_rows,
        "history_limit": HISTORY_LIMIT,
    }


def resolve_stock_at_close(
    history: list[dict],
    *,
    operational_date: date,
    current_stock: Decimal | None = None,
    history_limit: int = HISTORY_LIMIT,
) -> HistoricalStockResolution:
    if not history:
        if current_stock == Decimal("0"):
            return HistoricalStockResolution(
                stock=Decimal("0"),
                evidence={
                    "method": "no_history_current_zero",
                    "history_rows": 0,
                    "history_limit": history_limit,
                },
            )
        raise HistoricalInventoryCaptureError("Producto sin historial suficiente para acreditar el saldo de cierre.")

    dated_rows = [(point_stock_history_instant(row), row) for row in history]
    if any(historical_waste_effect(row).status == "INVALID" for _, row in dated_rows):
        raise HistoricalInventoryCaptureError("Efecto histórico de merma sin identidad o contrato original válido.")
    at_or_before = [(stamp, row) for stamp, row in dated_rows
                    if stamp.astimezone(POINT_BUSINESS_TIMEZONE).date() <= operational_date]
    if at_or_before:
        _stamp, boundary = max(
            at_or_before,
            key=lambda item: (item[0], int(item[1].get("FK_Movimiento") or 0)),
        )
        effect = historical_waste_effect(boundary)
        if effect.status == "INVALID" or boundary.get("Cancelado") and effect.status != "VALID":
            raise HistoricalInventoryCaptureError("El movimiento límite está cancelado y no acredita un saldo.")
        return HistoricalStockResolution(
            stock=_decimal(boundary.get("Existencia_nueva"), field="Existencia_nueva"),
            evidence=_movement_evidence(
                boundary,
                method="latest_movement_at_or_before_close",
                history_rows=len(history),
            ),
        )

    if len(history) >= history_limit:
        raise HistoricalInventoryCaptureError(
            "El historial máximo de Point no alcanza el cierre solicitado; se requiere reporte oficial."
        )

    _stamp, first_later = min(
        dated_rows,
        key=lambda item: (item[0], int(item[1].get("FK_Movimiento") or 0)),
    )
    effect = historical_waste_effect(first_later)
    if effect.status == "INVALID" or first_later.get("Cancelado") and effect.status != "VALID":
        raise HistoricalInventoryCaptureError("El movimiento límite está cancelado y no acredita un saldo.")
    return HistoricalStockResolution(
        stock=_decimal(first_later.get("Existencia_anterior"), field="Existencia_anterior"),
        evidence=_movement_evidence(
            first_later,
            method="opening_before_first_later_movement",
            history_rows=len(history),
        ),
    )


class HistoricalPointInventoryClosingCapture:
    def __init__(self, *, client):
        self.client = client

    @staticmethod
    def _current_stock_by_branch(rows: Iterable[dict]) -> dict[str, Decimal]:
        result = {}
        for row in rows:
            branch_id = str(row.get("PK_Sucursal") or "").strip()
            if not branch_id:
                continue
            result[branch_id] = _decimal(row.get("Cantidad"), field="Cantidad")
        return result

    def _call_point(self, operation):
        try:
            return operation()
        except Exception:
            self.client.login()
            return operation()

    def capture(
        self,
        *,
        operational_date: date,
        branches: list[PointBranch],
        products: list[PointProduct],
    ) -> HistoricalInventoryCaptureResult:
        if not branches or not products:
            raise HistoricalInventoryCaptureError("El manifiesto requiere sucursales y productos Point.")
        invalid_branches = [branch.external_id for branch in branches if not str(branch.external_id).isdigit()]
        invalid_products = [product.external_id for product in products if not str(product.external_id).isdigit()]
        if invalid_branches or invalid_products:
            raise HistoricalInventoryCaptureError(
                f"El manifiesto contiene identificadores Point no numéricos: "
                f"sucursales={invalid_branches}, productos={invalid_products}."
            )

        expected_branch_ids = [branch.id for branch in branches]
        expected_product_ids = [product.id for product in products]
        resume_closing = next(
            (
                closing
                for closing in PointHistoricalInventoryClosing.objects.filter(
                    operational_date=operational_date,
                    status__in=[PointHistoricalInventoryClosing.STATUS_DRAFT, PointHistoricalInventoryClosing.STATUS_VERIFIED],
                    source=PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
                )
                .prefetch_related("lines__branch", "lines__product")
                .order_by("-id")
                if (set(closing.expected_branch_ids) == set(expected_branch_ids)
                    or (closing.status == PointHistoricalInventoryClosing.STATUS_VERIFIED
                        and set(closing.expected_branch_ids) < set(expected_branch_ids)))
                and set(closing.expected_product_ids) == set(expected_product_ids)
            ),
            None,
        )
        resolved = [
            {
                "branch": line.branch,
                "product": line.product,
                "stock": line.stock,
                "evidence": line.evidence,
            }
            for line in (resume_closing.lines.all() if resume_closing else ())
        ]
        existing_keys = {
            (row["branch"].id, row["product"].id) for row in resolved
        }
        if resume_closing is not None and resume_closing.status == PointHistoricalInventoryClosing.STATUS_VERIFIED:
            original_keys = {(branch_id, product_id) for branch_id in resume_closing.expected_branch_ids
                for product_id in resume_closing.expected_product_ids}
            if not original_keys or existing_keys != original_keys:
                raise HistoricalInventoryCaptureError("El cierre verificado no coincide con su manifiesto; no se puede ampliar.")
            if set(resume_closing.expected_branch_ids) == set(expected_branch_ids):
                return HistoricalInventoryCaptureResult(closing=resume_closing,
                    resolved_count=len(existing_keys), unresolved_count=0)

        # Import local: el auditor comparte el parser de fechas de este módulo.
        from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService

        history_service = AuditStockHistoryService(client=self.client)
        self.client.login()
        unresolved = []
        for product in products:
            pending_branches = [
                branch
                for branch in branches
                if (branch.id, product.id) not in existing_keys
            ]
            if not pending_branches:
                continue
            try:
                current_rows = self._call_point(
                    lambda: self.client.get_product_stock(product.external_id)
                )
                current = self._current_stock_by_branch(current_rows)
            except Exception as exc:
                for branch in pending_branches:
                    unresolved.append({
                        "branch_id": branch.id,
                        "branch_external_id": branch.external_id,
                        "product_id": product.id,
                        "product_external_id": product.external_id,
                        "reason": str(exc),
                    })
                continue
            for branch in pending_branches:
                try:
                    cached = history_service._existing_import(branch, product)
                    self._call_point(lambda: history_service.capture(
                        branch, product, operational_date.replace(day=1),
                        force=bool(cached and (
                            (cached.row_count == 0 and current.get(str(branch.external_id)) != Decimal("0"))
                            or ("fetched_movement_ids" not in cached.raw_metadata
                                and cached.row_count != cached.raw_metadata.get("fetched_rows"))))))
                    history_record = history_service._existing_import(branch, product)
                    history_rows = history_record.rows.all()
                    fetched_ids = history_record.raw_metadata.get("fetched_movement_ids")
                    if fetched_ids is not None:
                        history_rows = history_rows.filter(row_number__in=fetched_ids)
                    history = list(history_rows.values_list("raw_payload", flat=True))
                    resolution = resolve_stock_at_close(
                        history,
                        operational_date=operational_date,
                        current_stock=current.get(str(branch.external_id)),
                        history_limit=HISTORY_LIMIT,
                    )
                except Exception as exc:
                    unresolved.append({
                        "branch_id": branch.id,
                        "branch_external_id": branch.external_id,
                        "product_id": product.id,
                        "product_external_id": product.external_id,
                        "reason": str(exc),
                    })
                    continue
                resolved.append({
                    "branch": branch,
                    "product": product,
                    "stock": resolution.stock,
                    "evidence": resolution.evidence,
                })

        fingerprint_payload = {
            "operational_date": operational_date.isoformat(),
            "lines": [
                [row["branch"].external_id, row["product"].external_id, str(row["stock"]), row["evidence"]]
                for row in sorted(resolved, key=lambda item: (int(item["branch"].external_id), int(item["product"].external_id)))
            ],
            "unresolved": sorted(
                unresolved,
                key=lambda item: (int(item["branch_external_id"]), int(item["product_external_id"])),
            ),
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        expected_count = len(branches) * len(products)
        status = (
            PointHistoricalInventoryClosing.STATUS_VERIFIED
            if not unresolved and len(resolved) == expected_count
            else PointHistoricalInventoryClosing.STATUS_DRAFT
        )
        if (resume_closing is not None and resume_closing.status == PointHistoricalInventoryClosing.STATUS_VERIFIED
            and status != PointHistoricalInventoryClosing.STATUS_VERIFIED):
            raise HistoricalInventoryCaptureError(
                f"La ampliación no está completa; se conserva el cierre previo. "
                f"Pendientes: {len(unresolved)}. Ejemplos: {unresolved[:3]}")
        metadata = {
            **(resume_closing.metadata if resume_closing is not None else {}),
            "method": "point_stock_history_boundary",
            "expected_line_count": expected_count,
            "resolved_line_count": len(resolved),
            "unresolved_count": len(unresolved),
            "unresolved": unresolved,
        }

        with transaction.atomic():
            lock_product_month_sources([operational_date])
            if resume_closing is not None:
                closing = PointHistoricalInventoryClosing.objects.select_for_update().get(
                    pk=resume_closing.pk
                )
                if closing.source_fingerprint != resume_closing.source_fingerprint:
                    raise HistoricalInventoryCaptureError("El cierre cambió durante la consulta; se conserva la versión vigente.")
                if set(closing.expected_branch_ids) != set(expected_branch_ids):
                    if ProductoMonthClosure.objects.filter(
                        month_start=operational_date.replace(day=1), is_locked=True,
                    ).exists():
                        raise HistoricalInventoryCaptureError("El mes está bloqueado; no se puede ampliar su cierre.")
                    metadata["extension_previous_fingerprint"] = closing.source_fingerprint
                    metadata["reused_line_count"] = len(existing_keys)
                created = False
                closing.status = status
                closing.source_fingerprint = fingerprint
                closing.expected_branch_ids = expected_branch_ids
                closing.expected_product_ids = expected_product_ids
                closing.metadata = metadata
                closing.retrieved_at = timezone.now()
                closing.save(
                    update_fields=[
                        "status",
                        "source_fingerprint",
                        "expected_branch_ids",
                        "expected_product_ids",
                        "metadata",
                        "retrieved_at",
                        "updated_at",
                    ]
                )
            else:
                closing, created = PointHistoricalInventoryClosing.objects.get_or_create(
                    operational_date=operational_date,
                    source_fingerprint=fingerprint,
                    defaults={
                        "status": status,
                        "source": PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
                        "expected_branch_ids": expected_branch_ids,
                        "expected_product_ids": expected_product_ids,
                        "metadata": metadata,
                        "retrieved_at": timezone.now(),
                    },
                )
            if created:
                PointHistoricalInventoryClosingLine.objects.bulk_create([
                    PointHistoricalInventoryClosingLine(
                        closing=closing,
                        branch=row["branch"],
                        product=row["product"],
                        stock=row["stock"],
                        evidence=row["evidence"],
                    )
                    for row in resolved
                ])
            elif resume_closing is not None:
                PointHistoricalInventoryClosingLine.objects.bulk_create([
                    PointHistoricalInventoryClosingLine(
                        closing=closing,
                        branch=row["branch"],
                        product=row["product"],
                        stock=row["stock"],
                        evidence=row["evidence"],
                    )
                    for row in resolved
                    if (row["branch"].id, row["product"].id) not in existing_keys
                ])
        return HistoricalInventoryCaptureResult(
            closing=closing,
            resolved_count=len(resolved),
            unresolved_count=len(unresolved),
        )
