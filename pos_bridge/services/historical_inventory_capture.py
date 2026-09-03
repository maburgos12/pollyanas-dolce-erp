from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable

from django.db import transaction
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointProduct,
)


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


def _decimal(value, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalInventoryCaptureError(f"{field} inválida en historial Point: {value!r}") from exc


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

    dated_rows = [(_movement_datetime(row), row) for row in history]
    at_or_before = [(stamp, row) for stamp, row in dated_rows if stamp.date() <= operational_date]
    if at_or_before:
        _stamp, boundary = max(
            at_or_before,
            key=lambda item: (item[0], int(item[1].get("FK_Movimiento") or 0)),
        )
        if boundary.get("Cancelado"):
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
    if first_later.get("Cancelado"):
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

        self.client.login()
        resolved = []
        unresolved = []
        for product in products:
            current = self._current_stock_by_branch(self.client.get_product_stock(product.external_id))
            for branch in branches:
                try:
                    history = self.client.get_stock_history(
                        product.external_id,
                        branch.external_id,
                        movements=HISTORY_LIMIT,
                    )
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
        metadata = {
            "method": "point_stock_history_boundary",
            "expected_line_count": expected_count,
            "resolved_line_count": len(resolved),
            "unresolved_count": len(unresolved),
            "unresolved": unresolved,
        }

        with transaction.atomic():
            closing, created = PointHistoricalInventoryClosing.objects.get_or_create(
                operational_date=operational_date,
                source_fingerprint=fingerprint,
                defaults={
                    "status": status,
                    "source": PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
                    "expected_branch_ids": [branch.id for branch in branches],
                    "expected_product_ids": [product.id for product in products],
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
        return HistoricalInventoryCaptureResult(
            closing=closing,
            resolved_count=len(resolved),
            unresolved_count=len(unresolved),
        )
