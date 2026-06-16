from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from django.core.cache import cache
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointBranch
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError, ExtractionError
from pos_bridge.utils.helpers import normalize_text
from recetas.models import normalizar_codigo_point


class PointLiveInventoryLookupError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class PointLiveInventoryResult:
    product_code: str
    product_name: str
    point_product_id: str
    point_branch_id: str
    point_branch_name: str
    stock_qty: Decimal
    captured_at: datetime
    raw_payload: dict[str, Any]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PointLiveInventoryLookupError("Point devolvió una cantidad de stock inválida.") from exc


def _first_present(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


class PointLiveInventoryLookupService:
    def __init__(self, *, client_factory=None):
        self.enabled = _env_bool("PICKUP_LIVE_POINT_LOOKUP_ENABLED", False)
        self.timeout_seconds = _env_int("PICKUP_LIVE_POINT_LOOKUP_TIMEOUT_MS", 5000, minimum=1000) / 1000
        self.cache_seconds = _env_int("PICKUP_LIVE_POINT_LOOKUP_CACHE_SECONDS", 20, minimum=0)
        self.client_factory = client_factory or PointHttpSessionClient

    def get_stock(
        self,
        *,
        product_codes: list[str],
        sucursal: Sucursal,
        point_branch: PointBranch | None,
    ) -> PointLiveInventoryResult | None:
        if not self.enabled:
            return None

        codes = [code.strip() for code in product_codes if (code or "").strip()]
        if not codes:
            return None

        cache_key = self._cache_key(codes=codes, sucursal=sucursal, point_branch=point_branch)
        if self.cache_seconds > 0:
            cached = cache.get(cache_key)
            if cached:
                return self._result_from_cache(cached)

        settings = load_point_bridge_settings()
        try:
            with self.client_factory(settings) as client:
                client.login(branch_hint=sucursal.nombre or sucursal.codigo)
                product = self._find_product(client=client, codes=codes)
                product_id = _first_present(product, ("PK", "PK_Producto", "id", "Id"))
                if product_id is None:
                    raise PointLiveInventoryLookupError("Point no devolvió PK para el producto de stock.")
                stock_rows = client.get_product_stock(product_id, timeout=self.timeout_seconds)
        except (AuthenticationError, ConfigurationError, ExtractionError, OSError, TimeoutError) as exc:
            raise PointLiveInventoryLookupError(str(exc)) from exc

        branch_row = self._find_branch_row(stock_rows=stock_rows, sucursal=sucursal, point_branch=point_branch)
        captured_at = timezone.now()
        result = PointLiveInventoryResult(
            product_code=str(_first_present(product, ("Codigo", "codigo", "SKU", "sku")) or codes[0]),
            product_name=str(_first_present(product, ("Nombre", "nombre", "Name", "name")) or ""),
            point_product_id=str(product_id),
            point_branch_id=str(_first_present(branch_row, ("PK_Sucursal", "pk_sucursal", "SucursalID", "id_sucursal")) or ""),
            point_branch_name=str(_first_present(branch_row, ("Sucursal", "sucursal", "NombreSucursal", "name")) or ""),
            stock_qty=_decimal(_first_present(branch_row, ("Cantidad", "cantidad", "Stock", "stock"))),
            captured_at=captured_at,
            raw_payload=branch_row,
        )
        if self.cache_seconds > 0:
            cache.set(cache_key, self._result_to_cache(result), timeout=self.cache_seconds)
        return result

    def _find_product(self, *, client: PointHttpSessionClient, codes: list[str]) -> dict[str, Any]:
        target_codes = {normalizar_codigo_point(code) for code in codes if code}
        fallback_rows: list[dict[str, Any]] = []
        for code in codes:
            rows = client.get_stock_products(text=code, timeout=self.timeout_seconds)
            fallback_rows.extend(rows)
            for row in rows:
                row_codes = {
                    normalizar_codigo_point(str(value))
                    for value in (
                        _first_present(row, ("Codigo", "codigo", "SKU", "sku")),
                        _first_present(row, ("Codigo_Barra", "codigo_barra", "CodigoBarra")),
                    )
                    if value not in (None, "")
                }
                if row_codes & target_codes:
                    return row
        if len(fallback_rows) == 1:
            return fallback_rows[0]
        raise PointLiveInventoryLookupError("Point no devolvió un producto exacto para el código solicitado.")

    def _find_branch_row(
        self,
        *,
        stock_rows: list[dict[str, Any]],
        sucursal: Sucursal,
        point_branch: PointBranch | None,
    ) -> dict[str, Any]:
        target_ids = {str(point_branch.external_id).strip()} if point_branch and point_branch.external_id else set()
        target_names = {
            normalize_text(value)
            for value in (
                getattr(point_branch, "name", "") if point_branch else "",
                sucursal.codigo,
                sucursal.nombre,
            )
            if value
        }
        for row in stock_rows:
            row_id = str(_first_present(row, ("PK_Sucursal", "pk_sucursal", "SucursalID", "id_sucursal")) or "").strip()
            if row_id and row_id in target_ids:
                return row
            row_name = normalize_text(str(_first_present(row, ("Sucursal", "sucursal", "NombreSucursal", "name")) or ""))
            if row_name and row_name in target_names:
                return row
        raise PointLiveInventoryLookupError("Point no devolvió existencia para la sucursal solicitada.")

    def _cache_key(self, *, codes: list[str], sucursal: Sucursal, point_branch: PointBranch | None) -> str:
        code_key = ",".join(normalizar_codigo_point(code) for code in codes)
        branch_key = str(point_branch.external_id if point_branch else sucursal.codigo).strip().lower()
        return f"pos_bridge:pickup_live_point:{code_key}:{branch_key}"

    def _result_to_cache(self, result: PointLiveInventoryResult) -> dict[str, Any]:
        return {
            "product_code": result.product_code,
            "product_name": result.product_name,
            "point_product_id": result.point_product_id,
            "point_branch_id": result.point_branch_id,
            "point_branch_name": result.point_branch_name,
            "stock_qty": str(result.stock_qty),
            "captured_at": result.captured_at.isoformat(),
            "raw_payload": result.raw_payload,
        }

    def _result_from_cache(self, payload: dict[str, Any]) -> PointLiveInventoryResult:
        return PointLiveInventoryResult(
            product_code=payload["product_code"],
            product_name=payload["product_name"],
            point_product_id=payload["point_product_id"],
            point_branch_id=payload["point_branch_id"],
            point_branch_name=payload["point_branch_name"],
            stock_qty=Decimal(str(payload["stock_qty"])),
            captured_at=datetime.fromisoformat(payload["captured_at"]),
            raw_payload=payload.get("raw_payload") or {},
        )
