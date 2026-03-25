from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from maestros.models import CostoInsumo, Proveedor
from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.inventory_page import PointInventoryPage
from pos_bridge.browser.session import BrowserSessionManager
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.auth_service import PointAuthService
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from recetas.utils.normalizacion import normalizar_nombre


@dataclass(slots=True)
class PointInventoryCostRow:
    branch_name: str
    category_name: str
    point_internal_id: str
    point_code: str
    point_name: str
    point_category: str
    quantity: Decimal
    unit: str
    unit_cost: Decimal
    total_cost: Decimal
    last_movement: str
    raw_row: list[str]


class PointInventoryCostCaptureService:
    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        identity_service: PointRecipeIdentityService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.identity_service = identity_service or PointRecipeIdentityService()
        self.auth_service = PointAuthService(self.settings)

    @staticmethod
    def _dec(value) -> Decimal:
        try:
            return Decimal(str(value or 0))
        except Exception:
            return Decimal("0")

    def _normalize_supply_row(self, *, category_name: str, branch_name: str, row: list[str]) -> PointInventoryCostRow | None:
        cells = [str(cell or "").strip() for cell in row]
        if len(cells) < 8:
            return None
        if len(cells) >= 9:
            point_internal_id, point_code, point_name, point_category, quantity, unit, unit_cost, total_cost, *rest = cells
            last_movement = rest[0] if rest else ""
        else:
            point_internal_id = ""
            point_code, point_name, point_category, quantity, unit, unit_cost, total_cost, last_movement = cells[:8]
        return PointInventoryCostRow(
            branch_name=branch_name,
            category_name=category_name,
            point_internal_id=point_internal_id,
            point_code=point_code,
            point_name=point_name,
            point_category=point_category,
            quantity=self._dec(quantity),
            unit=unit,
            unit_cost=self._dec(unit_cost),
            total_cost=self._dec(total_cost),
            last_movement=last_movement,
            raw_row=cells,
        )

    def _expand_search_scope(
        self,
        *,
        queries: list[str] | None = None,
        point_codes: list[str] | None = None,
    ) -> tuple[list[str], list[str]]:
        raw_queries = [str(q or "").strip() for q in (queries or []) if str(q or "").strip()]
        raw_codes = [str(code or "").strip().upper() for code in (point_codes or []) if str(code or "").strip()]

        expanded_queries: list[str] = []
        expanded_codes: list[str] = []
        seen_queries: set[str] = set()
        seen_codes: set[str] = set()

        def add_query(value: str) -> None:
            text = str(value or "").strip()
            key = normalizar_nombre(text)
            if not text or not key or key in seen_queries:
                return
            seen_queries.add(key)
            expanded_queries.append(text)

        def add_code(value: str) -> None:
            text = str(value or "").strip().upper()
            if not text or text in seen_codes:
                return
            seen_codes.add(text)
            expanded_codes.append(text)

        for code in raw_codes:
            add_code(code)
        for query in raw_queries:
            add_query(query)

        for query in raw_queries:
            resolved = self.identity_service.resolve_insumo(point_name=query)
            if resolved.insumo is None:
                continue
            insumo = resolved.insumo
            add_query(insumo.nombre)
            add_query(insumo.nombre_point)
            for alias_name in insumo.aliases.values_list("nombre", flat=True):
                add_query(alias_name)
            add_code(insumo.codigo_point)

        return expanded_queries, expanded_codes

    def capture_matches(
        self,
        *,
        branch_hint: str = "ALMACEN",
        queries: list[str] | None = None,
        point_codes: list[str] | None = None,
    ) -> list[PointInventoryCostRow]:
        queries, point_codes = self._expand_search_scope(queries=queries, point_codes=point_codes)
        normalized_queries = [normalizar_nombre(q) for q in queries]
        results: list[PointInventoryCostRow] = []

        client = PlaywrightBrowserClient(self.settings)
        with BrowserSessionManager(client) as session:
            # ALMACEN es un filtro dentro de Existencias, no un workspace confiable de login.
            self.auth_service.login(session, branch_hint=None)
            page = PointInventoryPage(session.page, self.settings)
            page.open_inventory_module()
            branches = page.list_branches()
            target = next(
                (
                    branch
                    for branch in branches
                    if branch_hint.strip().upper() in str(branch.get("label") or "").upper()
                ),
                None,
            )
            if target:
                page.select_branch(target)
                branch_name = str(target.get("label") or branch_hint)
            else:
                branch_name = branch_hint

            category_options = page.list_category_options(kind="supplies")
            for option in category_options:
                label = str(option.get("label") or "").strip()
                value = str(option.get("value") or "").strip()
                if not value or "SELECCIONE" in label.upper():
                    continue
                page.select_category(value, kind="supplies")
                payload = page.extract_inventory_table(kind="supplies")
                for raw_row in payload.get("rows") or []:
                    normalized = self._normalize_supply_row(category_name=label, branch_name=branch_name, row=raw_row)
                    if normalized is None:
                        continue
                    haystack = normalizar_nombre(f"{normalized.point_code} {normalized.point_name} {normalized.point_category}")
                    if point_codes and normalized.point_code.upper() in point_codes:
                        results.append(normalized)
                        continue
                    if normalized_queries and any(query and query in haystack for query in normalized_queries):
                        results.append(normalized)
        return results

    def persist_cost_row(self, row: PointInventoryCostRow, *, supplier_name: str = "POINT EXISTENCIA ALMACEN") -> tuple[CostoInsumo | None, bool, str]:
        resolved = self.identity_service.resolve_insumo(point_code=row.point_code, point_name=row.point_name)
        if resolved.insumo is None:
            return None, False, "NO_MATCH_ERP"
        if row.unit_cost <= 0:
            return None, False, "UNIT_COST_ZERO"

        supplier, _ = Proveedor.objects.get_or_create(nombre=supplier_name, defaults={"activo": True})
        source_hash = hashlib.sha256(
            f"POINT_EXISTENCIA_ALMACEN|{resolved.insumo.id}|{row.branch_name}|{row.point_code}|{row.unit_cost}|{row.last_movement}".encode("utf-8")
        ).hexdigest()
        cost, created = CostoInsumo.objects.get_or_create(
            source_hash=source_hash,
            defaults={
                "insumo": resolved.insumo,
                "proveedor": supplier,
                "fecha": datetime.fromisoformat(row.last_movement).date() if row.last_movement else datetime.today().date(),
                "moneda": "MXN",
                "costo_unitario": row.unit_cost,
                "raw": {
                    "source": "POINT_EXISTENCIA_ALMACEN",
                    "branch": row.branch_name,
                    "category": row.category_name,
                    "point_code": row.point_code,
                    "point_name": row.point_name,
                    "point_category": row.point_category,
                    "quantity": str(row.quantity),
                    "unit": row.unit,
                    "unit_cost": str(row.unit_cost),
                    "total_cost": str(row.total_cost),
                    "last_movement": row.last_movement,
                    "raw_row": row.raw_row,
                },
            },
        )
        return cost, created, "CREATED" if created else "EXISTS"
