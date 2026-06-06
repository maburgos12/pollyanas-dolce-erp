from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from maestros.models import CostoInsumo, Proveedor
from pos_bridge.models import PointProduct
from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.inventory_page import PointInventoryPage
from pos_bridge.browser.session import BrowserSessionManager
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.auth_service import PointAuthService
from pos_bridge.services.point_cost_validation import validate_point_inventory_cost_row
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from reportes.models import ProductoReventaCosto
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
    # "supply" → sección Insumos (#tablaInsumosPA), "product" → sección Productos (#tablaProductosPA)
    kind: str = "supply"


@dataclass(slots=True)
class PointInventoryCostCaptureResult:
    branch_name: str
    rows_seen: int
    matches_found: int
    costs_created: int
    costs_existing: int
    unresolved_matches: int
    zero_cost_matches: int
    unresolved_samples: list[dict[str, str]]
    zero_cost_samples: list[dict[str, str]]
    rejected_matches: int = 0
    rejected_samples: list[dict[str, str]] | None = None
    resale_costs_created: int = 0
    resale_costs_existing: int = 0
    # Desglose por sección: insumos vs. productos de reventa
    supply_rows_seen: int = 0
    product_rows_seen: int = 0


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

    @staticmethod
    def _row_date(row: PointInventoryCostRow):
        if row.last_movement:
            try:
                return datetime.fromisoformat(row.last_movement.replace("Z", "+00:00")).date()
            except Exception:
                pass
        return datetime.today().date()

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

    def _normalize_product_row(
        self, *, category_name: str, branch_name: str, row: list[str]
    ) -> PointInventoryCostRow | None:
        """
        Parsea una fila de la tabla de Productos en Point (#tablaProductosPA).
        Columnas esperadas (proveedor ya pre-filtrado en el dropdown):
          [internal_id?,] código, nombre, cantidad, unidad, costo_unitario, costo_total, último_movimiento[, opciones]
        """
        # Quitar celdas vacías al final (columna "Opciones" suele tener botones sin texto)
        cells = [str(cell or "").strip() for cell in row]
        while cells and not cells[-1]:
            cells.pop()
        if len(cells) < 5:
            return None
        # Variante con id interno (9+ cols) o sin él (7-8 cols)
        if len(cells) >= 9:
            point_internal_id, point_code, point_name, quantity, unit, unit_cost, total_cost, *rest = cells
            last_movement = rest[0] if rest else ""
        elif len(cells) >= 7:
            point_internal_id = ""
            point_code, point_name, quantity, unit, unit_cost, total_cost, *rest = cells
            last_movement = rest[0] if rest else ""
        elif len(cells) >= 6:
            point_internal_id = ""
            point_code, point_name, quantity, unit, unit_cost, total_cost = cells[:6]
            last_movement = ""
        else:
            point_internal_id = ""
            point_code = cells[0] if len(cells) > 0 else ""
            point_name = cells[1] if len(cells) > 1 else ""
            quantity = cells[2] if len(cells) > 2 else "0"
            unit = cells[3] if len(cells) > 3 else ""
            unit_cost = cells[4] if len(cells) > 4 else "0"
            total_cost = "0"
            last_movement = ""
        return PointInventoryCostRow(
            branch_name=branch_name,
            category_name=category_name,
            point_internal_id=point_internal_id,
            point_code=point_code,
            point_name=point_name,
            point_category="",  # la tabla de productos no tiene columna de categoría propia
            quantity=self._dec(quantity),
            unit=unit,
            unit_cost=self._dec(unit_cost),
            total_cost=self._dec(total_cost),
            last_movement=last_movement,
            raw_row=cells,
            kind="product",
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
        include_all_rows: bool = False,
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

            # ── Paso 1: sección Insumos (#tablaInsumosPA) ─────────────────────
            supply_options = page.list_category_options(kind="supplies")
            for option in supply_options:
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
                    if include_all_rows:
                        results.append(normalized)
                        continue
                    haystack = normalizar_nombre(f"{normalized.point_code} {normalized.point_name} {normalized.point_category}")
                    if point_codes and normalized.point_code.upper() in point_codes:
                        results.append(normalized)
                        continue
                    if normalized_queries and any(query and query in haystack for query in normalized_queries):
                        results.append(normalized)

            # ── Paso 2: sección Productos (#tablaProductosPA) ─────────────────
            # Aquí viven los productos de reventa (pirotecnia, bebidas, decorativos…).
            # El dropdown de categoría filtra por proveedor, no por categoría de insumo.
            product_options = page.list_category_options(kind="products")
            for option in product_options:
                label = str(option.get("label") or "").strip()
                value = str(option.get("value") or "").strip()
                if not value or "SELECCIONE" in label.upper():
                    continue
                page.select_category(value, kind="products")
                payload = page.extract_inventory_table(kind="products")
                for raw_row in payload.get("rows") or []:
                    normalized = self._normalize_product_row(category_name=label, branch_name=branch_name, row=raw_row)
                    if normalized is None:
                        continue
                    if include_all_rows:
                        results.append(normalized)
                        continue
                    haystack = normalizar_nombre(f"{normalized.point_code} {normalized.point_name}")
                    if point_codes and normalized.point_code.upper() in point_codes:
                        results.append(normalized)
                        continue
                    if normalized_queries and any(query and query in haystack for query in normalized_queries):
                        results.append(normalized)
        return results

    def capture_all_rows(
        self,
        *,
        branch_hint: str = "ALMACEN",
    ) -> list[PointInventoryCostRow]:
        return self.capture_matches(
            branch_hint=branch_hint,
            include_all_rows=True,
        )

    def _resolve_point_product(self, row: PointInventoryCostRow) -> PointProduct | None:
        codes: list[str] = []
        for code in [row.point_internal_id, row.point_code]:
            code = str(code or "").strip()
            if code and code not in codes:
                codes.append(code)
        if codes:
            product = PointProduct.objects.filter(sku__in=codes).order_by("id").first()
            if product is not None:
                return product
            product = PointProduct.objects.filter(external_id__in=codes).order_by("id").first()
            if product is not None:
                return product

        normalized_name = normalizar_nombre(row.point_name)
        if normalized_name:
            product = PointProduct.objects.filter(normalized_name=normalized_name).order_by("id").first()
            if product is not None:
                return product
        return None

    def persist_cost_row(self, row: PointInventoryCostRow, *, supplier_name: str = "POINT EXISTENCIA ALMACEN") -> tuple[CostoInsumo | None, bool, str]:
        resolved = self.identity_service.resolve_insumo(point_code=row.point_code, point_name=row.point_name)
        if resolved.insumo is None:
            return None, False, "NO_MATCH_ERP"
        if row.unit_cost <= 0:
            return None, False, "UNIT_COST_ZERO"
        validation = validate_point_inventory_cost_row(row, resolved.insumo)
        if not validation.ok:
            return None, False, ",".join(validation.reasons)

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

    def persist_resale_product_cost_row(
        self,
        row: PointInventoryCostRow,
        *,
        supplier_name: str = "POINT EXISTENCIA ALMACEN",
    ) -> tuple[ProductoReventaCosto | None, bool, str]:
        if row.unit_cost <= 0:
            return None, False, "UNIT_COST_ZERO"
        product = self._resolve_point_product(row)
        if product is None:
            return None, False, "NO_MATCH_POINT_PRODUCT"

        source_hash = hashlib.sha256(
            f"POINT_REVENTA_ALMACEN|{product.id}|{row.branch_name}|{row.point_code}|{row.unit_cost}|{row.last_movement}".encode("utf-8")
        ).hexdigest()
        cost, created = ProductoReventaCosto.objects.get_or_create(
            source_hash=source_hash,
            defaults={
                "producto_point": product,
                "costo_unitario": row.unit_cost,
                "fecha_vigencia": self._row_date(row),
                "fuente": ProductoReventaCosto.FUENTE_POINT_ALMACEN,
                "proveedor_nombre": supplier_name,
                "unidad": row.unit,
                "cantidad_snapshot": row.quantity,
                "metadata": {
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

    def capture_and_persist_all(
        self,
        *,
        branch_hint: str = "ALMACEN",
        supplier_name: str = "POINT EXISTENCIA ALMACEN",
        sample_limit: int = 12,
    ) -> PointInventoryCostCaptureResult:
        rows = self.capture_all_rows(branch_hint=branch_hint)
        created = 0
        existing = 0
        unresolved = 0
        zero_cost = 0
        rejected = 0
        resale_created = 0
        resale_existing = 0
        unresolved_samples: list[dict[str, str]] = []
        zero_cost_samples: list[dict[str, str]] = []
        rejected_samples: list[dict[str, str]] = []
        branch_name = branch_hint

        for row in rows:
            branch_name = row.branch_name or branch_name

            if row.kind == "product":
                # Filas de la sección Productos → solo ProductoReventaCosto
                _resale_cost, resale_was_created, resale_status = self.persist_resale_product_cost_row(
                    row, supplier_name=supplier_name
                )
                if resale_status == "CREATED":
                    resale_created += 1
                elif resale_status == "EXISTS":
                    resale_existing += 1
                elif resale_status == "NO_MATCH_POINT_PRODUCT":
                    unresolved += 1
                    if len(unresolved_samples) < sample_limit:
                        unresolved_samples.append({
                            "point_code": row.point_code,
                            "point_name": row.point_name,
                            "category": row.category_name,
                            "kind": "product",
                        })
                elif resale_status == "UNIT_COST_ZERO":
                    zero_cost += 1
                    if len(zero_cost_samples) < sample_limit:
                        zero_cost_samples.append({
                            "point_code": row.point_code,
                            "point_name": row.point_name,
                            "category": row.category_name,
                            "kind": "product",
                        })
                continue  # no intentar persist_cost_row para filas de productos

            # Filas de la sección Insumos → intentar ProductoReventaCosto Y CostoInsumo
            _resale_cost, resale_was_created, resale_status = self.persist_resale_product_cost_row(
                row,
                supplier_name=supplier_name,
            )
            if resale_status == "CREATED":
                resale_created += 1
            elif resale_status == "EXISTS":
                resale_existing += 1

            _cost, was_created, status = self.persist_cost_row(row, supplier_name=supplier_name)
            if status == "NO_MATCH_ERP":
                unresolved += 1
                if len(unresolved_samples) < sample_limit:
                    unresolved_samples.append(
                        {
                            "point_code": row.point_code,
                            "point_name": row.point_name,
                            "category": row.category_name,
                            "kind": "supply",
                        }
                    )
                continue
            if status == "UNIT_COST_ZERO":
                zero_cost += 1
                if len(zero_cost_samples) < sample_limit:
                    zero_cost_samples.append(
                        {
                            "point_code": row.point_code,
                            "point_name": row.point_name,
                            "category": row.category_name,
                            "kind": "supply",
                        }
                    )
                continue
            if status not in {"CREATED", "EXISTS"}:
                rejected += 1
                if len(rejected_samples) < sample_limit:
                    rejected_samples.append(
                        {
                            "point_code": row.point_code,
                            "point_name": row.point_name,
                            "category": row.category_name,
                            "unit": row.unit,
                            "quantity": str(row.quantity),
                            "unit_cost": str(row.unit_cost),
                            "kind": "supply",
                            "status": status,
                        }
                    )
                continue
            if was_created:
                created += 1
            else:
                existing += 1

        product_rows = sum(1 for r in rows if r.kind == "product")
        supply_rows = len(rows) - product_rows
        return PointInventoryCostCaptureResult(
            branch_name=branch_name,
            rows_seen=len(rows),
            matches_found=created + existing + unresolved + zero_cost + rejected,
            costs_created=created,
            costs_existing=existing,
            unresolved_matches=unresolved,
            zero_cost_matches=zero_cost,
            rejected_matches=rejected,
            unresolved_samples=unresolved_samples,
            zero_cost_samples=zero_cost_samples,
            rejected_samples=rejected_samples,
            resale_costs_created=resale_created,
            resale_costs_existing=resale_existing,
            supply_rows_seen=supply_rows,
            product_rows_seen=product_rows,
        )
