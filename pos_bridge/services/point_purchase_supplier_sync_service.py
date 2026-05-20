"""
Extrae el historial de compras desde Point (/InventoryPurchases/tab_registro_compras)
para obtener el proveedor de cada insumo y actualizar Insumo.proveedor_principal.

La lógica de selección es: proveedor más reciente (última fecha de compra) del insumo.
Si hay empate de fecha, se elige el más frecuente en ese rango.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.session import BrowserSessionManager
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.auth_service import PointAuthService
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService


@dataclass(slots=True)
class PointPurchaseRow:
    folio: str
    sucursal: str
    proveedor: str
    fecha: str
    articulo: str
    cantidad: Decimal
    unidad: str
    precio_unitario: Decimal


@dataclass
class SupplierSyncResult:
    rows_seen: int = 0
    insumos_updated: int = 0
    insumos_sin_match: int = 0
    insumos_sin_cambio: int = 0
    errores: list[str] = field(default_factory=list)
    proveedor_map: dict[str, str] = field(default_factory=dict)


class PointPurchaseSupplierSyncService:
    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        identity_service: PointRecipeIdentityService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.identity_service = identity_service or PointRecipeIdentityService()
        self.auth_service = PointAuthService(self.settings)

    def scrape_purchase_rows(
        self,
        *,
        desde: date | None = None,
        hasta: date | None = None,
    ) -> list[PointPurchaseRow]:
        if hasta is None:
            hasta = date.today()
        if desde is None:
            desde = hasta - timedelta(days=365)

        desde_str = desde.strftime("%d/%m/%Y")
        hasta_str = hasta.strftime("%d/%m/%Y")

        rows: list[PointPurchaseRow] = []
        client = PlaywrightBrowserClient(self.settings)

        with BrowserSessionManager(client) as session:
            self.auth_service.login(session, branch_hint=None)
            page = session.page
            base_url = (self.settings.base_url or "").rstrip("/")
            page.goto(
                f"{base_url}/InventoryPurchases/tab_registro_compras",
                wait_until="domcontentloaded",
            )
            try:
                page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
            except Exception:
                pass

            self._set_date_range(page, desde_str, hasta_str)
            self._trigger_search(page)

            rows = self._scrape_all_pages(page)

        return rows

    def _set_date_range(self, page, desde_str: str, hasta_str: str) -> None:
        date_input_candidates = [
            "#txt_fecha_ini_RC",
            "#txt_fechaInicio_RC",
            "#fecha_ini_RC",
            "input[name='fecha_ini_RC']",
            "input[id*='fecha_ini']",
            "input[id*='fecha_inicio']",
        ]
        end_date_candidates = [
            "#txt_fecha_fin_RC",
            "#txt_fechaFin_RC",
            "#fecha_fin_RC",
            "input[name='fecha_fin_RC']",
            "input[id*='fecha_fin']",
            "input[id*='fecha_final']",
        ]

        for sel in date_input_candidates:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=2000)
                el.fill("")
                el.type(desde_str)
                break
            except Exception:
                continue

        for sel in end_date_candidates:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=2000)
                el.fill("")
                el.type(hasta_str)
                break
            except Exception:
                continue

    def _trigger_search(self, page) -> None:
        search_candidates = [
            "#btn_buscar_RC",
            "#btnBuscarRC",
            "button[id*='buscar']",
            "input[type='button'][id*='buscar']",
            "button:has-text('Buscar')",
            "input[value='Buscar']",
        ]
        for sel in search_candidates:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=2000)
                el.click()
                try:
                    page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
                except Exception:
                    pass
                return
            except Exception:
                continue

        page.keyboard.press("Enter")
        try:
            page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
        except Exception:
            pass

    def _scrape_all_pages(self, page) -> list[PointPurchaseRow]:
        rows: list[PointPurchaseRow] = []
        page_num = 0

        while True:
            page_num += 1
            page_rows = self._scrape_page(page)
            rows.extend(page_rows)

            if not self._go_next_page(page):
                break

            if page_num > 200:
                break

        return rows

    def _scrape_page(self, page) -> list[PointPurchaseRow]:
        rows: list[PointPurchaseRow] = []

        table_candidates = [
            "#tbl_compras_RC",
            "#tblComprasRC",
            "table[id*='compras']",
            "table[id*='RC']",
            ".table-compras",
        ]

        table_html = None
        for sel in table_candidates:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=3000)
                table_html = el.inner_html()
                break
            except Exception:
                continue

        if not table_html:
            try:
                table_html = page.locator("table").first.inner_html()
            except Exception:
                return rows

        rows = self._parse_purchase_table(table_html)
        return rows

    def _parse_purchase_table(self, table_html: str) -> list[PointPurchaseRow]:
        """
        La tabla de compras en Point tiene filas maestro (Folio/Proveedor/Fecha) y
        filas detalle (Artículo/Cantidad/Unidad/Precio).
        Las filas maestro tienen atributo onclick con el folio o tienen estilo especial.
        """
        from html.parser import HTMLParser

        rows: list[PointPurchaseRow] = []
        current_folio = ""
        current_sucursal = ""
        current_proveedor = ""
        current_fecha = ""

        class TableParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.in_tr = False
                self.in_td = False
                self.current_row_cells: list[str] = []
                self.all_rows: list[list[str]] = []
                self.row_attrs: list[dict] = []
                self._current_tr_attrs: dict = {}

            def handle_starttag(self, tag, attrs):
                if tag == "tr":
                    self.in_tr = True
                    self.current_row_cells = []
                    self._current_tr_attrs = dict(attrs)
                elif tag == "td" and self.in_tr:
                    self.in_td = True

            def handle_endtag(self, tag):
                if tag == "tr" and self.in_tr:
                    self.in_tr = False
                    if self.current_row_cells:
                        self.all_rows.append(self.current_row_cells[:])
                        self.row_attrs.append(self._current_tr_attrs.copy())
                    self.current_row_cells = []
                elif tag == "td":
                    self.in_td = False

            def handle_data(self, data):
                if self.in_td:
                    text = data.strip()
                    if self.current_row_cells and not text:
                        return
                    if text:
                        if self.current_row_cells and self.in_td:
                            self.current_row_cells[-1] = (self.current_row_cells[-1] + " " + text).strip()
                        else:
                            self.current_row_cells.append(text)

            def handle_starttag_with_cell_open(self, tag, attrs):
                if tag == "td" and self.in_tr:
                    self.current_row_cells.append("")
                    self.in_td = True

        parser = TableParser()

        class CellParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.in_td = False
                self.cells: list[str] = []
                self._cell_text = ""

            def handle_starttag(self, tag, attrs):
                if tag == "td":
                    self.in_td = True
                    self._cell_text = ""

            def handle_endtag(self, tag):
                if tag == "td":
                    self.in_td = False
                    self.cells.append(self._cell_text.strip())

            def handle_data(self, data):
                if self.in_td:
                    self._cell_text += data

        import re as re_mod

        tr_pattern = re_mod.compile(r"<tr([^>]*)>(.*?)</tr>", re_mod.IGNORECASE | re_mod.DOTALL)
        td_pattern = re_mod.compile(r"<td[^>]*>(.*?)</td>", re_mod.IGNORECASE | re_mod.DOTALL)
        tag_pattern = re_mod.compile(r"<[^>]+>")

        for tr_match in tr_pattern.finditer(table_html):
            tr_attrs_str = tr_match.group(1)
            tr_content = tr_match.group(2)

            cells = []
            for td_match in td_pattern.finditer(tr_content):
                raw = td_match.group(1)
                text = tag_pattern.sub("", raw).strip()
                text = re_mod.sub(r"\s+", " ", text)
                cells.append(text)

            if not cells or all(c == "" for c in cells):
                continue

            is_header_row = (
                "onclick" in tr_attrs_str.lower()
                or "cursor" in tr_attrs_str.lower()
                or "compra" in tr_attrs_str.lower()
            )

            if len(cells) >= 4 and is_header_row:
                # Header row: Folio | Sucursal | Proveedor | Fecha | ...
                current_folio = cells[0]
                current_sucursal = cells[1] if len(cells) > 1 else ""
                current_proveedor = cells[2] if len(cells) > 2 else ""
                current_fecha = cells[3] if len(cells) > 3 else ""

            elif len(cells) >= 4 and current_proveedor:
                # Detect article detail rows: Artículo | Cantidad | Unidad | Precio unitario
                articulo = cells[0]
                # Skip rows that look like table headers
                if articulo.lower() in ("artículo", "articulo", "producto", "descripción"):
                    continue
                try:
                    cantidad_raw = cells[1].replace(",", "").replace("$", "").strip()
                    cantidad = Decimal(cantidad_raw) if cantidad_raw else Decimal("0")
                except Exception:
                    cantidad = Decimal("0")

                unidad = cells[2] if len(cells) > 2 else ""
                try:
                    precio_raw = cells[3].replace(",", "").replace("$", "").strip()
                    precio = Decimal(precio_raw) if precio_raw else Decimal("0")
                except Exception:
                    precio = Decimal("0")

                if articulo and articulo.lower() not in ("", "nan"):
                    rows.append(PointPurchaseRow(
                        folio=current_folio,
                        sucursal=current_sucursal,
                        proveedor=current_proveedor,
                        fecha=current_fecha,
                        articulo=articulo,
                        cantidad=cantidad,
                        unidad=unidad,
                        precio_unitario=precio,
                    ))

        return rows

    def _go_next_page(self, page) -> bool:
        next_candidates = [
            "#btn_siguiente_RC",
            "#btnSiguienteRC",
            "a[id*='siguiente']",
            "button[id*='siguiente']",
            "a:has-text('Siguiente')",
            "a:has-text('>')",
            ".pagination .next:not(.disabled)",
        ]
        for sel in next_candidates:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=1000)
                is_disabled = el.get_attribute("disabled") or el.get_attribute("class", "")
                if "disabled" in str(is_disabled or "").lower():
                    return False
                el.click()
                try:
                    page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
                except Exception:
                    pass
                return True
            except Exception:
                continue
        return False

    def build_insumo_supplier_map(
        self,
        rows: list[PointPurchaseRow],
    ) -> dict[str, str]:
        """
        Devuelve {point_name_upper: proveedor_name} con el proveedor más reciente por insumo.
        En caso de empate de fecha, gana el más frecuente.
        """
        from collections import defaultdict

        # insumo_name → [(fecha_str, proveedor)]
        insumo_proveedores: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for row in rows:
            if row.articulo and row.proveedor:
                insumo_proveedores[row.articulo.upper()].append((row.fecha, row.proveedor))

        result: dict[str, str] = {}
        for articulo, entries in insumo_proveedores.items():
            if not entries:
                continue
            # Sort by fecha desc and pick most recent
            entries_sorted = sorted(entries, key=lambda x: x[0], reverse=True)
            most_recent_fecha = entries_sorted[0][0]
            most_recent = [p for f, p in entries_sorted if f == most_recent_fecha]
            # Most frequent among the most recent
            from collections import Counter
            winner = Counter(most_recent).most_common(1)[0][0]
            result[articulo] = winner

        return result
