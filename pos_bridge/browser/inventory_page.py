from __future__ import annotations

from pos_bridge.browser.waits import click_first, find_first, wait_for_any
from pos_bridge.selectors.inventory_selectors import (
    BRANCH_SELECT_CANDIDATES,
    INVENTORY_TABLE_CANDIDATES,
    INSUMOS_TABLE_CANDIDATES,
    NEXT_PAGE_CANDIDATES,
    PRODUCT_CATEGORY_SELECT_CANDIDATES,
    SUPPLY_CATEGORY_SELECT_CANDIDATES,
)
from pos_bridge.selectors.menu_selectors import INVENTORY_EXISTENCES_CANDIDATES, INVENTORY_MENU_CANDIDATES
from pos_bridge.utils.exceptions import NavigationError
from pos_bridge.utils.helpers import select_candidates


class PointInventoryPage:
    def __init__(self, page, bridge_settings):
        self.page = page
        self.settings = bridge_settings

    def open_inventory_module(self) -> None:
        ready_selectors = (
            select_candidates(self.settings.selector_overrides, "inventory.table", INVENTORY_TABLE_CANDIDATES)
            + select_candidates(self.settings.selector_overrides, "inventory.branch_select", BRANCH_SELECT_CANDIDATES)
        )
        base_url = (self.settings.base_url or "").rstrip("/")
        if base_url:
            try:
                self.page.goto(f"{base_url}/Stock/Index", wait_until="domcontentloaded")
                try:
                    self.page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
                except Exception:
                    pass
                wait_for_any(
                    self.page,
                    ready_selectors,
                    "módulo de inventario",
                    timeout_ms=self.settings.timeout_ms,
                )
                return
            except Exception:
                pass

        click_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "menu.inventory", INVENTORY_MENU_CANDIDATES),
            "menú de inventario",
            timeout_ms=self.settings.timeout_ms,
        )
        click_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "menu.inventory_existences", INVENTORY_EXISTENCES_CANDIDATES),
            "submenú de existencias",
            timeout_ms=self.settings.timeout_ms,
        )
        try:
            self.page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
        except Exception:
            pass
        wait_for_any(
            self.page,
            ready_selectors,
            "módulo de inventario",
            timeout_ms=self.settings.timeout_ms,
        )

    def list_branches(self) -> list[dict]:
        branch_locator = find_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "inventory.branch_select", BRANCH_SELECT_CANDIDATES),
            timeout_ms=1000,
        )
        if branch_locator is None:
            return []

        options = branch_locator.locator("option").evaluate_all(
            """(nodes) => nodes.map((node) => ({
                value: (node.value || "").trim(),
                label: (node.textContent || "").trim()
            }))"""
        )
        return [option for option in options if option.get("label")]

    def select_branch(self, branch: dict) -> dict:
        branch_locator = find_first(
            self.page,
            select_candidates(self.settings.selector_overrides, "inventory.branch_select", BRANCH_SELECT_CANDIDATES),
            timeout_ms=500,
        )
        if branch_locator is None:
            if branch:
                return branch
            raise NavigationError("No se encontró selector de sucursal para el módulo de inventario.")

        value = str(branch.get("value") or "").strip()
        label = str(branch.get("label") or "").strip()
        if value:
            branch_locator.select_option(value=value)
        elif label:
            branch_locator.select_option(label=label)
        else:
            raise NavigationError("La sucursal recibida no contiene value ni label.", context={"branch": branch})

        try:
            self.page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
        except Exception:
            pass
        self.page.wait_for_timeout(700)
        return {"value": value or label, "label": label or value}

    def list_category_options(self, kind: str = "products") -> list[dict]:
        selector_key = "inventory.product_category_select" if kind == "products" else "inventory.supply_category_select"
        selector_defaults = PRODUCT_CATEGORY_SELECT_CANDIDATES if kind == "products" else SUPPLY_CATEGORY_SELECT_CANDIDATES
        category_locator = find_first(
            self.page,
            select_candidates(self.settings.selector_overrides, selector_key, selector_defaults),
            timeout_ms=1000,
        )
        if category_locator is None:
            return []
        options = category_locator.locator("option").evaluate_all(
            """(nodes) => nodes.map((node) => ({
                value: (node.value || '').trim(),
                label: (node.textContent || '').trim()
            }))"""
        )
        return [option for option in options if option.get("label")]

    def select_category(self, option_value: str, kind: str = "products") -> None:
        selector_key = "inventory.product_category_select" if kind == "products" else "inventory.supply_category_select"
        selector_defaults = PRODUCT_CATEGORY_SELECT_CANDIDATES if kind == "products" else SUPPLY_CATEGORY_SELECT_CANDIDATES
        category_locator = find_first(
            self.page,
            select_candidates(self.settings.selector_overrides, selector_key, selector_defaults),
            timeout_ms=1000,
        )
        if category_locator is None:
            raise NavigationError(f"No se encontró selector de categoría para {kind}.")
        category_locator.select_option(value=option_value)
        try:
            self.page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
        except Exception:
            pass
        self.page.wait_for_timeout(900)

    def extract_inventory_table(self, kind: str = "products") -> dict:
        table_selector = select_candidates(
            self.settings.selector_overrides,
            "inventory.table" if kind == "products" else "inventory.supplies_table",
            INVENTORY_TABLE_CANDIDATES if kind == "products" else INSUMOS_TABLE_CANDIDATES,
        )
        next_button_selector = select_candidates(
            self.settings.selector_overrides,
            "inventory.next_button" if kind == "products" else "inventory.supplies_next_button",
            NEXT_PAGE_CANDIDATES,
        )
        direct_payload = self.page.evaluate(
            """(selectors) => {
                const cleanCell = (value) => {
                    if (value == null) return '';
                    if (typeof value !== 'string') return String(value).trim();
                    const wrapper = document.createElement('div');
                    wrapper.innerHTML = value;
                    return (wrapper.textContent || wrapper.innerText || value).trim();
                };

                for (const selector of selectors) {
                    const table = document.querySelector(selector);
                    if (!table) continue;
                    const headers = Array.from(table.querySelectorAll('thead th')).map((cell) => (cell.textContent || '').trim());
                    const tableId = table.id || '';
                    if (
                        window.jQuery
                        && window.jQuery.fn
                        && window.jQuery.fn.dataTable
                        && tableId
                        && window.jQuery.fn.dataTable.isDataTable(`#${tableId}`)
                    ) {
                        const api = window.jQuery(table).DataTable();
                        const rows = api.rows({ search: 'applied' }).data().toArray().map((row) => {
                            if (Array.isArray(row)) {
                                return row.map(cleanCell);
                            }
                            return Object.values(row || {}).map(cleanCell);
                        });
                        return { headers, rows };
                    }
                }
                return null;
            }""",
            table_selector,
        )
        if direct_payload:
            return direct_payload

        headers: list[str] = []
        rows: list[list[str]] = []

        for _ in range(self.settings.max_pages_per_branch):
            table = find_first(self.page, table_selector, timeout_ms=self.settings.timeout_ms)
            if table is None:
                raise NavigationError("No se pudo ubicar la tabla de inventario.")

            payload = table.evaluate(
                """(table) => {
                    const headerNodes = Array.from(table.querySelectorAll('thead th'));
                    const headers = headerNodes.length
                        ? headerNodes.map((cell) => (cell.textContent || '').trim())
                        : Array.from(table.querySelectorAll('tr:first-child th, tr:first-child td'))
                            .map((cell) => (cell.textContent || '').trim());
                    const bodyRows = Array.from(table.querySelectorAll('tbody tr'));
                    const sourceRows = bodyRows.length ? bodyRows : Array.from(table.querySelectorAll('tr')).slice(headers.length ? 1 : 0);
                    const rows = sourceRows.map((row) =>
                        Array.from(row.querySelectorAll('th, td')).map((cell) => (cell.textContent || '').trim())
                    );
                    return { headers, rows };
                }"""
            )
            headers = payload.get("headers") or headers
            rows.extend(payload.get("rows") or [])

            if rows and all(len(row) == 1 and (row[0] or "").strip().lower() == "no hay datos disponibles" for row in rows):
                break

            next_button = find_first(self.page, next_button_selector, timeout_ms=500)
            if next_button is None:
                break
            disabled = next_button.get_attribute("disabled")
            css_class = (next_button.get_attribute("class") or "").lower()
            aria_disabled = (next_button.get_attribute("aria-disabled") or "").lower()
            current_text = (next_button.inner_text() or "").strip().lower()
            parent_class = (next_button.locator("..").get_attribute("class") or "").lower() if next_button else ""
            if (
                disabled is not None
                or aria_disabled == "true"
                or "disabled" in css_class
                or "disabled" in parent_class
                or current_text == "anterior"
            ):
                break
            next_button.click()
            self.page.wait_for_timeout(700)

        return {"headers": headers, "rows": rows}
