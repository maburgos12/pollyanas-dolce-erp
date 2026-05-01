from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.login_page import PointLoginPage
from pos_bridge.browser.workspace_page import PointWorkspacePage
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointProduct


CATALOG_PATH = "/Catalogos/IndexProductos"


@dataclass(frozen=True)
class CatalogPriceRow:
    sku: str
    precio: Decimal
    active: bool
    temporada: bool


def _parse_price(value: str) -> Decimal | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    cleaned = cleaned.replace("$", "").replace(",", "").replace("MXN", "").strip()
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    try:
        return Decimal(match.group(0)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _parse_active(value: str) -> bool:
    normalized = " ".join((value or "").strip().lower().split())
    if not normalized:
        return True
    return normalized not in {"no", "false", "falso", "inactivo", "inactiva", "deshabilitado", "0", "baja"}


def _extract_visible_table_rows(page, *, temporada: bool) -> list[CatalogPriceRow]:
    raw_rows = page.evaluate(
        """() => {
            const normalize = (value) => (value || '').toString().trim();
            const visible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style && style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
            };
            const tables = Array.from(document.querySelectorAll('table')).filter(visible);
            const output = [];
            for (const table of tables) {
                const headerCells = Array.from(table.querySelectorAll('thead th, thead td'));
                const headers = headerCells.map((cell) => normalize(cell.innerText).toLowerCase());
                const bodyRows = Array.from(table.querySelectorAll('tbody tr')).filter(visible);
                for (const row of bodyRows) {
                    const cells = Array.from(row.querySelectorAll('td')).map((cell) => {
                        const input = cell.querySelector('input, select, textarea');
                        const value = input ? (input.value || input.checked || '') : '';
                        return normalize(value || cell.innerText);
                    });
                    if (cells.some(Boolean)) {
                        output.push({headers, cells});
                    }
                }
            }
            return output;
        }"""
    )
    parsed: list[CatalogPriceRow] = []
    for raw in raw_rows:
        headers = [str(item or "").lower() for item in raw.get("headers") or []]
        cells = [str(item or "").strip() for item in raw.get("cells") or []]
        if not cells:
            continue

        def header_index(candidates: tuple[str, ...]) -> int | None:
            for index, header in enumerate(headers):
                if any(candidate in header for candidate in candidates):
                    return index
            return None

        sku_index = header_index(("codigo", "código", "clave", "sku"))
        price_index = header_index(("precio", "venta", "importe"))
        active_index = header_index(("activo", "estatus", "estado"))

        if sku_index is None:
            sku_index = 0
        if price_index is None:
            for index, cell in enumerate(cells):
                if index == sku_index:
                    continue
                if _parse_price(cell) is not None:
                    price_index = index
                    break
        if price_index is None or sku_index >= len(cells) or price_index >= len(cells):
            continue

        sku = cells[sku_index].strip()
        precio = _parse_price(cells[price_index])
        if not sku or precio is None or precio <= 0:
            continue

        active = True
        if active_index is not None and active_index < len(cells):
            active = _parse_active(cells[active_index])

        parsed.append(CatalogPriceRow(sku=sku, precio=precio, active=active, temporada=temporada))
    return parsed


def _click_temporada_tab(page, timeout_ms: int) -> bool:
    locators = [
        page.get_by_role("tab", name=re.compile("temporada", re.I)),
        page.get_by_role("link", name=re.compile("temporada", re.I)),
        page.get_by_role("button", name=re.compile("temporada", re.I)),
        page.locator("text=/Productos de Temporada/i"),
        page.locator("text=/Temporada/i"),
    ]
    for locator in locators:
        try:
            if locator.count() <= 0:
                continue
            candidate = locator.first
            candidate.wait_for(state="visible", timeout=1500)
            candidate.click()
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except Exception:
                pass
            page.wait_for_timeout(500)
            return True
        except Exception:
            continue
    return False


def scrape_catalog_prices(*, branch_hint: str = "") -> tuple[list[CatalogPriceRow], bool]:
    settings = load_point_bridge_settings()
    client = PlaywrightBrowserClient(settings)
    browser = client.start()
    context = None
    try:
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        PointLoginPage(page, settings).open()
        PointLoginPage(page, settings).login(settings.username, settings.password)
        PointWorkspacePage(page, settings).select_workspace(branch_hint or None)

        catalog_url = urljoin(settings.base_url.rstrip("/") + "/", CATALOG_PATH.lstrip("/"))
        page.goto(catalog_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=settings.timeout_ms)
        except Exception:
            pass
        page.wait_for_timeout(750)

        rows = _extract_visible_table_rows(page, temporada=False)
        temporada_tab_found = _click_temporada_tab(page, settings.timeout_ms)
        if temporada_tab_found:
            rows.extend(_extract_visible_table_rows(page, temporada=True))
        return rows, temporada_tab_found
    finally:
        if context is not None:
            context.close()
        client.stop()


class Command(BaseCommand):
    help = "Sincroniza precios del catálogo de Point hacia pos_bridge_products."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Extrae y muestra cambios sin guardar.")
        parser.add_argument("--branch", default="", help="Sucursal/workspace Point a usar para abrir el catálogo.")

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        branch_hint = (options.get("branch") or "").strip()

        try:
            rows, temporada_tab_found = scrape_catalog_prices(branch_hint=branch_hint)
        except Exception as exc:
            raise CommandError(f"No se pudieron extraer precios del catálogo Point: {exc}") from exc

        by_sku: dict[str, CatalogPriceRow] = {}
        for row in rows:
            by_sku[row.sku] = row

        if not by_sku:
            raise CommandError("Point no devolvió filas de catálogo con código y precio.")

        existing = {
            product.sku: product
            for product in PointProduct.objects.filter(sku__in=sorted(by_sku.keys()))
        }
        now = timezone.now()
        updated = 0
        not_found: list[str] = []
        preview: list[dict[str, object]] = []

        with transaction.atomic():
            for sku, row in sorted(by_sku.items()):
                product = existing.get(sku)
                if product is None:
                    not_found.append(sku)
                    continue
                preview.append(
                    {
                        "sku": sku,
                        "name": product.name,
                        "precio": str(row.precio),
                        "precio_temporada": row.temporada,
                        "precio_activo": row.active,
                    }
                )
                if not dry_run:
                    product.precio = row.precio
                    product.precio_temporada = row.temporada
                    product.precio_activo = row.active
                    product.precio_actualizado_en = now
                    product.save(
                        update_fields=[
                            "precio",
                            "precio_temporada",
                            "precio_activo",
                            "precio_actualizado_en",
                            "updated_at",
                        ]
                    )
                updated += 1
            if dry_run:
                transaction.set_rollback(True)

        payload = {
            "dry_run": dry_run,
            "catalog_rows": len(rows),
            "unique_skus": len(by_sku),
            "updated": updated,
            "not_found": len(not_found),
            "temporada_tab_found": temporada_tab_found,
            "preview": preview[:20],
            "not_found_sample": not_found[:20],
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
