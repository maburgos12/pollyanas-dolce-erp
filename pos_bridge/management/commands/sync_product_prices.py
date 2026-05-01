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
    table_texts = page.evaluate(
        """() => {
            const visible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style && style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
            };
            return Array.from(document.querySelectorAll('table'))
                .filter(visible)
                .map((table) => (table.innerText || '').trim())
                .filter(Boolean);
        }"""
    )
    parsed: list[CatalogPriceRow] = []
    for table_text in table_texts:
        lines = [line.strip() for line in str(table_text or "").splitlines() if line.strip()]
        if len(lines) < 2:
            continue
        headers = [cell.strip().lower() for cell in lines[0].split("\t")]

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
            continue

        for line in lines[1:]:
            cells = [cell.strip() for cell in line.split("\t")]
            if sku_index >= len(cells) or price_index >= len(cells):
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


def _select_largest_page_size(page) -> None:
    selects = page.locator("select")
    for index in range(selects.count()):
        select = selects.nth(index)
        try:
            values = select.evaluate(
                """(node) => Array.from(node.options || []).map((option) => option.value || option.textContent || '')"""
            )
            numeric_values = [int(str(value).strip()) for value in values if str(value).strip().isdigit()]
            if not numeric_values:
                continue
            select.select_option(str(max(numeric_values)))
            page.wait_for_timeout(1000)
            return
        except Exception:
            continue


def _click_next_page(page) -> bool:
    return bool(
        page.evaluate(
            """() => {
                const visible = (node) => {
                    const style = window.getComputedStyle(node);
                    const rect = node.getBoundingClientRect();
                    return style && style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                };
                const candidates = Array.from(document.querySelectorAll('a, button'))
                    .filter((node) => visible(node))
                    .filter((node) => (node.innerText || node.textContent || '').trim().toLowerCase() === 'siguiente')
                    .filter((node) => !String(node.className || '').toLowerCase().includes('disabled'))
                    .filter((node) => !String(node.parentElement?.className || '').toLowerCase().includes('disabled'))
                    .filter((node) => node.getAttribute('aria-disabled') !== 'true');
                const target = candidates[candidates.length - 1];
                if (!target) return false;
                target.click();
                return true;
            }"""
        )
    )


def _extract_paginated_rows(page, *, temporada: bool) -> list[CatalogPriceRow]:
    _select_largest_page_size(page)
    rows: list[CatalogPriceRow] = []
    seen_pages: set[tuple[str, ...]] = set()
    for _page_num in range(25):
        page_rows = _extract_visible_table_rows(page, temporada=temporada)
        if not page_rows:
            break
        signature = tuple(row.sku for row in page_rows[:5])
        if signature in seen_pages:
            break
        seen_pages.add(signature)
        rows.extend(page_rows)
        if not _click_next_page(page):
            break
        page.wait_for_timeout(500)
    return rows


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

        rows = _extract_paginated_rows(page, temporada=False)
        temporada_tab_found = _click_temporada_tab(page, settings.timeout_ms)
        if temporada_tab_found:
            rows.extend(_extract_paginated_rows(page, temporada=True))
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
