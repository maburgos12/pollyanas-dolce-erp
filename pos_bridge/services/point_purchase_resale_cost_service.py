from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal

from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.session import BrowserSessionManager
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.models import PointProduct
from pos_bridge.services.auth_service import PointAuthService
from pos_bridge.services.point_purchase_supplier_sync_service import _ms
from recetas.utils.normalizacion import normalizar_nombre
from reportes.models import ProductoReventaCosto


PURCHASE_ARTICLE_PRODUCT_ALIASES = {
    # Point registra la compra física de la tarjeta por el diseño/proveedor,
    # pero el producto vendido conserva el nombre comercial de temporada.
    "tarjeta happy mothers day": ("tarjeta de regalo dia de las madres", "tarjeta de regalo día de las madres", "78421", "1411"),
}


@dataclass(slots=True)
class PointPurchaseResaleCostSyncResult:
    purchases_seen: int = 0
    details_seen: int = 0
    matched_products: int = 0
    created: int = 0
    existing: int = 0
    dry_run_created: int = 0
    zero_or_invalid_cost: int = 0
    unresolved: int = 0
    imported_products: set[str] = field(default_factory=set)
    unresolved_samples: list[str] = field(default_factory=list)


class PointPurchaseResaleCostSyncService:
    """
    Extrae costos de adquisición para productos de reventa desde Compras Point.

    Point Existencias/ALMACEN puede devolver costo 0 cuando no hay stock. La fuente
    correcta para el costo de adquisición histórico es InventoryPurchases/GetComprabyId.
    """

    def __init__(self, bridge_settings: PointBridgeSettings | None = None):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.auth_service = PointAuthService(self.settings)

    @staticmethod
    def _decimal(value) -> Decimal:
        text = str(value or "").strip().replace(",", "")
        if not text:
            return Decimal("0")
        try:
            return Decimal(text)
        except Exception:
            return Decimal("0")

    @staticmethod
    def _parse_purchase_date(raw_value) -> date:
        text = str(raw_value or "").strip()
        if not text:
            return date.today()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except Exception:
            pass
        compact = "".join(ch for ch in text if ch.isdigit())
        if len(compact) >= 8:
            try:
                return date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))
            except Exception:
                pass
        return date.today()

    def _product_index(self) -> dict[str, PointProduct]:
        products = PointProduct.objects.filter(active=True).only("id", "name", "normalized_name", "external_id", "sku")
        index: dict[str, PointProduct] = {}
        for product in products:
            for key in {
                normalizar_nombre(product.name),
                normalizar_nombre(product.normalized_name),
                normalizar_nombre(product.external_id),
                normalizar_nombre(product.sku),
            }:
                if key:
                    index.setdefault(key, product)
        return index

    def _resolve_product(self, article_name: str, product_index: dict[str, PointProduct]) -> PointProduct | None:
        article_key = normalizar_nombre(article_name)
        product = product_index.get(article_key)
        if product is not None:
            return product
        for alias in PURCHASE_ARTICLE_PRODUCT_ALIASES.get(article_key, ()):
            product = product_index.get(normalizar_nombre(alias))
            if product is not None:
                return product
        return None

    def fetch_purchase_payloads(
        self,
        *,
        desde: date,
        hasta: date,
        max_compras: int = 800,
    ) -> tuple[list[dict], dict[str, list[dict]]]:
        base = (self.settings.base_url or "").rstrip("/")
        client = PlaywrightBrowserClient(self.settings)
        details_by_purchase: dict[str, list[dict]] = {}

        with BrowserSessionManager(client) as session:
            self.auth_service.login(session, branch_hint=None)
            page = session.page
            page.goto(f"{base}/InventoryPurchases/Index", wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
            except Exception:
                pass

            compras_url = (
                f"{base}/InventoryPurchases/GetCompras"
                f"?fechaInicio={_ms(desde)}&fechaFin={_ms(hasta)}&fkproveedor=&fkSucursal=null"
            )
            compras_resp = page.request.get(compras_url)
            compras = compras_resp.json()
            if not isinstance(compras, list):
                return [], {}

            compras.sort(key=lambda row: row.get("Fecha_compra", ""), reverse=True)
            compras = compras[:max_compras]

            for compra in compras:
                purchase_id = str(compra.get("FK_Movimiento") or "").strip()
                if not purchase_id:
                    continue
                detail_resp = page.request.get(
                    f"{base}/InventoryPurchases/GetComprabyId?fkCompra={purchase_id}",
                    timeout=8000,
                )
                if detail_resp.status != 200:
                    continue
                try:
                    details = detail_resp.json()
                except Exception:
                    continue
                details_by_purchase[purchase_id] = details if isinstance(details, list) else []

        return compras, details_by_purchase

    def sync_purchase_payloads(
        self,
        *,
        purchases: list[dict],
        details_by_purchase: dict[str, list[dict]],
        apply: bool = False,
        sample_limit: int = 25,
    ) -> PointPurchaseResaleCostSyncResult:
        product_index = self._product_index()
        result = PointPurchaseResaleCostSyncResult(purchases_seen=len(purchases))

        for purchase in purchases:
            purchase_id = str(purchase.get("FK_Movimiento") or purchase.get("purchase_id") or "").strip()
            if not purchase_id:
                continue
            purchase_date = self._parse_purchase_date(purchase.get("Fecha_compra") or purchase.get("purchase_date"))
            supplier = str(purchase.get("Proveedor") or purchase.get("supplier") or "POINT COMPRAS").strip()
            branch = str(purchase.get("Sucursal") or purchase.get("branch") or "").strip()
            folio = str(purchase.get("Folio") or purchase.get("folio") or "").strip()

            for detail in details_by_purchase.get(purchase_id) or []:
                result.details_seen += 1
                article = str(detail.get("Articulo") or detail.get("articulo") or "").strip()
                if not article:
                    continue

                unit_cost = self._decimal(detail.get("Costo_unitario") or detail.get("costo_unitario"))
                if unit_cost <= 0:
                    result.zero_or_invalid_cost += 1
                    continue

                product = self._resolve_product(article, product_index)
                if product is None:
                    result.unresolved += 1
                    if len(result.unresolved_samples) < sample_limit:
                        result.unresolved_samples.append(article)
                    continue

                result.matched_products += 1
                result.imported_products.add(product.name)
                source_hash = hashlib.sha256(
                    (
                        "POINT_PRODUCT_HISTORY|"
                        f"{purchase_id}|{product.id}|{article}|{unit_cost}|"
                        f"{detail.get('Cantidad') or detail.get('cantidad')}|{detail.get('Unidad') or detail.get('unidad')}"
                    ).encode("utf-8")
                ).hexdigest()

                defaults = {
                    "producto_point": product,
                    "costo_unitario": unit_cost,
                    "fecha_vigencia": purchase_date,
                    "fuente": ProductoReventaCosto.FUENTE_POINT_HISTORIAL,
                    "proveedor_nombre": supplier,
                    "unidad": str(detail.get("Unidad") or detail.get("unidad") or "").strip(),
                    "cantidad_snapshot": self._decimal(detail.get("Cantidad") or detail.get("cantidad")),
                    "metadata": {
                        "source": "POINT_PRODUCT_HISTORY",
                        "purchase_id": purchase_id,
                        "folio": folio,
                        "branch": branch,
                        "supplier": supplier,
                        "article_name": article,
                        "quantity": detail.get("Cantidad") or detail.get("cantidad"),
                        "unit": detail.get("Unidad") or detail.get("unidad"),
                        "unit_cost": str(unit_cost),
                        "total_cost": detail.get("Costo_total") or detail.get("costo_total"),
                        "raw": detail,
                    },
                }

                if not apply:
                    if ProductoReventaCosto.objects.filter(source_hash=source_hash).exists():
                        result.existing += 1
                    else:
                        result.dry_run_created += 1
                    continue

                _cost, created = ProductoReventaCosto.objects.get_or_create(
                    source_hash=source_hash,
                    defaults=defaults,
                )
                if created:
                    result.created += 1
                else:
                    result.existing += 1

        return result

    def sync_from_point(
        self,
        *,
        desde: date | None = None,
        hasta: date | None = None,
        dias: int = 120,
        max_compras: int = 800,
        apply: bool = False,
    ) -> PointPurchaseResaleCostSyncResult:
        hasta = hasta or date.today()
        desde = desde or (hasta - timedelta(days=dias))
        purchases, details_by_purchase = self.fetch_purchase_payloads(
            desde=desde,
            hasta=hasta,
            max_compras=max_compras,
        )
        return self.sync_purchase_payloads(
            purchases=purchases,
            details_by_purchase=details_by_purchase,
            apply=apply,
        )
