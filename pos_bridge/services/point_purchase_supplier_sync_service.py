"""
Extrae el historial de compras desde Point via API (GetCompras + GetComprabyId)
para obtener el proveedor más reciente de cada insumo.

Navegación:
  1. Login → Home/Index (donde jQuery está disponible)
  2. Navega a /InventoryPurchases/Index (carga jQuery + define getCompras)
  3. Usa page.request para llamar las APIs directamente con cookies de sesión

APIs de Point:
  GET /InventoryPurchases/GetCompras?fechaInicio={ms}&fechaFin={ms}&fkproveedor=&fkSucursal=null
    → [{FK_Movimiento, Folio, Fecha_compra, FK_Proveedor, Proveedor, Sucursal, ...}]
  GET /InventoryPurchases/GetComprabyId?fkCompra={FK_Movimiento}
    → [{Articulo, Cantidad, Unidad, Costo_unitario, Costo_total}]
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta, timezone, datetime


@dataclass(slots=True)
class PointPurchaseRow:
    fk_movimiento: int
    folio: str
    proveedor: str
    fecha_compra: str
    sucursal: str


@dataclass(slots=True)
class PointPurchaseDetailRow:
    articulo: str
    cantidad: float
    unidad: str
    costo_unitario: float


@dataclass
class SupplierSyncResult:
    compras_total: int = 0
    compras_processed: int = 0
    articles_mapped: int = 0
    insumos_updated: int = 0
    insumos_sin_match: int = 0
    insumos_sin_cambio: int = 0
    errores: list[str] = field(default_factory=list)
    proveedor_map: dict[str, str] = field(default_factory=dict)


def _ms(d: date) -> int:
    """Convierte date a milliseconds UTC para las APIs de Point."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


class PointPurchaseSupplierSyncService:
    def __init__(self, bridge_settings=None):
        from pos_bridge.browser.client import PlaywrightBrowserClient
        from pos_bridge.browser.session import BrowserSessionManager
        from pos_bridge.config import load_point_bridge_settings
        from pos_bridge.services.auth_service import PointAuthService

        self.settings = bridge_settings or load_point_bridge_settings()
        self._PlaywrightBrowserClient = PlaywrightBrowserClient
        self._BrowserSessionManager = BrowserSessionManager
        self._PointAuthService = PointAuthService

    def _base_url(self) -> str:
        return (self.settings.base_url or "").rstrip("/")

    def fetch_purchase_map(
        self,
        *,
        desde: date | None = None,
        hasta: date | None = None,
        max_compras: int = 5000,
        stop_after_no_new: int = 100,
    ) -> dict[str, str]:
        """
        Retorna {articulo_name_upper: proveedor_name} usando la compra más reciente.

        Itera las compras ordenadas por fecha DESC. Para cada compra nueva busca sus
        artículos en GetComprabyId. Para cuando haya `stop_after_no_new` compras
        consecutivas sin artículos nuevos (indica que ya cubrimos todos los activos).
        """
        if hasta is None:
            hasta = date.today()
        if desde is None:
            desde = hasta - timedelta(days=365)

        fI = _ms(desde)
        fF = _ms(hasta)
        base = self._base_url()

        auth_service = self._PointAuthService(self.settings)
        client = self._PlaywrightBrowserClient(self.settings)

        insumo_proveedor: dict[str, str] = {}

        with self._BrowserSessionManager(client) as session:
            auth_service.login(session, branch_hint=None)
            page = session.page

            # Navegar a la página completa que carga jQuery y define getCompras
            page.goto(f"{base}/InventoryPurchases/Index", wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
            except Exception:
                pass

            # Obtener todas las compras del período
            compras_resp = page.request.get(
                f"{base}/InventoryPurchases/GetCompras",
                params={
                    "fechaInicio": fI,
                    "fechaFin": fF,
                    "fkproveedor": "",
                    "fkSucursal": "null",
                },
            )
            compras = compras_resp.json()
            if not isinstance(compras, list):
                return {}

            # Ordenar por fecha DESC (más reciente primero) para que la primera vez que
            # veamos un artículo sea con su proveedor más reciente
            compras.sort(key=lambda c: c.get("Fecha_compra", ""), reverse=True)
            compras = compras[:max_compras]

            consecutive_no_new = 0

            for compra in compras:
                fk = compra.get("FK_Movimiento")
                proveedor = (compra.get("Proveedor") or "").strip()
                if not fk or not proveedor:
                    continue

                detail_resp = page.request.get(
                    f"{base}/InventoryPurchases/GetComprabyId",
                    params={"fkCompra": fk},
                )
                if detail_resp.status != 200:
                    continue

                try:
                    detalles = detail_resp.json()
                except Exception:
                    continue

                if not isinstance(detalles, list):
                    continue

                found_new = False
                for d in detalles:
                    articulo = (d.get("Articulo") or "").strip().upper()
                    if not articulo:
                        continue
                    if articulo not in insumo_proveedor:
                        insumo_proveedor[articulo] = proveedor
                        found_new = True

                if found_new:
                    consecutive_no_new = 0
                else:
                    consecutive_no_new += 1

                if consecutive_no_new >= stop_after_no_new:
                    break

        return insumo_proveedor
