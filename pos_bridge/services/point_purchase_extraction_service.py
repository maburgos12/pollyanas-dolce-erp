"""Extrae compras de Point con su cantidad, no solo el costo.

`PointPurchaseSupplierSyncService` ya consultaba estos mismos endpoints, pero solo
conservaba el nombre del artículo para mapear proveedores y descartaba la cantidad.
Sin cantidad no hay entrada de inventario, y el kardex quedaba sin las compras:
auditado en producción, 86 de 260 insumos con saldo teórico negativo.

Las filas se persisten con `PointPurchaseCostImportService.persist_purchases`, el
mismo camino que usa el import desde archivos, para que la llave de idempotencia y
el formato de `raw` no diverjan entre las dos rutas.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.utils.exceptions import ExtractionError

# El kardex de insumos es el de ALMACEN_1: las compras de otras sucursales no entran.
ALMACEN_BRANCH = "almacen"


@dataclass
class PurchaseExtractionResult:
    purchases_seen: int = 0
    purchases_kept: int = 0
    lines_kept: int = 0
    branches_skipped: dict[str, int] = field(default_factory=dict)


def _epoch_ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000)


def _parse_purchase_date(raw_value: str) -> date | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


class PointPurchaseExtractionService:
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

    def fetch_purchases(
        self,
        *,
        desde: date,
        hasta: date,
        solo_almacen: bool = True,
    ) -> tuple[list[dict], PurchaseExtractionResult]:
        """Devuelve las compras del rango en el formato que espera `persist_purchases`."""
        base = self._base_url()
        auth_service = self._PointAuthService(self.settings)
        client = self._PlaywrightBrowserClient(self.settings)
        result = PurchaseExtractionResult()
        purchases: list[dict] = []

        # Point invalida la sesión anterior cuando la misma cuenta entra de nuevo y
        # "domicilios Point automatico" corre cada 60 s: sin candado esta extracción
        # pierde la sesión a media iteración.
        with point_account_session_lock(wait=True):
            with self._BrowserSessionManager(client) as session:
                auth_service.login(session, branch_hint=None)
                page = session.page
                page.goto(f"{base}/InventoryPurchases/Index", wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
                except Exception:  # noqa: BLE001 - la carga diferida no es crítica aquí
                    pass

                compras_resp = page.request.get(
                    f"{base}/InventoryPurchases/GetCompras"
                    f"?fechaInicio={_epoch_ms(desde)}&fechaFin={_epoch_ms(hasta)}"
                    "&fkproveedor=&fkSucursal=null"
                )
                if compras_resp.status != 200:
                    raise ExtractionError(
                        f"Point respondió {compras_resp.status} al listar compras.",
                        context={"desde": desde.isoformat(), "hasta": hasta.isoformat()},
                    )
                compras = compras_resp.json()
                if not isinstance(compras, list):
                    raise ExtractionError("Formato inesperado en el listado de compras Point.")

                result.purchases_seen = len(compras)

                for compra in compras:
                    branch = str(compra.get("Sucursal") or "").strip()
                    if solo_almacen and branch.lower() != ALMACEN_BRANCH:
                        result.branches_skipped[branch or "(sin sucursal)"] = (
                            result.branches_skipped.get(branch or "(sin sucursal)", 0) + 1
                        )
                        continue

                    fk = compra.get("FK_Movimiento")
                    if not fk:
                        continue

                    detail_resp = page.request.get(
                        f"{base}/InventoryPurchases/GetComprabyId?fkCompra={fk}"
                    )
                    if detail_resp.status != 200:
                        continue
                    try:
                        detalles = detail_resp.json()
                    except Exception:  # noqa: BLE001 - una compra ilegible no aborta el rango
                        continue
                    if not isinstance(detalles, list) or not detalles:
                        continue

                    lines = []
                    for detalle in detalles:
                        articulo = str(detalle.get("Articulo") or "").strip()
                        if not articulo:
                            continue
                        lines.append(
                            {
                                "articulo": articulo,
                                "cantidad": detalle.get("Cantidad"),
                                "unidad": detalle.get("Unidad"),
                                "costo_unitario": detalle.get("Costo_unitario"),
                                "costo_total": detalle.get("Costo_total"),
                                "raw": detalle,
                            }
                        )
                    if not lines:
                        continue

                    purchases.append(
                        {
                            "purchase_id": str(fk),
                            "folio": str(compra.get("Folio") or "").strip(),
                            "branch": branch,
                            "supplier": str(compra.get("Proveedor") or "").strip(),
                            "purchase_date": _parse_purchase_date(compra.get("Fecha_compra")),
                            "lines": lines,
                        }
                    )
                    result.purchases_kept += 1
                    result.lines_kept += len(lines)

        return purchases, result
