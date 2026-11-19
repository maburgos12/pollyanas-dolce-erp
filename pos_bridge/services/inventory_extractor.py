from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.inventory_page import PointInventoryPage
from pos_bridge.browser.screenshots import capture_screenshot
from pos_bridge.browser.session import BrowserSessionManager
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.auth_service import PointAuthService
from pos_bridge.services.normalizer_service import PointNormalizerService
from pos_bridge.services.parser_service import PointInventoryParserService
from pos_bridge.utils.dates import local_now, timestamp_token
from pos_bridge.utils.exceptions import ExtractionError, PosBridgeError
from pos_bridge.utils.helpers import normalize_text, safe_slug, write_json_file


# shortcut: Point exposes labels, so allow only aliases verified in its cuts until category IDs are available.
PRODUCT_CATEGORY_ROW_ALIASES = {
    "vaso preparado mini": "vasos mini",
    "vasos preparados grande": "vasos grande",
    "velas sparklers": "vela sparklers",
}


@dataclass
class ExtractedBranchInventory:
    branch: dict
    inventory_rows: list[dict]
    captured_at: object
    raw_export_path: str


class PointInventoryExtractor:
    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        parser: PointInventoryParserService | None = None,
        normalizer: PointNormalizerService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.parser = parser or PointInventoryParserService()
        self.normalizer = normalizer or PointNormalizerService()
        self.auth_service = PointAuthService(self.settings)

    def _write_raw_export(self, branch: dict, payload: dict) -> Path:
        filename = f"{timestamp_token()}_{safe_slug(branch['external_id'])}_inventory.json"
        path = self.settings.raw_exports_dir / filename
        return write_json_file(path, payload)

    @staticmethod
    def _apply_branch_filter(branches: list[dict], branch_filter: str | None) -> list[dict]:
        if not branch_filter:
            return branches
        branch_filter_norm = branch_filter.strip().lower()
        exact_matches = [
            branch
            for branch in branches
            if branch_filter_norm == str(branch.get("value", "")).strip().lower()
            or branch_filter_norm == str(branch.get("label", "")).strip().lower()
        ]
        if exact_matches:
            return exact_matches
        return [
            branch
            for branch in branches
            if branch_filter_norm in str(branch.get("value", "")).lower()
            or branch_filter_norm in str(branch.get("label", "")).lower()
        ]

    def _exclude_non_commercial_branches(self, branches: list[dict]) -> list[dict]:
        excluded = {
            str(label).strip().casefold()
            for label in getattr(self.settings, "sales_excluded_branches", [])
            if str(label).strip()
        }
        return [
            branch
            for branch in branches
            if str(branch.get("label", "")).strip().casefold() not in excluded
        ]

    def _extract_product_rows_by_category(self, inventory_page: PointInventoryPage, branch_id: str):
        options = [
            option for option in inventory_page.list_category_options(kind="products")
            if option.get("value") and "SELECCIONE" not in str(option.get("label", "")).upper()
        ]
        if not options:
            raise ExtractionError("Point no devolvió categorías para confirmar el corte.")

        normalized_rows = []
        category_audit = []
        table_payload = {"headers": [], "rows": []}
        seen_keys = set()
        for option in options:
            payload = inventory_page.read_product_category(branch_id, option["value"])
            parsed = self.parser.parse_inventory_table(payload, branch_id, "")
            expected = normalize_text(option["label"])
            accepted = {expected, PRODUCT_CATEGORY_ROW_ALIASES.get(expected, expected)}
            observed = {normalize_text(item["category"]) for item in parsed["items"] if item["category"]}
            if not observed <= accepted:
                raise ExtractionError(
                    f"Point no confirmó la tabla de la categoría {option['label']}.",
                    context={"category": option["label"], "observed_categories": sorted(observed)},
                )

            count = 0
            for item in parsed["items"]:
                item["category"] = item["category"] or option["label"]
                normalized = self.normalizer.normalize_inventory_row(item)
                key = normalized["external_id"] or normalized["sku"] or normalized["name"]
                if key not in seen_keys:
                    seen_keys.add(key)
                    normalized_rows.append(normalized)
                    count += 1
            category_audit.append({"category": option["label"], "category_id": option["value"], "rows": count})
            table_payload["headers"] = payload["headers"]
            table_payload["rows"].extend(payload["rows"])
        return normalized_rows, category_audit, table_payload

    def _extract_branch(self, inventory_page: PointInventoryPage, branch_option: dict) -> ExtractedBranchInventory:
        selected_branch = inventory_page.select_branch(branch_option, require_confirmation=True)
        branch_payload = self.normalizer.normalize_branch_payload(
            {
                "external_id": selected_branch["value"],
                "name": selected_branch["label"],
                "status": "ACTIVE",
                "metadata": {},
            }
        )
        try:
            normalized_rows, category_audit, table_payload = self._extract_product_rows_by_category(
                inventory_page, selected_branch["value"]
            )
        except ExtractionError as error:
            error.context.setdefault("branch_external_id", selected_branch["value"])
            raise
        if not normalized_rows:
            raise ExtractionError(
                f"Point devolvió cero productos para la sucursal {selected_branch['label']}; "
                "se rechaza la extracción para no publicar inventario parcial.",
                context={"branch": selected_branch, "category_audit": category_audit},
            )
        parsed = self.parser.parse_inventory_table(
            table_payload,
            branch_external_id=selected_branch["value"],
            branch_name=selected_branch["label"],
        )
        branch_payload["metadata"] = {
            "detected_columns": parsed["detected_columns"],
            "product_category_scan": category_audit,
        }
        raw_export_path = self._write_raw_export(
            branch_payload,
            {
                "branch": branch_payload,
                "captured_at": local_now().isoformat(),
                "table_payload": table_payload,
                "parsed": parsed,
            },
        )
        return ExtractedBranchInventory(
            branch=branch_payload,
            inventory_rows=normalized_rows,
            captured_at=local_now(),
            raw_export_path=str(raw_export_path),
        )

    def extract(self, *, branch_filter: str | None = None, limit_branches: int | None = None) -> list[ExtractedBranchInventory]:
        session = None
        try:
            client = PlaywrightBrowserClient(self.settings)
            with BrowserSessionManager(client) as session:
                # El workspace inicial solo habilita la sesión.
                # La sucursal objetivo se controla desde el dropdown de inventario.
                self.auth_service.login(session, branch_hint=branch_filter or "MATRIZ")
                inventory_page = PointInventoryPage(session.page, self.settings)
                inventory_page.open_inventory_module()
                branches = inventory_page.list_branches()
                branches = self._apply_branch_filter(branches, branch_filter)
                if not branch_filter:
                    # El inventario completo concilia tiendas comerciales. CEDIS,
                    # ALMACEN, PRODUCCION y DEVOLUCIONES tienen flujos operativos
                    # propios y no deben exigirse como catálogos de venta.
                    branches = self._exclude_non_commercial_branches(branches)
                if not branches:
                    fallback_branch = branch_filter or "default"
                    branches = [{"value": fallback_branch, "label": fallback_branch}]
                if limit_branches is not None:
                    branches = branches[:limit_branches]

                extracted: list[ExtractedBranchInventory] = []
                first_branch, *remaining_branches = branches
                extracted.append(self._extract_branch(inventory_page, first_branch))

            # Point degrada la respuesta después de recorrer varias sucursales en
            # una misma sesión. Renovarla por sucursal evita falsos inventarios
            # vacíos y mantiene aislado cualquier fallo posterior.
            for branch_option in remaining_branches:
                client = PlaywrightBrowserClient(self.settings)
                with BrowserSessionManager(client) as session:
                    self.auth_service.login(session, branch_hint=branch_option["label"])
                    inventory_page = PointInventoryPage(session.page, self.settings)
                    inventory_page.open_inventory_module()
                    extracted.append(self._extract_branch(inventory_page, branch_option))
            return extracted
        except PosBridgeError:
            raise
        except Exception as exc:
            screenshot_path = None
            if session is not None:
                try:
                    screenshot_path = str(capture_screenshot(session.page, self.settings, "inventory_extraction_failure"))
                except Exception:
                    screenshot_path = None
            raise ExtractionError(
                f"Fallo en extracción Point: {exc}",
                context={"screenshot_path": screenshot_path},
            ) from exc
