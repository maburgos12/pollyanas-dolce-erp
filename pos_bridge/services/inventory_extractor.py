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
from pos_bridge.utils.helpers import safe_slug, write_json_file


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

    def _apply_branch_filter(self, branches: list[dict], branch_filter: str | None) -> list[dict]:
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

    def _extract_product_rows_by_category(self, inventory_page: PointInventoryPage) -> tuple[list[dict], list[dict]]:
        category_options = inventory_page.list_category_options(kind="products")
        category_options = [
            option
            for option in category_options
            if option.get("value") and "SELECCIONE" not in str(option.get("label", "")).upper()
        ]
        if not category_options:
            parsed = self.parser.parse_inventory_table(inventory_page.extract_inventory_table(kind="products"), "", "")
            return parsed["items"], []

        all_rows: list[dict] = []
        audit_rows: list[dict] = []
        seen_keys: set[str] = set()

        for option in category_options:
            inventory_page.select_category(option["value"], kind="products")
            parsed = self.parser.parse_inventory_table(inventory_page.extract_inventory_table(kind="products"), "", "")
            category_rows = 0
            for item in parsed["items"]:
                item["category"] = item.get("category") or option["label"]
                normalized = self.normalizer.normalize_inventory_row(item)
                dedupe_key = normalized["external_id"] or normalized["sku"] or normalized["name"]
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                all_rows.append(normalized)
                category_rows += 1
            audit_rows.append({"category": option["label"], "rows": category_rows})
        return all_rows, audit_rows

    def extract(self, *, branch_filter: str | None = None, limit_branches: int | None = None) -> list[ExtractedBranchInventory]:
        client = PlaywrightBrowserClient(self.settings)
        session = None
        try:
            with BrowserSessionManager(client) as session:
                # El workspace inicial solo habilita la sesión.
                # La sucursal objetivo se controla desde el dropdown de inventario.
                self.auth_service.login(session, branch_hint=branch_filter)
                inventory_page = PointInventoryPage(session.page, self.settings)
                inventory_page.open_inventory_module()
                branches = inventory_page.list_branches()
                branches = self._apply_branch_filter(branches, branch_filter)
                if not branches:
                    fallback_branch = branch_filter or "default"
                    branches = [{"value": fallback_branch, "label": fallback_branch}]
                if limit_branches is not None:
                    branches = branches[:limit_branches]

                extracted: list[ExtractedBranchInventory] = []
                for branch_option in branches:
                    selected_branch = inventory_page.select_branch(branch_option)
                    table_payload = inventory_page.extract_inventory_table(kind="products")
                    branch_payload = self.normalizer.normalize_branch_payload(
                        {
                            "external_id": selected_branch["value"],
                            "name": selected_branch["label"],
                            "status": "ACTIVE",
                            "metadata": {},
                        }
                    )
                    normalized_rows, category_audit = self._extract_product_rows_by_category(inventory_page)
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
                    extracted.append(
                        ExtractedBranchInventory(
                            branch=branch_payload,
                            inventory_rows=normalized_rows,
                            captured_at=local_now(),
                            raw_export_path=str(raw_export_path),
                        )
                    )
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
