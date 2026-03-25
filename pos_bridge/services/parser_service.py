from __future__ import annotations

from decimal import Decimal, InvalidOperation

from pos_bridge.selectors.inventory_selectors import HEADER_ALIASES
from pos_bridge.utils.helpers import normalize_text


class PointInventoryParserService:
    def _header_map(self, headers: list[str]) -> dict[str, int]:
        normalized_headers = [normalize_text(header) for header in headers]
        mapping: dict[str, int] = {}
        for canonical, aliases in HEADER_ALIASES.items():
            for alias in aliases:
                alias_norm = normalize_text(alias)
                for index, header in enumerate(normalized_headers):
                    if alias_norm and alias_norm == header:
                        mapping[canonical] = index
                        break
                if canonical in mapping:
                    break
        return mapping

    def _value_at(self, row: list[str], mapping: dict[str, int], canonical: str, fallback_index: int | None = None) -> str:
        index = mapping.get(canonical, fallback_index)
        if index is None or index >= len(row):
            return ""
        return str(row[index]).strip()

    @staticmethod
    def _looks_numeric(value: str) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        try:
            Decimal(text.replace(",", ""))
        except (InvalidOperation, ValueError):
            return False
        return True

    def _parse_point_hidden_columns_row(self, values: list[str], headers: list[str]) -> dict | None:
        normalized_headers = [normalize_text(header) for header in headers]
        if normalized_headers[:3] != ["codigo", "producto", "cantidad"]:
            return None
        if len(values) < 5:
            return None
        if self._looks_numeric(values[2]):
            return None
        if not self._looks_numeric(values[4]):
            return None

        return {
            "external_id": values[0],
            "sku": values[1] or values[0],
            "name": values[2] or values[1] or values[0],
            "category": values[3] if len(values) > 3 else "",
            "stock": values[4] if len(values) > 4 else "",
            "min_stock": "",
            "max_stock": "",
        }

    def parse_inventory_table(self, table_payload: dict, branch_external_id: str, branch_name: str) -> dict:
        headers = [str(item).strip() for item in table_payload.get("headers", [])]
        rows = table_payload.get("rows", []) or []
        mapping = self._header_map(headers)
        items: list[dict] = []

        for row in rows:
            values = [str(cell).strip() for cell in row]
            if not any(values):
                continue
            row_signature = " ".join(normalize_text(cell) for cell in values if str(cell).strip())
            if row_signature == "no hay datos disponibles":
                continue
            point_hidden_payload = self._parse_point_hidden_columns_row(values, headers)
            if point_hidden_payload is not None:
                product_name = point_hidden_payload["name"]
                sku = point_hidden_payload["sku"]
                external_id = point_hidden_payload["external_id"]
                category = point_hidden_payload["category"]
                stock = point_hidden_payload["stock"]
                min_stock = point_hidden_payload["min_stock"]
                max_stock = point_hidden_payload["max_stock"]
            else:
                product_name = self._value_at(values, mapping, "name", fallback_index=1 if len(values) > 1 else 0)
                sku = self._value_at(values, mapping, "sku", fallback_index=0)
                external_id = self._value_at(values, mapping, "external_id", fallback_index=0) or sku or product_name
                category = self._value_at(values, mapping, "category")
                stock = self._value_at(values, mapping, "stock", fallback_index=2 if len(values) > 2 else None)
                min_stock = self._value_at(values, mapping, "min_stock", fallback_index=3 if len(values) > 3 else None)
                max_stock = self._value_at(values, mapping, "max_stock", fallback_index=4 if len(values) > 4 else None)
            item = {
                "branch_external_id": branch_external_id,
                "branch_name": branch_name,
                "external_id": external_id,
                "sku": sku or external_id,
                "name": product_name or external_id,
                "category": category,
                "stock": stock,
                "min_stock": min_stock,
                "max_stock": max_stock,
                "raw_payload": {"headers": headers, "row": values},
            }
            items.append(item)

        return {
            "headers": headers,
            "items": items,
            "detected_columns": mapping,
        }
