from __future__ import annotations

from pos_bridge.models.branch import PointBranch
from pos_bridge.utils.helpers import decimal_from_value, deterministic_id, normalize_text


class PointNormalizerService:
    def normalize_branch_payload(self, payload: dict) -> dict:
        external_id = str(payload.get("external_id") or payload.get("name") or "").strip()
        name = str(payload.get("name") or external_id).strip()
        status = str(payload.get("status") or PointBranch.STATUS_ACTIVE).upper()
        if status not in {PointBranch.STATUS_ACTIVE, PointBranch.STATUS_INACTIVE, PointBranch.STATUS_UNKNOWN}:
            status = PointBranch.STATUS_UNKNOWN
        return {
            "external_id": external_id or deterministic_id(name),
            "name": name,
            "status": status,
            "metadata": payload.get("metadata") or {},
        }

    def normalize_inventory_row(self, payload: dict) -> dict:
        name = str(payload.get("name") or "").strip()
        sku = str(payload.get("sku") or "").strip()
        external_id = str(payload.get("external_id") or sku or normalize_text(name)).strip()
        return {
            "external_id": external_id or deterministic_id(name, sku),
            "sku": sku or external_id,
            "name": name or external_id,
            "category": str(payload.get("category") or "").strip(),
            "stock": decimal_from_value(payload.get("stock")),
            "min_stock": decimal_from_value(payload.get("min_stock")),
            "max_stock": decimal_from_value(payload.get("max_stock")),
            "raw_payload": payload.get("raw_payload") or {},
        }

    def normalize_sales_row(self, payload: dict, *, branch: dict, sale_date) -> dict:
        external_id = str(
            payload.get("external_id")
            or payload.get("FK_Producto")
            or payload.get("Codigo")
            or deterministic_id(payload.get("Nombre"), sale_date, branch.get("external_id"))
        ).strip()
        sku = str(payload.get("sku") or payload.get("Codigo") or "").strip()
        name = str(payload.get("name") or payload.get("Nombre") or sku or external_id).strip()
        category = str(payload.get("category") or payload.get("Categoria") or "").strip()
        family = str(payload.get("family") or payload.get("Familia") or "").strip()
        return {
            "external_id": external_id,
            "sku": sku or external_id,
            "name": name or external_id,
            "category": category,
            "family": family,
            "branch_external_id": str(branch.get("external_id") or "").strip(),
            "branch_name": str(branch.get("name") or "").strip(),
            "sale_date": sale_date,
            "quantity": decimal_from_value(payload.get("quantity") or payload.get("Cantidad")),
            "tickets": int(payload.get("tickets") or 0),
            "gross_amount": decimal_from_value(payload.get("gross_amount") or payload.get("Bruto")),
            "discount_amount": decimal_from_value(payload.get("discount_amount") or payload.get("Descuento")),
            "total_amount": decimal_from_value(payload.get("total_amount") or payload.get("Venta")),
            "tax_amount": decimal_from_value(payload.get("tax_amount") or payload.get("IVA")),
            "net_amount": decimal_from_value(payload.get("net_amount") or payload.get("Venta_neta")),
            "raw_payload": payload,
        }
