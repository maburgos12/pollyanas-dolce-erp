from __future__ import annotations

from typing import Iterable


# Binding formal minimo entre aliases historicos del catalogo y keys reales del ERP AI Gateway.
# Solo incluye equivalencias claras y verificables; los aliases sin equivalencia exacta
# deben permanecer sin resolver hasta que exista una tool real compatible.
GATEWAY_TOOL_ALIAS_MAP: dict[str, str] = {
    "api.reportes_bi_dashboard": "erp.get_dashboard",
    "api.audit_logs": "erp.get_audit_logs",
    "api.pos_bridge_sync_jobs": "erp.get_sync_jobs",
    "api.pos_bridge_sales_summary": "erp.get_sales_summary",
    "api.pos_bridge_sales_branch": "erp.get_sales_by_branch",
    "api.pos_bridge_inventory_low_stock": "erp.get_inventory_low_stock",
    "api.control_discrepancias": "erp.get_discrepancies",
    "api.compras_solicitudes": "erp.get_purchase_requests",
    "api.compras_ordenes": "erp.get_purchase_orders",
}


def resolve_gateway_tool_alias(
    declared_tool_key: str,
    *,
    available_keys: Iterable[str] | None = None,
) -> str:
    if not declared_tool_key:
        return ""

    resolved_key = declared_tool_key
    if declared_tool_key.startswith("api."):
        resolved_key = GATEWAY_TOOL_ALIAS_MAP.get(declared_tool_key, "")

    if available_keys is None:
        return resolved_key if resolved_key.startswith("erp.") else ""

    available = set(available_keys)
    if resolved_key in available:
        return resolved_key
    return ""
