from __future__ import annotations

from pos_bridge.services.sync_service import PointSyncService


def run_recipe_gap_audit(
    *,
    triggered_by=None,
    branch_hint: str | None = None,
    product_codes: list[str] | None = None,
    limit: int | None = None,
):
    service = PointSyncService()
    return service.run_recipe_gap_audit(
        triggered_by=triggered_by,
        branch_hint=branch_hint,
        product_codes=product_codes,
        limit=limit,
    )
