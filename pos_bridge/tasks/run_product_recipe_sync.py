from __future__ import annotations

from pos_bridge.services.sync_service import PointSyncService


def run_product_recipe_sync(
    *,
    triggered_by=None,
    branch_hint: str | None = None,
    product_codes: list[str] | None = None,
    limit: int | None = None,
    include_without_recipe: bool = False,
):
    service = PointSyncService()
    return service.run_product_recipe_sync(
        triggered_by=triggered_by,
        branch_hint=branch_hint,
        product_codes=product_codes,
        limit=limit,
        include_without_recipe=include_without_recipe,
    )
