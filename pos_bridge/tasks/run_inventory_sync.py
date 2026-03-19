from __future__ import annotations

from pos_bridge.services.sync_service import PointSyncService


def run_inventory_sync(*, triggered_by=None, branch_filter: str | None = None, limit_branches: int | None = None):
    service = PointSyncService()
    return service.run_inventory_sync(
        triggered_by=triggered_by,
        branch_filter=branch_filter,
        limit_branches=limit_branches,
    )
