from __future__ import annotations

import hashlib

from inventario.models import AlmacenSyncRun


POINT_ALMACEN_BASELINE_PREFIX = "POINT_ALMACEN_BASELINE|"
TRUSTED_INSUMO_MATCH_METHODS = frozenset({"POINT_CODE", "ALIAS", "EXACT"})


def latest_point_almacen_baseline_run() -> AlmacenSyncRun | None:
    return (
        AlmacenSyncRun.objects.filter(
            status=AlmacenSyncRun.STATUS_OK,
            message__startswith=POINT_ALMACEN_BASELINE_PREFIX,
        )
        .order_by("-finished_at", "-id")
        .first()
    )


def point_transfer_origin_exit_hash(source_hash: str) -> str:
    return hashlib.sha256(f"{source_hash}|ALMACEN_ORIGIN_EXIT".encode()).hexdigest()
