from __future__ import annotations

from datetime import date, datetime
from typing import Any

from django.utils import timezone

from core.models import AuditLog
from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario


TRACE_IMPORT_INVENTORY = "IMPORT_INVENTORY"
TRACE_IMPORTED_MOVEMENT = "IMPORTED_MOVEMENT"
TRACE_MANUAL_MOVEMENT = "MANUAL_MOVEMENT"
TRACE_INVENTORY_ADJUSTMENT = "INVENTORY_ADJUSTMENT"
TRACE_MANUAL_EDIT = "MANUAL_EDIT"
TRACE_MERGE = "MERGE"
TRACE_SCHEDULED_SYNC = "SCHEDULED_SYNC"
TRACE_DRIVE_SYNC = "DRIVE_SYNC"
TRACE_MANUAL_SYNC = "MANUAL_SYNC"
TRACE_RECONSTRUCTED_MOVEMENT = "RECONSTRUCTED_MOVEMENT"
TRACE_RECONSTRUCTED_SYNC = "RECONSTRUCTED_SYNC"
TRACE_UNTRACED = "UNTRACED"


TRACE_LABELS = {
    TRACE_IMPORT_INVENTORY: "Importación de inventario",
    TRACE_IMPORTED_MOVEMENT: "Movimiento importado",
    TRACE_MANUAL_MOVEMENT: "Movimiento manual",
    TRACE_INVENTORY_ADJUSTMENT: "Ajuste de inventario",
    TRACE_MANUAL_EDIT: "Edición manual",
    TRACE_MERGE: "Merge de insumos",
    TRACE_SCHEDULED_SYNC: "Sync programado",
    TRACE_DRIVE_SYNC: "Sync Drive",
    TRACE_MANUAL_SYNC: "Sync manual",
    TRACE_RECONSTRUCTED_MOVEMENT: "Movimiento reconstruido",
    TRACE_RECONSTRUCTED_SYNC: "Sync reconstruido",
    TRACE_UNTRACED: "Sin traza suficiente",
}


def _as_iso(value: datetime | date | None) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if timezone.is_naive(value):
            value = timezone.make_aware(value, timezone.get_current_timezone())
        return value.isoformat()
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.get_current_timezone()).isoformat()


def _sync_source(run: AlmacenSyncRun | None) -> str:
    if not run:
        return TRACE_UNTRACED
    if run.source == AlmacenSyncRun.SOURCE_SCHEDULED:
        return TRACE_SCHEDULED_SYNC
    if run.source == AlmacenSyncRun.SOURCE_DRIVE:
        return TRACE_DRIVE_SYNC
    return TRACE_MANUAL_SYNC


def build_stock_trace(
    *,
    source: str,
    process: str,
    effective_at: datetime | date | None = None,
    reference: str = "",
    run: AlmacenSyncRun | None = None,
    user=None,
    details: dict[str, Any] | None = None,
    quality: str = "DIRECT",
) -> dict[str, Any]:
    username = getattr(user, "username", "") if getattr(user, "is_authenticated", False) else ""
    user_id = str(getattr(user, "id", "") or "") if getattr(user, "is_authenticated", False) else ""
    payload: dict[str, Any] = {
        "source": source,
        "label": TRACE_LABELS.get(source, source),
        "process": process,
        "reference": reference,
        "effective_at": _as_iso(effective_at),
        "recorded_at": _as_iso(timezone.now()),
        "quality": quality,
        "run_id": str(getattr(run, "id", "") or ""),
        "run_source": getattr(run, "source", "") or "",
        "run_started_at": _as_iso(getattr(run, "started_at", None)),
        "user_id": user_id,
        "username": username,
        "details": details or {},
    }
    return payload


def set_stock_trace(
    existencia: ExistenciaInsumo,
    *,
    source: str,
    process: str,
    effective_at: datetime | date | None = None,
    reference: str = "",
    run: AlmacenSyncRun | None = None,
    user=None,
    details: dict[str, Any] | None = None,
    quality: str = "DIRECT",
    save: bool = False,
    update_fields: list[str] | None = None,
) -> dict[str, Any]:
    trace = build_stock_trace(
        source=source,
        process=process,
        effective_at=effective_at,
        reference=reference,
        run=run,
        user=user,
        details=details,
        quality=quality,
    )
    existencia.trazabilidad_stock = trace
    if save:
        fields = list(update_fields or [])
        if "trazabilidad_stock" not in fields:
            fields.append("trazabilidad_stock")
        existencia.save(update_fields=fields)
    return trace


def attach_sync_trace(
    existencias: list[ExistenciaInsumo],
    *,
    run: AlmacenSyncRun,
    user=None,
) -> int:
    count = 0
    for existencia in existencias:
        current = dict(existencia.trazabilidad_stock or {})
        current["run_id"] = str(run.id)
        current["run_source"] = run.source
        current["run_started_at"] = _as_iso(run.started_at)
        current["recorded_at"] = _as_iso(timezone.now())
        if getattr(user, "is_authenticated", False):
            current["user_id"] = str(user.id)
            current["username"] = user.username
        if not current.get("source") or current.get("source") == TRACE_UNTRACED:
            current["source"] = _sync_source(run)
            current["label"] = TRACE_LABELS.get(current["source"], current["source"])
            current["process"] = "inventario.sync_almacen"
            current["quality"] = current.get("quality") or "DIRECT"
        existencia.trazabilidad_stock = current
        existencia.save(update_fields=["trazabilidad_stock"])
        count += 1
    return count


def infer_stock_trace(
    existencia: ExistenciaInsumo,
    *,
    start_date: date,
    end_date: date,
    latest_sync_run: AlmacenSyncRun | None = None,
) -> dict[str, Any]:
    latest_audit = (
        AuditLog.objects.filter(
            model="inventario.ExistenciaInsumo",
            object_id=str(existencia.id),
            timestamp__date__range=(start_date, end_date),
        )
        .order_by("-timestamp", "-id")
        .first()
    )
    if latest_audit:
        payload = dict(latest_audit.payload or {})
        source = TRACE_UNTRACED
        process = "inventario.auditlog"
        if latest_audit.action == "APPLY" and str(payload.get("source", "")).startswith("AJ-"):
            source = TRACE_INVENTORY_ADJUSTMENT
            process = "inventario.ajustes"
        elif latest_audit.action == "UPDATE":
            source = TRACE_MANUAL_EDIT
            process = "inventario.existencias"
        trace = build_stock_trace(
            source=source,
            process=process,
            effective_at=latest_audit.timestamp,
            reference=str(payload.get("source") or payload.get("reference") or latest_audit.action),
            user=latest_audit.user,
            details={"audit_action": latest_audit.action, "payload": payload},
            quality="DIRECT",
        )
        return trace

    latest_movement = (
        MovimientoInventario.objects.filter(
            insumo_id=existencia.insumo_id,
            fecha__date__range=(start_date, end_date),
        )
        .order_by("-fecha", "-id")
        .first()
    )
    sync_cutoff = getattr(latest_sync_run, "finished_at", None)
    if latest_movement and (not sync_cutoff or latest_movement.fecha >= sync_cutoff):
        return build_stock_trace(
            source=TRACE_RECONSTRUCTED_MOVEMENT,
            process="inventario.movimientos",
            effective_at=latest_movement.fecha,
            reference=latest_movement.referencia or str(latest_movement.id),
            details={
                "movement_id": latest_movement.id,
                "movement_type": latest_movement.tipo,
                "reconstructed": True,
            },
            quality="RECONSTRUCTED",
        )

    if latest_sync_run and latest_sync_run.started_at.date() >= start_date:
        return build_stock_trace(
            source=TRACE_RECONSTRUCTED_SYNC,
            process="inventario.sync_almacen_drive",
            effective_at=latest_sync_run.finished_at or latest_sync_run.started_at,
            reference=f"{latest_sync_run.source}:{latest_sync_run.id}",
            run=latest_sync_run,
            details={
                "rows_stock_read": latest_sync_run.rows_stock_read,
                "existencias_updated": latest_sync_run.existencias_updated,
                "reconstructed": True,
            },
            quality="RECONSTRUCTED",
        )

    return build_stock_trace(
        source=TRACE_UNTRACED,
        process="inventario.traceability",
        effective_at=existencia.actualizado_en,
        reference=str(existencia.id),
        details={"reason": "No hay auditoría directa ni sync/movimiento concluyente en la ventana auditada."},
        quality="INSUFFICIENT",
    )
