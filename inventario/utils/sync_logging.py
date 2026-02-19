from __future__ import annotations

from django.utils import timezone

from inventario.models import AlmacenSyncRun


def log_sync_run(
    *,
    source: str,
    status: str,
    summary=None,
    triggered_by=None,
    folder_name: str = "",
    target_month: str = "",
    fallback_used: bool = False,
    downloaded_sources: list[str] | None = None,
    pending_preview: list[dict] | None = None,
    message: str = "",
    started_at=None,
):
    downloaded_sources = downloaded_sources or []
    started_at = started_at or timezone.now()

    rows_stock_read = int(getattr(summary, "rows_stock_read", 0) or 0)
    rows_mov_read = int(getattr(summary, "rows_mov_read", 0) or 0)
    matched = int(getattr(summary, "matched", 0) or 0)
    unmatched = int(getattr(summary, "unmatched", 0) or 0)
    insumos_created = int(getattr(summary, "insumos_created", 0) or 0)
    existencias_updated = int(getattr(summary, "existencias_updated", 0) or 0)
    movimientos_created = int(getattr(summary, "movimientos_created", 0) or 0)
    movimientos_skipped_duplicate = int(getattr(summary, "movimientos_skipped_duplicate", 0) or 0)
    aliases_created = int(getattr(summary, "aliases_created", 0) or 0)
    if pending_preview is None:
        pending_preview = list(getattr(summary, "pendientes", [])[:200] or [])

    return AlmacenSyncRun.objects.create(
        source=source,
        status=status,
        triggered_by=triggered_by,
        started_at=started_at,
        finished_at=timezone.now(),
        folder_name=folder_name,
        target_month=target_month,
        fallback_used=fallback_used,
        downloaded_sources=", ".join(downloaded_sources),
        rows_stock_read=rows_stock_read,
        rows_mov_read=rows_mov_read,
        matched=matched,
        unmatched=unmatched,
        insumos_created=insumos_created,
        existencias_updated=existencias_updated,
        movimientos_created=movimientos_created,
        movimientos_skipped_duplicate=movimientos_skipped_duplicate,
        aliases_created=aliases_created,
        pending_preview=pending_preview,
        message=message,
    )
