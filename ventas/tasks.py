from __future__ import annotations

from celery import shared_task

from ventas.services.sales_truth import sync_authoritative_from_vps


@shared_task(name="ventas.sync_ventas_autoritativas")
def sync_ventas_autoritativas_task(periodo: str | None = None) -> dict:
    if periodo is None:
        from datetime import date

        today = date.today()
        periodo = f"{today.year - 1}-12" if today.month == 1 else f"{today.year}-{today.month - 1:02d}"
    return sync_authoritative_from_vps(periodo)
