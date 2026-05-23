from __future__ import annotations

import io

from celery import shared_task
from django.core.management import call_command


@shared_task(
    name="seguimiento.importar_agente_dg",
    bind=True,
    acks_late=True,
    max_retries=0,
    time_limit=900,
    soft_time_limit=840,
)
def task_importar_agente_dg_seguimiento(self, *, limit: int = 0) -> dict[str, object]:
    stdout = io.StringIO()
    call_command("importar_agente_dg_seguimiento", limit=int(limit or 0), stdout=stdout)
    return {
        "ok": True,
        "task_id": getattr(getattr(self, "request", None), "id", None),
        "output": stdout.getvalue()[-4000:],
    }
