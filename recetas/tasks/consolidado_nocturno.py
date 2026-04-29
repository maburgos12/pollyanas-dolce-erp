from __future__ import annotations

from datetime import date

from celery import shared_task
from django.utils import timezone

from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService


@shared_task(name="recetas.consolidado_nocturno_cedis")
def consolidado_nocturno_cedis(
    fecha_operacion: str | None = None,
    sincronizar_point: bool = True,
    sincronizar_inventario_cedis: bool = True,
) -> dict:
    target_date = date.fromisoformat(fecha_operacion) if fecha_operacion else timezone.localdate()
    consolidado = ConsolidadoNocturnoCedisService().consolidar(
        fecha_operacion=target_date,
        sincronizar_point=sincronizar_point,
        sincronizar_inventario_cedis=sincronizar_inventario_cedis,
    )
    return {
        "consolidado_id": consolidado.id,
        "fecha_operacion": consolidado.fecha_operacion.isoformat(),
        "estado": consolidado.estado,
        "plan_produccion_id": consolidado.plan_produccion_id,
        "inventory_sync_job_id": (consolidado.metadata or {}).get("inventory_sync_job_id"),
        "sync_job_id": consolidado.sync_job_id,
        "sucursales_esperadas": consolidado.sucursales_esperadas,
        "sucursales_con_solicitud": consolidado.sucursales_con_solicitud,
        "productos_consolidados": consolidado.productos_consolidados,
        "total_plan_produccion": str(consolidado.total_plan_produccion),
    }
