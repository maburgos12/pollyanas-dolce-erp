from __future__ import annotations

from celery import shared_task

from .services_consumo_bom import ConsumoInsumoAutoService, previous_day_bounds


@shared_task(name="inventario.generar_consumos_bom_dia_anterior")
def generar_consumos_bom_dia_anterior():
    start, end = previous_day_bounds()
    summary = ConsumoInsumoAutoService().generar_consumos_produccion(start, end, dry_run=False)
    return {
        "fecha_inicio": summary.fecha_inicio.isoformat(),
        "fecha_fin": summary.fecha_fin.isoformat(),
        "producciones_procesadas": summary.producciones_procesadas,
        "movimientos_generados": summary.movimientos_generados,
        "movimientos_creados": summary.movimientos_creados,
        "movimientos_actualizados": summary.movimientos_actualizados,
        "movimientos_sin_cambio": summary.movimientos_sin_cambio,
        "insumos_actualizados": summary.insumos_actualizados,
        "omitidos_sin_receta": summary.omitidos_sin_receta,
        "omitidos_bom_incompleto": summary.omitidos_bom_incompleto,
        "omitidos_unidad_incompatible": summary.omitidos_unidad_incompatible,
        "omitidos_sin_insumo_reventa": summary.omitidos_sin_insumo_reventa,
    }
