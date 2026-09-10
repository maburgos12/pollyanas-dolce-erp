"""Tareas de compras departamentales."""
from __future__ import annotations

import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name="compras.enviar_avisos_compra_realizada")
def enviar_avisos_compra_realizada(compra_id: int) -> dict[str, object]:
    """Envía los avisos pendientes de una compra sin duplicar los ya enviados."""
    from .models import CompraRealizadaDepartamental
    from .services_avisos_compra import enviar_avisos_pendientes

    compra = (CompraRealizadaDepartamental.objects
              .select_related("item__solicitud__solicitante")
              .filter(pk=compra_id).first())
    if compra is None:
        logger.error("[compras] Compra %s no encontrada para avisar.", compra_id)
        return {"compra_id": compra_id, "encontrada": False}
    avisos = enviar_avisos_pendientes(compra)
    return {
        "compra_id": compra_id,
        "encontrada": True,
        "resultados": {aviso.canal: aviso.estado for aviso in avisos},
    }
