from __future__ import annotations

from pos_bridge.models import PointTransferLine

from .models import ParadaRuta, PuntoLogistico


def point_transfer_enviada(line: PointTransferLine) -> bool:
    if line.sent_at:
        return True
    payload = line.raw_payload
    if not isinstance(payload, dict):
        return False
    transfer = payload.get("transfer")
    if not isinstance(transfer, dict):
        return False
    return transfer.get("isEnviado") is True


def parada_resuelta_operativamente(parada: ParadaRuta) -> bool:
    if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS:
        return parada.estado == ParadaRuta.ESTADO_VISITADA
    if parada.estado in {ParadaRuta.ESTADO_VISITADA, ParadaRuta.ESTADO_OMITIDA}:
        return True
    return parada.entrega_estado != ParadaRuta.ENTREGA_PENDIENTE
