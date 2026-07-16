from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from logistica.models import (
    DiscrepanciaLogistica,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaCargaSucursalEvento,
    RutaEntrega,
)
from logistica.domain_ruta import point_transfer_enviada
from logistica.services_contexto_operativo import validar_contexto_operativo
from logistica.services_discrepancias import jefe_inmediato_para_actor


class CargaSucursalError(Exception):
    def __init__(self, mensaje: str, *, codigo: str):
        super().__init__(mensaje)
        self.codigo = codigo


class ConflictoIdempotencia(CargaSucursalError):
    def __init__(self):
        super().__init__(
            "client_event_id ya fue utilizado con otro contenido.",
            codigo="conflicto_idempotencia",
        )


def _normalizar_lineas(lineas) -> list[dict]:
    return [
        {
            "linea_id": int(linea["linea_id"]),
            "source_hash": str(linea.get("source_hash") or ""),
            "cantidad_cargada": str(Decimal(str(linea["cantidad_cargada"]))),
            "motivo_diferencia": str(linea.get("motivo_diferencia") or ""),
            "notas": str(linea.get("notas") or ""),
        }
        for linea in lineas
    ]


def _payload_hash(*, parada_id: int, lineas) -> str:
    payload = {
        "parada_id": int(parada_id),
        "lineas": sorted(_normalizar_lineas(lineas), key=lambda linea: linea["linea_id"]),
    }
    serializado = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serializado.encode("utf-8")).hexdigest()


def _validar_cobertura(*, contexto, esperadas, recibidas):
    esperadas_por_id = {linea.id: linea for linea in esperadas}
    recibidas_por_id = {int(linea["linea_id"]): linea for linea in recibidas}
    if len(recibidas_por_id) != len(recibidas):
        raise CargaSucursalError("No se permite repetir productos en la captura.", codigo="producto_no_vigente")
    if set(recibidas_por_id) != set(esperadas_por_id):
        raise CargaSucursalError(
            "La captura ya no coincide con los productos vigentes de la sucursal.",
            codigo="producto_no_vigente",
        )
    if not set(recibidas_por_id).issubset(set(contexto.productos_permitidos)):
        raise CargaSucursalError("La captura contiene productos de otro tramo.", codigo="producto_no_vigente")
    for linea_id, captura in recibidas_por_id.items():
        if captura.get("source_hash") != esperadas_por_id[linea_id].source_hash:
            raise CargaSucursalError("La identidad de un producto cambió.", codigo="producto_no_vigente")
    return esperadas_por_id, recibidas_por_id


def _estatus_cantidad(*, esperada: Decimal, cargada: Decimal) -> str:
    if cargada == esperada:
        return RutaCargaChecklistLinea.ESTATUS_CARGADA
    if cargada < esperada:
        return RutaCargaChecklistLinea.ESTATUS_FALTANTE
    return RutaCargaChecklistLinea.ESTATUS_SOBRANTE


def _aplicar_linea(*, linea, captura, actor):
    cargada = Decimal(str(captura["cantidad_cargada"]))
    esperada = Decimal(str(linea.cantidad_enviada_esperada))
    if linea.estatus == RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED:
        if cargada != 0:
            raise CargaSucursalError(
                f"{linea.item_name} fue confirmado por Point con Enviado = 0 y no admite captura.",
                codigo="enviado_cero_no_editable",
            )
        return
    motivo = str(captura.get("motivo_diferencia") or "").strip()
    if cargada != esperada and not motivo:
        raise CargaSucursalError(
            f"Selecciona el motivo de la diferencia para {linea.item_name}.",
            codigo="motivo_diferencia_requerido",
        )
    linea.cantidad_cargada = cargada
    linea.estatus = _estatus_cantidad(esperada=esperada, cargada=cargada)
    linea.motivo_diferencia = motivo
    linea.notas = str(captura.get("notas") or "").strip()
    linea.validado_por = actor
    linea.validado_en = timezone.now()
    linea.save(
        update_fields=[
            "cantidad_cargada",
            "estatus",
            "motivo_diferencia",
            "notas",
            "validado_por",
            "validado_en",
            "actualizado_en",
        ]
    )
    if cargada != esperada:
        DiscrepanciaLogistica.objects.update_or_create(
            linea_carga=linea,
            origen=DiscrepanciaLogistica.ORIGEN_CARGA,
            estado__in=[
                DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE,
                DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
            ],
            defaults={
                "ruta": linea.checklist.ruta,
                "parada": linea.parada,
                "cantidad_enviada": esperada,
                "cantidad_cargada": cargada,
                "cantidad_recibida": None,
                "motivo": motivo,
                "notas": linea.notas,
                "asignado_a": jefe_inmediato_para_actor(actor),
                "creado_por": actor,
            },
        )


@transaction.atomic
def guardar_carga_sucursal(
    *,
    actor,
    ruta: RutaEntrega,
    contexto_token: str,
    parada_id: int,
    client_event_id: str,
    lineas,
) -> dict:
    client_event_id = str(client_event_id or "").strip()
    if not client_event_id:
        raise CargaSucursalError("client_event_id es obligatorio.", codigo="client_event_id_requerido")
    normalizadas = _normalizar_lineas(lineas)
    hash_actual = _payload_hash(parada_id=parada_id, lineas=normalizadas)
    ruta_bloqueada = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    existente = RutaCargaSucursalEvento.objects.select_for_update().filter(
        ruta=ruta_bloqueada,
        client_event_id=client_event_id,
    ).first()
    if existente:
        if existente.creado_por_id != actor.id or existente.payload_hash != hash_actual:
            raise ConflictoIdempotencia()
        return existente.respuesta

    contexto = validar_contexto_operativo(
        token=contexto_token,
        ruta=ruta_bloqueada,
        actor=actor,
        bloquear=True,
    )
    esperadas = list(
        RutaCargaChecklistLinea.objects.select_for_update()
        .filter(checklist__ruta=ruta_bloqueada, parada_id=parada_id)
        .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .select_related("checklist", "checklist__ruta", "parada")
        .order_by("id")
    )
    if not esperadas:
        raise CargaSucursalError("La sucursal no pertenece al tramo vigente.", codigo="sucursal_no_permitida")
    point_lineas = {
        linea.id: linea.point_transfer_line
        for linea in RutaCargaChecklistLinea.objects.filter(id__in=[row.id for row in esperadas])
        .exclude(point_transfer_line_id__isnull=True)
        .select_related("point_transfer_line")
    }
    pendientes_point = {
        linea.id
        for linea in esperadas
        if (
        linea.estatus == RutaCargaChecklistLinea.ESTATUS_PENDIENTE
        and linea.point_transfer_line_id
        and not point_transfer_enviada(point_lineas[linea.id])
        )
    }
    capturables = [
        linea
        for linea in esperadas
        if linea.id not in pendientes_point and linea.estatus != RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
    ]
    if not capturables:
        raise CargaSucursalError(
            "Esta sucursal todavía no tiene productos enviados que requieran captura.",
            codigo="sin_productos_capturables",
        )
    esperadas_por_id, recibidas_por_id = _validar_cobertura(
        contexto=contexto,
        esperadas=capturables,
        recibidas=normalizadas,
    )
    for linea_id in sorted(esperadas_por_id):
        _aplicar_linea(
            linea=esperadas_por_id[linea_id],
            captura=recibidas_por_id[linea_id],
            actor=actor,
        )

    checklist = esperadas[0].checklist
    if checklist.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists():
        checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
    else:
        checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        checklist.confirmado_por = actor
        checklist.confirmado_en = timezone.now()
    checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "actualizado_en"])
    respuesta = {
        "ruta_id": ruta_bloqueada.id,
        "parada_id": int(parada_id),
        "lineas_guardadas": len(capturables),
        "version_checklist": contexto.version_checklist,
    }
    RutaCargaSucursalEvento.objects.create(
        ruta=ruta_bloqueada,
        parada_id=parada_id,
        client_event_id=client_event_id,
        payload_hash=hash_actual,
        contexto_version=contexto.version_checklist,
        respuesta=respuesta,
        creado_por=actor,
    )
    return respuesta
