from __future__ import annotations

import hashlib
import json
from datetime import UTC
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from core.models import Notificacion
from core.notificaciones import crear_notificaciones

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
    notas = str(captura.get("notas") or "").strip()
    if (
        (linea.validado_en or linea.validado_por_id or linea.client_event_id)
        and (cargada, motivo, notas) == (linea.cantidad_cargada, linea.motivo_diferencia, linea.notas)
    ):
        return
    if cargada != esperada and not motivo:
        raise CargaSucursalError(
            f"Selecciona el motivo de la diferencia para {linea.item_name}.",
            codigo="motivo_diferencia_requerido",
        )
    linea.cantidad_cargada = cargada
    linea.estatus = _estatus_cantidad(esperada=esperada, cargada=cargada)
    linea.motivo_diferencia = motivo
    linea.notas = notas
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


def _avisar_carga_guamuchil(evento, point_lineas):
    ruta = evento.ruta
    sucursal = evento.parada.punto.sucursal
    if (
        not sucursal or sucursal.codigo != "GUAMUCHIL" or not sucursal.activa
        or ruta.fecha_ruta != timezone.localdate(evento.creado_en)
        or ruta.repartidor_id is None or ruta.repartidor.user_id != evento.creado_por_id
    ):
        return

    folios = set()
    for linea in point_lineas:
        if (
            linea.erp_destination_branch_id != sucursal.pk or not linea.transfer_external_id
            or not linea.is_current_snapshot or not linea.is_open
            or linea.is_received or linea.is_cancelled or not linea.sent_at
        ):
            continue
        payload = linea.raw_payload
        cabecera = payload.get("transfer") if isinstance(payload, dict) else None
        envio_point = cabecera.get("Fecha_envio") if isinstance(cabecera, dict) else None
        try:
            envio = parse_datetime(envio_point) if isinstance(envio_point, str) else None
        except ValueError:
            envio = None
        if envio and timezone.is_naive(envio):
            # Point entrega Fecha_envio en UTC sin offset en esta consulta.
            envio = envio.replace(tzinfo=UTC)
        if timezone.localdate(envio or linea.sent_at) == ruta.fecha_ruta:
            folios.add(linea.transfer_external_id)

    usuarios = get_user_model().objects.filter(is_active=True, userprofile__sucursal=sucursal)
    for folio in sorted(folios):
        objeto_id = f"{ruta.pk}:{evento.parada_id}:{folio}"
        notificados = Notificacion.objects.filter(
            tipo=Notificacion.TIPO_SISTEMA, objeto_tipo="recepcion_point_guamuchil", objeto_id=objeto_id,
        ).values_list("usuario_id", flat=True)
        crear_notificaciones(
            usuarios.exclude(pk__in=notificados),
            titulo="Carga de Guamúchil guardada",
            mensaje=(
                f"El repartidor guardó la carga del folio Point {folio} de la ruta {ruta.folio} "
                f"del {ruta.fecha_ruta:%d/%m/%Y}. Al recibir el producto, confirma en Point "
                "la recepción de este folio si sigue como Enviado."
            ),
            actor=evento.creado_por,
            objeto_tipo="recepcion_point_guamuchil",
            objeto_id=objeto_id,
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
    evento = RutaCargaSucursalEvento.objects.create(
        ruta=ruta_bloqueada,
        parada_id=parada_id,
        client_event_id=client_event_id,
        payload_hash=hash_actual,
        contexto_version=contexto.version_checklist,
        respuesta=respuesta,
        creado_por=actor,
    )
    _avisar_carga_guamuchil(evento, (point_lineas[pk] for pk in esperadas_por_id if pk in point_lineas))
    return respuesta
