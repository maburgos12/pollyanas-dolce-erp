from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from core.access import can_manage_submodule
from logistica.models import EventoRuta, ParadaEntregaEvidencia, ParadaRuta, PuntoLogistico, RutaEntrega


class EntregaIdempotenciaConflicto(ValidationError):
    """El identificador del cliente ya fue usado con otro payload."""


@dataclass(frozen=True)
class ConfirmacionEntregaResultado:
    parada: ParadaRuta
    evento: EventoRuta
    evidencia: ParadaEntregaEvidencia
    requiere_revision: bool
    idempotente: bool = False


@dataclass(frozen=True)
class RevisionEntregaResultado:
    parada: ParadaRuta
    evento: EventoRuta
    idempotente: bool = False


@dataclass(frozen=True)
class ReplayConfirmacionResultado:
    evidencia: ParadaEntregaEvidencia
    respuesta: dict | None


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _payload_hash(*, entrega_estado, motivo, ubicacion, evidencias) -> str:
    payload = {
        "entrega_estado": entrega_estado,
        "motivo": motivo,
        "ubicacion": ubicacion or {},
        "evidencias": list(evidencias or ()),
    }
    serializado = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serializado.encode("utf-8")).hexdigest()


def _actor_puede_confirmar(*, actor, ruta: RutaEntrega) -> bool:
    if not getattr(actor, "is_authenticated", False):
        return False
    if can_manage_submodule(actor, "logistica", "rutas"):
        return True
    repartidor = getattr(actor, "repartidor_logistica", None)
    return repartidor is not None and ruta.repartidor_id == repartidor.id


def _geocerca_real(*, ruta: RutaEntrega, parada: ParadaRuta) -> EventoRuta | None:
    eventos = (
        EventoRuta.objects.filter(
            ruta=ruta,
            parada=parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            ubicacion__isnull=False,
            latitud__isnull=False,
            longitud__isnull=False,
        )
        .select_related("ubicacion")
        .order_by("-creado_en", "-id")
    )
    for evento in eventos:
        ubicacion = evento.ubicacion
        metadata = evento.metadata or {}
        if metadata.get("origen_servicio") != "registrar_ubicacion_ruta":
            continue
        if metadata.get("ubicacion_confiable") is not True:
            continue
        if metadata.get("ruta_id") != ruta.id or ubicacion.ruta_id != ruta.id:
            continue
        if metadata.get("repartidor_id") != ruta.repartidor_id or ubicacion.repartidor_id != ruta.repartidor_id:
            continue
        if metadata.get("unidad_id") != ruta.unidad_operativa_id or ubicacion.unidad_id != ruta.unidad_operativa_id:
            continue
        if evento.creado_por_id != getattr(ruta.repartidor, "user_id", None):
            continue
        if evento.latitud != ubicacion.latitud or evento.longitud != ubicacion.longitud:
            continue
        if evento.distancia_metros is None or evento.distancia_metros > parada.radio_geocerca_metros:
            continue
        return evento
    return None


def tiene_llegada_geocerca_confiable(*, ruta: RutaEntrega, parada: ParadaRuta) -> bool:
    return _geocerca_real(ruta=ruta, parada=parada) is not None


def obtener_respuesta_idempotente(
    *, ruta, parada, actor, entrega_estado, motivo, client_event_id, evidencias=(), ubicacion=None
):
    if not getattr(actor, "pk", None) or not str(client_event_id or "").strip():
        return None
    payload_hash = _payload_hash(
        entrega_estado=entrega_estado,
        motivo=motivo,
        ubicacion=ubicacion,
        evidencias=evidencias,
    )
    existente = ParadaEntregaEvidencia.objects.filter(
        ruta=ruta,
        capturado_por=actor,
        client_event_id=client_event_id,
    ).first()
    if not existente:
        return None
    if existente.parada_id != parada.id or existente.metadata.get("payload_hash") != payload_hash:
        raise EntregaIdempotenciaConflicto("client_event_id ya fue usado con un payload diferente.")
    return ReplayConfirmacionResultado(
        evidencia=existente,
        respuesta=existente.metadata.get("respuesta_api"),
    )


def guardar_respuesta_idempotente(*, evidencia: ParadaEntregaEvidencia, respuesta):
    metadata = dict(evidencia.metadata or {})
    metadata["respuesta_api"] = _json_safe(respuesta)
    evidencia.metadata = metadata
    evidencia.save(update_fields=["metadata"])


def _validar_confirmacion(*, ruta: RutaEntrega, parada: ParadaRuta, actor, entrega_estado, motivo, client_event_id):
    if parada.ruta_id != ruta.id:
        raise ValidationError("La parada no pertenece a la ruta indicada.")
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        raise ValidationError("Solo se pueden confirmar entregas de una ruta en seguimiento.")
    if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS:
        raise ValidationError("CEDIS usa su operación de recarga y no admite confirmación de entrega.")
    if ruta.paradas.filter(
        punto__tipo=PuntoLogistico.TIPO_CEDIS,
        estado=ParadaRuta.ESTADO_PENDIENTE,
        orden__lt=parada.orden,
    ).exists():
        raise ValidationError("Primero registra la recarga CEDIS del tramo anterior.")
    if not _actor_puede_confirmar(actor=actor, ruta=ruta):
        raise PermissionDenied("No puedes confirmar entregas de esta ruta.")
    if entrega_estado not in {
        ParadaRuta.ENTREGA_ENTREGADA,
        ParadaRuta.ENTREGA_CON_DIFERENCIA,
        ParadaRuta.ENTREGA_NO_ENTREGADA,
    }:
        raise ValidationError("Selecciona un estado final de entrega válido.")
    if not str(motivo or "").strip():
        raise ValidationError("El motivo de la confirmación es obligatorio.")
    if not str(client_event_id or "").strip():
        raise ValidationError("client_event_id es obligatorio.")


@transaction.atomic
def confirmar_entrega_parada(
    *,
    ruta,
    parada,
    actor,
    entrega_estado,
    motivo,
    client_event_id,
    evidencias=(),
    ubicacion=None,
):
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    parada = ParadaRuta.objects.select_for_update().select_related("punto").get(pk=parada.pk)
    payload_hash = _payload_hash(
        entrega_estado=entrega_estado,
        motivo=motivo,
        ubicacion=ubicacion,
        evidencias=evidencias,
    )
    existente = None
    if getattr(actor, "pk", None) and str(client_event_id or "").strip():
        existente = (
            ParadaEntregaEvidencia.objects.select_for_update()
            .filter(ruta=ruta, capturado_por=actor, client_event_id=client_event_id)
            .first()
        )
    if existente:
        if existente.parada_id != parada.id or existente.metadata.get("payload_hash") != payload_hash:
            raise EntregaIdempotenciaConflicto("client_event_id ya fue usado con un payload diferente.")
        evento = EventoRuta.objects.get(pk=existente.metadata["evento_id"])
        return ConfirmacionEntregaResultado(
            parada=parada,
            evento=evento,
            evidencia=existente,
            requiere_revision=parada.revision_entrega_estado != ParadaRuta.REVISION_NO_REQUERIDA,
            idempotente=True,
        )

    _validar_confirmacion(
        ruta=ruta,
        parada=parada,
        actor=actor,
        entrega_estado=entrega_estado,
        motivo=motivo,
        client_event_id=client_event_id,
    )

    if parada.entrega_estado != ParadaRuta.ENTREGA_PENDIENTE:
        raise ValidationError("La parada ya tiene una confirmación de entrega distinta.")

    llegada = _geocerca_real(ruta=ruta, parada=parada)
    requiere_revision = llegada is None
    datos_revision = _json_safe(dict(ubicacion or {}))
    causa = str(datos_revision.get("causa") or "SIN_GEOFENCE_VALIDADA") if requiere_revision else ""
    now = timezone.now()
    parada.entrega_estado = entrega_estado
    parada.entrega_confirmada_en = now
    parada.entrega_confirmada_por = actor
    parada.entrega_notas = str(motivo).strip()
    parada.revision_entrega_estado = (
        ParadaRuta.REVISION_PENDIENTE if requiere_revision else ParadaRuta.REVISION_NO_REQUERIDA
    )
    parada.revision_entrega_causa = causa
    parada.revision_entrega_datos = datos_revision
    parada.save(
        update_fields=[
            "entrega_estado",
            "entrega_confirmada_en",
            "entrega_confirmada_por",
            "entrega_notas",
            "revision_entrega_estado",
            "revision_entrega_causa",
            "revision_entrega_datos",
            "actualizado_en",
        ]
    )

    tipo_evento = EventoRuta.TIPO_ENTREGA_EXCEPCIONAL if requiere_revision else EventoRuta.TIPO_ENTREGA
    evento = EventoRuta.objects.create(
        ruta=ruta,
        parada=parada,
        ubicacion=llegada.ubicacion if llegada else None,
        tipo=tipo_evento,
        severidad=EventoRuta.SEVERIDAD_ALERTA if requiere_revision else EventoRuta.SEVERIDAD_OK,
        descripcion=(
            f"Entrega excepcional registrada: {parada.get_entrega_estado_display()}."
            if requiere_revision
            else f"Entrega confirmada: {parada.get_entrega_estado_display()}."
        ),
        latitud=llegada.latitud if llegada else datos_revision.get("latitud"),
        longitud=llegada.longitud if llegada else datos_revision.get("longitud"),
        distancia_metros=llegada.distancia_metros if llegada else datos_revision.get("distancia_metros"),
        metadata={
            "client_event_id": client_event_id,
            "entrega_estado": entrega_estado,
            "requiere_revision": requiere_revision,
            "causa": causa,
            "origen": "servicio_entregas",
        },
        creado_por=actor,
    )
    evidencias_payload = list(evidencias or ())
    filas_evidencia = evidencias_payload or [{}]
    evidencia = None
    for index, item in enumerate(filas_evidencia):
        item = dict(item)
        evidencia_item = ParadaEntregaEvidencia.objects.create(
            ruta=ruta,
            parada=parada,
            linea_carga_id=item.get("linea_carga_id"),
            tipo=item.get("tipo") or ParadaEntregaEvidencia.TIPO_CONFIRMACION,
            cantidad_entregada=item.get("cantidad_entregada"),
            comentario=item.get("comentario") or str(motivo).strip(),
            latitud=item.get("latitud") or datos_revision.get("latitud"),
            longitud=item.get("longitud") or datos_revision.get("longitud"),
            precision_metros=item.get("precision_metros") or datos_revision.get("precision_metros"),
            client_event_id=(client_event_id if index == 0 else item.get("client_event_id") or ""),
            capturado_por=actor,
            metadata={
                "payload_hash": payload_hash if index == 0 else "",
                "evento_id": evento.id,
                "origen": "servicio_entregas",
            },
        )
        if evidencia is None:
            evidencia = evidencia_item
    return ConfirmacionEntregaResultado(
        parada=parada,
        evento=evento,
        evidencia=evidencia,
        requiere_revision=requiere_revision,
    )


@transaction.atomic
def revisar_entrega_excepcional(*, parada, actor, decision, motivo):
    parada = ParadaRuta.objects.select_for_update().select_related("ruta").get(pk=parada.pk)
    if not can_manage_submodule(actor, "logistica", "rutas"):
        raise PermissionDenied("No tienes permiso para revisar entregas excepcionales.")
    if decision not in {ParadaRuta.REVISION_AUTORIZADA, ParadaRuta.REVISION_RECHAZADA}:
        raise ValidationError("La decisión debe ser AUTORIZADA o RECHAZADA.")
    motivo = str(motivo or "").strip()
    if not motivo:
        raise ValidationError("El motivo de resolución es obligatorio.")
    if parada.revision_entrega_estado == decision:
        evento = EventoRuta.objects.filter(
            parada=parada,
            tipo=(
                EventoRuta.TIPO_ENTREGA_AUTORIZADA
                if decision == ParadaRuta.REVISION_AUTORIZADA
                else EventoRuta.TIPO_ENTREGA_RECHAZADA
            ),
        ).latest("creado_en")
        return RevisionEntregaResultado(parada=parada, evento=evento, idempotente=True)
    if parada.revision_entrega_estado != ParadaRuta.REVISION_PENDIENTE:
        raise ValidationError("La entrega no tiene una revisión excepcional pendiente.")

    parada.revision_entrega_estado = decision
    parada.revision_entrega_revisada_por = actor
    parada.revision_entrega_revisada_en = timezone.now()
    parada.revision_entrega_resolucion = motivo
    parada.save(
        update_fields=[
            "revision_entrega_estado",
            "revision_entrega_revisada_por",
            "revision_entrega_revisada_en",
            "revision_entrega_resolucion",
            "actualizado_en",
        ]
    )
    tipo = (
        EventoRuta.TIPO_ENTREGA_AUTORIZADA
        if decision == ParadaRuta.REVISION_AUTORIZADA
        else EventoRuta.TIPO_ENTREGA_RECHAZADA
    )
    evento = EventoRuta.objects.create(
        ruta=parada.ruta,
        parada=parada,
        tipo=tipo,
        severidad=EventoRuta.SEVERIDAD_OK if decision == ParadaRuta.REVISION_AUTORIZADA else EventoRuta.SEVERIDAD_ALERTA,
        descripcion=f"Entrega excepcional {decision.lower()}: {motivo}",
        metadata={"decision": decision, "motivo": motivo, "origen": "servicio_entregas"},
        creado_por=actor,
    )
    return RevisionEntregaResultado(parada=parada, evento=evento)
