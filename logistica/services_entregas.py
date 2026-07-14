from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from django.core.exceptions import PermissionDenied, ValidationError
from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.access import can_manage_submodule
from core.models import Notificacion
from core.notificaciones import crear_notificaciones
from logistica.domain_ruta import parada_resuelta_operativamente
from logistica.models import EventoRuta, ParadaEntregaEvidencia, ParadaRuta, PuntoLogistico, RutaEntrega
from rrhh.services_identidad import nombre_operativo_usuario


ORIGEN_PWA = "PWA"
ORIGEN_AJUSTE_ADMIN = "AJUSTE_ADMIN"
ORIGEN_SISTEMA = "SISTEMA"
ORIGENES_CONFIRMACION = {ORIGEN_PWA, ORIGEN_AJUSTE_ADMIN, ORIGEN_SISTEMA}
CAUSAS_EXCEPCION = {
    "GPS_SIN_SENAL", "FUERA_DE_RADIO", "AJUSTE_ADMINISTRATIVO",
    "GEOFENCE_LEGACY_NO_CONFIABLE", "PRECISION_INSUFICIENTE", "UBICACION_TARDIA",
    "SALTO_IMOSIBLE", "SALTO_IMPOSIBLE", "SUCURSAL_SIN_COORDENADAS", "GPS_DENEGADO",
    "CLIENTE_LEGACY", "SIN_GEOFENCE_VALIDADA",
}


def _notificar_revision(*, parada, actor, causa):
    destinatarios = [
        user for user in get_user_model().objects.filter(is_active=True)
        if can_manage_submodule(user, "logistica", "rutas")
    ]
    crear_notificaciones(
        destinatarios,
        titulo=f"Entrega por revisar: {parada.ruta.folio}",
        mensaje=f"{parada.punto_nombre_snapshot}: {causa}. Revisa la evidencia y autoriza o rechaza.",
        url="/logistica/rutas/revisiones/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="logistica.ParadaRuta",
        objeto_id=parada.id,
        excluir=actor,
    )


class EntregaIdempotenciaConflicto(ValidationError):
    """El identificador del cliente ya fue usado con otro payload."""


class EntregaEvidenciaIdConflicto(ValidationError):
    """Un identificador secundario de evidencia ya fue utilizado."""


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


@transaction.atomic
def resolver_alerta_historica(*, evento, actor, motivo):
    evento = EventoRuta.objects.select_for_update().get(pk=evento.pk)
    if not can_manage_submodule(actor, "logistica", "rutas"):
        raise PermissionDenied("No tienes permiso para resolver alertas históricas.")
    if evento.tipo != EventoRuta.TIPO_INCONSISTENCIA_ENTREGA:
        raise ValidationError("El evento no es una alerta histórica de entrega.")
    motivo = str(motivo or "").strip()
    if not motivo:
        raise ValidationError("El motivo de resolución es obligatorio.")
    if evento.revision_alerta_estado == EventoRuta.REVISION_ALERTA_RESUELTA:
        return evento
    evento.revision_alerta_estado = EventoRuta.REVISION_ALERTA_RESUELTA
    evento.revision_alerta_motivo = motivo
    evento.revision_alerta_resuelta_por = actor
    evento.revision_alerta_resuelta_en = timezone.now()
    evento.save(update_fields=[
        "revision_alerta_estado", "revision_alerta_motivo",
        "revision_alerta_resuelta_por", "revision_alerta_resuelta_en",
    ])
    return evento


@dataclass(frozen=True)
class ReplayConfirmacionResultado:
    evidencia: ParadaEntregaEvidencia
    respuesta: dict | None


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _datetime_api(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return timezone.localtime(value).isoformat()
    return value.isoformat()


def _snapshot_dominio_parada(*, parada: ParadaRuta, geocerca_confiable: bool) -> dict:
    punto = parada.punto
    return _json_safe(
        {
            "id": parada.id,
            "ruta": parada.ruta_id,
            "punto": {
                "id": punto.id,
                "sucursal": punto.sucursal_id,
                "sucursal_nombre": punto.sucursal.nombre if punto.sucursal_id else "",
                "nombre": punto.nombre,
                "tipo": punto.tipo,
                "tipo_display": punto.get_tipo_display(),
                "latitud": str(punto.latitud) if punto.latitud is not None else None,
                "longitud": str(punto.longitud) if punto.longitud is not None else None,
                "radio_geocerca_metros": punto.radio_geocerca_metros,
                "activo": punto.activo,
                "notas": punto.notas,
            },
            "orden": parada.orden,
            "punto_nombre_snapshot": parada.punto_nombre_snapshot,
            "latitud_geocerca": str(parada.latitud_geocerca) if parada.latitud_geocerca is not None else None,
            "longitud_geocerca": str(parada.longitud_geocerca) if parada.longitud_geocerca is not None else None,
            "radio_geocerca_metros": parada.radio_geocerca_metros,
            "hora_estimada": _datetime_api(parada.hora_estimada),
            "hora_llegada_real": _datetime_api(parada.hora_llegada_real),
            "hora_salida_real": _datetime_api(parada.hora_salida_real),
            "estado": parada.estado,
            "estado_display": parada.get_estado_display(),
            "entrega_estado": parada.entrega_estado,
            "entrega_estado_display": parada.get_entrega_estado_display(),
            "entrega_confirmada_en": _datetime_api(parada.entrega_confirmada_en),
            "entrega_confirmada_por_nombre": (
                nombre_operativo_usuario(parada.entrega_confirmada_por)
                if parada.entrega_confirmada_por_id
                else ""
            ),
            "entrega_notas": parada.entrega_notas,
            "geocerca_confiable": geocerca_confiable,
            "operativamente_resuelta": parada_resuelta_operativamente(parada),
            "revision_entrega_estado": parada.revision_entrega_estado,
            "revision_entrega_causa": parada.revision_entrega_causa,
            "revision_entrega_datos": parada.revision_entrega_datos,
            "revision_entrega_revisada_en": _datetime_api(parada.revision_entrega_revisada_en),
            "revision_entrega_revisada_por": parada.revision_entrega_revisada_por_id,
            "revision_entrega_revisada_por_nombre": (
                nombre_operativo_usuario(parada.revision_entrega_revisada_por)
                if parada.revision_entrega_revisada_por_id
                else ""
            ),
            "revision_entrega_resolucion": parada.revision_entrega_resolucion,
            "distancia_llegada_metros": (
                str(parada.distancia_llegada_metros) if parada.distancia_llegada_metros is not None else None
            ),
            "notas": parada.notas,
        }
    )


def _payload_hash(*, entrega_estado, motivo, ubicacion, evidencias, origen=None) -> str:
    payload = {
        "entrega_estado": entrega_estado,
        "motivo": motivo,
        "ubicacion": ubicacion or {},
        "evidencias": list(evidencias or ()),
        "origen": origen,
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
        .select_related("ubicacion", "ubicacion__repartidor")
        .order_by("-creado_en", "-id")
    )
    for evento in eventos:
        if _evento_geocerca_es_confiable(evento=evento, ruta=ruta, parada=parada):
            return evento
    return None


def _evento_geocerca_es_confiable(*, evento, ruta, parada):
    ubicacion = evento.ubicacion
    metadata = evento.metadata or {}
    return bool(
        metadata.get("origen_servicio") == "registrar_ubicacion_ruta"
        and metadata.get("ubicacion_confiable") is True
        and metadata.get("ruta_id") == ruta.id
        and ubicacion.ruta_id == ruta.id
        and metadata.get("repartidor_id") == ruta.repartidor_id
        and ubicacion.repartidor_id == ruta.repartidor_id
        and metadata.get("unidad_id") == ruta.unidad_operativa_id
        and ubicacion.unidad_id == ruta.unidad_operativa_id
        and evento.creado_por_id == ubicacion.repartidor.user_id
        and evento.latitud == ubicacion.latitud
        and evento.longitud == ubicacion.longitud
        and evento.distancia_metros is not None
        and evento.distancia_metros <= parada.radio_geocerca_metros
    )


def geocercas_confiables_por_parada(paradas) -> set[int]:
    paradas = list(paradas)
    if not paradas:
        return set()
    por_id = {parada.id: parada for parada in paradas}
    eventos = EventoRuta.objects.filter(
        parada_id__in=por_id,
        tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
        ubicacion__isnull=False,
        latitud__isnull=False,
        longitud__isnull=False,
    ).select_related("ubicacion", "ubicacion__repartidor").order_by("-creado_en", "-id")
    confiables = set()
    for evento in eventos:
        parada = por_id[evento.parada_id]
        if evento.parada_id not in confiables and _evento_geocerca_es_confiable(
            evento=evento, ruta=parada.ruta, parada=parada
        ):
            confiables.add(evento.parada_id)
    return confiables


def tiene_llegada_geocerca_confiable(*, ruta: RutaEntrega, parada: ParadaRuta) -> bool:
    return _geocerca_real(ruta=ruta, parada=parada) is not None


def obtener_respuesta_idempotente(
    *, ruta, parada, actor, entrega_estado, motivo, client_event_id, evidencias=(), ubicacion=None, origen=None
):
    if not getattr(actor, "pk", None) or not str(client_event_id or "").strip():
        return None
    payload_hash = _payload_hash(
        entrega_estado=entrega_estado,
        motivo=motivo,
        ubicacion=ubicacion,
        evidencias=evidencias,
        origen=origen,
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
    origen=None,
):
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    parada = ParadaRuta.objects.select_for_update().select_related("punto").get(pk=parada.pk)
    payload_hash = _payload_hash(
        entrega_estado=entrega_estado,
        motivo=motivo,
        ubicacion=ubicacion,
        evidencias=evidencias,
        origen=origen,
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
    if origen not in ORIGENES_CONFIRMACION:
        raise ValidationError("El origen estructurado de la confirmación es obligatorio.")

    if parada.entrega_estado != ParadaRuta.ENTREGA_PENDIENTE:
        raise ValidationError("La parada ya tiene una confirmación de entrega distinta.")

    llegada = _geocerca_real(ruta=ruta, parada=parada)
    datos_revision = _json_safe(dict(ubicacion or {}))
    requiere_revision = (
        llegada is None
        or origen == ORIGEN_AJUSTE_ADMIN
        or datos_revision.get("causa") == "CLIENTE_LEGACY"
    )
    if requiere_revision:
        causa = str(datos_revision.get("causa") or "SIN_GEOFENCE_VALIDADA")
        if causa not in CAUSAS_EXCEPCION:
            raise ValidationError("La causa excepcional no pertenece al catálogo permitido.")
        requeridos = {"causa", "client_timestamp", "client_version"}
        if not requeridos.issubset(datos_revision):
            raise ValidationError("La excepción requiere causa, client_timestamp y client_version.")
    else:
        causa = ""
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
            "origen_confirmacion": origen,
        },
        creado_por=actor,
    )
    evidencias_payload = list(evidencias or ())
    filas_evidencia = evidencias_payload or [{}]
    evidencia = None
    evidencias_creadas = []
    for index, item in enumerate(filas_evidencia):
        item = dict(item)
        evidencia_client_id = client_event_id if index == 0 else item.get("client_event_id") or ""
        if index > 0 and evidencia_client_id and ParadaEntregaEvidencia.objects.filter(
            ruta=ruta, capturado_por=actor, client_event_id=evidencia_client_id
        ).exists():
            raise EntregaEvidenciaIdConflicto("client_event_id secundario ya fue usado por otra evidencia.")
        try:
            with transaction.atomic():
                evidencia_item = ParadaEntregaEvidencia.objects.create(
                    ruta=ruta, parada=parada, linea_carga_id=item.get("linea_carga_id"),
                    tipo=item.get("tipo") or ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                    cantidad_entregada=item.get("cantidad_entregada"),
                    comentario=item.get("comentario") or str(motivo).strip(),
                    latitud=item.get("latitud") or datos_revision.get("latitud"),
                    longitud=item.get("longitud") or datos_revision.get("longitud"),
                    precision_metros=item.get("precision_metros") or datos_revision.get("precision_metros"),
                    client_event_id=evidencia_client_id, capturado_por=actor,
                    metadata={"payload_hash": payload_hash if index == 0 else "", "evento_id": evento.id, "origen": "servicio_entregas"},
                )
        except IntegrityError as exc:
            constraint = getattr(getattr(exc, "__cause__", None), "diag", None)
            if getattr(constraint, "constraint_name", None) == "paradaevidencia_evento_cliente_unico":
                raise EntregaEvidenciaIdConflicto("client_event_id ya fue usado por otra evidencia.") from exc
            raise
        if evidencia is None:
            evidencia = evidencia_item
        evidencias_creadas.append(evidencia_item)
    metadata_evidencia = dict(evidencia.metadata or {})
    metadata_evidencia["snapshot_dominio"] = _snapshot_dominio_parada(
        parada=parada,
        geocerca_confiable=not requiere_revision,
    )
    metadata_evidencia["evidencia_ids"] = [fila.id for fila in evidencias_creadas]
    evidencia.metadata = metadata_evidencia
    evidencia.save(update_fields=["metadata"])
    if requiere_revision:
        _notificar_revision(parada=parada, actor=actor, causa=causa)
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
    if decision not in {ParadaRuta.REVISION_AUTORIZADA, ParadaRuta.REVISION_RECHAZADA, ParadaRuta.REVISION_CORREGIDA}:
        raise ValidationError("La decisión debe ser AUTORIZADA, RECHAZADA o CORREGIDA.")
    motivo = str(motivo or "").strip()
    if not motivo:
        raise ValidationError("El motivo de resolución es obligatorio.")
    if parada.revision_entrega_estado == decision:
        tipos_por_decision = {
            ParadaRuta.REVISION_AUTORIZADA: EventoRuta.TIPO_ENTREGA_AUTORIZADA,
            ParadaRuta.REVISION_RECHAZADA: EventoRuta.TIPO_ENTREGA_RECHAZADA,
            ParadaRuta.REVISION_CORREGIDA: EventoRuta.TIPO_ENTREGA_CORREGIDA,
        }
        evento = EventoRuta.objects.filter(
            parada=parada,
            tipo=tipos_por_decision[decision],
        ).latest("creado_en")
        return RevisionEntregaResultado(parada=parada, evento=evento, idempotente=True)
    estado_origen_valido = (
        parada.revision_entrega_estado == ParadaRuta.REVISION_PENDIENTE
        or (parada.revision_entrega_estado == ParadaRuta.REVISION_RECHAZADA and decision == ParadaRuta.REVISION_CORREGIDA)
    )
    if not estado_origen_valido:
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
    tipos_por_decision = {
        ParadaRuta.REVISION_AUTORIZADA: EventoRuta.TIPO_ENTREGA_AUTORIZADA,
        ParadaRuta.REVISION_RECHAZADA: EventoRuta.TIPO_ENTREGA_RECHAZADA,
        ParadaRuta.REVISION_CORREGIDA: EventoRuta.TIPO_ENTREGA_CORREGIDA,
    }
    tipo = tipos_por_decision[decision]
    evento = EventoRuta.objects.create(
        ruta=parada.ruta,
        parada=parada,
        tipo=tipo,
        severidad=(
            EventoRuta.SEVERIDAD_OK
            if decision in {ParadaRuta.REVISION_AUTORIZADA, ParadaRuta.REVISION_CORREGIDA}
            else EventoRuta.SEVERIDAD_ALERTA
        ),
        descripcion=f"Entrega excepcional {decision.lower()}: {motivo}",
        metadata={"decision": decision, "motivo": motivo, "origen": "servicio_entregas"},
        creado_por=actor,
    )
    if decision == ParadaRuta.REVISION_CORREGIDA:
        ParadaEntregaEvidencia.objects.create(
            ruta=parada.ruta,
            parada=parada,
            tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
            comentario=motivo,
            capturado_por=actor,
            client_event_id=f"revision-corregida-{evento.id}",
            metadata={
                "origen": "revision_jefe",
                "decision": decision,
                "evento_correccion_id": evento.id,
                "preserva_evidencia_original": True,
            },
        )
    return RevisionEntregaResultado(parada=parada, evento=evento)
