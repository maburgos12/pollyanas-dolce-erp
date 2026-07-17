from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Case, F, IntegerField, Q, When
from django.utils import timezone

from .domain_ruta import parada_resuelta_operativamente
from .models import (
    BitacoraSalidaLlegada,
    EventoRuta,
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaEntrega,
    UbicacionRuta,
)

logger = logging.getLogger(__name__)

GEOCERCA_PERMANENCIA_VISITA_MINUTOS = 5
RUTA_NOCTURNA_HORA_CORTE = 22
RECARGA_CEDIS_LEASE_ENCOLADA = timedelta(minutes=2)
RECARGA_CEDIS_LEASE_PROCESO = timedelta(minutes=10)
RECARGA_CEDIS_BACKOFF = timedelta(minutes=5)
# EventoRuta técnico con metadata durable: estado, lease_hasta,
# proximo_intento_en e intento. No pertenece a las alertas administrativas.
RECARGA_CEDIS_LEASE_TIPO = "recarga_cedis_lease_interno"


class LiberacionRutaError(ValidationError):
    error_code = "ruta_no_liberada"
    http_status = 400

    def __init__(self, message, *, error_code=None):
        super().__init__(message)
        if error_code:
            self.error_code = error_code


class LiberacionRutaConflicto(LiberacionRutaError):
    error_code = "ruta_conflicto"
    http_status = 409


@dataclass(frozen=True)
class GeocercaResultado:
    parada: ParadaRuta | None
    distancia_metros: int | None
    dentro: bool
    dentro_geocerca_planeada: bool
    parada_planeada_mas_cercana: ParadaRuta | None
    distancia_planeada_metros: int | None


def repartidor_es_chofer_de_ruta(*, ruta: RutaEntrega, repartidor: Repartidor | None) -> bool:
    """Autoriza la operación PWA únicamente al chofer canónico de la ruta."""
    if repartidor is None:
        return False
    return repartidor.id == ruta.repartidor_id


def repartidor_participa_en_ruta(*, ruta: RutaEntrega, repartidor: Repartidor | None) -> bool:
    """Autoriza al titular y al acompañante explícitamente asignado."""
    if repartidor is None:
        return False
    return repartidor.id in {ruta.repartidor_id, ruta.acompanante_id}


def _rutas_operativas_candidatas(repartidor: Repartidor, *, hoy=None):
    hoy = hoy or timezone.localdate()
    ayer = hoy - timedelta(days=1)
    corte = timezone.make_aware(datetime.combine(ayer, time(hour=RUTA_NOCTURNA_HORA_CORTE)))
    return (
        RutaEntrega.objects.select_related(
            "unidad_operativa",
            "repartidor__user",
            "acompanante__user",
            "bitacora_salida",
        )
        .filter(
            Q(repartidor=repartidor) | Q(acompanante=repartidor),
            estatus__in=[RutaEntrega.ESTATUS_EN_RUTA, RutaEntrega.ESTATUS_PLANEADA],
        )
        .filter(Q(fecha_ruta=hoy) | Q(fecha_ruta=ayer, created_at__gte=corte))
        .annotate(
            _estatus_operativo_prioridad=Case(
                When(estatus=RutaEntrega.ESTATUS_EN_RUTA, then=0),
                default=1,
                output_field=IntegerField(),
            ),
            _fecha_operativa_prioridad=Case(
                When(fecha_ruta=hoy, then=0),
                default=1,
                output_field=IntegerField(),
            ),
        )
        .order_by("_estatus_operativo_prioridad", "_fecha_operativa_prioridad", "-id")
    )


def ruta_operativa_para_repartidor(repartidor: Repartidor, *, hoy=None) -> RutaEntrega | None:
    return _rutas_operativas_candidatas(repartidor, hoy=hoy).first()


def ruta_es_operativa_hoy(ruta: RutaEntrega, *, hoy=None) -> bool:
    if not ruta.repartidor_id:
        return False
    seleccionada = ruta_operativa_para_repartidor(ruta.repartidor, hoy=hoy)
    return seleccionada is not None and seleccionada.id == ruta.id


@transaction.atomic
def liberar_ruta_con_turno(
    *,
    ruta: RutaEntrega,
    actor,
    bitacora: BitacoraSalidaLlegada | None = None,
) -> RutaEntrega:
    """Libera una ruta bajo un único contrato de turno, unidad y checklist."""
    from .services_carga_ruta import checklist_bloquea_salida

    ruta = (
        RutaEntrega.objects.select_for_update(of=("self",))
        .select_related("repartidor", "acompanante", "unidad_operativa", "bitacora_salida")
        .get(pk=ruta.pk)
    )
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise LiberacionRutaError("La ruta ya está cerrada o cancelada y no puede liberarse.")
    if not ruta.repartidor_id:
        raise LiberacionRutaError("No se puede liberar la ruta: asigna repartidor.")
    if not ruta.unidad_operativa_id:
        raise LiberacionRutaError("No se puede liberar la ruta: asigna unidad operativa.")

    try:
        actor_repartidor = actor.repartidor_logistica
    except (AttributeError, Repartidor.DoesNotExist):
        actor_repartidor = None
    operador_turno = (
        actor_repartidor
        if repartidor_participa_en_ruta(ruta=ruta, repartidor=actor_repartidor)
        else ruta.repartidor
    )

    rutas_activas_ajenas = (
        RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA)
        .exclude(pk=ruta.pk)
    )
    if rutas_activas_ajenas.filter(repartidor_id=ruta.repartidor_id).exists():
        raise LiberacionRutaConflicto(
            "No se puede liberar la ruta: el repartidor ya tiene otra ruta en curso (otra ruta activa)."
        )
    if rutas_activas_ajenas.filter(unidad_operativa_id=ruta.unidad_operativa_id).exists():
        raise LiberacionRutaConflicto(
            "No se puede liberar la ruta: la unidad ya tiene otra ruta en curso (otra ruta activa)."
        )

    bitacora_solicitada_id = bitacora.pk if bitacora is not None else None
    if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and ruta.bitacora_salida_id:
        if bitacora_solicitada_id and bitacora_solicitada_id != ruta.bitacora_salida_id:
            raise LiberacionRutaError(
                "La ruta ya fue liberada con otro turno; conserva la bitácora original.",
                error_code="turno_ruta_distinto",
            )
        bitacora = (
            BitacoraSalidaLlegada.objects.select_for_update()
            .select_related("repartidor", "unidad")
            .filter(pk=ruta.bitacora_salida_id, cerrada=False)
            .first()
        )
    else:
        turnos_abiertos = list(
            BitacoraSalidaLlegada.objects.select_for_update(of=("self",))
            .select_related("repartidor", "unidad")
            .filter(repartidor_id=operador_turno.id, cerrada=False)
            .order_by("-hora_salida", "-id")
            [:2]
        )
        if len(turnos_abiertos) > 1:
            raise LiberacionRutaError(
                "El repartidor tiene más de un turno abierto; cierra el turno incorrecto antes de liberar la ruta.",
                error_code="turno_ambiguo",
            )
        bitacora_explicita = None
        if bitacora_solicitada_id:
            bitacora_explicita = (
                BitacoraSalidaLlegada.objects.select_for_update(of=("self",))
                .filter(pk=bitacora_solicitada_id, cerrada=False)
                .first()
            )
            if bitacora_explicita and bitacora_explicita.repartidor_id != operador_turno.id:
                raise LiberacionRutaError(
                    "El turno activo pertenece a otro repartidor.",
                    error_code="repartidor_ruta_distinto",
                )
        bitacora = turnos_abiertos[0] if turnos_abiertos else None
        if bitacora_solicitada_id and bitacora is not None and bitacora_solicitada_id != bitacora.id:
            raise LiberacionRutaError(
                "La bitácora indicada no corresponde al único turno abierto del repartidor.",
                error_code="turno_ruta_distinto",
            )
    if bitacora is None:
        raise LiberacionRutaError(
            "El repartidor no tiene un turno activo.",
            error_code="sin_turno",
        )
    if bitacora.repartidor_id != operador_turno.id:
        raise LiberacionRutaError(
            "El turno activo pertenece a otro repartidor.",
            error_code="repartidor_ruta_distinto",
        )
    if bitacora.unidad_id != ruta.unidad_operativa_id:
        raise LiberacionRutaError(
            "El turno activo no corresponde a la unidad asignada a la ruta.",
            error_code="unidad_ruta_distinta",
        )
    if not ruta.paradas.exists():
        raise LiberacionRutaError("No se puede liberar la ruta: agrega al menos una parada.")

    blocker = checklist_bloquea_salida(ruta)
    if blocker:
        raise LiberacionRutaError(blocker)

    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA or ruta.bitacora_salida_id != bitacora.id:
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.bitacora_salida = bitacora
        ruta.hora_inicio_real = ruta.hora_inicio_real or bitacora.hora_salida or timezone.now()
        try:
            with transaction.atomic():
                ruta.save(
                    update_fields=[
                        "estatus",
                        "bitacora_salida",
                        "hora_inicio_real",
                        "updated_at",
                    ]
                )
        except IntegrityError as exc:
            raise LiberacionRutaConflicto(
                "No se puede liberar la ruta: el repartidor o la unidad ya tiene otra ruta en curso (otra ruta activa)."
            ) from exc

    evento_salida = (
        EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA)
        .order_by("id")
        .first()
    )
    if evento_salida is None:
        EventoRuta.objects.create(
            ruta=ruta,
            tipo=EventoRuta.TIPO_SALIDA,
            severidad=EventoRuta.SEVERIDAD_INFO,
            descripcion="Ruta liberada con turno activo validado.",
            creado_por=actor if getattr(actor, "is_authenticated", False) else None,
            metadata={"bitacora_salida_id": bitacora.id},
        )
    return ruta


def _decimal(value, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError({field_name: "Valor geográfico inválido."}) from exc


def validar_coordenadas(latitud, longitud) -> tuple[Decimal, Decimal]:
    latitud_dec = _decimal(latitud, "latitud")
    longitud_dec = _decimal(longitud, "longitud")
    errors = {}
    if not Decimal("-90") <= latitud_dec <= Decimal("90"):
        errors["latitud"] = "La latitud debe estar entre -90 y 90."
    if not Decimal("-180") <= longitud_dec <= Decimal("180"):
        errors["longitud"] = "La longitud debe estar entre -180 y 180."
    if latitud_dec == Decimal("0") and longitud_dec == Decimal("0"):
        errors["latitud"] = "Las coordenadas 0,0 no son válidas para seguimiento."
    if errors:
        raise ValidationError(errors)
    return latitud_dec, longitud_dec


def distancia_metros(lat1, lon1, lat2, lon2) -> int:
    lat1_dec, lon1_dec = validar_coordenadas(lat1, lon1)
    lat2_dec, lon2_dec = validar_coordenadas(lat2, lon2)

    radius = 6371000
    phi1 = math.radians(float(lat1_dec))
    phi2 = math.radians(float(lat2_dec))
    delta_phi = math.radians(float(lat2_dec - lat1_dec))
    delta_lambda = math.radians(float(lon2_dec - lon1_dec))
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return int(round(radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))))


def evaluar_geocercas(ruta: RutaEntrega, latitud, longitud) -> GeocercaResultado:
    elegible_mas_cercana: ParadaRuta | None = None
    distancia_elegible: int | None = None
    planeada_mas_cercana: ParadaRuta | None = None
    distancia_planeada: int | None = None
    dentro_geocerca_planeada = False
    for parada in ruta.paradas.select_related("punto").all():
        distance = distancia_metros(latitud, longitud, parada.latitud_geocerca, parada.longitud_geocerca)
        if distancia_planeada is None or distance < distancia_planeada:
            planeada_mas_cercana = parada
            distancia_planeada = distance
        if distance <= parada.radio_geocerca_metros:
            dentro_geocerca_planeada = True
        if parada_resuelta_operativamente(parada):
            continue
        if distancia_elegible is None or distance < distancia_elegible:
            elegible_mas_cercana = parada
            distancia_elegible = distance

    return GeocercaResultado(
        parada=elegible_mas_cercana,
        distancia_metros=distancia_elegible,
        dentro=bool(
            elegible_mas_cercana is not None
            and distancia_elegible is not None
            and distancia_elegible <= elegible_mas_cercana.radio_geocerca_metros
        ),
        dentro_geocerca_planeada=dentro_geocerca_planeada,
        parada_planeada_mas_cercana=planeada_mas_cercana,
        distancia_planeada_metros=distancia_planeada,
    )


def _repartidor_usuario(user) -> Repartidor:
    try:
        return user.repartidor_logistica
    except Repartidor.DoesNotExist as exc:
        raise PermissionDenied("No tienes perfil de repartidor registrado.") from exc


def _bitacora_abierta(repartidor: Repartidor, ruta: RutaEntrega) -> BitacoraSalidaLlegada:
    if ruta.bitacora_salida_id:
        bitacora = ruta.bitacora_salida
        if bitacora.repartidor_id != repartidor.id:
            raise PermissionDenied("La bitácora de salida no pertenece al repartidor de la ruta.")
        if bitacora.cerrada:
            raise ValidationError("La bitácora asignada a la ruta ya está cerrada.")
        if ruta.unidad_operativa_id and bitacora.unidad_id != ruta.unidad_operativa_id:
            raise ValidationError("La unidad de la bitácora no coincide con la unidad asignada a la ruta.")
        return bitacora

    bitacora = (
        BitacoraSalidaLlegada.objects.select_related("unidad", "repartidor")
        .filter(repartidor=repartidor, cerrada=False)
        .order_by("-hora_salida", "-id")
        .first()
    )
    if not bitacora:
        raise ValidationError("Necesitas un turno abierto antes de registrar seguimiento de ruta.")
    if ruta.unidad_operativa_id and bitacora.unidad_id != ruta.unidad_operativa_id:
        raise ValidationError("Tu turno abierto pertenece a otra unidad.")
    return bitacora


def _payload_value(payload: dict, key: str):
    return payload[key] if key in payload else None


def crear_evento_ruta_once(
    *,
    ruta: RutaEntrega,
    tipo: str,
    descripcion: str,
    severidad: str = EventoRuta.SEVERIDAD_INFO,
    user=None,
    parada: ParadaRuta | None = None,
    ubicacion: UbicacionRuta | None = None,
    latitud=None,
    longitud=None,
    distancia_metros_value: int | None = None,
    metadata: dict | None = None,
    ventana_minutos: int = 15,
) -> EventoRuta | None:
    if ventana_minutos > 0:
        since = timezone.now() - timezone.timedelta(minutes=ventana_minutos)
        duplicate = EventoRuta.objects.filter(ruta=ruta, tipo=tipo, creado_en__gte=since)
        if parada:
            duplicate = duplicate.filter(parada=parada)
        if duplicate.exists():
            return None
    return EventoRuta.objects.create(
        ruta=ruta,
        tipo=tipo,
        severidad=severidad,
        descripcion=descripcion,
        parada=parada,
        ubicacion=ubicacion,
        latitud=latitud,
        longitud=longitud,
        distancia_metros=distancia_metros_value,
        metadata=metadata or {},
        creado_por=user if getattr(user, "is_authenticated", False) else None,
    )


def _marcar_visitada_por_permanencia(
    *,
    ruta: RutaEntrega,
    parada: ParadaRuta,
    ubicacion_actual: UbicacionRuta,
    distancia_metros_value: int | None,
) -> bool:
    primera_pendiente = next(
        (
            candidata
            for candidata in ruta.paradas.select_related("punto").order_by("orden", "id")
            if not parada_resuelta_operativamente(candidata)
        ),
        None,
    )
    if not primera_pendiente or primera_pendiente.id != parada.id:
        return False
    primera_llegada = (
        EventoRuta.objects.filter(
            ruta=ruta,
            parada=parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            metadata__origen_servicio="registrar_ubicacion_ruta",
            metadata__ubicacion_confiable=True,
            metadata__ruta_id=ruta.id,
            metadata__repartidor_id=ruta.repartidor_id,
            metadata__unidad_id=ruta.unidad_operativa_id,
            ubicacion__ruta_id=ruta.id,
            ubicacion__repartidor_id=ruta.repartidor_id,
            ubicacion__unidad_id=ruta.unidad_operativa_id,
            latitud=F("ubicacion__latitud"),
            longitud=F("ubicacion__longitud"),
        )
        .exclude(ubicacion_id=ubicacion_actual.id)
        .order_by("creado_en")
        .first()
    )
    if not primera_llegada:
        return False
    if primera_llegada.creado_en > timezone.now() - timezone.timedelta(minutes=GEOCERCA_PERMANENCIA_VISITA_MINUTOS):
        return False
    actualizado_en = timezone.now()
    filas_actualizadas = (
        ParadaRuta.objects.filter(pk=parada.pk, estado=parada.estado)
        .exclude(estado=ParadaRuta.ESTADO_VISITADA)
        .update(
            estado=ParadaRuta.ESTADO_VISITADA,
            hora_llegada_real=primera_llegada.creado_en,
            distancia_llegada_metros=distancia_metros_value,
            actualizado_en=actualizado_en,
        )
    )
    if filas_actualizadas != 1:
        return False
    parada.estado = ParadaRuta.ESTADO_VISITADA
    parada.hora_llegada_real = primera_llegada.creado_en
    parada.distancia_llegada_metros = distancia_metros_value
    parada.actualizado_en = actualizado_en
    ruta.recompute_route_control()
    ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])
    return True


def _clave_lease_recarga_cedis(*, ruta_id: int, parada_id: int) -> str:
    return f"recarga-auto-lease:{ruta_id}:{parada_id}"


def _datetime_lease(value):
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _obtener_lease_recarga_bloqueado(*, ruta_id: int, parada_id: int, actor=None) -> EventoRuta:
    clave = _clave_lease_recarga_cedis(ruta_id=ruta_id, parada_id=parada_id)
    defaults = {
        "ruta_id": ruta_id,
        "parada_id": parada_id,
        "tipo": EventoRuta.TIPO_INCIDENCIA_MANUAL,
        "severidad": EventoRuta.SEVERIDAD_INFO,
        "descripcion": "Control interno de ejecución automática de recarga CEDIS.",
        "metadata": {
            "tipo": RECARGA_CEDIS_LEASE_TIPO,
            "estado": "NUEVA",
            "lease_hasta": None,
            "proximo_intento_en": None,
            "intento": 0,
        },
        "revision_alerta_estado": EventoRuta.REVISION_ALERTA_RESUELTA,
        "revision_alerta_motivo": "Registro técnico interno; no requiere revisión administrativa.",
        "creado_por": actor if getattr(actor, "is_authenticated", False) else None,
    }
    lease, creado = EventoRuta.objects.get_or_create(clave_auditoria=clave, defaults=defaults)
    if not creado:
        lease = EventoRuta.objects.select_for_update().get(pk=lease.pk)
    return lease


def _guardar_metadata_lease(lease: EventoRuta, metadata: dict) -> None:
    lease.metadata = metadata
    lease.save(update_fields=["metadata"])


@transaction.atomic
def _reclamar_lease_recarga_para_encolar(*, ruta: RutaEntrega, parada: ParadaRuta, user) -> str | None:
    from .services_carga_ruta import _evento_recarga_existente

    if _evento_recarga_existente(ruta_id=ruta.id, parada_id=parada.id):
        return None

    lease = _obtener_lease_recarga_bloqueado(
        ruta_id=ruta.id,
        parada_id=parada.id,
        actor=user,
    )
    now = timezone.now()
    metadata = dict(lease.metadata or {})
    estado = metadata.get("estado")
    lease_hasta = _datetime_lease(metadata.get("lease_hasta"))
    proximo_intento = _datetime_lease(metadata.get("proximo_intento_en"))
    if estado in {"ENCOLADA", "EN_PROCESO"} and lease_hasta and lease_hasta > now:
        return None
    if estado == "BACKOFF" and proximo_intento and proximo_intento > now:
        return None
    if estado == "SUPERADA":
        return None

    intento = int(metadata.get("intento") or 0) + 1
    lease_token = str(uuid4())
    metadata.update(
        {
            "tipo": RECARGA_CEDIS_LEASE_TIPO,
            "estado": "ENCOLADA",
            "lease_hasta": (now + RECARGA_CEDIS_LEASE_ENCOLADA).isoformat(),
            "proximo_intento_en": None,
            "intento": intento,
            "generacion": intento,
            "lease_token": lease_token,
            "user_id": getattr(user, "id", None),
            "actualizado_en": now.isoformat(),
        }
    )
    _guardar_metadata_lease(lease, metadata)
    return lease_token


@transaction.atomic
def _actualizar_lease_recarga(
    *,
    ruta_id: int,
    parada_id: int,
    estado: str,
    estado_sync: str | None = None,
    lease_duracion: timedelta | None = None,
    proximo_intento_duracion: timedelta | None = None,
    error: Exception | None = None,
    lease_token: str,
) -> bool:
    lease = _obtener_lease_recarga_bloqueado(ruta_id=ruta_id, parada_id=parada_id)
    now = timezone.now()
    metadata = dict(lease.metadata or {})
    if not lease_token or metadata.get("lease_token") != lease_token:
        return False
    metadata.update(
        {
            "tipo": RECARGA_CEDIS_LEASE_TIPO,
            "estado": estado,
            "estado_sync": estado_sync,
            "lease_hasta": (now + lease_duracion).isoformat() if lease_duracion else None,
            "proximo_intento_en": (
                (now + proximo_intento_duracion).isoformat()
                if proximo_intento_duracion is not None
                else None
            ),
            "actualizado_en": now.isoformat(),
        }
    )
    if error is not None:
        metadata["ultimo_error"] = type(error).__name__
    _guardar_metadata_lease(lease, metadata)
    return True


def _lease_token_recarga_es_actual(*, ruta_id: int, parada_id: int, lease_token: str) -> bool:
    if not lease_token:
        return False
    return EventoRuta.objects.filter(
        clave_auditoria=_clave_lease_recarga_cedis(ruta_id=ruta_id, parada_id=parada_id),
        metadata__lease_token=lease_token,
    ).exists()


@transaction.atomic
def _reclamar_lease_recarga_para_procesar(
    *, ruta_id: int, parada_id: int, lease_token: str, user_id: int | None = None
) -> tuple[bool, str]:
    from .services_carga_ruta import _evento_recarga_existente

    lease = _obtener_lease_recarga_bloqueado(ruta_id=ruta_id, parada_id=parada_id)
    metadata = dict(lease.metadata or {})
    now = timezone.now()
    if not lease_token or metadata.get("lease_token") != lease_token:
        return False, "OBSOLETA"
    if _evento_recarga_existente(ruta_id=ruta_id, parada_id=parada_id):
        metadata.update(
            {
                "tipo": RECARGA_CEDIS_LEASE_TIPO,
                "estado": "SUPERADA",
                "lease_hasta": None,
                "proximo_intento_en": None,
                "actualizado_en": now.isoformat(),
            }
        )
        _guardar_metadata_lease(lease, metadata)
        return False, "SUPERADA"

    estado = metadata.get("estado")
    lease_hasta = _datetime_lease(metadata.get("lease_hasta"))
    proximo_intento = _datetime_lease(metadata.get("proximo_intento_en"))
    if estado == "EN_PROCESO" and lease_hasta and lease_hasta > now:
        return False, "EN_PROCESO"
    if estado == "BACKOFF" and proximo_intento and proximo_intento > now:
        return False, "BACKOFF"
    if estado == "SUPERADA":
        return False, "SUPERADA"

    metadata.update(
        {
            "tipo": RECARGA_CEDIS_LEASE_TIPO,
            "estado": "EN_PROCESO",
            "lease_hasta": (now + RECARGA_CEDIS_LEASE_PROCESO).isoformat(),
            "proximo_intento_en": None,
            "intento": max(1, int(metadata.get("intento") or 0)),
            "user_id": user_id,
            "actualizado_en": now.isoformat(),
        }
    )
    _guardar_metadata_lease(lease, metadata)
    return True, "EN_PROCESO"


def _agendar_recarga_cedis_si_pendiente(*, ruta: RutaEntrega, parada: ParadaRuta, user) -> bool:
    if (
        parada.punto.tipo != PuntoLogistico.TIPO_CEDIS
        or parada.orden == 1
        or parada.estado != ParadaRuta.ESTADO_VISITADA
    ):
        return False
    lease_token = _reclamar_lease_recarga_para_encolar(ruta=ruta, parada=parada, user=user)
    if not lease_token:
        return False

    ruta_id = ruta.id
    parada_id = parada.id
    user_id = getattr(user, "id", None)

    def encolar_recarga():
        from .tasks import procesar_recarga_cedis_automatica

        try:
            procesar_recarga_cedis_automatica.delay(
                ruta_id=ruta_id,
                parada_id=parada_id,
                user_id=user_id,
                lease_token=lease_token,
            )
        except Exception as exc:
            try:
                _actualizar_lease_recarga(
                    ruta_id=ruta_id,
                    parada_id=parada_id,
                    estado="FALLA_ENCOLADO",
                    proximo_intento_duracion=timedelta(0),
                    error=exc,
                    lease_token=lease_token,
                )
            except Exception:
                logger.exception(
                    "No se pudo liberar el lease de recarga CEDIS para ruta=%s parada=%s.",
                    ruta_id,
                    parada_id,
                )
            logger.exception(
                "No se pudo encolar la recarga CEDIS automática para ruta=%s parada=%s; "
                "se reintentará con la siguiente ubicación confiable.",
                ruta_id,
                parada_id,
            )

    transaction.on_commit(encolar_recarga)
    return True


def _timestamp_dispositivo_confiable(timestamp_dispositivo) -> tuple[bool, str]:
    if not timestamp_dispositivo:
        return True, ""
    now = timezone.now()
    age_seconds = (now - timestamp_dispositivo).total_seconds()
    if age_seconds > 5 * 60:
        return False, f"Ubicación capturada hace {int(age_seconds // 60)} minutos."
    if age_seconds < -2 * 60:
        return False, "El reloj del dispositivo viene adelantado respecto al servidor."
    return True, ""


def _precision_confiable(precision_metros) -> tuple[bool, str]:
    if precision_metros is None:
        return True, ""
    if Decimal(str(precision_metros)) > Decimal("100"):
        return False, f"Precisión GPS baja: {precision_metros} m."
    return True, ""


def _salto_fisico_confiable(ruta: RutaEntrega, latitud, longitud, timestamp_dispositivo) -> tuple[bool, str, int | None]:
    previous = ruta.ubicaciones.order_by("-timestamp_servidor", "-id").first()
    if not previous:
        return True, "", None
    distance = distancia_metros(previous.latitud, previous.longitud, latitud, longitud)
    previous_time = previous.timestamp_dispositivo or previous.timestamp_servidor
    current_time = timestamp_dispositivo or timezone.now()
    delta_seconds = (current_time - previous_time).total_seconds()
    if delta_seconds <= 0 and distance > 80:
        return False, "La ubicación llegó fuera de secuencia respecto a la señal anterior.", distance
    if delta_seconds <= 0:
        return True, "", distance
    speed_kmh = (distance / 1000) / (delta_seconds / 3600)
    if distance > 500 and speed_kmh > 120:
        return False, f"Salto GPS improbable: {distance} m en {int(delta_seconds)} s.", distance
    return True, "", distance


@transaction.atomic
def registrar_ubicacion_ruta(*, user, ruta: RutaEntrega, payload: dict, ip_registro: str | None = None) -> UbicacionRuta:
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        raise ValidationError("La ruta debe estar en estatus En ruta para registrar seguimiento.")
    if not ruta_es_operativa_hoy(ruta):
        raise ValidationError("La ruta activa no corresponde al día operativo actual.")
    if not ruta.repartidor_id:
        raise ValidationError("La ruta debe tener un repartidor asignado antes de aceptar seguimiento.")
    if not ruta.unidad_operativa_id:
        raise ValidationError("La ruta debe tener una unidad asignada antes de aceptar seguimiento.")

    repartidor = _repartidor_usuario(user)
    if not repartidor_participa_en_ruta(ruta=ruta, repartidor=repartidor):
        raise PermissionDenied("Esta ruta está asignada a otro repartidor.")

    bitacora = _bitacora_abierta(repartidor, ruta)
    unidad = ruta.unidad_operativa

    if not ruta.bitacora_salida_id or not ruta.hora_inicio_real:
        ruta.bitacora_salida = bitacora
        ruta.hora_inicio_real = ruta.hora_inicio_real or timezone.now()
        ruta.save(update_fields=["bitacora_salida", "hora_inicio_real", "updated_at"])

    latitud, longitud = validar_coordenadas(payload.get("latitud"), payload.get("longitud"))
    timestamp_dispositivo = _payload_value(payload, "timestamp_dispositivo")
    duplicate = None
    if timestamp_dispositivo:
        duplicate = UbicacionRuta.objects.filter(
            ruta=ruta,
            repartidor=repartidor,
            latitud=latitud,
            longitud=longitud,
            timestamp_dispositivo=timestamp_dispositivo,
        ).order_by("-id").first()
    if duplicate:
        duplicate._alertas_tracking = ["duplicado_cliente"]
        return duplicate

    tracking_origen = payload.get("tracking_origen") or "automatico_geocerca"
    automatico_pwa = tracking_origen == "automatico_pwa"
    timestamp_ok, timestamp_reason = _timestamp_dispositivo_confiable(timestamp_dispositivo)
    precision_ok, precision_reason = _precision_confiable(_payload_value(payload, "precision_metros"))
    salto_ok, salto_reason, salto_distancia = _salto_fisico_confiable(ruta, latitud, longitud, timestamp_dispositivo)
    alertas_tracking = []

    ubicacion = UbicacionRuta.objects.create(
        ruta=ruta,
        repartidor=repartidor,
        unidad=unidad,
        latitud=latitud,
        longitud=longitud,
        precision_metros=_payload_value(payload, "precision_metros"),
        velocidad_kmh=_payload_value(payload, "velocidad_kmh"),
        bateria_porcentaje=_payload_value(payload, "bateria_porcentaje"),
        timestamp_dispositivo=timestamp_dispositivo,
        ip_registro=ip_registro,
    )

    if not timestamp_ok:
        alertas_tracking.append("ubicacion_tardia")
        crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_UBICACION_TARDIA,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion=timestamp_reason,
            user=user,
            ubicacion=ubicacion,
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            metadata={"origen": tracking_origen},
            ventana_minutos=10,
        )
    if not precision_ok:
        alertas_tracking.append("precision_baja")
        crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_GPS_PRECISION_BAJA,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion=precision_reason,
            user=user,
            ubicacion=ubicacion,
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            metadata={"origen": tracking_origen, "precision_metros": str(ubicacion.precision_metros)},
            ventana_minutos=10,
        )
    if not salto_ok:
        alertas_tracking.append("salto_imposible")
        crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_SALTO_IMPOSIBLE,
            severidad=EventoRuta.SEVERIDAD_CRITICA,
            descripcion=salto_reason,
            user=user,
            ubicacion=ubicacion,
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            distancia_metros_value=salto_distancia,
            metadata={"origen": tracking_origen},
            ventana_minutos=10,
        )

    ubicacion_confiable = timestamp_ok and precision_ok and salto_ok

    resultado = evaluar_geocercas(ruta, ubicacion.latitud, ubicacion.longitud)
    if resultado.parada and resultado.dentro and ubicacion_confiable:
        metadata_llegada = {
            "origen_servicio": "registrar_ubicacion_ruta",
            "ubicacion_confiable": True,
            "tracking_origen": tracking_origen,
            "ruta_id": ruta.id,
            "repartidor_id": repartidor.id,
            "unidad_id": unidad.id,
        }
        evento_llegada = crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion=f"Llegada detectada en {resultado.parada.punto_nombre_snapshot}.",
            user=user,
            parada=resultado.parada,
            ubicacion=ubicacion,
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            distancia_metros_value=resultado.distancia_metros,
            metadata=metadata_llegada,
            ventana_minutos=60,
        )
        if evento_llegada is None:
            evento_llegada = (
                EventoRuta.objects.select_for_update()
                .filter(
                    ruta=ruta,
                    parada=resultado.parada,
                    tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
                    creado_en__gte=timezone.now() - timezone.timedelta(minutes=60),
                    metadata__origen_servicio="registrar_ubicacion_ruta",
                    metadata__ubicacion_confiable=True,
                )
                .order_by("-creado_en", "-id")
                .first()
            )
            if evento_llegada is None:
                evento_llegada = EventoRuta.objects.create(
                    ruta=ruta,
                    parada=resultado.parada,
                    ubicacion=ubicacion,
                    tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
                    severidad=EventoRuta.SEVERIDAD_OK,
                    descripcion=f"Llegada detectada en {resultado.parada.punto_nombre_snapshot}.",
                    latitud=ubicacion.latitud,
                    longitud=ubicacion.longitud,
                    distancia_metros=resultado.distancia_metros,
                    metadata=metadata_llegada,
                    creado_por=user,
                )
        if resultado.parada.estado != ParadaRuta.ESTADO_VISITADA:
            _marcar_visitada_por_permanencia(
                ruta=ruta,
                parada=resultado.parada,
                ubicacion_actual=ubicacion,
                distancia_metros_value=resultado.distancia_metros,
            )
    elif ruta.paradas.exists() and not resultado.dentro_geocerca_planeada:
        ubicacion.fuera_de_geocerca = True
        ubicacion.save(update_fields=["fuera_de_geocerca"])
        confirmado = payload.get("fuera_de_ruta_confirmado") is True
        motivo = (payload.get("desvio_motivo") or "").strip()
        descripcion_desvio = (
            "Desvío confirmado fuera del corredor autorizado de la ruta."
            if confirmado
            else (
                "Desvío detectado automáticamente por GPS fuera de geocerca."
                if automatico_pwa or tracking_origen == "automatico_geocerca"
                else "Desvío detectado por registro manual fuera de geocerca."
            )
        )
        motivo_desvio = motivo or (
            "Desvío detectado automáticamente por GPS fuera de geocerca."
            if automatico_pwa or tracking_origen == "automatico_geocerca"
            else "Registro fuera de geocerca."
        )
        evento_desvio = crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_DESVIO,
            severidad=EventoRuta.SEVERIDAD_CRITICA,
            descripcion=descripcion_desvio,
            user=user,
            parada=resultado.parada_planeada_mas_cercana,
            ubicacion=ubicacion,
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            distancia_metros_value=resultado.distancia_planeada_metros,
            metadata={
                "punto_mas_cercano": (
                    resultado.parada_planeada_mas_cercana.punto_nombre_snapshot
                    if resultado.parada_planeada_mas_cercana
                    else None
                ),
                "motivo": motivo_desvio,
                "origen": "repartidor_confirmado" if confirmado else tracking_origen,
            },
            ventana_minutos=0 if confirmado else 15,
        )
        if evento_desvio and automatico_pwa and not confirmado:
            from .tasks import notificar_desvio_ruta_automatico

            try:
                notificar_desvio_ruta_automatico.delay(evento_desvio.id)
            except Exception:
                logger.exception("No se pudo encolar notificar_desvio_ruta_automatico para evento %s", evento_desvio.id)

    parada_planeada = resultado.parada_planeada_mas_cercana
    if (
        ubicacion_confiable
        and parada_planeada is not None
        and resultado.distancia_planeada_metros is not None
        and resultado.distancia_planeada_metros <= parada_planeada.radio_geocerca_metros
    ):
        _agendar_recarga_cedis_si_pendiente(
            ruta=ruta,
            parada=parada_planeada,
            user=user,
        )

    ubicacion._alertas_tracking = alertas_tracking
    return ubicacion


def detectar_gps_perdido(ruta: RutaEntrega, *, umbral_minutos: int = 10) -> EventoRuta | None:
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        return None

    latest = ruta.ubicaciones.order_by("-timestamp_servidor").first()
    if not latest:
        inicio = ruta.hora_inicio_real
        if not inicio:
            return None
        minutes = (timezone.now() - inicio).total_seconds() / 60
        if minutes < umbral_minutos:
            return None
        if EventoRuta.objects.filter(
            ruta=ruta,
            tipo=EventoRuta.TIPO_GPS_PERDIDO,
            ubicacion__isnull=True,
            latitud__isnull=True,
            longitud__isnull=True,
            metadata__sin_primera_senal=True,
        ).exists():
            return None
        return crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_GPS_PERDIDO,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion=f"Sin primera señal GPS por {int(minutes)} minutos desde la salida.",
            metadata={
                "detectado_por": "celery",
                "umbral_minutos": umbral_minutos,
                "minutos_sin_senal": int(minutes),
                "sin_primera_senal": True,
                "inicio_sin_senal": inicio.isoformat(),
            },
            ventana_minutos=umbral_minutos,
        )
    minutes = (timezone.now() - latest.timestamp_servidor).total_seconds() / 60
    if minutes < umbral_minutos:
        return None
    if EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO, ubicacion=latest).exists():
        return None
    return crear_evento_ruta_once(
        ruta=ruta,
        tipo=EventoRuta.TIPO_GPS_PERDIDO,
        severidad=EventoRuta.SEVERIDAD_ALERTA,
        descripcion=f"Sin señal GPS por {int(minutes)} minutos.",
        ubicacion=latest,
        latitud=latest.latitud,
        longitud=latest.longitud,
        metadata={
            "detectado_por": "celery",
            "umbral_minutos": umbral_minutos,
            "minutos_sin_senal": int(minutes),
            "ultima_ubicacion_id": latest.id,
            "ultima_senal_servidor": latest.timestamp_servidor.isoformat(),
            "sin_primera_senal": False,
        },
        ventana_minutos=umbral_minutos,
    )


def resumen_control_rutas(*, fecha=None, limit: int = 50) -> dict:
    fecha = fecha or timezone.localdate()
    rutas = (
        RutaEntrega.objects.select_related("repartidor__user", "unidad_operativa", "bitacora_salida")
        .prefetch_related("paradas__punto", "eventos")
        .filter(fecha_ruta=fecha)
        .order_by("-estatus", "-id")[:limit]
    )
    rows = []
    for ruta in rutas:
        latest = ruta.ubicaciones.order_by("-timestamp_servidor").first()
        gps_minutos = int((timezone.now() - latest.timestamp_servidor).total_seconds() / 60) if latest else None
        gps_atrasado = ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and (gps_minutos is None or gps_minutos >= 10)
        eventos_abiertos = ruta.eventos.filter(severidad__in=[EventoRuta.SEVERIDAD_ALERTA, EventoRuta.SEVERIDAD_CRITICA]).count()
        rows.append(
            {
                "ruta": ruta,
                "ultima_ubicacion": latest,
                "paradas_total": ruta.paradas.count(),
                "paradas_visitadas": ruta.paradas.filter(estado=ParadaRuta.ESTADO_VISITADA).count(),
                "eventos_alerta": eventos_abiertos,
                "gps_minutos": gps_minutos,
                "gps_atrasado": gps_atrasado,
            }
        )
    return {
        "fecha": fecha,
        "rutas": rows,
        "eventos_criticos": EventoRuta.objects.filter(ruta__fecha_ruta=fecha, severidad=EventoRuta.SEVERIDAD_CRITICA).count(),
        "desvios": EventoRuta.objects.filter(ruta__fecha_ruta=fecha, tipo=EventoRuta.TIPO_DESVIO).count(),
        "gps_perdido": EventoRuta.objects.filter(ruta__fecha_ruta=fecha, tipo=EventoRuta.TIPO_GPS_PERDIDO).count(),
    }
