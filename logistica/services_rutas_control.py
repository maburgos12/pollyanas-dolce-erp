from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from .models import BitacoraSalidaLlegada, EventoRuta, ParadaRuta, Repartidor, RutaEntrega, UbicacionRuta


@dataclass(frozen=True)
class GeocercaResultado:
    parada: ParadaRuta | None
    distancia_metros: int | None
    dentro: bool


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
    closest: ParadaRuta | None = None
    closest_distance: int | None = None
    for parada in ruta.paradas.select_related("punto").all():
        distance = distancia_metros(latitud, longitud, parada.latitud_geocerca, parada.longitud_geocerca)
        if closest_distance is None or distance < closest_distance:
            closest = parada
            closest_distance = distance

    if closest is None or closest_distance is None:
        return GeocercaResultado(parada=None, distancia_metros=None, dentro=False)
    return GeocercaResultado(
        parada=closest,
        distancia_metros=closest_distance,
        dentro=closest_distance <= closest.radio_geocerca_metros,
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


@transaction.atomic
def registrar_ubicacion_ruta(*, user, ruta: RutaEntrega, payload: dict, ip_registro: str | None = None) -> UbicacionRuta:
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        raise ValidationError("La ruta debe estar en estatus En ruta para registrar seguimiento.")
    if not ruta.repartidor_id:
        raise ValidationError("La ruta debe tener un repartidor asignado antes de aceptar seguimiento.")
    if not ruta.unidad_operativa_id:
        raise ValidationError("La ruta debe tener una unidad asignada antes de aceptar seguimiento.")

    repartidor = _repartidor_usuario(user)
    if ruta.repartidor_id != repartidor.id:
        raise PermissionDenied("Esta ruta está asignada a otro repartidor.")

    bitacora = _bitacora_abierta(repartidor, ruta)
    unidad = ruta.unidad_operativa

    if not ruta.bitacora_salida_id or not ruta.hora_inicio_real:
        ruta.bitacora_salida = bitacora
        ruta.hora_inicio_real = ruta.hora_inicio_real or timezone.now()
        ruta.save(update_fields=["bitacora_salida", "hora_inicio_real", "updated_at"])

    latitud, longitud = validar_coordenadas(payload.get("latitud"), payload.get("longitud"))
    ubicacion = UbicacionRuta.objects.create(
        ruta=ruta,
        repartidor=repartidor,
        unidad=unidad,
        latitud=latitud,
        longitud=longitud,
        precision_metros=_payload_value(payload, "precision_metros"),
        velocidad_kmh=_payload_value(payload, "velocidad_kmh"),
        bateria_porcentaje=_payload_value(payload, "bateria_porcentaje"),
        timestamp_dispositivo=_payload_value(payload, "timestamp_dispositivo"),
        ip_registro=ip_registro,
    )

    resultado = evaluar_geocercas(ruta, ubicacion.latitud, ubicacion.longitud)
    if resultado.parada and resultado.dentro:
        if resultado.parada.estado != ParadaRuta.ESTADO_VISITADA:
            resultado.parada.estado = ParadaRuta.ESTADO_VISITADA
            resultado.parada.hora_llegada_real = timezone.now()
            resultado.parada.distancia_llegada_metros = resultado.distancia_metros
            resultado.parada.save(update_fields=["estado", "hora_llegada_real", "distancia_llegada_metros", "actualizado_en"])
            ruta.recompute_route_control()
            ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])
        crear_evento_ruta_once(
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
            ventana_minutos=60,
        )
    elif ruta.paradas.exists():
        ubicacion.fuera_de_geocerca = True
        ubicacion.save(update_fields=["fuera_de_geocerca"])
        if payload.get("fuera_de_ruta_confirmado") is True:
            motivo = (payload.get("desvio_motivo") or "").strip()
            crear_evento_ruta_once(
                ruta=ruta,
                tipo=EventoRuta.TIPO_DESVIO,
                severidad=EventoRuta.SEVERIDAD_CRITICA,
                descripcion="Desvío confirmado fuera del corredor autorizado de la ruta.",
                user=user,
                parada=resultado.parada,
                ubicacion=ubicacion,
                latitud=ubicacion.latitud,
                longitud=ubicacion.longitud,
                distancia_metros_value=resultado.distancia_metros,
                metadata={
                    "punto_mas_cercano": resultado.parada.punto_nombre_snapshot if resultado.parada else None,
                    "motivo": motivo,
                },
            )

    return ubicacion


def detectar_gps_perdido(ruta: RutaEntrega, *, umbral_minutos: int = 10) -> EventoRuta | None:
    latest = ruta.ubicaciones.order_by("-timestamp_servidor").first()
    if not latest:
        inicio = ruta.hora_inicio_real
        if not inicio:
            return None
        minutes = (timezone.now() - inicio).total_seconds() / 60
        if minutes < umbral_minutos:
            return None
        return crear_evento_ruta_once(
            ruta=ruta,
            tipo=EventoRuta.TIPO_GPS_PERDIDO,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion=f"Sin primera señal GPS por {int(minutes)} minutos desde la salida.",
            ventana_minutos=umbral_minutos,
        )
    minutes = (timezone.now() - latest.timestamp_servidor).total_seconds() / 60
    if minutes < umbral_minutos:
        return None
    return crear_evento_ruta_once(
        ruta=ruta,
        tipo=EventoRuta.TIPO_GPS_PERDIDO,
        severidad=EventoRuta.SEVERIDAD_ALERTA,
        descripcion=f"Sin señal GPS por {int(minutes)} minutos.",
        ubicacion=latest,
        latitud=latest.latitud,
        longitud=latest.longitud,
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
        if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA:
            detectar_gps_perdido(ruta)
        latest = ruta.ubicaciones.order_by("-timestamp_servidor").first()
        eventos_abiertos = ruta.eventos.filter(severidad__in=[EventoRuta.SEVERIDAD_ALERTA, EventoRuta.SEVERIDAD_CRITICA]).count()
        rows.append(
            {
                "ruta": ruta,
                "ultima_ubicacion": latest,
                "paradas_total": ruta.paradas.count(),
                "paradas_visitadas": ruta.paradas.filter(estado=ParadaRuta.ESTADO_VISITADA).count(),
                "eventos_alerta": eventos_abiertos,
                "gps_minutos": int((timezone.now() - latest.timestamp_servidor).total_seconds() / 60) if latest else None,
            }
        )
    return {
        "fecha": fecha,
        "rutas": rows,
        "eventos_criticos": EventoRuta.objects.filter(ruta__fecha_ruta=fecha, severidad=EventoRuta.SEVERIDAD_CRITICA).count(),
        "desvios": EventoRuta.objects.filter(ruta__fecha_ruta=fecha, tipo=EventoRuta.TIPO_DESVIO).count(),
        "gps_perdido": EventoRuta.objects.filter(ruta__fecha_ruta=fecha, tipo=EventoRuta.TIPO_GPS_PERDIDO).count(),
    }
