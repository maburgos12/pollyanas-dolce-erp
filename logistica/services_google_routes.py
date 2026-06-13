from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import requests
from django.conf import settings
from django.utils import timezone

from .models import RutaEntrega
from .services_rutas_control import distancia_metros


@dataclass(frozen=True)
class RutaProgramadaResult:
    polyline: str
    distancia_metros: int
    duracion_segundos: int
    fuente: str


def _duration_seconds(value: str | None) -> int:
    if not value:
        return 0
    if value.endswith("s"):
        value = value[:-1]
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _coords_for_ruta(ruta: RutaEntrega) -> list[tuple[float, float]]:
    coords = []
    for parada in ruta.paradas.order_by("orden", "id"):
        coords.append((float(parada.latitud_geocerca), float(parada.longitud_geocerca)))
    return coords


def _fallback_polyline(coords: list[tuple[float, float]]) -> str:
    return "|".join(f"{lat:.6f},{lng:.6f}" for lat, lng in coords)


def _fallback_route(coords: list[tuple[float, float]]) -> RutaProgramadaResult:
    distancia = 0
    for origin, destination in zip(coords, coords[1:]):
        distancia += distancia_metros(Decimal(str(origin[0])), Decimal(str(origin[1])), Decimal(str(destination[0])), Decimal(str(destination[1])))
    return RutaProgramadaResult(
        polyline=_fallback_polyline(coords),
        distancia_metros=distancia,
        duracion_segundos=0,
        fuente="FALLBACK",
    )


def _google_route(coords: list[tuple[float, float]]) -> RutaProgramadaResult | None:
    api_key = getattr(settings, "GOOGLE_SERVER_API_KEY", "")
    if not api_key or len(coords) < 2:
        return None

    origin = coords[0]
    destination = coords[-1]
    payload = {
        "origin": {"location": {"latLng": {"latitude": origin[0], "longitude": origin[1]}}},
        "destination": {"location": {"latLng": {"latitude": destination[0], "longitude": destination[1]}}},
        "intermediates": [
            {"location": {"latLng": {"latitude": lat, "longitude": lng}}}
            for lat, lng in coords[1:-1]
        ],
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE",
        "polylineQuality": "HIGH_QUALITY",
        "polylineEncoding": "ENCODED_POLYLINE",
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline",
    }

    try:
        response = requests.post(
            "https://routes.googleapis.com/directions/v2:computeRoutes",
            json=payload,
            headers=headers,
            timeout=getattr(settings, "GOOGLE_ROUTES_TIMEOUT_SECONDS", 10),
        )
        if response.status_code >= 400:
            return None
        data = response.json()
    except (requests.RequestException, ValueError):
        return None

    routes = data.get("routes") or []
    if not routes:
        return None
    route = routes[0]
    return RutaProgramadaResult(
        polyline=((route.get("polyline") or {}).get("encodedPolyline") or _fallback_polyline(coords)),
        distancia_metros=int(route.get("distanceMeters") or 0),
        duracion_segundos=_duration_seconds(route.get("duration")),
        fuente="GOOGLE",
    )


def recalcular_ruta_programada(ruta: RutaEntrega) -> RutaProgramadaResult:
    coords = _coords_for_ruta(ruta)
    if len(coords) < 2:
        result = RutaProgramadaResult(polyline=_fallback_polyline(coords), distancia_metros=0, duracion_segundos=0, fuente="FALLBACK")
    else:
        result = _google_route(coords) or _fallback_route(coords)

    ruta.ruta_programada_polyline = result.polyline
    ruta.ruta_programada_distancia_metros = result.distancia_metros
    ruta.ruta_programada_duracion_segundos = result.duracion_segundos
    ruta.ruta_programada_fuente = result.fuente
    ruta.ruta_programada_actualizada_en = timezone.now()
    if result.distancia_metros and not ruta.km_estimado:
        ruta.km_estimado = Decimal(result.distancia_metros) / Decimal("1000")
        ruta.save(
            update_fields=[
                "ruta_programada_polyline",
                "ruta_programada_distancia_metros",
                "ruta_programada_duracion_segundos",
                "ruta_programada_fuente",
                "ruta_programada_actualizada_en",
                "km_estimado",
                "updated_at",
            ]
        )
    else:
        ruta.save(
            update_fields=[
                "ruta_programada_polyline",
                "ruta_programada_distancia_metros",
                "ruta_programada_duracion_segundos",
                "ruta_programada_fuente",
                "ruta_programada_actualizada_en",
                "updated_at",
            ]
        )
    return result
