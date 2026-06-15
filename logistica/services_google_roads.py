from __future__ import annotations

import hashlib
from dataclasses import dataclass

import requests
from django.conf import settings
from django.core.cache import cache


@dataclass(frozen=True)
class SnappedPathResult:
    coordinates: list[tuple[float, float]]
    source: str
    warning: str = ""


def _dedupe_consecutive(coords: list[tuple[float, float]]) -> list[tuple[float, float]]:
    cleaned = []
    previous = None
    for coord in coords:
        if coord == previous:
            continue
        cleaned.append(coord)
        previous = coord
    return cleaned


def _sample_coords(coords: list[tuple[float, float]], max_points: int) -> list[tuple[float, float]]:
    if len(coords) <= max_points:
        return coords
    if max_points <= 2:
        return [coords[0], coords[-1]]

    last_index = len(coords) - 1
    sampled = []
    for index in range(max_points):
        source_index = round(index * last_index / (max_points - 1))
        sampled.append(coords[source_index])
    return _dedupe_consecutive(sampled)


def _cache_key(ruta_id: int, coords: list[tuple[float, float]]) -> str:
    digest_source = "|".join(f"{lat:.6f},{lng:.6f}" for lat, lng in coords)
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:24]
    return f"logistica:roads:snap:{ruta_id}:{digest}"


def _raw_result(coords: list[tuple[float, float]], warning: str) -> SnappedPathResult:
    return SnappedPathResult(coordinates=coords, source="RAW", warning=warning)


def snap_gps_path_to_roads(*, ruta_id: int, coords: list[tuple[float, float]]) -> SnappedPathResult:
    coords = _dedupe_consecutive(coords)
    if len(coords) < 2:
        return _raw_result(coords, "insufficient_points")

    if not getattr(settings, "GOOGLE_ROADS_SNAP_ENABLED", True):
        return _raw_result(coords, "disabled")

    api_key = getattr(settings, "GOOGLE_SERVER_API_KEY", "")
    if not api_key:
        return _raw_result(coords, "missing_api_key")

    max_points = max(min(int(getattr(settings, "GOOGLE_ROADS_SNAP_MAX_POINTS", 100) or 100), 100), 2)
    sampled = _sample_coords(coords, max_points)
    key = _cache_key(ruta_id, sampled)
    cached = cache.get(key)
    if cached:
        return SnappedPathResult(
            coordinates=[tuple(point) for point in cached.get("coordinates", [])],
            source=cached.get("source", "GOOGLE_ROADS"),
            warning=cached.get("warning", ""),
        )

    params = {
        "path": "|".join(f"{lat:.6f},{lng:.6f}" for lat, lng in sampled),
        "interpolate": "true",
        "key": api_key,
    }
    try:
        response = requests.get(
            "https://roads.googleapis.com/v1/snapToRoads",
            params=params,
            timeout=getattr(settings, "GOOGLE_ROADS_TIMEOUT_SECONDS", 10),
        )
        if response.status_code >= 400:
            warning = f"google_roads_http_{response.status_code}"
            cache.set(key, {"coordinates": coords, "source": "RAW", "warning": warning}, timeout=60 * 15)
            return _raw_result(coords, warning)
        data = response.json()
    except (requests.RequestException, ValueError):
        warning = "google_roads_request_failed"
        cache.set(key, {"coordinates": coords, "source": "RAW", "warning": warning}, timeout=60 * 5)
        return _raw_result(coords, warning)

    snapped = []
    for point in data.get("snappedPoints") or []:
        location = point.get("location") or {}
        lat = location.get("latitude")
        lng = location.get("longitude")
        if lat is None or lng is None:
            continue
        snapped.append((float(lat), float(lng)))

    snapped = _dedupe_consecutive(snapped)
    if len(snapped) < 2:
        warning = "google_roads_empty"
        cache.set(key, {"coordinates": coords, "source": "RAW", "warning": warning}, timeout=60 * 15)
        return _raw_result(coords, warning)

    cache.set(
        key,
        {"coordinates": snapped, "source": "GOOGLE_ROADS", "warning": ""},
        timeout=60 * 15,
    )
    return SnappedPathResult(coordinates=snapped, source="GOOGLE_ROADS")
