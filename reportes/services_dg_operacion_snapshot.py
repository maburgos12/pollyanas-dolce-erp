from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.db import transaction
from django.utils import timezone

from core.models import Sucursal
from reportes.models import DgOperacionSnapshot


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, Sucursal):
        return {
            "__type__": "sucursal",
            "id": value.id,
            "codigo": value.codigo,
            "nombre": value.nombre,
            "activa": value.activa,
        }
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _hydrate(value: Any) -> Any:
    if isinstance(value, list):
        return [_hydrate(item) for item in value]
    if not isinstance(value, dict):
        return value

    marker = value.get("__type__")
    if marker == "date":
        return date.fromisoformat(str(value["value"]))
    if marker == "datetime":
        raw = str(value["value"])
        parsed = datetime.fromisoformat(raw)
        if timezone.is_naive(parsed):
            return timezone.make_aware(parsed, timezone.get_current_timezone())
        return parsed
    if marker == "decimal":
        return Decimal(str(value["value"]))
    if marker == "sucursal":
        return {
            "id": value.get("id"),
            "codigo": value.get("codigo", ""),
            "nombre": value.get("nombre", ""),
            "activa": value.get("activa", True),
        }
    return {key: _hydrate(item) for key, item in value.items()}


def serialize_dg_operacion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _json_safe(payload)


def hydrate_dg_operacion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    hydrated = _hydrate(payload)
    return hydrated if isinstance(hydrated, dict) else {}


def get_dg_operacion_snapshot_payload(fecha_operacion: date) -> dict[str, Any] | None:
    snapshot = (
        DgOperacionSnapshot.objects.filter(
            fecha_operacion=fecha_operacion,
            status=DgOperacionSnapshot.STATUS_READY,
        )
        .only("payload")
        .first()
    )
    if snapshot is None:
        return None
    return hydrate_dg_operacion_payload(snapshot.payload)


def get_latest_dg_operacion_snapshot_payload() -> tuple[date, dict[str, Any]] | None:
    snapshot = (
        DgOperacionSnapshot.objects.filter(status=DgOperacionSnapshot.STATUS_READY)
        .only("fecha_operacion", "payload")
        .order_by("-fecha_operacion")
        .first()
    )
    if snapshot is None:
        return None
    return snapshot.fecha_operacion, hydrate_dg_operacion_payload(snapshot.payload)


def refresh_dg_operacion_snapshot(
    *,
    fecha_operacion: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    group_by: str = "day",
) -> DgOperacionSnapshot:
    from recetas.views.plan import _build_dg_operacion_dashboard_payload

    payload = _build_dg_operacion_dashboard_payload(
        start_date=start_date,
        end_date=end_date,
        group_by=group_by,
        fecha_operacion=fecha_operacion,
    )
    payload_fecha = payload["fecha_operacion"]
    serialized_payload = serialize_dg_operacion_payload(payload)
    now = timezone.now()
    with transaction.atomic():
        snapshot, _ = DgOperacionSnapshot.objects.update_or_create(
            fecha_operacion=payload_fecha,
            defaults={
                "payload": serialized_payload,
                "status": DgOperacionSnapshot.STATUS_READY,
                "source_cutoff_at": now,
                "generated_at": now,
                "metadata": {
                    "group_by": group_by,
                    "start_date": start_date.isoformat() if start_date else "",
                    "end_date": end_date.isoformat() if end_date else "",
                    "payload_sections": sorted(serialized_payload.keys()),
                },
                "last_error": "",
            },
        )
    return snapshot
