from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings
from django.utils import timezone

from syncfy_client.models import CuentaBancaria, MovimientoBancario
from syncfy_client.services.base import SyncfyClient


def timestamp_to_datetime(value: int | float | str | None) -> datetime:
    if value in (None, ""):
        return timezone.now()
    timestamp = int(float(value))
    tz = ZoneInfo(getattr(settings, "TIME_ZONE", "America/Mazatlan"))
    return datetime.fromtimestamp(timestamp, tz=dt_timezone.utc).astimezone(tz)


def rango_unix_syncfy(*, dias_atras: int | None = None, now: datetime | None = None) -> tuple[int, int]:
    now = now or timezone.now()
    if timezone.is_naive(now):
        now = timezone.make_aware(now, timezone=timezone.get_current_timezone())
    dias = max(1, int(dias_atras if dias_atras is not None else getattr(settings, "SYNCFY_DIAS_ATRAS", 7)))
    inicio = now - timedelta(days=dias)
    return int(inicio.timestamp()), int(now.timestamp())


def descargar_transacciones(
    *,
    id_credential: str,
    token: str,
    dt_refresh_from: int,
    dt_refresh_to: int,
    client: SyncfyClient | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    client = client or SyncfyClient()
    transacciones: list[dict[str, Any]] = []
    skip = 0
    while True:
        response = client.get(
            "/transactions",
            params={
                "id_credential": id_credential,
                "dt_refresh_from": dt_refresh_from,
                "dt_refresh_to": dt_refresh_to,
                "limit": limit,
                "skip": skip,
            },
            token=token,
        )
        batch = response if isinstance(response, list) else []
        batch = [item for item in batch if isinstance(item, dict)]
        if not batch:
            break
        transacciones.extend(batch)
        if len(batch) < limit:
            break
        skip += limit
    return transacciones


def guardar_transacciones(
    *,
    cuenta: CuentaBancaria,
    transacciones: list[dict[str, Any]],
) -> tuple[int, int]:
    total = 0
    nuevos = 0
    for transaccion in transacciones:
        id_transaction = str(transaccion.get("id_transaction") or "").strip()
        if not id_transaction:
            continue
        amount = _decimal(transaccion.get("amount"))
        tipo = MovimientoBancario.TIPO_ABONO if amount >= 0 else MovimientoBancario.TIPO_CARGO
        _, created = MovimientoBancario.objects.get_or_create(
            id_transaction=id_transaction,
            defaults={
                "cuenta": cuenta,
                "descripcion": str(transaccion.get("description") or ""),
                "monto": abs(amount),
                "tipo": tipo,
                "moneda": str(transaccion.get("currency") or "MXN"),
                "fecha_transaccion": timestamp_to_datetime(transaccion.get("dt_transaction")),
                "fecha_refresh": timestamp_to_datetime(transaccion.get("dt_refresh")),
                "extra_raw": transaccion,
            },
        )
        total += 1
        if created:
            nuevos += 1
    return total, nuevos


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, TypeError):
        return Decimal("0")
