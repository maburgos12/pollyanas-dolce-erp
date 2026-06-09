from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from syncfy_client.models import CuentaBancaria
from syncfy_client.services.base import SyncfyClient


def buscar_sites_bancarios(
    *,
    keyword: str,
    is_business: int = 1,
    token: str,
    client: SyncfyClient | None = None,
) -> list[dict[str, Any]]:
    client = client or SyncfyClient()
    response = client.get(
        "/catalogues/sites",
        params={"keyword": keyword, "is_business": is_business},
        token=token,
    )
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    return []


def obtener_cuentas(
    *,
    id_credential: str,
    token: str,
    client: SyncfyClient | None = None,
) -> list[dict[str, Any]]:
    client = client or SyncfyClient()
    response = client.get("/accounts", params={"id_credential": id_credential}, token=token)
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    return []


def actualizar_cuenta_desde_syncfy(cuenta: CuentaBancaria, account: dict[str, Any]) -> CuentaBancaria:
    cuenta.id_account = str(account.get("id_account") or cuenta.id_account or "")
    cuenta.numero_cuenta = str(account.get("number") or cuenta.numero_cuenta or "")
    balance = account.get("balance")
    if balance not in (None, ""):
        try:
            cuenta.saldo_actual = Decimal(str(balance))
        except (InvalidOperation, TypeError):
            pass
    cuenta.save(update_fields=["id_account", "numero_cuenta", "saldo_actual", "actualizado_en"])
    return cuenta


def seleccionar_account(cuenta: CuentaBancaria, accounts: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not accounts:
        return None
    if cuenta.id_account:
        for account in accounts:
            if str(account.get("id_account") or "") == cuenta.id_account:
                return account
    return accounts[0]
