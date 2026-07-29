from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

import requests

from config import ERP_API_KEY, ERP_BASE_URL, ERP_ENDPOINT

log = logging.getLogger("erp_client")


def _headers() -> dict[str, str]:
    return {
        "X-API-Key": ERP_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def ping_erp() -> bool:
    try:
        response = requests.get(
            f"{ERP_BASE_URL}/health/",
            headers={"X-API-Key": ERP_API_KEY, "Accept": "application/json"},
            timeout=8,
            allow_redirects=True,
        )
        return response.status_code in (200, 302, 401)
    except Exception as exc:
        log.warning("ERP no responde: %s", exc)
        return False


def send_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {"contract_version": 2, "batch_id": "", "results": []}

    endpoint = ERP_ENDPOINT.rstrip("/")
    if not endpoint.endswith("/v2"):
        endpoint = f"{endpoint}/v2"
    url = f"{ERP_BASE_URL}{endpoint}/"
    batch_id = str(uuid4())
    try:
        response = requests.post(
            url,
            json={"contract_version": 2, "batch_id": batch_id, "events": events},
            headers=_headers(),
            timeout=20,
        )
    except Exception as exc:
        log.error("Error enviando eventos al ERP: %s", exc)
        raise
    if response.status_code != 200:
        raise RuntimeError(f"ERP respondio {response.status_code}: {response.text[:500]}")
    body = response.json()
    if not isinstance(body, dict) or body.get("contract_version") != 2 or not isinstance(body.get("results"), list):
        raise ValueError("ERP devolvio una respuesta v2 invalida")
    return body
