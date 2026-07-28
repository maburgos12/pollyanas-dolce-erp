from __future__ import annotations

import logging
from typing import Any

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
        return {"procesados": 0, "errores": 0, "duplicados": 0}

    url = f"{ERP_BASE_URL}{ERP_ENDPOINT}"
    try:
        response = requests.post(url, json={"eventos": events}, headers=_headers(), timeout=20)
        if response.status_code == 200:
            return response.json()
        log.error("ERP respondio %s: %s", response.status_code, response.text[:500])
    except Exception as exc:
        log.error("Error enviando eventos al ERP: %s", exc)
    return {"procesados": 0, "errores": len(events), "duplicados": 0}
