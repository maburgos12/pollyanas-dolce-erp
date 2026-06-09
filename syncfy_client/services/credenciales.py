from __future__ import annotations

import time
from typing import Any

from django.conf import settings

from syncfy_client.services.base import SyncfyClient, SyncfyServiceError


JOB_COMPLETED = 3
JOB_ERROR = 4


def registrar_credencial(
    *,
    id_site: str,
    credentials: dict[str, Any],
    token: str,
    client: SyncfyClient | None = None,
) -> str:
    client = client or SyncfyClient()
    response = client.post(
        "/credentials",
        json={"id_site": id_site, "credentials": credentials},
        token=token,
    )
    id_credential = ""
    if isinstance(response, dict):
        id_credential = str(response.get("id_credential") or "")
    if not id_credential:
        raise ValueError("La respuesta de Syncfy no contiene id_credential")
    return id_credential


def listar_credenciales(*, token: str, client: SyncfyClient | None = None) -> list[dict[str, Any]]:
    client = client or SyncfyClient()
    response = client.get("/credentials", token=token)
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    return []


def refrescar_credencial(*, id_credential: str, token: str, client: SyncfyClient | None = None) -> str:
    client = client or SyncfyClient()
    response = client.post(
        "/credentials/pulls",
        json={"id_credential": id_credential},
        token=token,
    )
    if isinstance(response, dict):
        return str(response.get("id_job") or response.get("id_job_uuid") or "")
    return ""


def obtener_job(*, id_job: str, token: str, client: SyncfyClient | None = None) -> dict[str, Any]:
    client = client or SyncfyClient()
    response = client.get("/jobs", params={"id_job": id_job}, token=token)
    if isinstance(response, list) and response:
        first = response[0]
        return first if isinstance(first, dict) else {}
    if isinstance(response, dict):
        return response
    return {}


def esperar_job(
    *,
    id_job: str,
    token: str,
    client: SyncfyClient | None = None,
    poll_interval_seconds: int | None = None,
    max_attempts: int | None = None,
) -> dict[str, Any]:
    interval = poll_interval_seconds
    if interval is None:
        interval = int(getattr(settings, "SYNCFY_POLL_INTERVAL_SECONDS", 30) or 30)
    attempts = max_attempts
    if attempts is None:
        attempts = int(getattr(settings, "SYNCFY_POLL_MAX_ATTEMPTS", 20) or 20)
    client = client or SyncfyClient()

    last_job: dict[str, Any] = {}
    for attempt in range(max(1, attempts)):
        last_job = obtener_job(id_job=id_job, token=token, client=client)
        status = int(last_job.get("status") or 0)
        if status == JOB_COMPLETED:
            return last_job
        if status == JOB_ERROR:
            raise SyncfyServiceError(f"Syncfy job {id_job} termino con error", payload=last_job)
        if attempt < attempts - 1:
            time.sleep(max(0, interval))
    raise SyncfyServiceError(f"Syncfy job {id_job} no termino dentro del tiempo esperado", payload=last_job)
