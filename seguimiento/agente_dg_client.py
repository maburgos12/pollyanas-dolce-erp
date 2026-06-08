"""Cliente HTTP del ERP hacia el Agente DG (app.pollyanasdolce.com).

Fase 2 (write-back): permite que el ERP actualice un paso de proyecto en el Agente DG
vía su API oficial (PATCH /api/minutas/project-steps/{id}), de modo que se disparen los
efectos del propio sistema (WhatsApp, notificaciones, Google Calendar). NO se escribe
directo a su base de datos.

Configuración por variables de entorno (en el .env del ERP):
    AGENTE_DG_API_BASE_URL   p.ej. https://stg.pollyanasdolce.com (staging) o
                                   https://app.pollyanasdolce.com (producción)
    AGENTE_DG_API_EMAIL      correo de la cuenta de servicio con rol DG
    AGENTE_DG_API_PASSWORD   contraseña de esa cuenta

Si no está configurado, is_configured() devuelve False y las vistas siguen operando en
modo local (sin write-back), sin romperse.
"""
from __future__ import annotations

import logging
import os
import threading
import time

import requests

logger = logging.getLogger(__name__)

_TIMEOUT = 12
_USER_AGENT = "Mozilla/5.0 (PollyanaERP; +seguimiento)"
_TOKEN_TTL_SECONDS = 25 * 60  # el access_token dura más; re-login preventivo a los 25 min

_token_cache: dict[str, object] = {"value": None, "exp": 0.0}
_lock = threading.Lock()


class AgenteDGError(Exception):
    """Error al comunicarse con la API del Agente DG."""


def _base_url() -> str:
    return (os.getenv("AGENTE_DG_API_BASE_URL") or "").rstrip("/")


def _email() -> str:
    return (os.getenv("AGENTE_DG_API_EMAIL") or "").strip()


def _password() -> str:
    return os.getenv("AGENTE_DG_API_PASSWORD") or ""


def is_configured() -> bool:
    return bool(_base_url() and _email() and _password())


def _login() -> str:
    resp = requests.post(
        f"{_base_url()}/api/auth/login",
        json={"email": _email(), "password": _password()},
        headers={"User-Agent": _USER_AGENT, "Content-Type": "application/json"},
        timeout=_TIMEOUT,
    )
    if resp.status_code >= 400:
        raise AgenteDGError(f"Login Agente DG falló: {resp.status_code} {resp.text[:160]}")
    token = (resp.json() or {}).get("access_token")
    if not token:
        raise AgenteDGError("Login Agente DG sin access_token en la respuesta")
    _token_cache["value"] = token
    _token_cache["exp"] = time.time() + _TOKEN_TTL_SECONDS
    return token


def _get_token(force: bool = False) -> str:
    with _lock:
        if not force and _token_cache["value"] and time.time() < float(_token_cache["exp"]):
            return str(_token_cache["value"])
        return _login()


def _request(method: str, path: str, **kwargs) -> requests.Response:
    if not is_configured():
        raise AgenteDGError("La API del Agente DG no está configurada.")
    token = _get_token()
    headers = {"Authorization": f"Bearer {token}", "User-Agent": _USER_AGENT}
    headers.update(kwargs.pop("headers", {}))
    url = f"{_base_url()}{path}"
    resp = requests.request(method, url, headers=headers, timeout=_TIMEOUT, **kwargs)
    if resp.status_code == 401:
        # Token vencido/ inválido → re-login una vez y reintentar.
        token = _get_token(force=True)
        headers["Authorization"] = f"Bearer {token}"
        resp = requests.request(method, url, headers=headers, timeout=_TIMEOUT, **kwargs)
    return resp


def get_projects() -> list:
    resp = _request("GET", "/api/minutas/projects")
    if resp.status_code >= 400:
        raise AgenteDGError(f"GET projects falló: {resp.status_code} {resp.text[:160]}")
    return resp.json()


def patch_step(step_id: int, **fields) -> dict:
    """Actualiza un paso de proyecto en el Agente DG.

    Campos soportados por la API: status, checklist_items, title, description,
    deliverable_text, priority, requires_approval, due_at, order_index, etc.
    """
    if not step_id:
        raise AgenteDGError("patch_step requiere step_id")
    resp = _request("PATCH", f"/api/minutas/project-steps/{int(step_id)}", json=fields)
    if resp.status_code >= 400:
        raise AgenteDGError(f"PATCH step {step_id} falló: {resp.status_code} {resp.text[:200]}")
    return resp.json()
