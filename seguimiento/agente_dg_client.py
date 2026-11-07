"""Cliente HTTP del ERP hacia el Agente DG (app.pollyanasdolce.com).

Fase 2 (write-back): permite que el ERP actualice acuerdos del Agente DG
vía su API oficial, de modo que se disparen los efectos del propio sistema
(WhatsApp, notificaciones, Google Calendar). NO se escribe directo a su base de
datos.

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


def get_users() -> list:
    """Lista usuarios del Agente DG para resolver colaboradores por e-mail."""
    resp = _request("GET", "/api/users/")
    if resp.status_code >= 400:
        raise AgenteDGError(f"GET users falló: {resp.status_code} {resp.text[:160]}")
    data = resp.json()
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise AgenteDGError("No se pudo validar el catálogo de colaboradores del origen.")
    return data


def create_minute_agreement(**fields) -> dict:
    """Crea una minuta/acuerdo en app.pollyanasdolce.com."""
    resp = _request("POST", "/api/minutas/", json=fields)
    if resp.status_code >= 400:
        raise AgenteDGError(f"POST minuta falló: {resp.status_code} {resp.text[:200]}")
    data = _minute_action_response(resp)
    if type(data.get("id")) is not int or data.get("collaborator_user_id") != fields.get("collaborator_user_id"):
        raise AgenteDGError("El origen no confirmó la identidad del acuerdo creado.")
    return data


def send_minute_agreement(minuta_id: int, *, send_email: bool, send_whatsapp: bool,
                          event_type: str = "assignment", event_key: str | None = None,
                          event_message: str = "", force_resend: bool = False,
                          resend_reason: str = "", request_id: str | None = None) -> dict:
    """Solicita el despacho común sin convertir un reintento en reenvío."""
    payload = {"minute_ids": [int(minuta_id)], "send_email": send_email,
               "send_whatsapp": send_whatsapp, "force_resend": force_resend, "event_type": event_type}
    if event_key:
        payload["event_key"] = event_key
    if event_message:
        payload["event_message"] = event_message[:2000]
    if force_resend:
        payload.update(resend_reason=resend_reason, request_id=request_id)
    resp = _request("POST", "/api/minutas/send-selected", json=payload)
    if resp.status_code >= 400:
        raise AgenteDGError(f"No se confirmó el despacho: HTTP {resp.status_code}")
    data = resp.json()
    results = data.get("results") if isinstance(data, dict) else None
    matches = [row for row in (results or []) if isinstance(row, dict) and row.get("minute_id") == int(minuta_id)]
    if len(matches) != 1 or not isinstance(matches[0].get("channels"), dict):
        raise AgenteDGError("El origen no confirmó resultados por canal. Consultar antes de reintentar.")
    return matches[0]


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


def patch_minute_agreement(minuta_id: int, **fields) -> dict:
    """Actualiza una minuta/acuerdo en el Agente DG."""
    if not minuta_id:
        raise AgenteDGError("patch_minute_agreement requiere minuta_id")
    resp = _request("PATCH", f"/api/minutas/{int(minuta_id)}", json=fields)
    if resp.status_code >= 400:
        raise AgenteDGError(f"PATCH minuta {minuta_id} falló: {resp.status_code} {resp.text[:200]}")
    return resp.json()


def patch_commitment_status(commitment_id: int, *, status: str, comment: str = "") -> dict:
    """Actualiza el estatus de un compromiso en el Agente DG."""
    if not commitment_id:
        raise AgenteDGError("patch_commitment_status requiere commitment_id")
    payload = {"status": status}
    if comment:
        payload["comment"] = comment
    resp = _request("PATCH", f"/api/commitments/{int(commitment_id)}/status", json=payload)
    if resp.status_code >= 400:
        raise AgenteDGError(f"PATCH commitment {commitment_id} status falló: {resp.status_code} {resp.text[:200]}")
    return resp.json()


def get_minute_agreement(minuta_id: int) -> dict:
    """La API de minutas solo expone lectura por lista; seleccionar el ID exacto."""
    resp = _request('GET', '/api/minutas/', params={'include_archived': 'true'})
    if resp.status_code >= 400:
        raise AgenteDGError(f'No se pudo consultar la minuta: HTTP {resp.status_code}')
    rows = resp.json()
    matches = [row for row in rows if row.get('id') == minuta_id] if isinstance(rows, list) else []
    if len(matches) != 1:
        raise AgenteDGError('No se encontró la minuta exacta en origen.')
    return matches[0]


def _minute_action_response(resp) -> dict:
    if resp.status_code >= 400:
        raise AgenteDGError(f"No se confirmó la acción: HTTP {resp.status_code}. Consultar antes de repetir.")
    try:
        data = resp.json()
    except ValueError as exc:
        raise AgenteDGError("El origen no devolvió una confirmación válida.") from exc
    if not isinstance(data, dict):
        raise AgenteDGError("Respuesta de origen no válida.")
    return data


def post_minute_comment(minuta_id: int, comment: str, *, request_id: str | None = None, origin_actor=None, notify_recipient="director") -> dict:
    payload = {"entity_type": "MinuteAgreement", "entity_id": int(minuta_id), "comment": comment,
               "notify_recipient": notify_recipient, "origin_actor": origin_actor}
    if request_id:
        payload["request_id"] = request_id
    return _minute_action_response(_request("POST", "/api/collaborator/comments", json=payload))


def post_minute_extension(minuta_id: int, requested_date: str, reason: str, *, origin_actor=None, request_id=None) -> dict:
    return _minute_action_response(_request("POST", "/api/collaborator/extension-requests/v2", json={
        "agreement_id": int(minuta_id), "requested_date": requested_date, "reason": reason, "category": "otro",
        "origin_actor": origin_actor, "request_id": request_id}))


def review_minute_extension(request_id: str, *, status: str, comment: str) -> dict:
    return _minute_action_response(_request("PATCH", f"/api/dg/extension-requests/{request_id}/review", json={
        "status": status, "reviewer_comment": comment}))


def post_minute_evidence(minuta_id: int, file, comment: str, *, origin_actor=None, request_id=None) -> dict:
    import json
    return _minute_action_response(_request("POST", "/api/collaborator/evidence/upload", data={
        "agreement_id": int(minuta_id), "comment": comment, "origin_actor": json.dumps(origin_actor),
        "request_id": request_id}, files={"file": (file.name, file, file.content_type)}))


def get_minute_file(minuta_id: int, kind: str, file_id: str) -> bytes:
    if kind not in {"attachments", "evidence"}:
        raise AgenteDGError("Tipo de archivo no válido.")
    if kind == "attachments":
        identifier = str(int(file_id))
    else:
        import uuid
        identifier = str(uuid.UUID(file_id))
    response = _request("GET", f"/api/minutas/{int(minuta_id)}/{kind}/{identifier}", stream=True)
    try:
        if response.status_code >= 400:
            raise AgenteDGError("Archivo no disponible en origen.")
        result = bytearray()
        for chunk in response.iter_content(chunk_size=65536):
            if len(result) + len(chunk) > 15 * 1024 * 1024:
                raise AgenteDGError("El archivo excede el límite permitido.")
            result.extend(chunk)
        return bytes(result)
    finally:
        response.close()


def get_minute_creation(request_id: str) -> dict:
    import uuid
    token = str(uuid.UUID(request_id))
    return _minute_action_response(_request("GET", f"/api/minutas/creation-requests/{token}"))
