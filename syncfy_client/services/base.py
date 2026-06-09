from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from django.conf import settings


class SyncfyConfigurationError(RuntimeError):
    """Raised when Syncfy settings are incomplete."""


class SyncfyServiceError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Any | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class SyncfyAuthError(SyncfyServiceError):
    """Raised when Syncfy rejects credentials or a session token."""


@dataclass(frozen=True)
class SyncfyConfig:
    api_key: str
    id_user: str
    base_url: str
    timeout_seconds: int = 60


def get_syncfy_config(*, require_id_user: bool = True) -> SyncfyConfig:
    config = SyncfyConfig(
        api_key=(getattr(settings, "SYNCFY_API_KEY", "") or "").strip(),
        id_user=(getattr(settings, "SYNCFY_ID_USER", "") or "").strip(),
        base_url=(getattr(settings, "SYNCFY_BASE_URL", "") or "").strip().rstrip("/"),
        timeout_seconds=int(getattr(settings, "SYNCFY_TIMEOUT_SECONDS", 60) or 60),
    )
    missing = []
    if not config.api_key:
        missing.append("SYNCFY_API_KEY")
    if require_id_user and not config.id_user:
        missing.append("SYNCFY_ID_USER")
    if not config.base_url:
        missing.append("SYNCFY_BASE_URL")
    if missing:
        raise SyncfyConfigurationError("Configuracion Syncfy incompleta: " + ", ".join(missing))
    return config


class SyncfyClient:
    def __init__(
        self,
        *,
        config: SyncfyConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.config = config or get_syncfy_config()
        self.session = session or requests.Session()

    def post(
        self,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        token: str | None = None,
        api_key_auth: bool = False,
    ) -> Any:
        return self._request("POST", path, json=json, token=token, api_key_auth=api_key_auth)

    def get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> Any:
        params = dict(params or {})
        if token and "token" not in params:
            params["token"] = token
        return self._request("GET", path, params=params, token=token)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        token = kwargs.pop("token", None)
        api_key_auth = bool(kwargs.pop("api_key_auth", False))
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("Accept", "application/json")
        if kwargs.get("json") is not None:
            headers.setdefault("Content-Type", "application/json")
        if token:
            headers["Authorization"] = f"token {token}"
        elif api_key_auth:
            headers["Authorization"] = f"api_key {self.config.api_key}"

        url = f"{self.config.base_url}/{path.lstrip('/')}"
        try:
            response = self.session.request(
                method,
                url,
                headers=headers,
                timeout=self.config.timeout_seconds,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise SyncfyServiceError(f"Error de conexion Syncfy: {exc.__class__.__name__}") from exc

        return parse_syncfy_response(response)


def parse_syncfy_response(response: requests.Response) -> Any:
    status_code = int(getattr(response, "status_code", 0) or 0)
    try:
        payload = response.json()
    except ValueError as exc:
        if status_code == 401:
            raise SyncfyAuthError("Syncfy respondio HTTP 401", status_code=status_code) from exc
        raise SyncfyServiceError(
            f"Syncfy respondio HTTP {status_code} sin JSON valido",
            status_code=status_code,
        ) from exc

    if status_code == 401:
        raise SyncfyAuthError("Syncfy respondio HTTP 401", status_code=status_code, payload=payload)
    if status_code >= 400:
        raise SyncfyServiceError(
            syncfy_error_message(payload, fallback=f"Syncfy respondio HTTP {status_code}"),
            status_code=status_code,
            payload=payload,
        )

    code = payload.get("code") if isinstance(payload, dict) else None
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if code and int(code) >= 400:
        error_class = SyncfyAuthError if int(code) == 401 else SyncfyServiceError
        raise error_class(syncfy_error_message(payload), status_code=int(code), payload=payload)
    if errors:
        raise SyncfyServiceError(syncfy_error_message(payload), status_code=status_code, payload=payload)

    if isinstance(payload, dict) and "response" in payload:
        return payload["response"]
    return payload


def syncfy_error_message(payload: Any, *, fallback: str = "Error Syncfy") -> str:
    if not isinstance(payload, dict):
        return fallback
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = []
        for error in errors:
            if isinstance(error, dict):
                messages.append(str(error.get("message") or error.get("detail") or error))
            else:
                messages.append(str(error))
        return "; ".join(messages)
    if isinstance(errors, str) and errors:
        return errors
    message = payload.get("message") or payload.get("error")
    return str(message or fallback)
