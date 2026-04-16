from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import urljoin

import requests

from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError, ExtractionError
from pos_bridge.utils.helpers import normalize_text


@dataclass
class PointAuthenticatedSession:
    session: requests.Session
    account_id: str = ""
    workspace_name: str = ""


class PointHttpSessionService:
    SIGN_IN_PATH = "/Account/SignIn_click"
    WORKSPACES_PATH = "/Account/get_workSpaces"
    ACCOUNT_TOKEN_PATH = "/Account/get_acctok"
    DEFAULT_ACCOUNT_ID = "83852AED-D4FB-E611-814F-06B55B5505BA"

    def __init__(self, bridge_settings):
        self.settings = bridge_settings

    def _base_url(self) -> str:
        if not (self.settings.base_url or "").strip():
            raise ConfigurationError("Falta POINT_BASE_URL para abrir Point.")
        return self.settings.base_url.rstrip("/") + "/"

    def _timeout_seconds(self) -> float:
        return self.settings.timeout_ms / 1000

    @staticmethod
    def _ajax_headers() -> dict[str, str]:
        # Point distingue requests reales de navegador por esta cabecera.
        return {"X-Requested-With": "XMLHttpRequest"}

    def _parse_json(self, response: requests.Response, *, label: str) -> dict:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ExtractionError(
                f"Point devolvió una respuesta no JSON en {label}.",
                context={"status_code": response.status_code, "body_preview": response.text[:500]},
            ) from exc
        if not isinstance(payload, dict):
            raise ExtractionError(
                f"Point devolvió un payload inválido en {label}.",
                context={"status_code": response.status_code, "body_preview": response.text[:500]},
            )
        return payload

    def _sign_in(self, session: requests.Session) -> None:
        # Point suele requerir una visita previa al portal antes del POST AJAX
        # de autenticación. Sin este preflight, algunas sesiones responden con
        # redirect/404 intermitentes aunque las credenciales sean correctas.
        session.get(self._base_url(), timeout=self._timeout_seconds())
        response = session.post(
            urljoin(self._base_url(), self.SIGN_IN_PATH.lstrip("/")),
            data={
                "user": self.settings.username,
                "pass": self.settings.password,
                # La UI real de Point envía timeZone=0 en SignIn_click.
                # Replicamos exactamente ese contrato para evitar sesiones
                # divergentes entre navegador y extractor.
                "timeZone": "0",
            },
            headers=self._ajax_headers(),
            timeout=self._timeout_seconds(),
        )
        response.raise_for_status()
        payload = self._parse_json(response, label="login Point")
        redirect_to = str(payload.get("redirectToUrl") or "").strip()
        if not redirect_to:
            raise AuthenticationError("Point no devolvió redirectToUrl al autenticar.")
        follow_up = session.get(urljoin(self._base_url(), redirect_to.lstrip("/")), timeout=self._timeout_seconds())
        follow_up.raise_for_status()

    def _get_workspaces(self, session: requests.Session) -> list[dict]:
        response = session.post(
            urljoin(self._base_url(), self.WORKSPACES_PATH.lstrip("/")),
            data={},
            headers=self._ajax_headers(),
            timeout=self._timeout_seconds(),
        )
        response.raise_for_status()
        payload = self._parse_json(response, label="workspaces Point")
        try:
            accounts = json.loads(payload.get("json") or "[]")
        except json.JSONDecodeError as exc:
            raise ExtractionError("Point devolvió workspaces inválidos.") from exc
        return [account for account in accounts if account.get("ACC_ID")]

    def _current_account_id(self, session: requests.Session) -> str | None:
        response = session.get(urljoin(self._base_url(), "Account/workSpaces"), timeout=self._timeout_seconds())
        response.raise_for_status()
        text = str(getattr(response, "text", "") or "")
        match = re.search(r"accIdActual\s*=\s*'([^']+)'", text)
        if match:
            return match.group(1).strip() or None
        match = re.search(r"accIdActual\s*=\s*\"([^\"]+)\"", text)
        if match:
            return match.group(1).strip() or None
        # Algunas sesiones de Point no inyectan `accIdActual` ni devuelven
        # workspaces en el endpoint AJAX, pero el account id activo sigue
        # siendo estable para este tenant. Usamos el id conocido como último
        # fallback determinístico para no bloquear el backfill.
        return self.DEFAULT_ACCOUNT_ID

    def _resolve_account(
        self,
        *,
        accounts: list[dict],
        branch_external_id: str | None,
        branch_display_name: str | None,
        current_account_id: str | None,
    ) -> tuple[str, str | None]:
        if not accounts:
            raise AuthenticationError("Point no devolvió cuentas accesibles para el usuario configurado.")

        branch_token = str(branch_external_id or "").strip().lower()
        branch_name_token = normalize_text(branch_display_name or "")
        matched_workspace_name: str | None = None

        for account in accounts:
            try:
                workspaces = json.loads(account.get("JSON_WORKSPACES") or "[]")
            except json.JSONDecodeError:
                continue
            for workspace in workspaces:
                workspace_branch_id = str(workspace.get("id_suc") or "").strip().lower()
                workspace_name = str(workspace.get("wsName") or workspace.get("wsAvName") or "").strip()
                workspace_name_token = normalize_text(workspace_name)
                if branch_token and workspace_branch_id == branch_token:
                    return account["ACC_ID"], workspace_name or None
                if branch_name_token and workspace_name_token and branch_name_token == workspace_name_token:
                    return account["ACC_ID"], workspace_name or None

        if current_account_id:
            for account in accounts:
                if str(account.get("ACC_ID") or "").strip() == str(current_account_id).strip():
                    return str(account["ACC_ID"]).strip(), matched_workspace_name

        for account in accounts:
            if str(account.get("ACC_ID") or "").strip() == self.DEFAULT_ACCOUNT_ID:
                return str(account["ACC_ID"]).strip(), matched_workspace_name

        return str(accounts[0]["ACC_ID"]).strip(), matched_workspace_name

    def _select_account(
        self,
        *,
        session: requests.Session,
        account_id: str,
        branch_external_id: str | None,
        branch_display_name: str | None,
    ) -> None:
        set_current = session.post(
            urljoin(self._base_url(), "Account/SetCurrentAccount"),
            data={"accId": account_id},
            headers=self._ajax_headers(),
            timeout=self._timeout_seconds(),
        )
        set_current.raise_for_status()
        set_current_payload = self._parse_json(set_current, label="selección de cuenta activa Point")
        if not set_current_payload.get("success"):
            raise AuthenticationError("Point no confirmó la selección de la cuenta activa.")
        session.get(urljoin(self._base_url(), "Home/Index"), timeout=self._timeout_seconds())

        payload = {"acid": account_id}
        branch_id = str(branch_external_id or "").strip()
        branch_name = str(branch_display_name or "").strip()
        if branch_id:
            payload["sucid"] = branch_id
        if branch_name:
            payload["sucname"] = branch_name
        response = session.post(
            urljoin(self._base_url(), self.ACCOUNT_TOKEN_PATH.lstrip("/")),
            data=payload,
            headers=self._ajax_headers(),
            timeout=self._timeout_seconds(),
        )
        response.raise_for_status()
        payload = self._parse_json(response, label="selección de workspace Point")
        redirect_to = payload.get("redirectToUrl")
        if not redirect_to:
            raise AuthenticationError("Point no devolvió redirectToUrl al seleccionar la cuenta.")
        follow_up = session.get(urljoin(self._base_url(), redirect_to.lstrip("/")), timeout=self._timeout_seconds())
        follow_up.raise_for_status()

    def create(
        self,
        *,
        branch_external_id: str | None = None,
        branch_display_name: str | None = None,
    ) -> PointAuthenticatedSession:
        session = requests.Session()
        session.headers.update(self._ajax_headers())
        self._sign_in(session)
        accounts = self._get_workspaces(session)
        current_account_id = self._current_account_id(session)
        account_id, resolved_name = self._resolve_account(
            accounts=accounts,
            branch_external_id=branch_external_id,
            branch_display_name=branch_display_name,
            current_account_id=current_account_id,
        )
        self._select_account(
            session=session,
            account_id=account_id,
            branch_external_id=branch_external_id,
            branch_display_name=branch_display_name or resolved_name,
        )
        return PointAuthenticatedSession(
            session=session,
            account_id=account_id,
            workspace_name=resolved_name or "",
        )
