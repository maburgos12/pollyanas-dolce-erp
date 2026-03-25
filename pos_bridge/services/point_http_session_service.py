from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urljoin

import requests

from pos_bridge.utils.exceptions import AuthenticationError, ExtractionError


@dataclass
class PointAuthenticatedSession:
    session: requests.Session


class PointHttpSessionService:
    SIGN_IN_PATH = "/Account/SignIn_click"
    WORKSPACES_PATH = "/Account/get_workSpaces"
    ACCOUNT_TOKEN_PATH = "/Account/get_acctok"

    def __init__(self, bridge_settings):
        self.settings = bridge_settings

    def _base_url(self) -> str:
        return self.settings.base_url.rstrip("/") + "/"

    def _timeout_seconds(self) -> float:
        return self.settings.timeout_ms / 1000

    def _sign_in(self, session: requests.Session) -> None:
        response = session.post(
            urljoin(self._base_url(), self.SIGN_IN_PATH.lstrip("/")),
            data={
                "user": self.settings.username,
                "pass": self.settings.password,
                # Point espera UTC offset en horas. Phoenix mantiene UTC-7.
                "timeZone": "-7",
            },
            timeout=self._timeout_seconds(),
        )
        response.raise_for_status()
        payload = response.json()
        redirect_to = str(payload.get("redirectToUrl") or "").strip()
        if not redirect_to:
            raise AuthenticationError("Point no devolvió redirectToUrl al autenticar.")

    def _get_workspaces(self, session: requests.Session) -> list[dict]:
        response = session.post(
            urljoin(self._base_url(), self.WORKSPACES_PATH.lstrip("/")),
            data={},
            timeout=self._timeout_seconds(),
        )
        response.raise_for_status()
        payload = response.json()
        try:
            accounts = json.loads(payload.get("json") or "[]")
        except json.JSONDecodeError as exc:
            raise ExtractionError("Point devolvió workspaces inválidos.") from exc
        return [account for account in accounts if account.get("ACC_ID")]

    def _resolve_account(
        self,
        *,
        accounts: list[dict],
        branch_external_id: str | None,
    ) -> tuple[str, str | None]:
        if branch_external_id:
            branch_token = str(branch_external_id).strip().lower()
            for account in accounts:
                try:
                    workspaces = json.loads(account.get("JSON_WORKSPACES") or "[]")
                except json.JSONDecodeError:
                    continue
                for workspace in workspaces:
                    if str(workspace.get("id_suc")).strip().lower() == branch_token:
                        return account["ACC_ID"], str(workspace.get("wsName") or "").strip() or None
        if not accounts:
            raise AuthenticationError("Point no devolvió cuentas accesibles para el usuario configurado.")
        return accounts[0]["ACC_ID"], None

    def _select_account(
        self,
        *,
        session: requests.Session,
        account_id: str,
        branch_external_id: str | None,
        branch_display_name: str | None,
    ) -> None:
        payload = {"acid": account_id}
        if branch_external_id:
            payload["sucid"] = str(branch_external_id)
        if branch_display_name:
            payload["sucname"] = branch_display_name
        response = session.post(
            urljoin(self._base_url(), self.ACCOUNT_TOKEN_PATH.lstrip("/")),
            data=payload,
            timeout=self._timeout_seconds(),
        )
        response.raise_for_status()
        redirect_to = response.json().get("redirectToUrl")
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
        self._sign_in(session)
        accounts = self._get_workspaces(session)
        account_id, resolved_name = self._resolve_account(accounts=accounts, branch_external_id=branch_external_id)
        self._select_account(
            session=session,
            account_id=account_id,
            branch_external_id=branch_external_id,
            branch_display_name=branch_display_name or resolved_name,
        )
        return PointAuthenticatedSession(session=session)
