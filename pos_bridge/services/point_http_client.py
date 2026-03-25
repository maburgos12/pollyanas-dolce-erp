from __future__ import annotations

import json
from typing import Any

import requests

from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError, ExtractionError
from pos_bridge.utils.helpers import normalize_text


class PointHttpSessionClient:
    """Cliente HTTP determinístico para Point sin automatización visual."""

    def __init__(self, settings):
        self.settings = settings
        self.session = requests.Session()
        self._workspace = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self) -> None:
        self.session.close()

    def _url(self, path: str) -> str:
        base = (self.settings.base_url or "").rstrip("/")
        return f"{base}/{path.lstrip('/')}"

    def _parse_json(self, response: requests.Response, *, label: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise ExtractionError(
                f"Point devolvió una respuesta no JSON en {label}.",
                context={"status_code": response.status_code, "body_preview": response.text[:500]},
            ) from exc

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        timeout = kwargs.pop("timeout", max(self.settings.timeout_ms // 1000, 5))
        response = self.session.request(method, self._url(path), timeout=timeout, **kwargs)
        response.raise_for_status()
        return response

    def login(self, *, branch_hint: str | None = None) -> dict:
        if not self.settings.base_url:
            raise ConfigurationError("Falta POINT_BASE_URL para abrir Point.")
        if not self.settings.username or not self.settings.password:
            raise ConfigurationError("Faltan POINT_USERNAME y/o POINT_PASSWORD para autenticar Point.")

        response = self._request(
            "POST",
            "/Account/SignIn_click",
            data={
                "user": self.settings.username,
                "pass": self.settings.password,
                "timeZone": -7,
            },
        )
        payload = self._parse_json(response, label="login Point")
        redirect = str(payload.get("redirectToUrl") or "").strip()
        if not redirect:
            raise AuthenticationError("Point no devolvió redirectToUrl al autenticarse.", context={"payload": payload})

        workspaces_payload = self._fetch_workspaces_payload()
        workspace = self._select_workspace(workspaces_payload, branch_hint=branch_hint)
        acctok_response = self._request(
            "POST",
            "/Account/get_acctok",
            data={
                "acid": workspace["account_id"],
                "sucid": workspace["branch_id"],
                "sucname": workspace["branch_name"],
            },
        )
        acctok_payload = self._parse_json(acctok_response, label="selección de workspace Point")
        next_url = str(acctok_payload.get("redirectToUrl") or "").strip() or "/Home/Index"
        self._request("GET", next_url)
        self._workspace = workspace
        return workspace

    def _fetch_workspaces_payload(self) -> list[dict]:
        response = self._request("POST", "/Account/get_workSpaces", data={})
        payload = self._parse_json(response, label="workspaces Point")
        raw_json = payload.get("json")
        if not raw_json:
            raise AuthenticationError("Point no devolvió workspaces tras autenticar.", context={"payload": payload})
        try:
            accounts = json.loads(raw_json)
        except (TypeError, ValueError) as exc:
            raise AuthenticationError("No se pudo parsear el catálogo de workspaces Point.") from exc
        if not isinstance(accounts, list):
            raise AuthenticationError("El catálogo de workspaces Point tiene formato inesperado.")
        return accounts

    def _select_workspace(self, accounts: list[dict], *, branch_hint: str | None = None) -> dict:
        candidates: list[dict] = []
        for account in accounts:
            try:
                workspaces = json.loads(account.get("JSON_WORKSPACES") or "[]")
            except (TypeError, ValueError):
                workspaces = []
            for workspace in workspaces:
                candidates.append(
                    {
                        "account_id": account.get("ACC_ID"),
                        "account_name": account.get("ACC_NAME") or "",
                        "branch_id": workspace.get("id_suc"),
                        "branch_name": workspace.get("wsName") or workspace.get("wsAvName") or "",
                        "workspace_id": workspace.get("wsID") or "",
                    }
                )
        if not candidates:
            raise AuthenticationError("Point no devolvió sucursales disponibles para la cuenta de lectura.")

        if branch_hint:
            target = normalize_text(branch_hint)
            for candidate in candidates:
                haystack = normalize_text(f"{candidate['branch_name']} {candidate['account_name']}")
                if target and target in haystack:
                    return candidate
        return candidates[0]

    def get_products(
        self,
        *,
        categoria: str | None = None,
        familia: str | None = None,
        text_art: str = "",
        subcategorias: str | None = None,
        activo: bool = True,
    ) -> list[dict]:
        response = self._request(
            "GET",
            "/Catalogos/get_productos",
            params={
                "categoria": categoria,
                "familia": familia,
                "textArt": text_art,
                "subcategorias": subcategorias,
                "activo": str(bool(activo)).lower(),
            },
        )
        try:
            return json.loads(response.text)
        except ValueError as exc:
            raise ExtractionError(
                "Point devolvió un catálogo de productos inválido.",
                context={"body_preview": response.text[:500]},
            ) from exc

    def get_product_detail(self, product_id: int | str) -> dict:
        response = self._request("GET", "/Catalogos/get_producto_byID", params={"id_producto": product_id})
        return self._parse_json(response, label="detalle de producto Point")

    def get_product_bom(self, product_id: int | str) -> list[dict]:
        response = self._request("GET", "/Catalogos/getBomsByProducts", params={"pkProducto": product_id})
        try:
            data = json.loads(response.text)
        except ValueError as exc:
            raise ExtractionError(
                "Point devolvió un BOM inválido.",
                context={"product_id": product_id, "body_preview": response.text[:500]},
            ) from exc
        if not isinstance(data, list):
            raise ExtractionError("Point devolvió un BOM con formato inesperado.", context={"product_id": product_id})
        return data

    def get_articulos(self, *, search: str = "", category: int | str | None = None) -> list[dict]:
        response = self._request(
            "GET",
            "/Catalogos/get_articulos",
            params={
                "art": search,
                "cat": category,
            },
        )
        try:
            data = json.loads(response.text)
        except ValueError as exc:
            raise ExtractionError(
                "Point devolvió un catálogo de insumos inválido.",
                context={"body_preview": response.text[:500], "search": search},
            ) from exc
        if not isinstance(data, list):
            raise ExtractionError("Point devolvió un catálogo de insumos con formato inesperado.", context={"search": search})
        return data

    def get_articulo_detail(self, articulo_id: int | str) -> dict:
        response = self._request("GET", "/Catalogos/ArticuloGetbyid", params={"pkArticulo": articulo_id})
        return self._parse_json(response, label="detalle de insumo Point")
