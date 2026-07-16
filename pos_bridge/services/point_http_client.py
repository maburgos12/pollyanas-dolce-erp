from __future__ import annotations

import json
import re
import time
from typing import Any

import requests

from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError, ExtractionError
from pos_bridge.utils.helpers import normalize_text


class PointHttpSessionClient:
    """Cliente HTTP determinístico para Point sin automatización visual."""

    DEFAULT_ACCOUNT_ID = "83852AED-D4FB-E611-814F-06B55B5505BA"

    def __init__(self, settings, *, audit_callback=None):
        self.settings = settings
        self.audit_callback = audit_callback
        self.session = self._build_session()
        self._workspace = None

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"X-Requested-With": "XMLHttpRequest"})
        return session

    def _reset_session(self) -> None:
        self.session.close()
        self.session = self._build_session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self) -> None:
        self.session.close()

    def _audit(self, event: str, *, message: str, context: dict | None = None) -> None:
        if self.audit_callback is None:
            return
        self.audit_callback(event=event, message=message, context=context or {})

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
        attempts = max(1, int(getattr(self.settings, "retry_attempts", 1) or 1))
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                response = self.session.request(method, self._url(path), timeout=timeout, **kwargs)
                if response.status_code >= 500 and attempt < attempts:
                    self._audit(
                        "point_http_retry",
                        message="Point devolvió 5xx transitorio; se reintentará la solicitud.",
                        context={"path": path, "method": method, "attempt": attempt, "status_code": response.status_code},
                    )
                    time.sleep(min(attempt, 3))
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= attempts:
                    raise
                self._audit(
                    "point_http_retry",
                    message="Solicitud HTTP a Point falló de forma transitoria; se reintentará.",
                    context={"path": path, "method": method, "attempt": attempt, "error": str(exc)},
                )
                time.sleep(min(attempt, 3))

        if last_error is not None:
            raise last_error
        raise ExtractionError(f"Point no respondió correctamente en {path}.")

    def login(self, *, branch_hint: str | None = None) -> dict:
        # Recordado para que un relogin automático (p.ej. a media enumeración
        # de catálogo) regrese al MISMO workspace y no al default.
        self._last_branch_hint = branch_hint
        if not self.settings.base_url:
            raise ConfigurationError("Falta POINT_BASE_URL para abrir Point.")
        if not self.settings.username or not self.settings.password:
            raise ConfigurationError("Faltan POINT_USERNAME y/o POINT_PASSWORD para autenticar Point.")

        attempts = max(1, int(getattr(self.settings, "retry_attempts", 1) or 1))
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                return self._login_once(branch_hint=branch_hint)
            except (AuthenticationError, ExtractionError) as exc:
                last_error = exc
                preview = str(getattr(exc, "context", {}).get("body_preview") or "")
                session_expired = "Sesión Expirada" in preview or "Sesion Expirada" in preview or "Session Expired" in preview
                if attempt >= attempts or not session_expired:
                    raise
                self._audit(
                    "point_relogin",
                    message="Point devolvió sesión expirada durante login; se reiniciará la sesión y se reintentará.",
                    context={"attempt": attempt, "branch_hint": branch_hint or ""},
                )
                self._reset_session()
                time.sleep(min(attempt, 3))

        if last_error is not None:
            raise last_error
        raise AuthenticationError("Point no permitió iniciar sesión.")

    def _login_once(self, *, branch_hint: str | None = None) -> dict:
        # Igualamos el flujo real del navegador: abrir Point antes del POST AJAX
        # de login para que se inicialicen cookies/contexto de sesión.
        self._request("GET", "/")

        response = self._request(
            "POST",
            "/Account/SignIn_click",
            data={
                "user": self.settings.username,
                "pass": self.settings.password,
                # La UI real de Point envía timeZone=0 en SignIn_click.
                "timeZone": 0,
            },
        )
        payload = self._parse_json(response, label="login Point")
        redirect = str(payload.get("redirectToUrl") or "").strip()
        if not redirect:
            raise AuthenticationError("Point no devolvió redirectToUrl al autenticarse.", context={"payload": payload})

        accounts = self._fetch_workspaces_payload()
        current_account_id = self._extract_current_account_id()
        workspace = self._select_workspace(accounts, branch_hint=branch_hint)
        if not workspace.get("account_id"):
            workspace["account_id"] = current_account_id or self.DEFAULT_ACCOUNT_ID
        elif current_account_id and not branch_hint:
            workspace["account_id"] = current_account_id
        set_current = self._request(
            "POST",
            "/Account/SetCurrentAccount",
            data={
                "accId": workspace["account_id"],
            },
        )
        set_current_payload = self._parse_json(set_current, label="selección de cuenta activa Point")
        if not set_current_payload.get("success"):
            raise AuthenticationError("Point no confirmó la selección de la cuenta activa.", context={"payload": set_current_payload})
        self._request("GET", "/Home/Index")
        acctok_response = self._request(
            "POST",
            "/Account/get_acctok",
            data={
                "acid": workspace["account_id"],
                "sucid": workspace.get("branch_id"),
                "sucname": workspace.get("branch_name"),
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

    def _extract_current_account_id(self) -> str | None:
        response = self._request("GET", "/Account/workSpaces")
        match = re.search(r"accIdActual\\s*=\\s*'([^']+)'", response.text)
        if match:
            return match.group(1).strip() or None
        match = re.search(r"accIdActual\\s*=\\s*\"([^\"]+)\"", response.text)
        if match:
            return match.group(1).strip() or None
        return None

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

    def get_stock_products(self, *, text: str, timeout: int | float | None = None) -> list[dict]:
        response = self._request(
            "GET",
            "/Stock/get_productos_insumos",
            params={"texto": text},
            **({"timeout": timeout} if timeout is not None else {}),
        )
        payload = self._parse_json(response, label="búsqueda de productos stock Point")
        if not isinstance(payload, list):
            raise ExtractionError("Point devolvió una búsqueda de stock con formato inesperado.", context={"text": text})
        return payload

    def get_product_stock(self, product_id: int | str, *, timeout: int | float | None = None) -> list[dict]:
        response = self._request(
            "GET",
            "/Stock/get_productos_existencia",
            params={"pk": product_id},
            **({"timeout": timeout} if timeout is not None else {}),
        )
        payload = self._parse_json(response, label="existencia producto Point")
        if not isinstance(payload, list):
            raise ExtractionError(
                "Point devolvió una existencia de producto con formato inesperado.",
                context={"product_id": product_id},
            )
        return payload

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

    # ------------------------------------------------------------------
    # Catálogo completo — Point corta CADA respuesta de catálogo a 150
    # filas (verificado 2026-07-15: el catálogo real tiene 334 productos y
    # 353 insumos, pero get_products()/get_articulos() sin filtro solo
    # devuelven 150). Para ver el catálogo entero se enumera con términos
    # de búsqueda: si un término satura el tope, se refina agregando una
    # letra, hasta que ninguna consulta llegue al límite.
    # ------------------------------------------------------------------

    CATALOG_PAGE_LIMIT = 150
    _ENUM_SEEDS = "abcdefghijklmnopqrstuvwxyz0123456789"
    # El refinamiento incluye dígitos: hay insumos que solo se discriminan
    # por número ("AL-22", "1414", "Rp25").
    _ENUM_REFINE = "abcdefghijklmnopqrstuvwxyz0123456789 "

    def get_all_products(self, **kwargs) -> list[dict]:
        """Catálogo completo de productos (rodea el tope de 150 filas)."""
        return self._enumerate_catalog(
            lambda term: self.get_products(text_art=term, **kwargs),
            pk_field="PK_Producto",
            label="productos",
        )

    def get_all_articulos(self, *, category: int | str | None = None) -> list[dict]:
        """Catálogo completo de insumos/artículos (rodea el tope de 150 filas)."""
        return self._enumerate_catalog(
            lambda term: self.get_articulos(search=term, category=category),
            pk_field="PK_Articulo",
            label="insumos",
        )

    def _enumerate_catalog(
        self,
        fetch,
        *,
        pk_field: str,
        label: str,
        page_limit: int | None = None,
        max_term_len: int = 4,
        max_failures: int = 10,
    ) -> list[dict]:
        limit = page_limit or self.CATALOG_PAGE_LIMIT
        found: dict[object, dict] = {}
        pending = list(self._ENUM_SEEDS)
        queries = 0
        failures = 0
        while pending:
            term = pending.pop()
            try:
                rows = fetch(term)
            except ExtractionError:
                failures += 1
                if failures > max_failures:
                    raise
                # La sesión puede expirar a media enumeración: relogin al mismo
                # workspace y un reintento.
                self.login(branch_hint=getattr(self, "_last_branch_hint", None))
                try:
                    rows = fetch(term)
                except ExtractionError:
                    continue
            queries += 1
            for row in rows:
                pk = row.get(pk_field)
                if pk is not None:
                    found[pk] = row
            if len(rows) >= limit and len(term) < max_term_len:
                pending.extend(term + char for char in self._ENUM_REFINE)
        self._audit(
            "catalog_enumeration",
            message=f"catálogo de {label} enumerado completo",
            context={"total": len(found), "queries": queries, "failures": failures},
        )
        return list(found.values())
