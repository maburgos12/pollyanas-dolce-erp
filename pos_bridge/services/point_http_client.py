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
            except (AuthenticationError, ExtractionError, requests.RequestException) as exc:
                last_error = exc
                preview = str(getattr(exc, "context", {}).get("body_preview") or "")
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                reentry_required = (
                    "Sesión Expirada" in preview
                    or "Sesion Expirada" in preview
                    or "Session Expired" in preview
                    or (isinstance(status_code, int) and status_code >= 500)
                )
                if attempt >= attempts or not reentry_required:
                    raise
                self._audit(
                    "point_relogin",
                    message="Point no completó la sesión; se volverá a ingresar y se reintentará.",
                    context={
                        "attempt": attempt,
                        "branch_hint": branch_hint or "",
                        "status_code": status_code,
                    },
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
        return self._catalog_rows(
            "/Catalogos/get_productos",
            params={
                "categoria": categoria,
                "familia": familia,
                "textArt": text_art,
                "subcategorias": subcategorias,
                "activo": str(bool(activo)).lower(),
            },
        )

    def _catalog_rows(self, path: str, *, params: dict | None = None) -> list[dict]:
        """Lectura con recuperación acotada de sesiones que devuelven HTML con HTTP 200."""
        attempts = max(1, int(getattr(self.settings, "retry_attempts", 1) or 1))
        for attempt in range(1, attempts + 1):
            try:
                response = self._request("GET", path, params=params or {})
                rows = self._parse_json(response, label=path)
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise ExtractionError("Point devolvió un catálogo con formato inesperado.", context={"path": path})
                return rows
            except (ExtractionError, requests.RequestException):
                if attempt == attempts:
                    raise
                self._audit(
                    "point_catalog_relogin",
                    message="Respuesta de catálogo inválida; recuperando la sesión de Point.",
                    context={"path": path, "attempt": attempt},
                )
                self._reset_session()
                self.login(branch_hint=getattr(self, "_last_branch_hint", None))

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

    def get_stock_history(
        self,
        product_id: int | str,
        branch_id: int | str,
        *,
        movements: int = 500,
    ) -> list[dict]:
        response = self._request(
            "GET",
            "/Stock/GetHistorial",
            params={
                "tipo": "false",
                "almacen": str(branch_id),
                "pkproducto": str(product_id),
                "movimientos": str(movements),
                "tipoMovimiento": "",
            },
        )
        payload = self._parse_json(response, label="historial de existencias Point")
        if not isinstance(payload, list):
            raise ExtractionError(
                "Point devolvió un historial de existencias con formato inesperado.",
                context={"product_id": product_id, "branch_id": branch_id},
            )
        return payload

    def get_insumo_categories(self, *, timeout: int | float | None = None) -> list[dict]:
        response = self._request(
            "GET",
            "/Catalogos/get_insumos",
            **({"timeout": timeout} if timeout is not None else {}),
        )
        payload = self._parse_json(response, label="categorías de insumos Point")
        if not isinstance(payload, list):
            raise ExtractionError("Point devolvió categorías de insumos con formato inesperado.")
        return payload

    def get_branch_insumos(
        self,
        *,
        branch_id: int | str,
        category_id: int | str,
        timeout: int | float | None = None,
    ) -> list[dict]:
        response = self._request(
            "GET",
            "/Stock/GetInsumosPA",
            params={"almacen": branch_id, "categoria": category_id},
            **({"timeout": timeout} if timeout is not None else {}),
        )
        payload = self._parse_json(response, label="existencia oficial de insumos Point")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError as exc:
                raise ExtractionError(
                    "Point devolvió una existencia oficial de insumos inválida.",
                    context={"branch_id": branch_id, "category_id": category_id},
                ) from exc
        if not isinstance(payload, list):
            raise ExtractionError(
                "Point devolvió una existencia oficial de insumos con formato inesperado.",
                context={"branch_id": branch_id, "category_id": category_id},
            )
        return payload

    def get_product_bom(self, product_id: int | str) -> list[dict]:
        return self._catalog_rows("/Catalogos/getBomsByProducts", params={"pkProducto": product_id})

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
        """Catálogo por familias oficiales; solo refina familias que saturen el tope."""
        cache = getattr(self, "_catalog_cache", None)
        if cache is None:
            cache = self._catalog_cache = {}
        cache_key = ("productos", tuple(sorted(kwargs.items())))
        if cache_key not in cache:
            rows = self.get_products(**kwargs)
            if len(rows) >= self.CATALOG_PAGE_LIMIT:
                families = self._catalog_rows("/Catalogos/get_familias")
                family_ids = {str(row["ID_Familia"]) for row in families if row.get("ID_Familia") is not None}
                if len(family_ids) != len(families) or not family_ids:
                    raise ExtractionError("Point no devolvió un catálogo de familias válido.")
                found = {row["PK_Producto"]: row for row in rows}
                if kwargs.get("familia") is not None:
                    family_ids = {str(kwargs["familia"])}
                for family_id in sorted(family_ids):
                    filters = {**kwargs, "familia": family_id}
                    family_rows = self.get_products(**filters)
                    if any(str(row.get("ID_Familia")) != family_id for row in family_rows):
                        raise ExtractionError("Point no respetó el filtro de familia; no se confirmó el catálogo completo.")
                    found.update({row["PK_Producto"]: row for row in family_rows})
                    if len(family_rows) >= self.CATALOG_PAGE_LIMIT:
                        family_rows = self._enumerate_catalog(
                            lambda term: self.get_products(text_art=term, **filters),
                            pk_field="PK_Producto", label=f"productos de familia {family_id}",
                        )
                    found.update({row["PK_Producto"]: row for row in family_rows})
                # Si Point admite productos sin familia, conservar el recorrido
                # general para no descartarlos al particionar el catálogo.
                if any(str(row.get("ID_Familia")) not in family_ids for row in rows):
                    found.update({row["PK_Producto"]: row for row in self._enumerate_catalog(
                        lambda term: self.get_products(text_art=term, **kwargs),
                        pk_field="PK_Producto", label="productos sin familia",
                    )})
                rows = list(found.values())
            cache[cache_key] = rows
            self._audit("catalog_enumeration", message="Catálogo de productos consultado.", context={"total": len(rows)})
        return list(cache[cache_key])

    def get_all_articulos(self, *, category: int | str | None = None) -> list[dict]:
        """Catálogo completo de insumos/artículos (rodea el tope de 150 filas)."""
        cache = getattr(self, "_catalog_cache", None)
        if cache is None:
            cache = self._catalog_cache = {}
        cache_key = ("insumos", category)
        if cache_key not in cache:
            cache[cache_key] = self._enumerate_catalog(
                lambda term: self.get_articulos(search=term, category=category),
                pk_field="PK_Articulo",
                label="insumos",
            )
        return list(cache[cache_key])

    def _enumerate_catalog(
        self,
        fetch,
        *,
        pk_field: str,
        label: str,
        page_limit: int | None = None,
        max_term_len: int = 4,
        max_failures: int = 10,
        pause_seconds: float = 0.05,
        relogin_every: int = 400,
    ) -> list[dict]:
        limit = page_limit or self.CATALOG_PAGE_LIMIT
        found: dict[object, dict] = {}
        pending = list(self._ENUM_SEEDS)
        queries = 0
        failures = 0
        while pending:
            term = pending.pop()
            if relogin_every and queries and queries % relogin_every == 0:
                # La sesión de Point muere a ~50 min (visto en producción:
                # respuestas vacías al final de una enumeración larga):
                # relogin PROACTIVO antes de que caduque.
                try:
                    self.login(branch_hint=getattr(self, "_last_branch_hint", None))
                except (AuthenticationError, ExtractionError):
                    pass  # el camino reactivo de abajo lo cubre
            try:
                rows = fetch(term)
            except ExtractionError:
                # Point regresa 500s transitorios cuando se le consulta muy
                # seguido (visto en producción a ~1,500 consultas): backoff,
                # relogin al mismo workspace y re-encolar el término para no
                # perder cobertura. Solo se aborta si el patrón persiste.
                failures += 1
                if failures > max_failures:
                    raise
                time.sleep(min(failures * 2, 15))
                try:
                    self.login(branch_hint=getattr(self, "_last_branch_hint", None))
                except (AuthenticationError, ExtractionError):
                    time.sleep(min(failures * 5, 30))
                pending.append(term)
                continue
            queries += 1
            for row in rows:
                pk = row.get(pk_field)
                if pk is not None:
                    found[pk] = row
            if len(rows) >= limit and len(term) < max_term_len:
                pending.extend(term + char for char in self._ENUM_REFINE)
            if pause_seconds:
                time.sleep(pause_seconds)
        self._audit(
            "catalog_enumeration",
            message=f"catálogo de {label} enumerado completo",
            context={"total": len(found), "queries": queries, "failures": failures},
        )
        return list(found.values())
