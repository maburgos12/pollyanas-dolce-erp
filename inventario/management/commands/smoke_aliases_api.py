from __future__ import annotations

import json
import os
import ssl
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from django.core.management.base import BaseCommand, CommandError


@dataclass
class HttpResult:
    status: int
    data: dict[str, Any]
    raw: str
    headers: dict[str, str]


def _http_json(
    *,
    method: str,
    url: str,
    token: str | None,
    payload: dict[str, Any] | None = None,
    timeout: int = 25,
    insecure: bool = False,
) -> HttpResult:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Token {token}"
    req = Request(url=url, data=body, method=method.upper(), headers=headers)
    ssl_ctx = None
    if insecure:
        ssl_ctx = ssl._create_unverified_context()
    try:
        with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                data = {}
            return HttpResult(
                status=int(resp.status),
                data=data,
                raw=raw,
                headers={k.lower(): v for k, v in dict(resp.headers).items()},
            )
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        data = {}
        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            data = {}
        return HttpResult(
            status=int(exc.code),
            data=data,
            raw=raw,
            headers={k.lower(): v for k, v in dict(exc.headers or {}).items()},
        )
    except URLError as exc:
        raise CommandError(f"No se pudo conectar a {url}: {exc}") from exc


class Command(BaseCommand):
    help = "Smoke test de endpoints API de aliases/pendientes de homologación de inventario."

    def add_arguments(self, parser):
        parser.add_argument(
            "--base-url",
            default=os.environ.get("ERP_BASE_URL", "https://pollyanas-dolce-erp-production.up.railway.app"),
            help="URL base del ERP (ej: https://...up.railway.app)",
        )
        parser.add_argument(
            "--token",
            default=os.environ.get("ERP_API_TOKEN", ""),
            help="Token DRF. Si no se envía, usar --username y --password para obtenerlo.",
        )
        parser.add_argument("--username", default=os.environ.get("ERP_API_USER", ""), help="Usuario para auth/token")
        parser.add_argument(
            "--password",
            default=os.environ.get("ERP_API_PASSWORD", ""),
            help="Contraseña para auth/token",
        )
        parser.add_argument("--timeout", type=int, default=25, help="Timeout HTTP por request en segundos")
        parser.add_argument(
            "--insecure",
            action="store_true",
            help="Desactiva validación TLS (solo diagnóstico/entornos controlados).",
        )

    def _build_url(self, base_url: str, path: str, query: dict[str, Any] | None = None) -> str:
        root = base_url.rstrip("/")
        qp = ""
        if query:
            qp = "?" + urlencode(query)
        return f"{root}{path}{qp}"

    def _assert_ok(self, label: str, result: HttpResult, expected: int = 200):
        if result.status != expected:
            detail = result.data.get("detail") if isinstance(result.data, dict) else ""
            raise CommandError(
                f"{label} falló: status={result.status} esperado={expected}. detail={detail or result.raw[:200]}"
            )

    def _get_token(self, base_url: str, username: str, password: str, timeout: int, insecure: bool) -> str:
        if not username or not password:
            raise CommandError("Sin --token, debes enviar --username y --password.")
        auth_url = self._build_url(base_url, "/api/auth/token/")
        result = _http_json(
            method="POST",
            url=auth_url,
            token=None,
            payload={"username": username, "password": password},
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Auth token", result, expected=200)
        token = str(result.data.get("token") or "").strip()
        if not token:
            raise CommandError("Respuesta de auth/token sin token.")
        return token

    def handle(self, *args, **options):
        base_url = str(options["base_url"]).strip()
        timeout = int(options["timeout"])
        insecure = bool(options.get("insecure"))
        token = str(options["token"] or "").strip()
        username = str(options["username"] or "").strip()
        password = str(options["password"] or "").strip()

        if not token:
            token = self._get_token(
                base_url=base_url,
                username=username,
                password=password,
                timeout=timeout,
                insecure=insecure,
            )
            self.stdout.write(self.style.SUCCESS("Token obtenido por /api/auth/token/."))

        health = _http_json(
            method="GET",
            url=self._build_url(base_url, "/health/"),
            token=None,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Health", health, expected=200)

        pendientes = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes/",
                query={"limit": 20, "offset": 0, "runs": 5, "runs_detail": 3, "include_runs": 1, "point_tipo": "TODOS"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Aliases pendientes", pendientes, expected=200)
        filters = pendientes.data.get("filters") or {}
        for key in ("limit", "offset", "runs", "runs_detail", "include_runs", "source", "point_tipo"):
            if key not in filters:
                raise CommandError(f"Aliases pendientes sin filters.{key}.")
        if "recent_runs" not in pendientes.data:
            raise CommandError("Aliases pendientes sin bloque recent_runs.")
        if "pagination" not in pendientes.data:
            raise CommandError("Aliases pendientes sin bloque pagination.")
        pagination = pendientes.data.get("pagination") or {}
        for key in ("limit", "offset", "almacen", "point", "recetas"):
            if key not in pagination:
                raise CommandError(f"Aliases pendientes sin pagination.{key}.")
        if "totales" not in pendientes.data or "items" not in pendientes.data:
            raise CommandError("Aliases pendientes no devolvió estructura base totales/items.")

        pendientes_filtered = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes/",
                query={"q": "fresa", "source": "POINT", "point_tipo": "TODOS", "limit": 10},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Aliases pendientes filtrado", pendientes_filtered, expected=200)
        f2 = pendientes_filtered.data.get("filters") or {}
        if str(f2.get("source") or "") != "POINT":
            raise CommandError("Aliases pendientes filtrado no devolvió source=POINT en filters.")
        if str(f2.get("q") or "") != "fresa":
            raise CommandError("Aliases pendientes filtrado no devolvió q=fresa en filters.")

        pendientes_csv = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes/",
                query={"export": "csv", "limit": 20, "runs": 5},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Aliases pendientes CSV", pendientes_csv, expected=200)
        if "seccion,id,tipo,codigo,nombre" not in pendientes_csv.raw:
            raise CommandError("Aliases pendientes CSV no devolvió cabecera esperada.")

        pendientes_xlsx = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes/",
                query={"export": "xlsx", "limit": 20, "runs": 5},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Aliases pendientes XLSX", pendientes_xlsx, expected=200)
        xlsx_content_type = str(pendientes_xlsx.headers.get("content-type", "")).lower()
        if "spreadsheetml" not in xlsx_content_type:
            raise CommandError("Aliases pendientes XLSX no devolvió content-type esperado.")

        unificados = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes-unificados/",
                query={"limit": 10, "offset": 0, "sort_by": "score_max", "sort_dir": "desc"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Aliases pendientes unificados", unificados, expected=200)
        p_filters = unificados.data.get("filters") or {}
        p_pag = unificados.data.get("pagination") or {}
        for key in ("limit", "offset", "sort_by", "sort_dir"):
            if key not in p_filters:
                raise CommandError(f"Unificados sin filters.{key}.")
        for key in ("has_next", "next_offset", "has_prev", "prev_offset", "limit", "offset"):
            if key not in p_pag:
                raise CommandError(f"Unificados sin pagination.{key}.")

        unificados_source = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes-unificados/",
                query={"limit": 10, "offset": 0, "source": "POINT", "min_sources": 1},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Unificados source", unificados_source, expected=200)
        source_filters = unificados_source.data.get("filters") or {}
        if str(source_filters.get("source") or "") != "POINT":
            raise CommandError("Unificados source no devolvió source=POINT en filters.")

        resolve_dry = _http_json(
            method="POST",
            url=self._build_url(base_url, "/api/inventario/aliases/pendientes-unificados/resolver/"),
            token=token,
            payload={
                "dry_run": True,
                "min_sources": 2,
                "score_min": 0,
                "only_suggested": True,
                "source": "POINT",
                "sort_by": "score_max",
                "sort_dir": "desc",
                "offset": 0,
                "limit": 20,
                "runs": 5,
            },
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Unificados resolver dry_run", resolve_dry, expected=200)
        resolve_totals = resolve_dry.data.get("totales") or {}
        resolve_filters = resolve_dry.data.get("filters") or {}
        if str(resolve_filters.get("source") or "") != "POINT":
            raise CommandError("Resolver dry_run no devolvió source=POINT en filters.")
        if str(resolve_filters.get("sort_by") or "") != "score_max":
            raise CommandError("Resolver dry_run no devolvió sort_by=score_max en filters.")
        if str(resolve_filters.get("sort_dir") or "") != "desc":
            raise CommandError("Resolver dry_run no devolvió sort_dir=desc en filters.")
        try:
            resolve_offset = int(resolve_filters.get("offset"))
        except (TypeError, ValueError):
            resolve_offset = -1
        if resolve_offset != 0:
            raise CommandError("Resolver dry_run no devolvió offset=0 en filters.")
        if "aliases_creados_preview" not in resolve_totals:
            raise CommandError("Resolver dry_run no devolvió totales.aliases_creados_preview.")
        if "aliases_actualizados_preview" not in resolve_totals:
            raise CommandError("Resolver dry_run no devolvió totales.aliases_actualizados_preview.")

        unificados_csv = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes-unificados/",
                query={"export": "csv", "limit": 20},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Unificados CSV", unificados_csv, expected=200)
        if "nombre_muestra,nombre_normalizado" not in unificados_csv.raw:
            raise CommandError("Unificados CSV no devolvió cabecera esperada.")

        unificados_xlsx = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/inventario/aliases/pendientes-unificados/",
                query={"export": "xlsx", "limit": 20},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Unificados XLSX", unificados_xlsx, expected=200)
        ux_content_type = str(unificados_xlsx.headers.get("content-type", "")).lower()
        if "spreadsheetml" not in ux_content_type:
            raise CommandError("Unificados XLSX no devolvió content-type esperado.")

        output = {
            "base_url": base_url,
            "smoke": {
                "health": {"status": health.status},
                "pendientes": {
                    "status": pendientes.status,
                    "totales": pendientes.data.get("totales", {}),
                    "recent_runs": len(pendientes.data.get("recent_runs") or []),
                },
                "pendientes_filtered": {
                    "status": pendientes_filtered.status,
                    "filters": pendientes_filtered.data.get("filters", {}),
                },
                "pendientes_export_csv": {"status": pendientes_csv.status},
                "pendientes_export_xlsx": {"status": pendientes_xlsx.status},
                "unificados": {
                    "status": unificados.status,
                    "summary": unificados.data.get("summary", {}),
                    "pagination": unificados.data.get("pagination", {}),
                },
                "unificados_source": {
                    "status": unificados_source.status,
                    "filters": source_filters,
                },
                "resolver_dry_run": {
                    "status": resolve_dry.status,
                    "filters": resolve_filters,
                    "totales": resolve_totals,
                },
                "unificados_export_csv": {"status": unificados_csv.status},
                "unificados_export_xlsx": {"status": unificados_xlsx.status},
            },
        }
        self.stdout.write(self.style.SUCCESS("Smoke aliases API OK"))
        self.stdout.write(json.dumps(output, ensure_ascii=False, indent=2, default=str))
