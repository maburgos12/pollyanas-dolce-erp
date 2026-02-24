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
    help = "Ejecuta smoke operativo de endpoints API de integraciones (dry-run y opcionalmente live)."

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
        parser.add_argument("--live", action="store_true", help="Ejecuta también la corrida live (con efectos reales)")
        parser.add_argument(
            "--confirm-live",
            default="",
            help='Confirmación para live. Debe ser exactamente "YES".',
        )
        parser.add_argument("--idle-days", type=int, default=30)
        parser.add_argument("--idle-limit", type=int, default=100)
        parser.add_argument("--retain-days", type=int, default=90)
        parser.add_argument("--max-delete", type=int, default=5000)

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
        run_live = bool(options["live"])
        confirm_live = str(options["confirm_live"] or "").strip().upper()

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

        resumen = _http_json(
            method="GET",
            url=self._build_url(base_url, "/api/integraciones/point/resumen/"),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Resumen integraciones", resumen, expected=200)

        historial = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/integraciones/point/operaciones/historial/",
                query={"limit": 10, "offset": 0, "sort_by": "timestamp", "sort_dir": "desc"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Historial operaciones", historial, expected=200)
        filters = historial.data.get("filters") or {}
        pagination = historial.data.get("pagination") or {}
        if filters.get("offset") != 0:
            raise CommandError("Historial operaciones no devolvió offset=0 en filters.")
        if str(filters.get("sort_by") or "") != "timestamp":
            raise CommandError("Historial operaciones no devolvió sort_by=timestamp en filters.")
        if str(filters.get("sort_dir") or "") != "desc":
            raise CommandError("Historial operaciones no devolvió sort_dir=desc en filters.")
        for key in ("has_next", "next_offset", "has_prev", "prev_offset", "limit", "offset"):
            if key not in pagination:
                raise CommandError(f"Historial operaciones sin campo pagination.{key}.")

        historial_sorted = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/integraciones/point/operaciones/historial/",
                query={"limit": 3, "offset": 0, "sort_by": "action", "sort_dir": "asc"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Historial operaciones sort", historial_sorted, expected=200)
        sorted_filters = historial_sorted.data.get("filters") or {}
        if str(sorted_filters.get("sort_by") or "") != "action":
            raise CommandError("Historial operaciones sort no devolvió sort_by=action en filters.")
        if str(sorted_filters.get("sort_dir") or "") != "asc":
            raise CommandError("Historial operaciones sort no devolvió sort_dir=asc en filters.")

        historial_search = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/integraciones/point/operaciones/historial/",
                query={"limit": 3, "user": "admin", "model": "integraciones", "q": "API"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Historial operaciones search", historial_search, expected=200)
        search_filters = historial_search.data.get("filters") or {}
        if str(search_filters.get("user") or "") != "admin":
            raise CommandError("Historial operaciones search no devolvió user=admin en filters.")
        if str(search_filters.get("model") or "") != "integraciones":
            raise CommandError("Historial operaciones search no devolvió model=integraciones en filters.")
        if str(search_filters.get("q") or "") != "API":
            raise CommandError("Historial operaciones search no devolvió q=API en filters.")
        historial_csv = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/integraciones/point/operaciones/historial/",
                query={"limit": 10, "export": "csv"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Historial operaciones CSV", historial_csv, expected=200)
        if "timestamp,usuario,action,model,object_id,payload" not in historial_csv.raw:
            raise CommandError("Historial operaciones CSV no devolvió cabecera esperada.")
        historial_xlsx = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/integraciones/point/operaciones/historial/",
                query={"limit": 10, "export": "xlsx"},
            ),
            token=token,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Historial operaciones XLSX", historial_xlsx, expected=200)
        xlsx_content_type = str(historial_xlsx.headers.get("content-type", "")).lower()
        if "spreadsheetml" not in xlsx_content_type:
            raise CommandError("Historial operaciones XLSX no devolvió content-type esperado.")

        deactivate_dry = _http_json(
            method="POST",
            url=self._build_url(base_url, "/api/integraciones/point/clientes/desactivar-inactivos/"),
            token=token,
            payload={
                "idle_days": int(options["idle_days"]),
                "limit": int(options["idle_limit"]),
                "dry_run": True,
            },
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Deactivate dry_run", deactivate_dry, expected=200)

        purge_dry = _http_json(
            method="POST",
            url=self._build_url(base_url, "/api/integraciones/point/logs/purgar/"),
            token=token,
            payload={
                "retain_days": int(options["retain_days"]),
                "max_delete": int(options["max_delete"]),
                "dry_run": True,
            },
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Purge dry_run", purge_dry, expected=200)

        maintenance_dry = _http_json(
            method="POST",
            url=self._build_url(base_url, "/api/integraciones/point/mantenimiento/ejecutar/"),
            token=token,
            payload={
                "idle_days": int(options["idle_days"]),
                "idle_limit": int(options["idle_limit"]),
                "retain_days": int(options["retain_days"]),
                "max_delete": int(options["max_delete"]),
                "dry_run": True,
            },
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_ok("Maintenance dry_run", maintenance_dry, expected=200)

        maintenance_live = None
        if run_live:
            if confirm_live != "YES":
                raise CommandError('Para ejecutar --live debes confirmar con --confirm-live YES')
            maintenance_live = _http_json(
                method="POST",
                url=self._build_url(base_url, "/api/integraciones/point/mantenimiento/ejecutar/"),
                token=token,
                payload={
                    "idle_days": int(options["idle_days"]),
                    "idle_limit": int(options["idle_limit"]),
                    "retain_days": int(options["retain_days"]),
                    "max_delete": int(options["max_delete"]),
                    "dry_run": False,
                },
                timeout=timeout,
                insecure=insecure,
            )
            self._assert_ok("Maintenance live", maintenance_live, expected=200)

        output = {
            "base_url": base_url,
            "smoke": {
                "health": {"status": health.status},
                "resumen": {"status": resumen.status},
                "historial": {
                    "status": historial.status,
                    "rows_returned": int(historial.data.get("totales", {}).get("rows_returned", 0)),
                    "pagination_ok": True,
                },
                "historial_sort": {
                    "status": historial_sorted.status,
                    "sort_ok": True,
                },
                "historial_search": {
                    "status": historial_search.status,
                    "search_ok": True,
                },
                "historial_csv": {
                    "status": historial_csv.status,
                    "header_ok": True,
                },
                "historial_xlsx": {
                    "status": historial_xlsx.status,
                    "content_type_ok": True,
                },
                "deactivate_dry_run": deactivate_dry.data,
                "purge_dry_run": purge_dry.data,
                "maintenance_dry_run": maintenance_dry.data,
                "maintenance_live": maintenance_live.data if maintenance_live else None,
            },
        }
        self.stdout.write(self.style.SUCCESS("Smoke API integraciones completado."))
        self.stdout.write(json.dumps(output, ensure_ascii=False, indent=2, default=str))
