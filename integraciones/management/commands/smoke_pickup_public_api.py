from __future__ import annotations

import json
import os
import ssl
from dataclasses import dataclass
from datetime import date
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


def _http_json(
    *,
    method: str,
    url: str,
    api_key: str | None = None,
    payload: dict[str, Any] | None = None,
    timeout: int = 15,
    insecure: bool = False,
) -> HttpResult:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if api_key:
        headers["X-API-Key"] = api_key
    req = Request(url=url, data=body, method=method.upper(), headers=headers)
    ssl_ctx = ssl._create_unverified_context() if insecure else None
    try:
        with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            body_bytes = resp.read()
            raw = body_bytes.decode("utf-8", errors="replace")
            try:
                data = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                data = {}
            return HttpResult(status=int(resp.status), data=data, raw=raw)
    except HTTPError as exc:
        body_bytes = exc.read()
        raw = body_bytes.decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            data = {}
        return HttpResult(status=int(exc.code), data=data, raw=raw)
    except URLError as exc:
        raise CommandError(f"No se pudo conectar a {url}: {exc}") from exc


class Command(BaseCommand):
    help = "Ejecuta smoke test del flujo pickup publico del ERP: health, availability y opcionalmente reserve/confirm/release."

    def add_arguments(self, parser):
        parser.add_argument(
            "--base-url",
            default=os.environ.get("ERP_BASE_URL", "http://127.0.0.1:8000"),
            help="URL base del ERP.",
        )
        parser.add_argument(
            "--api-key",
            default=os.environ.get("ERP_PUBLIC_API_KEY", ""),
            help="API key publica del ERP.",
        )
        parser.add_argument("--product-code", required=True, help="Codigo de producto/codigo_point.")
        parser.add_argument("--branch-code", required=True, help="Codigo de sucursal ERP.")
        parser.add_argument("--quantity", default="1", help="Cantidad solicitada.")
        parser.add_argument("--client-name", default="Smoke Pickup Web", help="Nombre cliente para la reserva.")
        parser.add_argument(
            "--external-reference",
            default="",
            help="Referencia externa idempotente. Si no se manda, se genera una por corrida.",
        )
        parser.add_argument("--hold-minutes", type=int, default=15, help="TTL de la reserva en minutos.")
        parser.add_argument(
            "--mode",
            choices=["availability", "reserve-release", "full-cycle"],
            default="availability",
            help="availability solo consulta. reserve-release crea y libera. full-cycle reserva, confirma y cancela.",
        )
        parser.add_argument(
            "--confirm-live",
            default="",
            help='Para ejecutar reserve-release o full-cycle debe ser exactamente "YES".',
        )
        parser.add_argument("--descripcion", default="Smoke pickup web", help="Descripcion para confirmar pedido.")
        parser.add_argument("--monto-estimado", default="0", help="Monto estimado opcional para confirmacion.")
        parser.add_argument(
            "--fecha-compromiso",
            default=date.today().isoformat(),
            help="Fecha compromiso YYYY-MM-DD para confirmacion.",
        )
        parser.add_argument("--prioridad", default="MEDIA", help="Prioridad del pedido CRM.")
        parser.add_argument(
            "--release-reason",
            default="Smoke test pickup cleanup",
            help="Motivo usado al liberar o cancelar la reserva.",
        )
        parser.add_argument("--timeout", type=int, default=15, help="Timeout HTTP por request.")
        parser.add_argument(
            "--insecure",
            action="store_true",
            help="Desactiva validacion TLS para diagnostico en entornos controlados.",
        )

    def _build_url(self, base_url: str, path: str, query: dict[str, Any] | None = None) -> str:
        root = base_url.rstrip("/")
        qp = f"?{urlencode(query)}" if query else ""
        return f"{root}{path}{qp}"

    def _log_result(self, label: str, result: HttpResult):
        self.stdout.write(self.style.NOTICE(f"{label}: HTTP {result.status}"))
        self.stdout.write(json.dumps(result.data, ensure_ascii=True, indent=2, sort_keys=True))

    def _assert_status(self, label: str, result: HttpResult, expected: int):
        if result.status != expected:
            detail = result.data.get("detail") if isinstance(result.data, dict) else ""
            raise CommandError(
                f"{label} fallo: status={result.status} esperado={expected}. "
                f"detail={detail or result.raw[:240]}"
            )

    def _ensure_live_confirmation(self, mode: str, confirm_live: str):
        if mode != "availability" and confirm_live.strip().upper() != "YES":
            raise CommandError(
                'Para ejecutar efectos reales usa --confirm-live YES. '
                'El modo availability no requiere confirmacion.'
            )

    def handle(self, *args, **options):
        base_url = str(options["base_url"]).strip()
        api_key = str(options["api_key"]).strip()
        product_code = str(options["product_code"]).strip()
        branch_code = str(options["branch_code"]).strip()
        quantity = str(options["quantity"]).strip() or "1"
        client_name = str(options["client_name"]).strip()
        external_reference = str(options["external_reference"]).strip() or f"SMOKE-{branch_code}-{product_code}"
        hold_minutes = int(options["hold_minutes"])
        mode = str(options["mode"]).strip()
        timeout = int(options["timeout"])
        insecure = bool(options.get("insecure"))

        if not api_key:
            raise CommandError("Falta --api-key o ERP_PUBLIC_API_KEY.")

        self._ensure_live_confirmation(mode, str(options["confirm_live"] or ""))

        health = _http_json(
            method="GET",
            url=self._build_url(base_url, "/api/public/v1/health/"),
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_status("Health publico", health, 200)
        self._log_result("Health publico", health)

        availability = _http_json(
            method="GET",
            url=self._build_url(
                base_url,
                "/api/public/v1/pickup-availability/",
                query={
                    "product_code": product_code,
                    "branch_code": branch_code,
                    "quantity": quantity,
                },
            ),
            api_key=api_key,
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_status("Pickup availability", availability, 200)
        self._log_result("Pickup availability", availability)

        status_value = str(availability.data.get("status") or "")
        available = bool(availability.data.get("available"))
        if mode == "availability":
            if status_value == "UNKNOWN":
                self.stdout.write(
                    self.style.WARNING(
                        "Diagnostico: la conectividad esta bien, pero el inventario no esta fresco. "
                        "La tienda debe tratar UNKNOWN como no prometer inventario."
                    )
                )
            return

        if not available:
            raise CommandError(
                f"No se puede continuar con {mode}: availability.available=false y status={status_value}. "
                "Si status=UNKNOWN, refresca inventario; si es OUT_OF_STOCK, cambia producto/sucursal."
            )

        reserve = _http_json(
            method="POST",
            url=self._build_url(base_url, "/api/public/v1/pickup-reservations/"),
            api_key=api_key,
            payload={
                "product_code": product_code,
                "branch_code": branch_code,
                "quantity": quantity,
                "cliente_nombre": client_name,
                "external_reference": external_reference,
                "hold_minutes": hold_minutes,
                "notes": f"Smoke test mode={mode}",
            },
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_status("Crear reserva", reserve, 201)
        self._log_result("Crear reserva", reserve)
        token = str(reserve.data.get("reservation_token") or "").strip()
        if not token:
            raise CommandError("Crear reserva no devolvio reservation_token.")

        if mode == "reserve-release":
            release = _http_json(
                method="POST",
                url=self._build_url(base_url, f"/api/public/v1/pickup-reservations/{token}/release/"),
                api_key=api_key,
                payload={"reason": str(options["release_reason"]).strip()},
                timeout=timeout,
                insecure=insecure,
            )
            self._assert_status("Liberar reserva", release, 200)
            self._log_result("Liberar reserva", release)
            return

        confirm = _http_json(
            method="POST",
            url=self._build_url(base_url, f"/api/public/v1/pickup-reservations/{token}/confirm/"),
            api_key=api_key,
            payload={
                "cliente_nombre": client_name,
                "descripcion": str(options["descripcion"]).strip(),
                "monto_estimado": str(options["monto_estimado"]).strip(),
                "fecha_compromiso": str(options["fecha_compromiso"]).strip(),
                "prioridad": str(options["prioridad"]).strip(),
            },
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_status("Confirmar reserva", confirm, 201)
        self._log_result("Confirmar reserva", confirm)

        release = _http_json(
            method="POST",
            url=self._build_url(base_url, f"/api/public/v1/pickup-reservations/{token}/release/"),
            api_key=api_key,
            payload={"reason": str(options["release_reason"]).strip()},
            timeout=timeout,
            insecure=insecure,
        )
        self._assert_status("Cancelar reserva confirmada", release, 200)
        self._log_result("Cancelar reserva confirmada", release)
