from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.helpers import sanitize_sensitive_data

HEADER_PATH = "/Clientes/get_encabezado_nota/"
DETAIL_PATH = "/Clientes/get_detalle_nota/"
PERSONAL_KEYS = {
    "apellidos",
    "cajero",
    "celular",
    "cliente",
    "correo",
    "direccion",
    "direccion1",
    "direccion2",
    "email",
    "fk_cajero",
    "fk_cliente",
    "nombre",
    "nombre_completo",
    "observaciones",
    "telefono",
    "telefono1",
    "telefono2",
}
SECRET_KEY_PARTS = ("authorization", "cookie", "password", "passwd", "secret", "token")


def sanitize_note_payload(value: Any) -> Any:
    value = sanitize_sensitive_data(value)
    if isinstance(value, dict):
        return {
            key: (
                "***"
                if str(key).lower() in PERSONAL_KEYS
                or any(part in str(key).lower() for part in SECRET_KEY_PARTS)
                else sanitize_note_payload(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_note_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_note_payload(item) for item in value)
    return value


def _response_payload(response) -> Any:
    try:
        return response.json()
    except (ValueError, json.JSONDecodeError):
        try:
            return json.loads(response.text)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CommandError("Point devolvió una respuesta de detalle no JSON.") from exc


def discover_note_detail(*, pk_nota: str, folio: str = "", sucursal: str = "") -> dict[str, Any]:
    settings = load_point_bridge_settings()
    auth_session = PointHttpSessionService(settings).create()
    params = {"id_nota": pk_nota}
    timeout = max(settings.timeout_ms / 1000, 60)
    try:
        responses = {}
        for label, path in (("header", HEADER_PATH), ("detail", DETAIL_PATH)):
            response = auth_session.session.get(
                urljoin(settings.base_url.rstrip("/") + "/", path.lstrip("/")),
                params=params,
                timeout=timeout,
            )
            response.raise_for_status()
            responses[label] = _response_payload(response)
    finally:
        auth_session.session.close()

    return {
        "classification": "DIRECT_API",
        "request": {
            "method": "GET",
            "header_path": HEADER_PATH,
            "detail_path": DETAIL_PATH,
            "params": params,
        },
        "reference": {"pk_nota": pk_nota, "folio": folio, "sucursal": sucursal},
        **responses,
    }


class Command(BaseCommand):
    help = "Prueba en solo lectura el detalle de una nota Point y guarda JSON sanitizado."

    def add_arguments(self, parser):
        parser.add_argument("--pk-nota", required=True)
        parser.add_argument("--folio", default="")
        parser.add_argument("--sucursal", default="")
        parser.add_argument("--output", required=True)

    def handle(self, *args, **options):
        pk_nota = str(options["pk_nota"] or "").strip()
        if not pk_nota:
            raise CommandError("--pk-nota es obligatorio.")

        result = discover_note_detail(
            pk_nota=pk_nota,
            folio=str(options["folio"] or "").strip(),
            sucursal=str(options["sucursal"] or "").strip(),
        )
        output = Path(options["output"]).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(sanitize_note_payload(result), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        self.stdout.write(self.style.SUCCESS(f"Detalle Point sanitizado guardado en {output}"))
