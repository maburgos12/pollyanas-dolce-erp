from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.point_note_detail_service import (
    POINT_NOTE_DETAIL_REQUIRED_FIELDS,
    POINT_NOTE_HEADER_REQUIRED_FIELDS,
)
from pos_bridge.services.point_http_session_service import PointHttpSessionService

HEADER_PATH = "/Clientes/get_encabezado_nota/"
DETAIL_PATH = "/Clientes/get_detalle_nota/"
HEADER_REQUIRED_FIELDS = set(POINT_NOTE_HEADER_REQUIRED_FIELDS)
DETAIL_REQUIRED_FIELDS = set(POINT_NOTE_DETAIL_REQUIRED_FIELDS)
HEADER_SAFE_FIELDS = {
    "Folio",
    "FK_Sucursal",
    "Fecha_Hora",
    "Importe",
    "Sucursal",
    "isCredito",
    "isFacturado",
}
DETAIL_SAFE_FIELDS = {
    "Cantidad",
    "Codigo",
    "Descuento",
    "Descuento_p",
    "Precio_Venta",
    "Producto",
    "Total",
}


def _safe_scalar(value: Any) -> Any:
    return value if value is None or isinstance(value, (str, int, float, bool)) else "***"


def _sanitize_rows(rows: Any, allowed_fields: set[str]) -> list[dict[str, Any]]:
    return [
        {key: _safe_scalar(value) if key in allowed_fields else "***" for key, value in row.items()}
        for row in rows
    ]


def sanitize_note_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"payload": "***"}
    request = value.get("request") if isinstance(value.get("request"), dict) else {}
    reference = value.get("reference") if isinstance(value.get("reference"), dict) else {}
    sanitized = {
        "classification": value.get("classification") if value.get("classification") == "DIRECT_API" else "***",
        "request": {
            "method": request.get("method") if request.get("method") == "GET" else "***",
            "header_path": request.get("header_path") if request.get("header_path") == HEADER_PATH else "***",
            "detail_path": request.get("detail_path") if request.get("detail_path") == DETAIL_PATH else "***",
            "params": {"id_nota": _safe_scalar((request.get("params") or {}).get("id_nota"))},
        },
        "reference": {
            "pk_nota": _safe_scalar(reference.get("pk_nota")),
            "folio": _safe_scalar(reference.get("folio")),
            "sucursal": _safe_scalar(reference.get("sucursal")),
        },
        "header": _sanitize_rows(value.get("header", []), HEADER_SAFE_FIELDS),
        "detail": _sanitize_rows(value.get("detail", []), DETAIL_SAFE_FIELDS),
    }
    for key in value.keys() - sanitized.keys():
        sanitized[key] = "***"
    return sanitized


def _response_payload(response, *, label: str, path: str) -> Any:
    try:
        return response.json()
    except (TypeError, ValueError, json.JSONDecodeError):
        try:
            return json.loads(response.text)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CommandError(f"Point devolvió JSON inválido en {label} ({path}).") from exc


def _validate_rows(payload: Any, *, label: str, path: str, required_fields: set[str]) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or not payload or not all(isinstance(row, dict) for row in payload):
        raise CommandError(f"Point devolvió una forma inválida en {label} ({path}).")
    for index, row in enumerate(payload):
        missing = required_fields - row.keys()
        if missing:
            raise CommandError(
                f"Point omitió campos mínimos en {label} ({path}), fila {index}: {', '.join(sorted(missing))}."
            )
    return payload


def discover_note_detail(*, pk_nota: str, folio: str = "", sucursal: str = "") -> dict[str, Any]:
    settings = load_point_bridge_settings()
    auth_session = PointHttpSessionService(settings).create()
    params = {"id_nota": pk_nota}
    timeout = settings.timeout_ms / 1000
    try:
        responses = {}
        endpoints = (
            ("cabecera", "header", HEADER_PATH, HEADER_REQUIRED_FIELDS),
            ("detalle", "detail", DETAIL_PATH, DETAIL_REQUIRED_FIELDS),
        )
        for label, result_key, path, required_fields in endpoints:
            try:
                response = auth_session.session.get(
                    urljoin(settings.base_url.rstrip("/") + "/", path.lstrip("/")),
                    params=params,
                    timeout=timeout,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                raise CommandError(f"Falló endpoint de {label} Point ({path}): {exc}.") from exc
            payload = _response_payload(response, label=label, path=path)
            responses[result_key] = _validate_rows(
                payload,
                label=label,
                path=path,
                required_fields=required_fields,
            )
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


def _write_json_atomic(output: Path, payload: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(payload, temp_file, ensure_ascii=False, indent=2, default=str)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, output)
    except OSError as exc:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise CommandError(f"No fue posible escribir la salida atómica {output}: {exc}.") from exc


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
        _write_json_atomic(output, sanitize_note_payload(result))
        self.stdout.write(self.style.SUCCESS(f"Detalle Point sanitizado guardado en {output}"))
