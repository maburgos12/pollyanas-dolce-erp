from __future__ import annotations

import json
import time as time_module
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from datetime import timezone as dt_timezone
from pathlib import Path
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.exceptions import ExtractionError
from pos_bridge.utils.helpers import decimal_from_value, deterministic_id, safe_slug, write_json_file


@dataclass
class ExtractedWasteLine:
    branch: dict
    movement_external_id: str
    movement_at: datetime
    responsible: str
    item_name: str
    item_code: str
    quantity: object
    unit: str
    unit_cost: object
    total_cost: object
    justification: str
    raw_payload: dict = field(default_factory=dict)
    source_hash: str = ""


class PointWasteExtractor:
    LIST_PATH = "/Mermas/get_mermas"
    DETAIL_PATH = "/Mermas/get_detalle"
    JUSTIFICATION_PATH = "/Mermas/get_justificacion"

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)

    def _to_epoch_ms(self, value: date) -> int:
        aware = timezone.make_aware(datetime.combine(value, time.min), timezone.get_current_timezone())
        return int(aware.timestamp() * 1000)

    def _raw_export_path(self, *, start_date: date, end_date: date, branch_filter: str | None) -> Path:
        token = timezone.localtime().strftime("%Y%m%d_%H%M%S")
        branch_token = safe_slug(branch_filter or "all")
        return self.settings.raw_exports_dir / f"{token}_point_waste_{start_date.isoformat()}_{end_date.isoformat()}_{branch_token}.json"

    def _create_auth_session(self, *, session_stack: ExitStack):
        attempts = max(1, int(getattr(self.settings, "retry_attempts", 1) or 1))
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                auth_session = self.http_session_service.create()
                session_stack.callback(auth_session.session.close)
                return auth_session
            except (ExtractionError, requests.RequestException) as exc:
                last_error = exc
                if attempt < attempts:
                    time_module.sleep(min(2 ** (attempt - 1), 5))
        raise ExtractionError("No fue posible abrir una sesión de Point para consultar mermas.") from last_error

    def _read_rows(self, *, auth_session, path: str, params: dict, label: str, session_stack: ExitStack):
        attempts = max(1, int(getattr(self.settings, "retry_attempts", 1) or 1))
        for attempt in range(1, attempts + 1):
            try:
                response = auth_session.session.get(
                    urljoin(self.settings.base_url.rstrip("/") + "/", path.lstrip("/")),
                    params=params,
                    timeout=self.settings.timeout_ms / 1000,
                )
                response.raise_for_status()
                payload = json.loads(response.text)
                if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
                    raise ExtractionError(
                        f"Point devolvió {label} con formato inesperado.",
                        context={"path": path, "payload_type": type(payload).__name__},
                    )
                return payload, auth_session
            except (json.JSONDecodeError, ExtractionError, requests.RequestException) as exc:
                if attempt == attempts:
                    if isinstance(exc, ExtractionError):
                        raise
                    raise ExtractionError(f"No fue posible leer {label} desde Point.") from exc
                auth_session = self._create_auth_session(session_stack=session_stack)

        raise ExtractionError(f"No fue posible leer {label} desde Point.")

    def extract(self, *, start_date: date, end_date: date, branch_filter: str | None = None) -> list[ExtractedWasteLine]:
        if start_date > end_date:
            raise ExtractionError("La fecha inicial de mermas no puede ser posterior a la fecha final.")
        with ExitStack() as session_stack:
            return self._extract(
                start_date=start_date,
                end_date=end_date,
                branch_filter=branch_filter,
                session_stack=session_stack,
            )

    def _extract(self, *, start_date: date, end_date: date, branch_filter: str | None, session_stack: ExitStack) -> list[ExtractedWasteLine]:
        # Point can omit initial-day movements when queried with exact boundaries.
        # Calendar padding broadens discovery; only operational dates are emitted.
        query_start = start_date - timedelta(days=1)
        query_end = end_date + timedelta(days=1)
        auth_session = self._create_auth_session(session_stack=session_stack)
        movements, auth_session = self._read_rows(
            auth_session=auth_session,
            path=self.LIST_PATH,
            params={
                "sucursal": branch_filter or "null",
                "fechaini": str(self._to_epoch_ms(query_start)),
                "fechafin": str(self._to_epoch_ms(query_end)),
            },
            label="el listado de mermas",
            session_stack=session_stack,
        )

        extracted: list[ExtractedWasteLine] = []
        raw_export = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "movements": [],
        }

        seen_movements: dict[str, dict] = {}
        operational_timezone = ZoneInfo("America/Mazatlan")
        for movement in movements:
            movement_id = str(movement.get("PK_Movimiento") or "").strip()
            if not movement_id:
                continue
            if movement_id in seen_movements:
                if movement != seen_movements[movement_id]:
                    raise ExtractionError(
                        "Point devolvió versiones distintas de la misma merma.",
                        context={"movement_id": movement_id},
                    )
                continue
            seen_movements[movement_id] = movement
            try:
                movement_at = datetime.fromisoformat(str(movement.get("Fecha")).replace("Z", "+00:00"))
            except (TypeError, ValueError) as exc:
                raise ExtractionError(
                    "Point devolvió una merma con fecha inválida.",
                    context={"movement_id": movement_id, "date": movement.get("Fecha")},
                ) from exc
            if timezone.is_naive(movement_at):
                movement_at = movement_at.replace(tzinfo=dt_timezone.utc)
            operational_date = movement_at.astimezone(operational_timezone).date()
            if not start_date <= operational_date <= end_date:
                continue
            branch_name = str(movement.get("Sucursal") or movement.get("Sucursal_corto") or "").strip()
            responsible = str(movement.get("Responsable") or "").strip()
            justifications, auth_session = self._read_rows(
                auth_session=auth_session,
                path=self.JUSTIFICATION_PATH,
                params={"id_mov": movement_id},
                label=f"las justificaciones de la merma {movement_id}",
                session_stack=session_stack,
            )
            details, auth_session = self._read_rows(
                auth_session=auth_session,
                path=self.DETAIL_PATH,
                params={"pk_movimiento": movement_id},
                label=f"el detalle de la merma {movement_id}",
                session_stack=session_stack,
            )
            justification_text = " | ".join(
                {
                    str(item.get("Justificacion") or "").strip()
                    for item in justifications
                    if str(item.get("Justificacion") or "").strip()
                }
            )
            movement_export = {"movement": movement, "justifications": justifications, "details": details}
            raw_export["movements"].append(movement_export)

            for index, detail in enumerate(details):
                item_name = str(detail.get("Articulo") or "").strip()
                quantity = decimal_from_value(detail.get("Cantidad"))
                unit_cost = decimal_from_value(detail.get("Costo_unitario"))
                total_cost = decimal_from_value(detail.get("Costo_total"))
                source_hash = deterministic_id("point_waste", movement_id, item_name, quantity, total_cost, index)
                extracted.append(
                    ExtractedWasteLine(
                        branch={"external_id": branch_name, "name": branch_name, "status": "ACTIVE", "metadata": {}},
                        movement_external_id=movement_id,
                        movement_at=movement_at,
                        responsible=responsible,
                        item_name=item_name,
                        item_code="",
                        quantity=quantity,
                        unit=str(detail.get("Unidad") or "").strip(),
                        unit_cost=unit_cost,
                        total_cost=total_cost,
                        justification=justification_text,
                        raw_payload=movement_export,
                        source_hash=source_hash,
                    )
                )

        write_json_file(self._raw_export_path(start_date=start_date, end_date=end_date, branch_filter=branch_filter), raw_export)
        return extracted
