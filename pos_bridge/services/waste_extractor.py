from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time
from datetime import timezone as dt_timezone
from pathlib import Path
from urllib.parse import urljoin

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

    def extract(self, *, start_date: date, end_date: date, branch_filter: str | None = None) -> list[ExtractedWasteLine]:
        auth_session = self.http_session_service.create()
        response = auth_session.session.get(
            urljoin(self.settings.base_url.rstrip("/") + "/", self.LIST_PATH.lstrip("/")),
            params={
                "sucursal": branch_filter or "null",
                "fechaini": str(self._to_epoch_ms(start_date)),
                "fechafin": str(self._to_epoch_ms(end_date)),
            },
            timeout=self.settings.timeout_ms / 1000,
        )
        response.raise_for_status()
        try:
            movements = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ExtractionError("Point devolvió un listado de mermas inválido.") from exc

        extracted: list[ExtractedWasteLine] = []
        raw_export = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "movements": [],
        }

        for movement in movements:
            movement_id = str(movement.get("PK_Movimiento") or "").strip()
            if not movement_id:
                continue
            movement_at = datetime.fromisoformat(str(movement.get("Fecha")).replace("Z", "+00:00"))
            if timezone.is_naive(movement_at):
                movement_at = movement_at.replace(tzinfo=dt_timezone.utc)
            branch_name = str(movement.get("Sucursal") or movement.get("Sucursal_corto") or "").strip()
            responsible = str(movement.get("Responsable") or "").strip()
            justification_response = auth_session.session.get(
                urljoin(self.settings.base_url.rstrip("/") + "/", self.JUSTIFICATION_PATH.lstrip("/")),
                params={"id_mov": movement_id},
                timeout=self.settings.timeout_ms / 1000,
            )
            justification_response.raise_for_status()
            detail_response = auth_session.session.get(
                urljoin(self.settings.base_url.rstrip("/") + "/", self.DETAIL_PATH.lstrip("/")),
                params={"pk_movimiento": movement_id},
                timeout=self.settings.timeout_ms / 1000,
            )
            detail_response.raise_for_status()
            justifications = json.loads(justification_response.text)
            details = json.loads(detail_response.text)
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
