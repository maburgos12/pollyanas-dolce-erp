from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from urllib.parse import urljoin

from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.exceptions import ExtractionError
from pos_bridge.utils.helpers import decimal_from_value, deterministic_id, safe_slug, write_json_file


@dataclass
class ExtractedProductionLine:
    branch: dict
    production_external_id: str
    detail_external_id: str
    production_date: date
    responsible: str
    item_name: str
    item_code: str
    unit: str
    unit_cost: object
    requested_quantity: object
    produced_quantity: object
    is_insumo: bool
    raw_payload: dict = field(default_factory=dict)
    source_hash: str = ""


class PointProductionEntryExtractor:
    LIST_PATH = "/Produccion/getProduccionGeneral/"
    DETAIL_PATH = "/Produccion/getProduccionDetalle/"

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
        return (
            self.settings.raw_exports_dir
            / f"{token}_point_production_{start_date.isoformat()}_{end_date.isoformat()}_{branch_token}.json"
        )

    def extract(self, *, start_date: date, end_date: date, branch_filter: str | None = None) -> list[ExtractedProductionLine]:
        auth_session = self.http_session_service.create()
        branch_name = (branch_filter or "TODAS LAS SUCURSALES").strip()
        branch_param = branch_filter or "null"
        response = auth_session.session.get(
            urljoin(self.settings.base_url.rstrip("/") + "/", self.LIST_PATH.lstrip("/")),
            params={
                "pkSucursal": branch_param,
                "sucursal": branch_name,
                "fechaInicio": str(self._to_epoch_ms(start_date)),
                "fechaFinal": str(self._to_epoch_ms(end_date)),
                "produccion": "3",
                "activo": "true",
                "estado": "PROCESADOS / TERMINADOS",
                "empresa": "MATRIZ",
            },
            timeout=self.settings.timeout_ms / 1000,
        )
        response.raise_for_status()
        try:
            productions = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ExtractionError("Point devolvió un listado de producción inválido.") from exc

        extracted: list[ExtractedProductionLine] = []
        raw_export = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "productions": [],
        }
        for production in productions:
            production_id = str(production.get("FK_Produccion") or "").strip()
            if not production_id:
                continue
            detail_response = auth_session.session.get(
                urljoin(self.settings.base_url.rstrip("/") + "/", self.DETAIL_PATH.lstrip("/")),
                params={"pkLista": production_id},
                timeout=self.settings.timeout_ms / 1000,
            )
            detail_response.raise_for_status()
            details = json.loads(detail_response.text)
            raw_export["productions"].append({"production": production, "details": details})
            production_date = datetime.fromisoformat(str(production.get("Fecha")).replace("Z", "+00:00")).date()
            branch_label = str(production.get("Sucursal") or "").strip()
            responsible = str(production.get("Usuario") or "").strip()
            for detail in details:
                detail_id = str(detail.get("PK_Produccion_detalle") or "").strip()
                item_code = str(detail.get("Codigo") or "").strip()
                item_name = str(detail.get("Nombre") or item_code or "").strip()
                requested = decimal_from_value(detail.get("Cantidad_solicitada"))
                produced = decimal_from_value(detail.get("Cantidad_producida"))
                source_hash = deterministic_id("point_production", production_id, detail_id)
                extracted.append(
                    ExtractedProductionLine(
                        branch={"external_id": branch_label, "name": branch_label, "status": "ACTIVE", "metadata": {}},
                        production_external_id=production_id,
                        detail_external_id=detail_id,
                        production_date=production_date,
                        responsible=responsible,
                        item_name=item_name,
                        item_code=item_code,
                        unit=str(detail.get("Unidad") or "").strip(),
                        unit_cost=decimal_from_value(detail.get("Precio_default")),
                        requested_quantity=requested,
                        produced_quantity=produced,
                        is_insumo=bool(detail.get("IsInsumo")),
                        raw_payload={"production": production, "detail": detail},
                        source_hash=source_hash,
                    )
                )

        write_json_file(
            self._raw_export_path(start_date=start_date, end_date=end_date, branch_filter=branch_filter),
            raw_export,
        )
        return extracted
