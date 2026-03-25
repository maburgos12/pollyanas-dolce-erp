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
from pos_bridge.utils.helpers import decimal_from_value, deterministic_id, normalize_text, safe_slug, write_json_file


@dataclass
class ExtractedTransferLine:
    origin_branch: dict
    destination_branch: dict
    transfer_external_id: str
    detail_external_id: str
    registered_at: datetime
    sent_at: datetime | None
    received_at: datetime | None
    requested_by: str
    sent_by: str
    received_by: str
    item_name: str
    item_code: str
    unit: str
    unit_cost: object
    requested_quantity: object
    sent_quantity: object
    received_quantity: object
    is_insumo: bool
    is_received: bool
    is_cancelled: bool
    is_finalized: bool
    raw_payload: dict = field(default_factory=dict)
    source_hash: str = ""


class PointTransferExtractor:
    LIST_PATH = "/Transfer/GetTransfer"
    DETAIL_PATH = "/Transfer/GetDetalle"
    ALL_BRANCHES_PATH = "/Home/Get_AllSucursales"

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
        return self.settings.raw_exports_dir / (
            f"{token}_point_transfers_{start_date.isoformat()}_{end_date.isoformat()}_{branch_token}.json"
        )

    def _parse_point_datetime(self, value: str | None) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_current_timezone())
        return dt

    def _fetch_all_branches(self, session) -> list[dict]:
        response = session.get(
            urljoin(self.settings.base_url.rstrip("/") + "/", self.ALL_BRANCHES_PATH.lstrip("/")),
            timeout=self.settings.timeout_ms / 1000,
        )
        response.raise_for_status()
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ExtractionError("Point devolvió sucursales inválidas para transferencias.") from exc
        return payload if isinstance(payload, list) else []

    def _resolve_branch_filter_value(self, session, branch_filter: str | None) -> str:
        token = str(branch_filter or "").strip()
        if not token:
            return ""
        if token.isdigit():
            return token

        target = normalize_text(token)
        all_branches = self._fetch_all_branches(session)
        for plaza in all_branches:
            for branch in plaza.get("Sucursales", []) or []:
                pk = str(branch.get("PK_Sucursal") or "").strip()
                name = normalize_text(branch.get("Sucursal") or "")
                if target in {name, normalize_text(branch.get("Sucursal_corto") or "")}:
                    return pk
        raise ExtractionError(f"No fue posible resolver la sucursal '{branch_filter}' en Point.")

    def extract(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None = None,
    ) -> list[ExtractedTransferLine]:
        auth_session = self.http_session_service.create()
        sucursal_param = self._resolve_branch_filter_value(auth_session.session, branch_filter)
        response = auth_session.session.get(
            urljoin(self.settings.base_url.rstrip("/") + "/", self.LIST_PATH.lstrip("/")),
            params={
                "sucursal": sucursal_param,
                "cancelado": "false",
                "recibido": "true",
                "fechaInicio": str(self._to_epoch_ms(start_date)),
                "fechaFin": str(self._to_epoch_ms(end_date)),
            },
            timeout=self.settings.timeout_ms / 1000,
        )
        response.raise_for_status()
        try:
            transfers = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ExtractionError("Point devolvió un listado de transferencias inválido.") from exc

        extracted: list[ExtractedTransferLine] = []
        raw_export = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "transfers": [],
        }

        for transfer in transfers:
            transfer_id = str(transfer.get("PK_Transferencia") or "").strip()
            if not transfer_id:
                continue
            detail_response = auth_session.session.get(
                urljoin(self.settings.base_url.rstrip("/") + "/", self.DETAIL_PATH.lstrip("/")),
                params={"idTransfer": transfer_id},
                timeout=self.settings.timeout_ms / 1000,
            )
            detail_response.raise_for_status()
            details = json.loads(detail_response.text)
            raw_export["transfers"].append({"transfer": transfer, "details": details})

            registered_at = self._parse_point_datetime(transfer.get("Fecha_registro"))
            if registered_at is None:
                continue
            sent_at = self._parse_point_datetime(transfer.get("Fecha_envio"))
            received_at = self._parse_point_datetime(transfer.get("Fecha_recepcion"))
            origin_name = str(transfer.get("Sucursal_Provee") or "").strip()
            destination_name = str(transfer.get("Sucursal_Solicitante") or "").strip()
            origin_external_id = str(transfer.get("fk_suc_provee") or "").strip() or origin_name
            destination_external_id = str(transfer.get("fk_suc_soli") or "").strip() or destination_name

            for detail in details:
                detail_id = str(detail.get("PK_Transf_Det") or "").strip()
                if not detail_id:
                    continue
                source_hash = deterministic_id("point_transfer", transfer_id, detail_id)
                extracted.append(
                    ExtractedTransferLine(
                        origin_branch={
                            "external_id": origin_external_id,
                            "name": origin_name,
                            "status": "ACTIVE",
                            "metadata": {},
                        },
                        destination_branch={
                            "external_id": destination_external_id,
                            "name": destination_name,
                            "status": "ACTIVE",
                            "metadata": {},
                        },
                        transfer_external_id=transfer_id,
                        detail_external_id=detail_id,
                        registered_at=registered_at,
                        sent_at=sent_at,
                        received_at=received_at,
                        requested_by=str(transfer.get("Usuario_Solicita") or "").strip(),
                        sent_by=str(transfer.get("Usuario_Envia") or "").strip(),
                        received_by=str(transfer.get("Usuario_Recibe") or "").strip(),
                        item_name=str(detail.get("Articulo") or "").strip(),
                        item_code=str(detail.get("Codigo") or "").strip(),
                        unit=str(detail.get("Unidad") or "").strip(),
                        unit_cost=decimal_from_value(detail.get("Costo")),
                        requested_quantity=decimal_from_value(detail.get("Solicitado")),
                        sent_quantity=decimal_from_value(detail.get("Enviado")),
                        received_quantity=decimal_from_value(detail.get("Recibido")),
                        is_insumo=bool(detail.get("isInsumo")),
                        is_received=bool(transfer.get("isRecibido")),
                        is_cancelled=bool(transfer.get("Cancelado")),
                        is_finalized=bool(transfer.get("isFinalizado")),
                        raw_payload={"transfer": transfer, "detail": detail},
                        source_hash=source_hash,
                    )
                )

        write_json_file(
            self._raw_export_path(start_date=start_date, end_date=end_date, branch_filter=branch_filter),
            raw_export,
        )
        return extracted
