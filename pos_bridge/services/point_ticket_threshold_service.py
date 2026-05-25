from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode, urljoin

from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.models import PointBranch
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.exceptions import ExtractionError
from pos_bridge.utils.helpers import normalize_text


@dataclass(frozen=True)
class PointTicketThresholdBranchResult:
    branch_name: str
    exact_count: int
    total_notes: int
    total_amount: Decimal


@dataclass(frozen=True)
class PointTicketThresholdResult:
    start_date: date
    end_date: date
    threshold_amount: Decimal
    exact_count: int
    total_notes: int
    total_amount: Decimal
    request_url: str
    source_endpoint: str
    branch_results: list[PointTicketThresholdBranchResult]


class PointTicketThresholdService:
    NOTES_BY_PLAZA_PATH = "/Report/NotasByPlaza"

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)

    def _to_epoch_ms(self, value: date) -> int:
        local_tz = timezone.get_current_timezone()
        aware = timezone.make_aware(datetime.combine(value, time.min), local_tz)
        return int(aware.timestamp() * 1000)

    def _build_params(self, *, start_date: date, end_date: date) -> dict[str, str]:
        return {
            "fi": str(self._to_epoch_ms(start_date)),
            "ff": str(self._to_epoch_ms(end_date)),
            "sucursal": "null",
            "credito": "null",
            "pkCanalVenta": "0",
            "pkPlaza": "null",
            "plaza": "TODAS LAS PLAZAS",
        }

    def _allowed_branch_names(self, branch_ids: list[int] | None) -> set[str]:
        if not branch_ids:
            return set()
        tokens: set[str] = set()
        branches = PointBranch.objects.filter(erp_branch_id__in=branch_ids).select_related("erp_branch")
        for branch in branches:
            for value in [
                branch.name,
                branch.external_id,
                branch.erp_branch.codigo if branch.erp_branch_id else "",
                branch.erp_branch.nombre if branch.erp_branch_id else "",
            ]:
                normalized = normalize_text(value)
                if normalized:
                    tokens.add(normalized)
        return tokens

    def _row_amount(self, row: dict) -> Decimal:
        raw_value = row.get("MONTO", row.get("Monto", row.get("Importe", row.get("Total", 0))))
        try:
            return Decimal(str(raw_value).replace(",", "").replace("$", "").strip() or "0")
        except (InvalidOperation, ValueError):
            return Decimal("0")

    def fetch_threshold_count(
        self,
        *,
        start_date: date,
        end_date: date,
        threshold_amount: Decimal,
        branch_ids: list[int] | None = None,
    ) -> PointTicketThresholdResult:
        allowed_branch_names = self._allowed_branch_names(branch_ids)
        params = self._build_params(start_date=start_date, end_date=end_date)
        request_url = urljoin(self.settings.base_url.rstrip("/") + "/", self.NOTES_BY_PLAZA_PATH.lstrip("/"))

        auth_session = self.http_session_service.create()
        try:
            response = auth_session.session.get(
                request_url,
                params=params,
                timeout=max(self.settings.timeout_ms / 1000, 60),
            )
            response.raise_for_status()
            payload = response.json() or []
        finally:
            try:
                auth_session.session.close()
            except Exception:  # noqa: BLE001
                pass

        if not isinstance(payload, list):
            raise ExtractionError("Point devolvió un payload inválido para notas por plaza.")

        branch_buckets: dict[str, dict[str, Decimal | int]] = {}
        exact_count = 0
        total_notes = 0
        total_amount = Decimal("0")
        for row in payload:
            if not isinstance(row, dict):
                continue
            branch_name = str(row.get("SUCURSAL") or row.get("Sucursal") or "").strip()
            if allowed_branch_names and normalize_text(branch_name) not in allowed_branch_names:
                continue
            amount = self._row_amount(row)
            total_notes += 1
            total_amount += amount
            bucket = branch_buckets.setdefault(
                branch_name,
                {"exact_count": 0, "total_notes": 0, "total_amount": Decimal("0")},
            )
            bucket["total_notes"] = int(bucket["total_notes"]) + 1
            bucket["total_amount"] = Decimal(bucket["total_amount"]) + amount
            if amount >= threshold_amount:
                exact_count += 1
                bucket["exact_count"] = int(bucket["exact_count"]) + 1

        branch_results = [
            PointTicketThresholdBranchResult(
                branch_name=branch_name,
                exact_count=int(bucket["exact_count"]),
                total_notes=int(bucket["total_notes"]),
                total_amount=Decimal(bucket["total_amount"]),
            )
            for branch_name, bucket in sorted(
                branch_buckets.items(),
                key=lambda item: (-int(item[1]["exact_count"]), item[0]),
            )
        ]
        return PointTicketThresholdResult(
            start_date=start_date,
            end_date=end_date,
            threshold_amount=threshold_amount,
            exact_count=exact_count,
            total_notes=total_notes,
            total_amount=total_amount,
            request_url=f"{request_url}?{urlencode(params)}",
            source_endpoint=self.NOTES_BY_PLAZA_PATH,
            branch_results=branch_results,
        )
