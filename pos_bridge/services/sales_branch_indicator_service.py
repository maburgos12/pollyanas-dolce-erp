from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from datetime import timezone as dt_timezone
from decimal import Decimal
from urllib.parse import urljoin

from django.db.models import Q
from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.models import PointBranch, PointDailyBranchIndicator
from pos_bridge.services.point_http_session_service import PointHttpSessionService


@dataclass
class PointBranchIndicatorPayload:
    branch: PointBranch
    indicator_date: date
    contado_amount: Decimal
    credito_amount: Decimal
    contado_tickets: int
    credito_tickets: int
    contado_avg_ticket: Decimal
    credito_avg_ticket: Decimal
    total_amount: Decimal
    total_tickets: int
    total_avg_ticket: Decimal
    raw_payload: dict


class PointSalesBranchIndicatorService:
    INDICATORS_PATH = "/Ventas/get_Ventas_ByDay"

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)

    @staticmethod
    def canonical_branches(*, branch_filter: str | None = None) -> list[PointBranch]:
        queryset = PointBranch.objects.filter(erp_branch__isnull=False).select_related("erp_branch").order_by("erp_branch_id", "id")
        if branch_filter:
            token = branch_filter.strip()
            queryset = queryset.filter(Q(name__icontains=token) | Q(external_id__iexact=token))
        grouped: dict[int, list[PointBranch]] = {}
        for branch in queryset:
            grouped.setdefault(branch.erp_branch_id, []).append(branch)

        def _sort_key(branch: PointBranch):
            external = (branch.external_id or "").strip()
            return (
                external.isdigit(),
                branch.status == PointBranch.STATUS_ACTIVE,
                branch.last_seen_at or datetime.min.replace(tzinfo=dt_timezone.utc),
                branch.updated_at or datetime.min.replace(tzinfo=dt_timezone.utc),
                branch.id,
            )

        selected: list[PointBranch] = []
        for branches in grouped.values():
            selected.append(max(branches, key=_sort_key))
        selected.sort(key=lambda branch: ((branch.erp_branch.codigo or ""), branch.name, branch.id))
        return selected

    def _to_epoch_ms(self, value: date) -> int:
        local_tz = timezone.get_current_timezone()
        dt = datetime.combine(value, time.min)
        aware = timezone.make_aware(dt, local_tz)
        return int(aware.timestamp() * 1000)

    def _format_point_date(self, value: date) -> str:
        return value.strftime("%d/%m/%Y")

    def _resolve_session_branch(self, *, branch_external_id: str | None = None) -> PointBranch:
        branches = self.canonical_branches(branch_filter=branch_external_id) if branch_external_id else self.canonical_branches()
        if not branches:
            raise PointBranch.DoesNotExist("No hay sucursales Point canónicas disponibles para autenticar la sesión.")
        if branch_external_id:
            for branch in branches:
                if branch.external_id == str(branch_external_id):
                    return branch
        return branches[0]

    def fetch_range(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_external_id: str | None = None,
    ) -> list[PointBranchIndicatorPayload]:
        session_branch = self._resolve_session_branch(branch_external_id=branch_external_id)
        auth_session = self.http_session_service.create(
            branch_external_id=session_branch.external_id,
            branch_display_name=session_branch.name,
        )
        request_url = urljoin(self.settings.base_url.rstrip("/") + "/", self.INDICATORS_PATH.lstrip("/"))
        response = auth_session.session.post(
            request_url,
            data={
                "fecha_inicio": self._format_point_date(start_date),
                "fecha_fin": self._format_point_date(end_date),
                "pkSucursal": str(branch_external_id) if branch_external_id else "null",
            },
            timeout=self.settings.timeout_ms / 1000,
        )
        response.raise_for_status()
        payload_list = response.json() or []
        branch_lookup = {branch.external_id: branch for branch in self.canonical_branches()}
        results: list[PointBranchIndicatorPayload] = []
        for payload in payload_list:
            point_external_id = str(payload.get("PK_Sucursal") or "").strip()
            point_branch = branch_lookup.get(point_external_id)
            if point_branch is None:
                continue
            indicator_date = datetime.fromisoformat(str(payload.get("Dia"))).date()
            total_amount = Decimal(str(payload.get("Monto") or 0))
            total_tickets = max(0, int(payload.get("Cantidad") or 0))
            total_avg_ticket = Decimal(str(payload.get("TicketPromedio") or 0)).quantize(Decimal("0.01"))
            results.append(
                PointBranchIndicatorPayload(
                    branch=point_branch,
                    indicator_date=indicator_date,
                    contado_amount=total_amount,
                    credito_amount=Decimal("0.00"),
                    contado_tickets=total_tickets,
                    credito_tickets=0,
                    contado_avg_ticket=total_avg_ticket,
                    credito_avg_ticket=Decimal("0.00"),
                    total_amount=total_amount,
                    total_tickets=total_tickets,
                    total_avg_ticket=total_avg_ticket,
                    raw_payload={"response": payload},
                )
            )
        return results

    def fetch_branch_day(self, *, branch: PointBranch, indicator_date: date) -> PointBranchIndicatorPayload:
        for payload in self.fetch_range(
            start_date=indicator_date,
            end_date=indicator_date,
            branch_external_id=branch.external_id,
        ):
            if payload.branch.id == branch.id:
                return payload
        raise PointDailyBranchIndicator.DoesNotExist(
            f"Point no devolvió indicador diario para {branch.external_id} en {indicator_date.isoformat()}."
        )

    def persist_branch_day(self, *, indicator_payload: PointBranchIndicatorPayload, sync_job=None) -> tuple[PointDailyBranchIndicator, bool]:
        defaults = {
            "sync_job": sync_job,
            "contado_amount": indicator_payload.contado_amount,
            "credito_amount": indicator_payload.credito_amount,
            "contado_tickets": indicator_payload.contado_tickets,
            "credito_tickets": indicator_payload.credito_tickets,
            "contado_avg_ticket": indicator_payload.contado_avg_ticket,
            "credito_avg_ticket": indicator_payload.credito_avg_ticket,
            "total_amount": indicator_payload.total_amount,
            "total_tickets": indicator_payload.total_tickets,
            "total_avg_ticket": indicator_payload.total_avg_ticket,
            "source_endpoint": self.INDICATORS_PATH,
            "raw_payload": indicator_payload.raw_payload,
        }
        indicator, created = PointDailyBranchIndicator.objects.update_or_create(
            branch=indicator_payload.branch,
            indicator_date=indicator_payload.indicator_date,
            defaults=defaults,
        )
        return indicator, created
