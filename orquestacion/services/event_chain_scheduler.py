from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from django.db.models import Exists, OuterRef

from orquestacion.services.rule_runners import RuleRunResult, run_sales_event_operational_chain_review
from ventas.models import (
    EventoVenta,
    EventoVentaFinancial,
    EventoVentaForecast,
    EventoVentaInputRequirement,
    EventoVentaProductionPlan,
    EventoVentaProjectionArtifact,
    EventoVentaPurchaseRequirement,
)


CHAIN_CANDIDATE_STATUSES = {
    EventoVenta.STATUS_APROBADO,
    EventoVenta.STATUS_APROBADO_AJUSTES,
    EventoVenta.STATUS_ENVIADO_PROD,
    EventoVenta.STATUS_VALIDADO_PROD,
    EventoVenta.STATUS_ENVIADO_COMPRAS,
    EventoVenta.STATUS_EN_EJECUCION,
}


@dataclass(frozen=True)
class SalesEventChainCandidate:
    event_id: int
    event_code: str
    status: str
    reasons: list[str]


def list_sales_event_chain_candidates(*, event_ids: list[int] | None = None, limit: int = 25) -> list[SalesEventChainCandidate]:
    qs = (
        EventoVenta.objects.filter(deleted_at__isnull=True)
        .annotate(
            has_forecast=Exists(EventoVentaForecast.objects.filter(sales_event_id=OuterRef("pk"))),
            has_artifacts=Exists(EventoVentaProjectionArtifact.objects.filter(sales_event_id=OuterRef("pk"))),
            has_financials=Exists(EventoVentaFinancial.objects.filter(sales_event_id=OuterRef("pk"))),
            has_production_plan=Exists(EventoVentaProductionPlan.objects.filter(sales_event_id=OuterRef("pk"))),
            has_confirmed_production=Exists(
                EventoVentaProductionPlan.objects.filter(sales_event_id=OuterRef("pk"), status="CONFIRMADO")
            ),
            has_input_requirements=Exists(EventoVentaInputRequirement.objects.filter(sales_event_id=OuterRef("pk"))),
            has_purchase_requirements=Exists(
                EventoVentaPurchaseRequirement.objects.filter(sales_event_id=OuterRef("pk"))
            ),
        )
        .order_by("main_date", "id")
    )
    if event_ids:
        qs = qs.filter(id__in=event_ids)
    else:
        qs = qs.filter(status__in=CHAIN_CANDIDATE_STATUSES)

    candidates: list[SalesEventChainCandidate] = []
    for event in qs[:limit]:
        reasons = _candidate_reasons(event)
        if not reasons:
            continue
        candidates.append(
            SalesEventChainCandidate(
                event_id=event.id,
                event_code=event.code,
                status=event.status,
                reasons=reasons,
            )
        )
    return candidates


def run_sales_event_chain_batch(
    *,
    event_ids: list[int] | None = None,
    reference_dt: datetime | None = None,
    created_by=None,
    trigger_source: str = "scheduler",
    limit: int = 25,
) -> list[RuleRunResult]:
    results: list[RuleRunResult] = []
    for candidate in list_sales_event_chain_candidates(event_ids=event_ids, limit=limit):
        results.append(
            run_sales_event_operational_chain_review(
                event_id=candidate.event_id,
                reference_dt=reference_dt,
                created_by=created_by,
                trigger_source=trigger_source,
            )
        )
    return results


def _candidate_reasons(event) -> list[str]:
    reasons: list[str] = []
    if event.has_forecast and event.status in {
        EventoVenta.STATUS_APROBADO,
        EventoVenta.STATUS_APROBADO_AJUSTES,
        EventoVenta.STATUS_ENVIADO_PROD,
    }:
        reasons.append("forecast_ready_pending_production_validation")
    if event.has_confirmed_production and not event.has_purchase_requirements:
        reasons.append("production_confirmed_pending_purchase_review")
    if event.has_input_requirements and event.has_purchase_requirements:
        reasons.append("purchase_review_in_progress")
    if event.has_artifacts or not event.has_financials:
        reasons.append("publication_or_financial_reconciliation_needed")
    if event.status in {EventoVenta.STATUS_ENVIADO_COMPRAS, EventoVenta.STATUS_EN_EJECUCION}:
        reasons.append("late_stage_operational_reconciliation")
    return reasons
