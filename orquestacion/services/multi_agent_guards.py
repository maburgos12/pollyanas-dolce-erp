from __future__ import annotations

from typing import TYPE_CHECKING

from ventas.models import EventoVenta
from ventas.views import _latest_projection_artifacts, _projection_artifact_dir, _week_scope_qs

if TYPE_CHECKING:
    from orquestacion.services.agent_runtime import BlockingFinding, Goal


PRODUCTION_READY_STATUSES = {
    EventoVenta.STATUS_APROBADO,
    EventoVenta.STATUS_APROBADO_AJUSTES,
    EventoVenta.STATUS_ENVIADO_PROD,
    EventoVenta.STATUS_VALIDADO_PROD,
    EventoVenta.STATUS_ENVIADO_COMPRAS,
}
PURCHASE_READY_STATUSES = {
    EventoVenta.STATUS_VALIDADO_PROD,
    EventoVenta.STATUS_ENVIADO_COMPRAS,
}


def observe_production_readiness(goal: "Goal") -> tuple[EventoVenta, dict[str, object], list["BlockingFinding"]]:
    from orquestacion.services.agent_runtime import BlockingFinding

    event = EventoVenta.objects.prefetch_related("production_plans", "forecasts").get(pk=goal.entity_id)
    production_plans = list(event.production_plans.all())
    week_qs = _week_scope_qs(event)
    findings: list[BlockingFinding] = []

    if event.status not in PRODUCTION_READY_STATUSES:
        findings.append(
            BlockingFinding(
                code="event_not_ready_for_production",
                severity="critical",
                summary="El evento aún no está en estado liberable para revisión productiva.",
                evidence={"event_id": event.id, "status": event.status},
            )
        )
    if not production_plans:
        findings.append(
            BlockingFinding(
                code="production_plan_missing",
                severity="high",
                summary="No existen planes de producción generados para el evento.",
                evidence={"event_id": event.id},
            )
        )
    if not week_qs.exists():
        findings.append(
            BlockingFinding(
                code="forecast_missing_for_production",
                severity="critical",
                summary="No existe forecast semanal para soportar el readiness productivo.",
                evidence={"event_id": event.id},
            )
        )

    observation = {
        "event_id": event.id,
        "event_status": event.status,
        "forecast_week_rows": week_qs.count(),
        "production_plan_count": len(production_plans),
        "confirmed_production_plan_count": sum(1 for plan in production_plans if plan.status == "CONFIRMADO"),
    }
    return event, observation, findings


def observe_purchase_review(goal: "Goal") -> tuple[EventoVenta, dict[str, object], list["BlockingFinding"]]:
    from orquestacion.services.agent_runtime import BlockingFinding

    event = EventoVenta.objects.prefetch_related(
        "production_plans",
        "input_requirements",
        "purchase_requirements",
    ).get(pk=goal.entity_id)
    findings: list[BlockingFinding] = []

    if event.status not in PURCHASE_READY_STATUSES:
        findings.append(
            BlockingFinding(
                code="event_not_validated_by_production",
                severity="critical",
                summary="Compras no debe arrancar antes de la validación de producción.",
                evidence={"event_id": event.id, "status": event.status},
            )
        )
    if not event.production_plans.filter(status="CONFIRMADO").exists():
        findings.append(
            BlockingFinding(
                code="production_not_confirmed",
                severity="high",
                summary="No existe producción confirmada para habilitar la revisión de compras.",
                evidence={"event_id": event.id},
            )
        )
    if not event.input_requirements.exists():
        findings.append(
            BlockingFinding(
                code="input_requirements_missing",
                severity="high",
                summary="No existen requerimientos de insumo para revisión de compras.",
                evidence={"event_id": event.id},
            )
        )
    if event.input_requirements.exists() and not event.purchase_requirements.exists():
        findings.append(
            BlockingFinding(
                code="purchase_requirements_missing",
                severity="warning",
                summary="Aún no existen requerimientos de compra generados para revisión.",
                evidence={"event_id": event.id},
            )
        )

    observation = {
        "event_id": event.id,
        "event_status": event.status,
        "confirmed_production_plan_count": event.production_plans.filter(status="CONFIRMADO").count(),
        "input_requirement_count": event.input_requirements.count(),
        "purchase_requirement_count": event.purchase_requirements.count(),
    }
    return event, observation, findings


def observe_reconciliation_guard(goal: "Goal") -> tuple[EventoVenta, dict[str, object], list["BlockingFinding"]]:
    from orquestacion.services.agent_runtime import BlockingFinding

    event = EventoVenta.objects.prefetch_related("projection_artifacts", "financials").get(pk=goal.entity_id)
    artifact_dir = _projection_artifact_dir(event)
    active_artifacts = list(_latest_projection_artifacts(event))
    disk_files = sorted(str(path) for path in artifact_dir.glob("*") if path.is_file()) if artifact_dir.exists() else []
    findings: list[BlockingFinding] = []

    if bool(active_artifacts) != bool(disk_files):
        findings.append(
            BlockingFinding(
                code="artifact_disk_db_mismatch",
                severity="high",
                summary="La base y el disco no coinciden en los artifacts activos del evento.",
                evidence={
                    "event_id": event.id,
                    "artifact_rows": len(active_artifacts),
                    "disk_files": len(disk_files),
                },
            )
        )
    if not event.financials.filter(scenario=event.scenario_focus).exists():
        findings.append(
            BlockingFinding(
                code="financial_publication_mismatch",
                severity="high",
                summary="No existen financieros vigentes del escenario foco para conciliar la publicación.",
                evidence={"event_id": event.id, "scenario_focus": event.scenario_focus},
            )
        )

    observation = {
        "event_id": event.id,
        "artifact_row_count": len(active_artifacts),
        "artifact_disk_file_count": len(disk_files),
        "financial_scenario_focus_present": event.financials.filter(scenario=event.scenario_focus).exists(),
    }
    return event, observation, findings
