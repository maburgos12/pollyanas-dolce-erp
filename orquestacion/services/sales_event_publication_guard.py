from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from core.branch_catalog import POINT_MATURE_BRANCH_CODES
from ventas.models import EventoVenta
from ventas.views import (
    COMPARABLE_METHODS,
    DIRECT_METHODS,
    FALLBACK_METHODS,
    _branch_projection_rows,
    _cleanup_projection_artifact_history,
    _latest_projection_artifacts,
    _projection_artifact_dir,
    _projection_artifact_variants,
    _week_scope_qs,
)

if TYPE_CHECKING:
    from orquestacion.services.agent_runtime import BlockingFinding, Goal


RELEASE_READY_STATUSES = {
    EventoVenta.STATUS_VALIDADO_PROD,
    EventoVenta.STATUS_ENVIADO_COMPRAS,
    EventoVenta.STATUS_EN_EJECUCION,
    EventoVenta.STATUS_CERRADO,
    EventoVenta.STATUS_EVALUADO,
}


def observe_sales_event_publication(goal: "Goal") -> tuple[EventoVenta, dict[str, object], list["BlockingFinding"]]:
    from orquestacion.services.agent_runtime import BlockingFinding

    event = EventoVenta.objects.prefetch_related(
        "projection_artifacts",
        "financials",
        "forecasts",
    ).get(pk=goal.entity_id)
    week_qs = _week_scope_qs(event)
    classified_methods = set(DIRECT_METHODS) | set(COMPARABLE_METHODS) | set(FALLBACK_METHODS) | {"no_data"}
    present_methods = {
        method
        for method in week_qs.exclude(explanation_json__base_method="").values_list(
            "explanation_json__base_method",
            flat=True,
        ).distinct()
        if method
    }
    unknown_methods = sorted(present_methods - classified_methods)
    branch_rows = _branch_projection_rows(week_qs)
    mature_branch_mismatches = [
        {
            "branch_code": row.get("branch__codigo"),
            "source_label": row.get("source_label"),
            "source_counts": row.get("source_counts") or {},
            "week_qty": row.get("total"),
        }
        for row in branch_rows
        if row.get("branch__codigo") in POINT_MATURE_BRANCH_CODES and row.get("source_label") == "Sin base suficiente"
    ]
    artifact_dir = _projection_artifact_dir(event)
    active_artifacts = list(_latest_projection_artifacts(event))
    variant_dirs = []
    base_dir = artifact_dir.parent.parent
    for variant in sorted(_projection_artifact_variants(event)):
        candidate = base_dir / variant
        if candidate.exists():
            variant_dirs.append(str(candidate))
    focused_financial = event.financials.filter(scenario=event.scenario_focus).first()
    live_files = []
    if artifact_dir.exists():
        live_files = sorted(str(path) for path in artifact_dir.glob("*") if path.is_file())

    findings: list[BlockingFinding] = []
    if unknown_methods:
        findings.append(
            BlockingFinding(
                code="unknown_base_methods",
                severity="critical",
                summary="Existen métodos de forecast sin taxonomía UI/guardrail.",
                evidence={
                    "event_id": event.id,
                    "unknown_methods": unknown_methods,
                },
            )
        )
    if mature_branch_mismatches:
        findings.append(
            BlockingFinding(
                code="mature_branch_without_base",
                severity="high",
                summary="Una sucursal madura aparece etiquetada como sin base suficiente.",
                evidence={
                    "event_id": event.id,
                    "branches": mature_branch_mismatches,
                },
            )
        )
    if goal.requested_action == "publish_if_safe" and event.status not in RELEASE_READY_STATUSES:
        findings.append(
            BlockingFinding(
                code="workflow_gate_not_ready",
                severity="critical",
                summary="El evento aún no alcanzó el estado mínimo para publicación final.",
                evidence={
                    "event_id": event.id,
                    "status": event.status,
                    "required_statuses": sorted(RELEASE_READY_STATUSES),
                },
            )
        )
    if goal.requested_action == "publish_if_safe" and not week_qs.exists():
        findings.append(
            BlockingFinding(
                code="forecast_missing",
                severity="critical",
                summary="No existe forecast semanal para publicar.",
                evidence={"event_id": event.id},
            )
        )
    if goal.requested_action == "publish_if_safe" and focused_financial is None:
        findings.append(
            BlockingFinding(
                code="financials_missing",
                severity="critical",
                summary="Faltan financieros vigentes del escenario foco antes de publicar.",
                evidence={
                    "event_id": event.id,
                    "scenario_focus": event.scenario_focus,
                },
            )
        )

    observation = {
        "event_id": event.id,
        "event_code": event.code,
        "event_status": event.status,
        "event_version": event.version,
        "goal_action": goal.requested_action,
        "forecast_week_rows": week_qs.count(),
        "base_methods_present": sorted(present_methods),
        "unknown_base_methods": unknown_methods,
        "active_artifact_count": len(active_artifacts),
        "active_artifact_rows": [
            {
                "id": artifact.id,
                "type": artifact.export_type,
                "version": artifact.forecast_version,
                "path": artifact.file_path,
            }
            for artifact in active_artifacts
        ],
        "live_artifact_dir": str(artifact_dir),
        "live_artifact_files": live_files,
        "variant_dirs": variant_dirs,
        "financial_scenario_focus_present": focused_financial is not None,
        "mature_branch_mismatches": mature_branch_mismatches,
        "branch_source_summary": [
            {
                "branch_code": row.get("branch__codigo"),
                "source_label": row.get("source_label"),
                "source_counts": row.get("source_counts") or {},
                "week_qty": row.get("total"),
            }
            for row in branch_rows
        ],
    }
    return event, observation, findings


def execute_sales_event_publication(goal: "Goal", event: EventoVenta, actor) -> dict[str, object]:
    from ventas.views import _persist_projection_artifacts

    cleanup_summary = _cleanup_projection_artifact_history(event)
    artifacts = _persist_projection_artifacts(event, actor, force=True)
    artifact_dir = _projection_artifact_dir(event)
    return {
        "cleanup_summary": cleanup_summary,
        "artifact_count": len(artifacts),
        "artifact_ids": [artifact.id for artifact in artifacts],
        "artifact_types": [artifact.export_type for artifact in artifacts],
        "artifact_dir": str(artifact_dir),
        "artifact_files": sorted(str(path) for path in artifact_dir.glob("*") if path.is_file()),
    }
