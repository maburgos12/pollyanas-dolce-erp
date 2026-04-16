from __future__ import annotations

import logging
from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.db import OperationalError, ProgrammingError
from django.db.models import Count
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from core.access import can_manage_orquestacion, can_view_orquestacion
from .models import (
    AgentDefinition,
    AgentGap,
    MemoryProposal,
    AgentSuggestion,
    AgentTask,
    OrchestrationRule,
    OrchestrationRun,
    QualityFinding,
    RemediationProposal,
)
from .services.quality_guard_runner import run_quality_guards, sync_quality_guards
from .services.memory_proposals import apply_memory_proposal, approve_memory_proposal, reject_memory_proposal
from .services.quality_findings import (
    accept_remediation_proposal,
    mark_remediation_implemented,
    reject_remediation_proposal,
)

logger = logging.getLogger(__name__)


def _memory_proposals_table_available() -> bool:
    try:
        MemoryProposal.objects.exists()
    except (OperationalError, ProgrammingError):
        logger.warning(
            "MemoryProposal table unavailable; continuing with degraded Orquestacion views.",
            exc_info=True,
        )
        return False
    return True


def _quality_loop_tables_available() -> bool:
    try:
        QualityFinding.objects.exists()
        RemediationProposal.objects.exists()
    except (OperationalError, ProgrammingError):
        logger.warning(
            "Quality loop tables unavailable; continuing with degraded Orquestacion views.",
            exc_info=True,
        )
        return False
    return True


def _build_rule_source_link(*, rule_code: str, details: dict | None = None, result_summary: dict | None = None) -> tuple[str, str]:
    details = details or {}
    result_summary = result_summary or {}

    if rule_code == "daily_production_plan_missing":
        plan_id = result_summary.get("plan_id")
        production_date = details.get("production_date") or result_summary.get("production_date")
        if plan_id:
            return (
                f"{reverse('recetas:plan_produccion')}?{urlencode({'plan_id': plan_id})}",
                "Abrir plan de producción",
            )
        if production_date:
            return (
                f"{reverse('recetas:plan_produccion')}?{urlencode({'periodo': str(production_date)[:7]})}",
                "Abrir producción",
            )
        return (reverse("recetas:plan_produccion"), "Abrir producción")

    if rule_code == "purchase_exception_requires_dg_approval":
        solicitud_folio = details.get("solicitud_folio")
        if solicitud_folio:
            return (
                f"{reverse('compras:solicitudes')}?{urlencode({'q': solicitud_folio})}",
                "Abrir solicitud de compra",
            )
        return (reverse("compras:solicitudes"), "Abrir compras")

    if rule_code == "inventory_adjustment_authorization_guard":
        params = {}
        ajuste_id = details.get("ajuste_id") or result_summary.get("ajuste_id")
        estatus = details.get("estatus") or result_summary.get("estatus")
        if ajuste_id:
            params["ajuste_id"] = ajuste_id
        if estatus:
            params["estatus"] = estatus
        if params:
            return (
                f"{reverse('inventario:ajustes')}?{urlencode(params)}",
                "Abrir ajuste",
            )
        return (reverse("inventario:ajustes"), "Abrir ajustes")

    if rule_code == "near_expiry_or_low_rotation_review":
        insumo_id = details.get("canonical_insumo_id")
        if insumo_id:
            return (
                f"{reverse('inventario:alertas')}?{urlencode({'q': insumo_id})}",
                "Abrir alertas de inventario",
            )
        return (reverse("inventario:alertas"), "Abrir alertas de inventario")

    if rule_code == "plan_demand_production_purchase_chain":
        plan_id = details.get("plan_id") or result_summary.get("plan_id")
        if plan_id:
            return (
                f"{reverse('recetas:plan_produccion')}?{urlencode({'plan_id': plan_id})}",
                "Abrir plan de producción",
            )
        return (reverse("recetas:plan_produccion"), "Abrir producción")

    if rule_code == "sales_event_operational_chain_review":
        event_id = details.get("event_id") or result_summary.get("event_id")
        if event_id:
            return (reverse("ventas:evento_detail", args=[event_id]), "Abrir evento comercial")
        return (reverse("ventas:eventos"), "Abrir eventos comerciales")

    return ("", "")


def _enrich_run_links(runs: list[OrchestrationRun]) -> None:
    for run in runs:
        rule_code = run.rule.code if run.rule_id else ""
        run.source_url, run.source_label = _build_rule_source_link(
            rule_code=rule_code,
            details=run.context_json or {},
            result_summary=run.result_summary_json or {},
        )


def _enrich_suggestion_links(suggestions: list[AgentSuggestion]) -> None:
    for suggestion in suggestions:
        rule_code = suggestion.task.run.rule.code if suggestion.task.run_id and suggestion.task.run.rule_id else ""
        suggestion.source_url, suggestion.source_label = _build_rule_source_link(
            rule_code=rule_code,
            details=suggestion.details_json or {},
            result_summary=suggestion.task.run.result_summary_json if suggestion.task.run_id else {},
        )


@login_required
def dashboard(request):
    if not can_view_orquestacion(request.user):
        raise PermissionDenied("No tienes permisos para ver Orquestación.")

    selected_rule = (request.GET.get("rule") or "").strip()
    selected_severity = (request.GET.get("severity") or "").strip()
    selected_decision = (request.GET.get("decision") or AgentSuggestion.DECISION_PENDING).strip()
    selected_run_status = (request.GET.get("run_status") or "").strip()

    agents = list(
        AgentDefinition.objects.select_related("owner_department")
        .prefetch_related("capabilities")
        .annotate(capabilities_count=Count("capabilities"))
        .order_by("priority_order", "name", "id")
    )
    rules = list(
        OrchestrationRule.objects.select_related("primary_agent", "secondary_agent")
        .order_by("-is_active", "name", "id")[:12]
    )
    runs_qs = OrchestrationRun.objects.select_related("rule", "created_by")
    if selected_rule:
        runs_qs = runs_qs.filter(rule__code=selected_rule)
    if selected_run_status in {choice[0] for choice in OrchestrationRun.STATUS_CHOICES}:
        runs_qs = runs_qs.filter(status=selected_run_status)
    recent_runs = list(runs_qs.order_by("-started_at", "-id")[:12])
    _enrich_run_links(recent_runs)

    suggestions_qs = AgentSuggestion.objects.select_related("task", "task__agent", "task__run", "task__run__rule")
    if selected_rule:
        suggestions_qs = suggestions_qs.filter(task__run__rule__code=selected_rule)
    if selected_severity in {choice[0] for choice in AgentSuggestion.SEVERITY_CHOICES}:
        suggestions_qs = suggestions_qs.filter(severity=selected_severity)
    if selected_decision in {choice[0] for choice in AgentSuggestion.DECISION_CHOICES}:
        suggestions_qs = suggestions_qs.filter(decision_status=selected_decision)
    filtered_suggestions = list(suggestions_qs.order_by("-created_at", "-id")[:12])
    _enrich_suggestion_links(filtered_suggestions)

    pending_suggestions = list(
        AgentSuggestion.objects.select_related("task", "task__agent", "task__run", "task__run__rule")
        .filter(decision_status=AgentSuggestion.DECISION_PENDING)
        .order_by("-created_at", "-id")[:12]
    )
    _enrich_suggestion_links(pending_suggestions)
    open_tasks = list(
        AgentTask.objects.select_related("agent", "assigned_branch")
        .exclude(status__in=[AgentTask.STATUS_RESOLVED, AgentTask.STATUS_CANCELLED])
        .order_by("-created_at", "-id")[:12]
    )
    open_gaps = list(AgentGap.objects.exclude(status=AgentGap.STATUS_IMPLEMENTED).order_by("-created_at", "-id")[:10])
    memory_proposals_available = _memory_proposals_table_available()
    quality_loop_available = _quality_loop_tables_available()
    if memory_proposals_available:
        memory_proposals_pending = list(
            MemoryProposal.objects.select_related("proposed_by_agent", "reviewed_by")
            .exclude(status=MemoryProposal.STATUS_APPLIED)
            .order_by("status", "-last_detected_at", "-id")[:8]
        )
        memory_proposals_pending_count = MemoryProposal.objects.filter(status=MemoryProposal.STATUS_PROPOSED).count()
    else:
        memory_proposals_pending = []
        memory_proposals_pending_count = 0
    if quality_loop_available:
        quality_findings_open = list(
            QualityFinding.objects.select_related("memory_proposal")
            .filter(status=QualityFinding.STATUS_OPEN)
            .order_by("-severity", "-last_seen_at", "-id")[:8]
        )
        remediation_pending = list(
            RemediationProposal.objects.select_related("finding", "finding__memory_proposal")
            .exclude(status__in=[RemediationProposal.STATUS_VALIDATED, RemediationProposal.STATUS_REJECTED])
            .order_by("status", "-updated_at", "-id")[:8]
        )
        quality_findings_open_count = QualityFinding.objects.filter(status=QualityFinding.STATUS_OPEN).count()
        remediation_pending_count = RemediationProposal.objects.exclude(
            status__in=[RemediationProposal.STATUS_VALIDATED, RemediationProposal.STATUS_REJECTED]
        ).count()
    else:
        quality_findings_open = []
        remediation_pending = []
        quality_findings_open_count = 0
        remediation_pending_count = 0

    summary = {
        "agents_total": AgentDefinition.objects.count(),
        "agents_active": AgentDefinition.objects.filter(status=AgentDefinition.STATUS_ACTIVE).count(),
        "rules_active": OrchestrationRule.objects.filter(is_active=True).count(),
        "runs_today": OrchestrationRun.objects.filter(started_at__date=timezone.localdate()).count(),
        "tasks_open": AgentTask.objects.exclude(status__in=[AgentTask.STATUS_RESOLVED, AgentTask.STATUS_CANCELLED]).count(),
        "suggestions_pending": AgentSuggestion.objects.filter(decision_status=AgentSuggestion.DECISION_PENDING).count(),
        "gaps_open": AgentGap.objects.exclude(status=AgentGap.STATUS_IMPLEMENTED).count(),
        "memory_proposals_pending": memory_proposals_pending_count,
        "quality_findings_open": quality_findings_open_count,
        "remediation_pending": remediation_pending_count,
    }
    severity_breakdown = {
        code: suggestions_qs.filter(severity=code).count()
        for code, _label in AgentSuggestion.SEVERITY_CHOICES
    }
    decision_breakdown = {
        code: suggestions_qs.filter(decision_status=code).count()
        for code, _label in AgentSuggestion.DECISION_CHOICES
    }
    run_status_breakdown = {
        code: runs_qs.filter(status=code).count()
        for code, _label in OrchestrationRun.STATUS_CHOICES
    }
    severity_breakdown_items = [
        {"code": code, "label": label, "count": severity_breakdown.get(code, 0)}
        for code, label in AgentSuggestion.SEVERITY_CHOICES
    ]
    decision_breakdown_items = [
        {"code": code, "label": label, "count": decision_breakdown.get(code, 0)}
        for code, label in AgentSuggestion.DECISION_CHOICES
    ]
    run_status_breakdown_items = [
        {"code": code, "label": label, "count": run_status_breakdown.get(code, 0)}
        for code, label in OrchestrationRun.STATUS_CHOICES
    ]

    return render(
        request,
        "orquestacion/dashboard.html",
        {
            "summary": summary,
            "agents": agents,
            "rules": rules,
            "recent_runs": recent_runs,
            "filtered_suggestions": filtered_suggestions,
            "pending_suggestions": pending_suggestions,
            "open_tasks": open_tasks,
            "open_gaps": open_gaps,
            "memory_proposals_pending": memory_proposals_pending,
            "memory_proposals_available": memory_proposals_available,
            "quality_findings_open": quality_findings_open,
            "quality_loop_available": quality_loop_available,
            "remediation_pending": remediation_pending,
            "filters": {
                "rule": selected_rule,
                "severity": selected_severity,
                "decision": selected_decision,
                "run_status": selected_run_status,
            },
            "rule_options": list(
                OrchestrationRule.objects.order_by("-is_active", "name").values("code", "name")
            ),
            "severity_options": AgentSuggestion.SEVERITY_CHOICES,
            "decision_options": AgentSuggestion.DECISION_CHOICES,
            "run_status_options": OrchestrationRun.STATUS_CHOICES,
            "severity_breakdown": severity_breakdown,
            "decision_breakdown": decision_breakdown,
            "run_status_breakdown": run_status_breakdown,
            "severity_breakdown_items": severity_breakdown_items,
            "decision_breakdown_items": decision_breakdown_items,
            "run_status_breakdown_items": run_status_breakdown_items,
        },
    )


@login_required
def memory_proposals(request):
    if not can_view_orquestacion(request.user):
        raise PermissionDenied("No tienes permisos para ver propuestas de memoria.")

    selected_status = (request.GET.get("status") or "").strip()
    selected_section = (request.GET.get("section") or "").strip()
    selected_agent = (request.GET.get("agent") or "").strip()

    memory_proposals_available = _memory_proposals_table_available()
    if memory_proposals_available:
        proposals_qs = MemoryProposal.objects.select_related(
            "proposed_by_agent",
            "reviewed_by",
            "run",
            "task",
            "suggestion",
        )
        if selected_status in {choice[0] for choice in MemoryProposal.STATUS_CHOICES}:
            proposals_qs = proposals_qs.filter(status=selected_status)
        if selected_section in {choice[0] for choice in MemoryProposal.SECTION_CHOICES}:
            proposals_qs = proposals_qs.filter(section=selected_section)
        if selected_agent:
            proposals_qs = proposals_qs.filter(proposed_by_agent__code=selected_agent)

        proposals = list(proposals_qs.order_by("status", "-last_detected_at", "-id")[:50])
        summary = {
            "proposed": MemoryProposal.objects.filter(status=MemoryProposal.STATUS_PROPOSED).count(),
            "approved": MemoryProposal.objects.filter(status=MemoryProposal.STATUS_APPROVED).count(),
            "rejected": MemoryProposal.objects.filter(status=MemoryProposal.STATUS_REJECTED).count(),
            "applied": MemoryProposal.objects.filter(status=MemoryProposal.STATUS_APPLIED).count(),
        }
    else:
        proposals = []
        summary = {
            "proposed": 0,
            "approved": 0,
            "rejected": 0,
            "applied": 0,
        }

    return render(
        request,
        "orquestacion/memory_proposals.html",
        {
            "summary": summary,
            "proposals": proposals,
            "memory_proposals_available": memory_proposals_available,
            "filters": {
                "status": selected_status,
                "section": selected_section,
                "agent": selected_agent,
            },
            "status_options": MemoryProposal.STATUS_CHOICES,
            "section_options": MemoryProposal.SECTION_CHOICES,
            "agent_options": list(
                AgentDefinition.objects.order_by("priority_order", "name").values("code", "name")
            ),
        },
    )


@login_required
def memory_proposal_detail(request, proposal_id: int):
    if not can_view_orquestacion(request.user):
        raise PermissionDenied("No tienes permisos para ver propuestas de memoria.")

    proposal = get_object_or_404(
        MemoryProposal.objects.select_related("proposed_by_agent", "reviewed_by", "run", "task", "suggestion"),
        pk=proposal_id,
    )

    if request.method == "POST":
        if not can_manage_orquestacion(request.user):
            raise PermissionDenied("No tienes permisos para administrar propuestas de memoria.")

        action = (request.POST.get("action") or "").strip()
        try:
            if action == "approve":
                approve_memory_proposal(
                    proposal,
                    actor=request.user,
                    statement=request.POST.get("statement") or proposal.statement,
                    summary=request.POST.get("summary") or proposal.summary,
                )
                messages.success(request, "Propuesta de memoria aprobada.")
            elif action == "reject":
                reject_memory_proposal(
                    proposal,
                    actor=request.user,
                    reason=request.POST.get("rejection_reason") or "Rechazada desde Orquestación.",
                )
                messages.success(request, "Propuesta de memoria rechazada.")
            elif action == "apply":
                apply_memory_proposal(proposal, actor=request.user)
                messages.success(request, "Propuesta aplicada a memory.md.")
            else:
                messages.error(request, "Acción de propuesta no válida.")
        except ValueError as exc:
            messages.error(request, str(exc))
        return redirect("orquestacion:memory_proposal_detail", proposal_id=proposal.id)

    return render(
        request,
        "orquestacion/memory_proposal_detail.html",
        {
            "proposal": proposal,
            "can_manage": can_manage_orquestacion(request.user),
        },
    )


@login_required
def quality_findings(request):
    if not can_view_orquestacion(request.user):
        raise PermissionDenied("No tienes permisos para ver hallazgos de calidad.")

    selected_status = (request.GET.get("status") or "").strip()
    selected_severity = (request.GET.get("severity") or "").strip()
    selected_category = (request.GET.get("category") or "").strip()

    quality_loop_available = _quality_loop_tables_available()
    if quality_loop_available:
        findings_qs = QualityFinding.objects.select_related("memory_proposal").prefetch_related("remediation_proposals")
        if selected_status in {choice[0] for choice in QualityFinding.STATUS_CHOICES}:
            findings_qs = findings_qs.filter(status=selected_status)
        if selected_severity in {choice[0] for choice in QualityFinding.SEVERITY_CHOICES}:
            findings_qs = findings_qs.filter(severity=selected_severity)
        if selected_category in {choice[0] for choice in QualityFinding.CATEGORY_CHOICES}:
            findings_qs = findings_qs.filter(category=selected_category)
        findings = list(findings_qs.order_by("status", "-last_seen_at", "-id")[:50])
        remediation_summary = {
            "proposed": RemediationProposal.objects.filter(status=RemediationProposal.STATUS_PROPOSED).count(),
            "accepted": RemediationProposal.objects.filter(status=RemediationProposal.STATUS_ACCEPTED).count(),
            "implemented": RemediationProposal.objects.filter(status=RemediationProposal.STATUS_IMPLEMENTED).count(),
            "validated": RemediationProposal.objects.filter(status=RemediationProposal.STATUS_VALIDATED).count(),
            "rejected": RemediationProposal.objects.filter(status=RemediationProposal.STATUS_REJECTED).count(),
        }
        summary = {
            "open": QualityFinding.objects.filter(status=QualityFinding.STATUS_OPEN).count(),
            "resolved": QualityFinding.objects.filter(status=QualityFinding.STATUS_RESOLVED).count(),
            "blocking": QualityFinding.objects.filter(status=QualityFinding.STATUS_OPEN, is_blocking=True).count(),
        }
    else:
        findings = []
        remediation_summary = {key: 0 for key in ["proposed", "accepted", "implemented", "validated", "rejected"]}
        summary = {"open": 0, "resolved": 0, "blocking": 0}

    return render(
        request,
        "orquestacion/quality_findings.html",
        {
            "quality_loop_available": quality_loop_available,
            "findings": findings,
            "summary": summary,
            "remediation_summary": remediation_summary,
            "filters": {
                "status": selected_status,
                "severity": selected_severity,
                "category": selected_category,
            },
            "status_options": QualityFinding.STATUS_CHOICES,
            "severity_options": QualityFinding.SEVERITY_CHOICES,
            "category_options": QualityFinding.CATEGORY_CHOICES,
            "can_manage": can_manage_orquestacion(request.user),
        },
    )


@login_required
def quality_finding_detail(request, finding_id: int):
    if not can_view_orquestacion(request.user):
        raise PermissionDenied("No tienes permisos para ver hallazgos de calidad.")

    finding = get_object_or_404(
        QualityFinding.objects.select_related("memory_proposal").prefetch_related("remediation_proposals"),
        pk=finding_id,
    )
    remediation = finding.remediation_proposals.order_by("created_at", "id").first()

    if request.method == "POST":
        if not can_manage_orquestacion(request.user):
            raise PermissionDenied("No tienes permisos para administrar hallazgos de calidad.")

        action = (request.POST.get("action") or "").strip()
        try:
            if action == "rerun_guard":
                summary = sync_quality_guards(run_quality_guards(base_dir=settings.BASE_DIR))
                finding.refresh_from_db()
                if remediation:
                    remediation.refresh_from_db()
                messages.success(
                    request,
                    (
                        "Quality guards ejecutados. "
                        f"PointDailySale: {summary['pointdailysale']['violations']} · "
                        f"Protected readers: {summary['protected_sales_reader']['violations']} · "
                        f"Publication gap: {summary['sales_publication_gap']['violations']}."
                    ),
                )
            elif remediation is None:
                messages.error(request, "El hallazgo no tiene remediación asociada.")
            elif action == "accept_remediation":
                accept_remediation_proposal(remediation, actor=request.user)
                messages.success(request, "Remediación aceptada.")
            elif action == "mark_implemented":
                mark_remediation_implemented(remediation, actor=request.user)
                messages.success(request, "Remediación marcada como implementada.")
            elif action == "reject_remediation":
                reject_remediation_proposal(
                    remediation,
                    actor=request.user,
                    reason=request.POST.get("rejection_reason") or "Rechazada desde Orquestación.",
                )
                messages.success(request, "Remediación rechazada.")
            else:
                messages.error(request, "Acción de remediación no válida.")
        except ValueError as exc:
            messages.error(request, str(exc))
        return redirect("orquestacion:quality_finding_detail", finding_id=finding.id)

    return render(
        request,
        "orquestacion/quality_finding_detail.html",
        {
            "finding": finding,
            "remediation": remediation,
            "can_manage": can_manage_orquestacion(request.user),
        },
    )
