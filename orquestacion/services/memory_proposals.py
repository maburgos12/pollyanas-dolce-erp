from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Iterable

from django.conf import settings
from django.utils import timezone

from core.models import AuditLog
from orquestacion.memory_control import append_controlled_memory_entry
from orquestacion.models import AgentDefinition, AgentSuggestion, AgentTask, MemoryProposal, OrchestrationRun


AUTO_APPROVAL_CATEGORY_TOOL_BINDING_GAP = "tool_binding_gap"
AUTO_APPROVAL_CATEGORY_RUNTIME_CONSTRAINT = "runtime_constraint"
AUTO_APPROVAL_CATEGORY_TEST_ENVIRONMENT_FACT = "test_environment_fact"
AUTO_APPROVAL_CATEGORY_VERIFIED_COMMAND_PATH = "verified_command_path"
AUTO_APPROVAL_CATEGORY_ARCHITECTURE_GUARD_VIOLATION = "architecture_guard_violation"
AUTO_APPROVAL_MANUAL_ONLY_CATEGORIES = {
    AUTO_APPROVAL_CATEGORY_RUNTIME_CONSTRAINT,
    AUTO_APPROVAL_CATEGORY_TEST_ENVIRONMENT_FACT,
    AUTO_APPROVAL_CATEGORY_VERIFIED_COMMAND_PATH,
    AUTO_APPROVAL_CATEGORY_ARCHITECTURE_GUARD_VIOLATION,
}
DEFAULT_AUTO_APPROVAL_CATEGORIES = [AUTO_APPROVAL_CATEGORY_TOOL_BINDING_GAP]


@dataclass(frozen=True)
class MemoryProposalResult:
    proposal: MemoryProposal
    created: bool
    reopened: bool = False


def propose_memory_update(
    *,
    section: str,
    statement: str,
    source_type: str,
    source_reference: str = "",
    summary: str = "",
    confidence_score: float = 0.0,
    evidence_refs: Iterable[str] | None = None,
    proposed_by_agent: AgentDefinition | None = None,
    run: OrchestrationRun | None = None,
    task: AgentTask | None = None,
    suggestion: AgentSuggestion | None = None,
) -> MemoryProposalResult:
    normalized_section = _normalize_section(section)
    normalized_statement = _normalize_text(statement)
    if not normalized_statement:
        raise ValueError("La propuesta de memoria requiere statement.")

    normalized_source_type = str(source_type or "").strip() or MemoryProposal.SOURCE_MANUAL
    normalized_source_reference = str(source_reference or "").strip()
    normalized_summary = (str(summary or "").strip() or normalized_statement[:255])[:255]
    normalized_evidence = _normalize_evidence(evidence_refs or [])
    if not normalized_evidence:
        raise ValueError("La propuesta de memoria requiere al menos una evidencia.")
    category = classify_memory_proposal(
        section=normalized_section,
        statement=normalized_statement,
        source_type=normalized_source_type,
        source_reference=normalized_source_reference,
        evidence_refs=normalized_evidence,
    )

    proposal_key = _build_proposal_key(
        section=normalized_section,
        statement=normalized_statement,
        source_type=normalized_source_type,
        source_reference=normalized_source_reference,
        proposed_by_agent=proposed_by_agent,
    )
    now = timezone.now()
    proposal, created = MemoryProposal.objects.get_or_create(
        proposal_key=proposal_key,
        defaults={
            "status": MemoryProposal.STATUS_PROPOSED,
            "section": normalized_section,
            "category": category,
            "summary": normalized_summary,
            "statement": normalized_statement,
            "confidence_score": float(confidence_score),
            "source_type": normalized_source_type,
            "source_reference": normalized_source_reference,
            "proposed_by_agent": proposed_by_agent,
            "run": run,
            "task": task,
            "suggestion": suggestion,
            "evidence_refs_json": normalized_evidence,
            "detected_count": 1,
            "first_detected_at": now,
            "last_detected_at": now,
        },
    )
    reopened = False
    if created:
        proposal = _maybe_auto_approve(proposal)
        _log_memory_proposal_audit("CREATE", proposal, actor=None, payload={"created": True})
        return MemoryProposalResult(proposal=proposal, created=True, reopened=False)

    update_fields = ["detected_count", "last_detected_at", "updated_at"]
    proposal.detected_count += 1
    proposal.last_detected_at = now
    proposal.evidence_refs_json = _merge_evidence(proposal.evidence_refs_json, normalized_evidence)
    update_fields.append("evidence_refs_json")
    if category != proposal.category:
        proposal.category = category
        update_fields.append("category")

    if normalized_summary and proposal.summary != normalized_summary:
        proposal.summary = normalized_summary
        update_fields.append("summary")
    if confidence_score > proposal.confidence_score:
        proposal.confidence_score = float(confidence_score)
        update_fields.append("confidence_score")
    if run and not proposal.run_id:
        proposal.run = run
        update_fields.append("run")
    if task and not proposal.task_id:
        proposal.task = task
        update_fields.append("task")
    if suggestion and not proposal.suggestion_id:
        proposal.suggestion = suggestion
        update_fields.append("suggestion")
    if proposed_by_agent and not proposal.proposed_by_agent_id:
        proposal.proposed_by_agent = proposed_by_agent
        update_fields.append("proposed_by_agent")
    if proposal.status == MemoryProposal.STATUS_REJECTED:
        proposal.status = MemoryProposal.STATUS_PROPOSED
        proposal.approval_mode = MemoryProposal.APPROVAL_MODE_MANUAL
        proposal.reviewed_by = None
        proposal.reviewed_at = None
        proposal.rejection_reason = ""
        proposal.auto_approval_reason = ""
        proposal.auto_approved_at = None
        reopened = True
        update_fields.extend(
            [
                "status",
                "approval_mode",
                "reviewed_by",
                "reviewed_at",
                "rejection_reason",
                "auto_approval_reason",
                "auto_approved_at",
            ]
        )
    proposal.save(update_fields=update_fields)
    proposal = _maybe_auto_approve(proposal)
    _log_memory_proposal_audit(
        "UPDATE",
        proposal,
        actor=None,
        payload={"created": False, "reopened": reopened, "detected_count": proposal.detected_count},
    )
    return MemoryProposalResult(proposal=proposal, created=False, reopened=reopened)


def approve_memory_proposal(
    proposal: MemoryProposal,
    *,
    actor,
    statement: str | None = None,
    summary: str | None = None,
) -> MemoryProposal:
    if statement is not None:
        proposal.statement = _normalize_text(statement)
    if summary is not None:
        proposal.summary = str(summary).strip()[:255] or proposal.summary
    proposal.status = MemoryProposal.STATUS_APPROVED
    proposal.approval_mode = MemoryProposal.APPROVAL_MODE_MANUAL
    proposal.reviewed_by = actor
    proposal.reviewed_at = timezone.now()
    proposal.rejection_reason = ""
    proposal.auto_approval_reason = ""
    proposal.auto_approved_at = None
    proposal.save(
        update_fields=[
            "statement",
            "summary",
            "status",
            "approval_mode",
            "reviewed_by",
            "reviewed_at",
            "rejection_reason",
            "auto_approval_reason",
            "auto_approved_at",
            "updated_at",
        ]
    )
    _log_memory_proposal_audit("APPROVE", proposal, actor=actor, payload={"status": proposal.status})
    return proposal


def reject_memory_proposal(proposal: MemoryProposal, *, actor, reason: str) -> MemoryProposal:
    proposal.status = MemoryProposal.STATUS_REJECTED
    proposal.approval_mode = MemoryProposal.APPROVAL_MODE_MANUAL
    proposal.reviewed_by = actor
    proposal.reviewed_at = timezone.now()
    proposal.rejection_reason = str(reason or "").strip()
    proposal.save(update_fields=["status", "approval_mode", "reviewed_by", "reviewed_at", "rejection_reason", "updated_at"])
    _log_memory_proposal_audit("REJECT", proposal, actor=actor, payload={"reason": proposal.rejection_reason})
    return proposal


def apply_memory_proposal(
    proposal: MemoryProposal,
    *,
    actor,
    base_dir: str | Path | None = None,
) -> MemoryProposal:
    if proposal.status not in {MemoryProposal.STATUS_APPROVED, MemoryProposal.STATUS_APPLIED}:
        raise ValueError("Solo se pueden aplicar propuestas aprobadas.")

    resolved_base_dir = base_dir or getattr(settings, "ORQUESTACION_MEMORY_BASE_DIR", settings.BASE_DIR)
    result = append_controlled_memory_entry(
        section=proposal.section,
        text=proposal.statement,
        evidence_refs=proposal.evidence_refs_json,
        source=_build_apply_source(proposal),
        actor=actor,
        base_dir=resolved_base_dir,
    )
    proposal.status = MemoryProposal.STATUS_APPLIED
    proposal.applied_at = timezone.now()
    proposal.applied_result_json = result.as_dict()
    proposal.reviewed_by = actor
    proposal.reviewed_at = proposal.reviewed_at or proposal.applied_at
    proposal.save(update_fields=["status", "applied_at", "applied_result_json", "reviewed_by", "reviewed_at", "updated_at"])
    _log_memory_proposal_audit("APPLY", proposal, actor=actor, payload=result.as_dict())
    return proposal


def propose_unresolved_tool_binding_gaps(
    *,
    goal_type: str,
    agent: AgentDefinition,
    run: OrchestrationRun,
    task: AgentTask,
    tool_registry_entries,
) -> list[MemoryProposalResult]:
    results: list[MemoryProposalResult] = []
    for entry in tool_registry_entries:
        if entry.binding_state != "unresolved_declared_tool":
            continue
        if not entry.declared_tool_key.startswith("api."):
            continue
        result = propose_memory_update(
            section=MemoryProposal.SECTION_GAP,
            summary=f"Tool sin binding real: {entry.declared_tool_key}",
            statement=(
                f"El agente {agent.code} declara la tool {entry.declared_tool_key} para el goal "
                f"{goal_type}, pero el gateway actual no tiene un binding real confirmado para esa key."
            ),
            source_type=MemoryProposal.SOURCE_AGENT_RUNTIME,
            source_reference=f"{agent.code}:{goal_type}:{entry.declared_tool_key}",
            confidence_score=0.98,
            evidence_refs=[
                "orquestacion/services/agent_runtime.py",
                "orquestacion/tool_binding.py",
                "docs/AGENTS_RUNTIME_SNAPSHOT.json",
            ],
            proposed_by_agent=agent,
            run=run,
            task=task,
        )
        results.append(result)
    return results


def classify_memory_proposal(
    *,
    section: str,
    statement: str,
    source_type: str,
    source_reference: str,
    evidence_refs: Iterable[str],
) -> str:
    normalized_statement = _normalize_text(statement).lower()
    normalized_source_reference = str(source_reference or "").strip().lower()
    normalized_evidence = [str(item).strip().lower() for item in evidence_refs if str(item).strip()]

    if (
        section == MemoryProposal.SECTION_GAP
        and source_type == MemoryProposal.SOURCE_AGENT_RUNTIME
        and "binding" in normalized_statement
        and "gateway" in normalized_statement
        and "api." in normalized_source_reference
    ):
        return AUTO_APPROVAL_CATEGORY_TOOL_BINDING_GAP

    if (
        section in {MemoryProposal.SECTION_FACT, MemoryProposal.SECTION_GAP}
        and source_type in {MemoryProposal.SOURCE_AGENT_RUNTIME, MemoryProposal.SOURCE_TEST_VALIDATION}
        and ("runtime" in normalized_statement or "constraint" in normalized_statement or "limita" in normalized_statement)
    ):
        return AUTO_APPROVAL_CATEGORY_RUNTIME_CONSTRAINT

    if (
        section == MemoryProposal.SECTION_FACT
        and source_type == MemoryProposal.SOURCE_TEST_VALIDATION
        and any("settings_test.py" in ref or "run_tests_local.sh" in ref for ref in normalized_evidence)
    ):
        return AUTO_APPROVAL_CATEGORY_TEST_ENVIRONMENT_FACT

    if (
        section == MemoryProposal.SECTION_FACT
        and source_type == MemoryProposal.SOURCE_TEST_VALIDATION
        and ("comando oficial" in normalized_statement or "ruta oficial" in normalized_statement)
    ):
        return AUTO_APPROVAL_CATEGORY_VERIFIED_COMMAND_PATH

    if (
        section == MemoryProposal.SECTION_GAP
        and source_type == MemoryProposal.SOURCE_QUALITY_GUARD
        and "pointdailysale" in normalized_source_reference
        and "allowlist" in normalized_statement
    ):
        return AUTO_APPROVAL_CATEGORY_ARCHITECTURE_GUARD_VIOLATION

    return ""


def evaluate_auto_approval(proposal: MemoryProposal) -> tuple[bool, str]:
    if not getattr(settings, "ORQUESTACION_MEMORY_AUTO_APPROVAL_ENABLED", True):
        return False, "feature_flag_disabled"
    allowed_categories = set(
        getattr(settings, "ORQUESTACION_MEMORY_AUTO_APPROVAL_CATEGORIES", DEFAULT_AUTO_APPROVAL_CATEGORIES)
    )
    if proposal.category not in allowed_categories:
        return False, "category_not_allowed"
    if proposal.category in AUTO_APPROVAL_MANUAL_ONLY_CATEGORIES:
        return False, "manual_only_category"
    if proposal.status != MemoryProposal.STATUS_PROPOSED:
        return False, "status_not_proposed"
    if proposal.section not in {MemoryProposal.SECTION_FACT, MemoryProposal.SECTION_GAP}:
        return False, "section_not_allowed"
    if proposal.source_type not in {MemoryProposal.SOURCE_AGENT_RUNTIME, MemoryProposal.SOURCE_TEST_VALIDATION}:
        return False, "source_type_not_allowed"
    if proposal.confidence_score < 0.95:
        return False, "confidence_below_threshold"
    if proposal.detected_count < 2:
        return False, "detected_count_below_threshold"
    if len(proposal.evidence_refs_json or []) < 2:
        return False, "insufficient_evidence"
    if _looks_like_business_or_opinion(proposal.statement):
        return False, "business_or_opinion_statement"
    return True, (
        f"autoapproved:{proposal.category}:"
        f"source={proposal.source_type}:"
        f"confidence={proposal.confidence_score:.2f}:"
        f"detected_count={proposal.detected_count}"
    )


def _maybe_auto_approve(proposal: MemoryProposal) -> MemoryProposal:
    approved, reason = evaluate_auto_approval(proposal)
    if not approved:
        return proposal
    proposal.status = MemoryProposal.STATUS_APPROVED
    proposal.approval_mode = MemoryProposal.APPROVAL_MODE_AUTO
    proposal.reviewed_by = None
    proposal.reviewed_at = timezone.now()
    proposal.auto_approval_reason = reason
    proposal.auto_approved_at = proposal.reviewed_at
    proposal.save(
        update_fields=[
            "status",
            "approval_mode",
            "reviewed_by",
            "reviewed_at",
            "auto_approval_reason",
            "auto_approved_at",
            "updated_at",
        ]
    )
    _log_memory_proposal_audit(
        "AUTO_APPROVE",
        proposal,
        actor=None,
        payload={"reason": reason, "category": proposal.category},
    )
    return proposal


def _normalize_section(section: str) -> str:
    candidate = str(section or "").strip().lower()
    if candidate not in {MemoryProposal.SECTION_FACT, MemoryProposal.SECTION_ERROR, MemoryProposal.SECTION_GAP}:
        raise ValueError("Sección inválida para propuesta de memoria.")
    return candidate


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_evidence(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = str(value or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def _merge_evidence(existing: Iterable[str], new_values: Iterable[str]) -> list[str]:
    return _normalize_evidence([*existing, *new_values])


def _build_proposal_key(
    *,
    section: str,
    statement: str,
    source_type: str,
    source_reference: str,
    proposed_by_agent: AgentDefinition | None,
) -> str:
    raw_key = "||".join(
        [
            section,
            _normalize_text(statement).lower(),
            str(source_type or "").strip().lower(),
            str(source_reference or "").strip().lower(),
            proposed_by_agent.code if proposed_by_agent else "",
        ]
    )
    return f"mem-{sha1(raw_key.encode('utf-8')).hexdigest()[:20]}"


def _build_apply_source(proposal: MemoryProposal) -> str:
    reference = proposal.source_reference or proposal.proposal_key
    return f"{proposal.source_type}.{reference}"


def _looks_like_business_or_opinion(statement: str) -> bool:
    normalized = _normalize_text(statement).lower()
    risky_fragments = [
        "debería",
        "deberia",
        "negocio",
        "operación",
        "operacion",
        "prioridad comercial",
        "conviene",
        "mejor",
        "recomendación",
        "recomendacion",
    ]
    return any(fragment in normalized for fragment in risky_fragments)


def _log_memory_proposal_audit(action: str, proposal: MemoryProposal, *, actor, payload: dict) -> None:
    AuditLog.objects.create(
        user=actor,
        action=action,
        model="orquestacion.MemoryProposal",
        object_id=str(proposal.id or proposal.proposal_key),
        payload={
            "proposal_key": proposal.proposal_key,
            "status": proposal.status,
            "section": proposal.section,
            **payload,
        },
    )
