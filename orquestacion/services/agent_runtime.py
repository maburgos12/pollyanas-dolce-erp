from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.utils import timezone

from core.models import AuditLog
from orquestacion.models import (
    AgentDefinition,
    AgentGoalDelegation,
    AgentLoopCheckpoint,
    AgentSuggestion,
    AgentTask,
    OrchestrationRun,
)
from orquestacion.services.memory_proposals import propose_unresolved_tool_binding_gaps
from orquestacion.tool_binding import resolve_gateway_tool_alias


BASE_CONTEXT_FILES = [
    "AGENTS.md",
    ".agent/skills/README.md",
    ".agent/skills/00-core/skill-erp-context/SKILL.md",
    ".agent/skills/00-core/skill-director-general-mode/SKILL.md",
]
GOAL_CONTEXT_FILES = {
    "sales_event_publication_guard": [
        ".agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md",
        ".agent/skills/60-automation-ops/skill-sales-event-publication-guard/SKILL.md",
    ],
    "production_readiness_guard": [
        ".agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md",
    ],
    "purchase_review_guard": [
        ".agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md",
    ],
    "reconciliation_guard": [
        ".agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md",
    ],
    "operational_chain_review": [
        ".agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md",
        ".agent/skills/60-automation-ops/skill-sales-event-publication-guard/SKILL.md",
    ],
}
SALES_EVENT_GOAL_TYPES = {
    "sales_event_publication_guard",
    "production_readiness_guard",
    "purchase_review_guard",
    "reconciliation_guard",
    "operational_chain_review",
}


@dataclass(frozen=True)
class Goal:
    goal_type: str
    objective: str
    agent_code: str = ""
    entity_type: str = ""
    entity_id: int | None = None
    requested_action: str = "review"
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AgentContext:
    goal_type: str
    files_in_order: list[str]
    loaded_files: list[str]
    missing_files: list[str]
    context_markdown: str
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AgentMemory:
    path: str
    raw_markdown: str
    sections: dict[str, list[str]]
    stable_facts: list[str]
    recurrent_errors: list[str]
    known_gaps: list[str]
    update_policy: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ToolRegistryEntry:
    tool_key: str
    description: str
    kind: str
    executable: bool
    source: str
    priority: int
    declared_tool_key: str = ""
    binding_state: str = ""
    operation_type: str = ""
    requires_approval: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ToolRegistry:
    entries: list[ToolRegistryEntry]

    def executable_entries(self) -> list[ToolRegistryEntry]:
        return [entry for entry in self.entries if entry.executable]

    def as_dict(self) -> dict[str, Any]:
        return {"entries": [entry.as_dict() for entry in self.entries]}


@dataclass(frozen=True)
class BlockingFinding:
    code: str
    severity: str
    summary: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionStep:
    iteration: int
    phase: str
    title: str
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AgentLoopState:
    iteration: int = 0
    status: str = "pending"
    decision: str = ""
    observation: dict[str, Any] = field(default_factory=dict)
    executed_actions: list[dict[str, Any]] = field(default_factory=list)
    blocking_findings: list[BlockingFinding] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["blocking_findings"] = [finding.as_dict() for finding in self.blocking_findings]
        return payload


@dataclass(frozen=True)
class ExecutionResult:
    run_id: int
    task_id: int
    status: str
    decision: str
    context: AgentContext
    memory: AgentMemory
    tool_registry: ToolRegistry
    blocking_findings: list[BlockingFinding]
    executed_actions: list[dict[str, Any]]
    observation: dict[str, Any]
    delegations: list[dict[str, Any]] = field(default_factory=list)
    limits: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "status": self.status,
            "decision": self.decision,
            "context": self.context.as_dict(),
            "memory": self.memory.as_dict(),
            "tool_registry": self.tool_registry.as_dict(),
            "blocking_findings": [finding.as_dict() for finding in self.blocking_findings],
            "executed_actions": self.executed_actions,
            "observation": self.observation,
            "delegations": self.delegations,
            "limits": self.limits,
        }


@dataclass(frozen=True)
class GoalHandlerDefinition:
    goal_type: str
    agent_code: str
    tool_hints: list[dict[str, Any]]
    observer: Callable[[Goal], tuple[Any, dict[str, Any], list[BlockingFinding]]]
    executor: Callable[[Goal, Any, Any], dict[str, Any]] | None = None
    blocking_rules: list[str] = field(default_factory=list)
    handoff_targets: list[str] = field(default_factory=list)


def build_agent_context(goal: Goal, *, base_dir: str | Path | None = None) -> AgentContext:
    root = Path(base_dir or Path.cwd())
    agent = _resolve_goal_agent(goal)
    goal_files = GOAL_CONTEXT_FILES.get(goal.goal_type, [])
    files_in_order = _dedupe_preserving_order([*BASE_CONTEXT_FILES, *agent.context_files_json, *goal_files])
    loaded_files: list[str] = []
    missing_files: list[str] = []
    warnings: list[str] = []
    chunks: list[str] = []

    for relative_path in files_in_order:
        candidate = root / relative_path
        if not candidate.exists():
            missing_files.append(relative_path)
            warnings.append(f"Falta archivo de contexto requerido: {relative_path}")
            continue
        loaded_files.append(relative_path)
        chunks.append(f"<!-- {relative_path} -->\n{candidate.read_text(encoding='utf-8')}".strip())

    return AgentContext(
        goal_type=goal.goal_type,
        files_in_order=files_in_order,
        loaded_files=loaded_files,
        missing_files=missing_files,
        context_markdown="\n\n".join(chunks).strip(),
        warnings=warnings,
    )


def load_agent_memory(*, base_dir: str | Path | None = None, relative_path: str = "memory.md") -> AgentMemory:
    root = Path(base_dir or Path.cwd())
    memory_path = root / relative_path
    raw_markdown = memory_path.read_text(encoding="utf-8") if memory_path.exists() else ""
    sections = _parse_markdown_sections(raw_markdown)
    return AgentMemory(
        path=str(memory_path),
        raw_markdown=raw_markdown,
        sections=sections,
        stable_facts=sections.get("hechos estables confirmados", []),
        recurrent_errors=sections.get("errores recurrentes a evitar", []),
        known_gaps=sections.get("gaps estables confirmados", []),
        update_policy=sections.get("politica de actualizacion", []),
    )


def resolve_tool_registry(goal: Goal) -> ToolRegistry:
    agent = _resolve_goal_agent(goal)
    handler = _goal_handlers()[goal.goal_type]
    gateway_tools = _load_gateway_tool_definitions()
    entries = [
        ToolRegistryEntry(
            tool_key="runtime.goal_loop",
            description="Loop compartido de carga, observación, decisión, ejecución, verificación y cierre.",
            kind="runtime",
            executable=True,
            source="orquestacion.services.agent_runtime.run_agent_goal",
            priority=1,
            declared_tool_key="runtime.goal_loop",
            binding_state="runtime_native",
        ),
        ToolRegistryEntry(
            tool_key="runtime.memory_loader",
            description="Carga y parseo de memoria persistente del ERP.",
            kind="runtime",
            executable=True,
            source="orquestacion.services.agent_runtime.load_agent_memory",
            priority=2,
            declared_tool_key="runtime.memory_loader",
            binding_state="runtime_native",
        ),
    ]

    for path in _dedupe_preserving_order([*agent.context_files_json, *GOAL_CONTEXT_FILES.get(goal.goal_type, [])]):
        entries.append(
            ToolRegistryEntry(
                tool_key=f"skill:{path}",
                description="Contexto/skill documental cargado por el runtime.",
                kind="skill",
                executable=False,
                source=path,
                priority=10,
                declared_tool_key=path,
                binding_state="context_markdown",
            )
        )

    for index, declared_tool_key in enumerate(agent.allowed_tools_json, start=20):
        resolved_gateway_key = resolve_gateway_tool_alias(declared_tool_key, available_keys=gateway_tools.keys())
        if resolved_gateway_key:
            gateway_tool = gateway_tools[resolved_gateway_key]
            entries.append(
                ToolRegistryEntry(
                    tool_key=resolved_gateway_key,
                    description=f"{gateway_tool.name}: {gateway_tool.description}",
                    kind="gateway",
                    executable=True,
                    source=f"{declared_tool_key} -> {resolved_gateway_key}",
                    priority=index,
                    declared_tool_key=declared_tool_key,
                    binding_state="gateway_alias_resolved",
                    operation_type=gateway_tool.operation_type,
                    requires_approval=gateway_tool.requires_approval,
                )
            )
            continue

        kind, executable = _infer_tool_kind(declared_tool_key)
        entries.append(
            ToolRegistryEntry(
                tool_key=declared_tool_key,
                description=f"Tool permitida para {agent.code}.",
                kind=kind,
                executable=executable,
                source=declared_tool_key,
                priority=index,
                declared_tool_key=declared_tool_key,
                binding_state="unresolved_declared_tool",
            )
        )

    for index, hint in enumerate(handler.tool_hints, start=60):
        entries.append(
            ToolRegistryEntry(
                tool_key=hint["tool_key"],
                description=hint["description"],
                kind=hint["kind"],
                executable=hint["executable"],
                source=hint["source"],
                priority=index,
                declared_tool_key=hint["tool_key"],
                binding_state="runtime_hint",
            )
        )
    return ToolRegistry(entries=entries)


def resolve_runtime_actor(username: str | None):
    if not username:
        return None
    user_model = get_user_model()
    return user_model.objects.filter(username=username).first()


def run_agent_goal(
    goal: Goal,
    *,
    actor=None,
    base_dir: str | Path | None = None,
    max_iterations: int = 8,
    parent_run: OrchestrationRun | None = None,
    parent_task: AgentTask | None = None,
    delegation_order: int | None = None,
) -> ExecutionResult:
    if goal.goal_type not in _goal_handlers():
        raise ValueError(f"Goal type no soportado todavía: {goal.goal_type}")
    if goal.entity_type != "ventas.EventoVenta" or not goal.entity_id:
        raise ValueError("El runtime multi-agente actual requiere entity_type='ventas.EventoVenta' y entity_id válido.")

    handler = _goal_handlers()[goal.goal_type]
    normalized_goal = Goal(
        goal_type=goal.goal_type,
        objective=goal.objective,
        agent_code=goal.agent_code or handler.agent_code,
        entity_type=goal.entity_type,
        entity_id=goal.entity_id,
        requested_action=goal.requested_action,
        metadata=goal.metadata,
    )
    agent = _resolve_goal_agent(normalized_goal)
    if normalized_goal.goal_type not in (agent.supported_goal_types_json or []):
        raise ValueError(
            f"El agente '{agent.code}' no soporta el goal '{normalized_goal.goal_type}'. "
            f"Soporta: {', '.join(agent.supported_goal_types_json or []) or 'ninguno'}."
        )

    context = build_agent_context(normalized_goal, base_dir=base_dir)
    memory = load_agent_memory(base_dir=base_dir)
    tool_registry = resolve_tool_registry(normalized_goal)
    run = OrchestrationRun.objects.create(
        run_key=f"agent-goal:{normalized_goal.goal_type}:{normalized_goal.entity_id}:{uuid4().hex[:10]}",
        trigger_source="agent_runtime",
        status=OrchestrationRun.STATUS_RUNNING,
        started_at=timezone.localtime(),
        context_json={
            "goal": normalized_goal.as_dict(),
            "context": context.as_dict(),
            "memory": memory.as_dict(),
            "tools": tool_registry.as_dict(),
            "runtime_limits": {"max_iterations": max_iterations},
            "parent_run_id": parent_run.id if parent_run else None,
            "parent_task_id": parent_task.id if parent_task else None,
        },
        created_by=actor,
    )
    task = AgentTask.objects.create(
        run=run,
        agent=agent,
        title=normalized_goal.objective,
        task_type=normalized_goal.goal_type,
        priority=AgentTask.PRIORITY_HIGH,
        status=AgentTask.STATUS_RUNNING,
        input_payload={
            "goal": normalized_goal.as_dict(),
            "requested_action": normalized_goal.requested_action,
            "parent_run_id": parent_run.id if parent_run else None,
        },
        resolution_note="Agente ejecutando loop operativo.",
    )
    memory_proposal_results = propose_unresolved_tool_binding_gaps(
        goal_type=normalized_goal.goal_type,
        agent=agent,
        run=run,
        task=task,
        tool_registry_entries=tool_registry.entries,
    )

    delegation_record = None
    if parent_run and parent_task:
        delegation_record = AgentGoalDelegation.objects.create(
            parent_run=parent_run,
            parent_task=parent_task,
            from_agent=parent_task.agent,
            to_agent=agent,
            goal_type=normalized_goal.goal_type,
            sequence_order=delegation_order or 1,
            status=AgentGoalDelegation.STATUS_RUNNING,
            details_json={"goal": normalized_goal.as_dict()},
        )

    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=1,
            phase=AgentLoopCheckpoint.PHASE_LOAD,
            title="Contexto, memoria y herramientas cargadas.",
            details={
                "loaded_files": context.loaded_files,
                "missing_files": context.missing_files,
                "memory_sections": sorted(memory.sections.keys()),
                "tool_keys": [entry.tool_key for entry in tool_registry.entries],
                "memory_proposal_ids": [result.proposal.id for result in memory_proposal_results],
            },
        ),
    )

    if normalized_goal.goal_type == "operational_chain_review":
        result = _run_operational_chain(
            normalized_goal,
            run=run,
            task=task,
            actor=actor,
            base_dir=base_dir,
        )
    else:
        result = _run_single_goal(
            normalized_goal,
            handler=handler,
            run=run,
            task=task,
            actor=actor,
            context=context,
            memory=memory,
            tool_registry=tool_registry,
        )

    if delegation_record:
        delegation_record.child_run = OrchestrationRun.objects.get(id=result.run_id)
        delegation_record.child_task = AgentTask.objects.get(id=result.task_id)
        delegation_record.status = _map_run_status_to_delegation(result.status)
        delegation_record.details_json = {
            **delegation_record.details_json,
            "decision": result.decision,
            "blocking_findings": [finding.as_dict() for finding in result.blocking_findings],
        }
        delegation_record.save(update_fields=["child_run", "child_task", "status", "details_json", "updated_at"])

    _log_runtime_audit(run=run, task=task, goal=normalized_goal, decision=result.decision, actor=actor)
    return result


def _run_single_goal(
    goal: Goal,
    *,
    handler: GoalHandlerDefinition,
    run: OrchestrationRun,
    task: AgentTask,
    actor,
    context: AgentContext,
    memory: AgentMemory,
    tool_registry: ToolRegistry,
) -> ExecutionResult:
    observed_entity, observation, blocking_findings = handler.observer(goal)
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=2,
            phase=AgentLoopCheckpoint.PHASE_OBSERVE,
            title="Estado vivo del objetivo observado.",
            details=observation,
        ),
    )

    can_execute = goal.requested_action == "publish_if_safe" and handler.executor is not None
    decision = "block" if blocking_findings else ("publish" if can_execute else "complete_review")
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=3,
            phase=AgentLoopCheckpoint.PHASE_THINK,
            title="Decisión del loop calculada.",
            details={
                "decision": decision,
                "blocking_findings": [finding.as_dict() for finding in blocking_findings],
            },
        ),
    )

    executed_actions: list[dict[str, Any]] = []
    if decision == "publish" and handler.executor:
        execution_payload = handler.executor(goal, observed_entity, actor)
        executed_actions.append({"action": "execute_goal", **execution_payload})
        _create_checkpoint(
            run=run,
            step=ExecutionStep(
                iteration=4,
                phase=AgentLoopCheckpoint.PHASE_ACT,
                title="Acción segura ejecutada por el agente.",
                details=execution_payload,
            ),
        )
    elif decision == "block":
        suggestion = AgentSuggestion.objects.create(
            task=task,
            suggestion_type=f"{goal.goal_type}_block",
            domain=task.agent.domain,
            severity=AgentSuggestion.SEVERITY_CRITICAL,
            summary=f"Goal bloqueado: {goal.goal_type}",
            details_json={
                "goal": goal.as_dict(),
                "blocking_findings": [finding.as_dict() for finding in blocking_findings],
            },
            recommended_action="Corregir bloqueos antes de reintentar el goal.",
            requires_approval=True,
        )
        executed_actions.append({"action": "create_blocking_suggestion", "suggestion_id": suggestion.id})
        _create_checkpoint(
            run=run,
            step=ExecutionStep(
                iteration=4,
                phase=AgentLoopCheckpoint.PHASE_BLOCK,
                title="El loop bloqueó el goal.",
                details={
                    "suggestion_id": suggestion.id,
                    "blocking_findings": [finding.as_dict() for finding in blocking_findings],
                },
            ),
        )

    verification = _verify_goal_outcome(goal, decision=decision)
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=5,
            phase=AgentLoopCheckpoint.PHASE_VERIFY,
            title="Validación final del goal.",
            details=verification,
        ),
    )
    return _finalize_run(
        run=run,
        task=task,
        goal=goal,
        context=context,
        memory=memory,
        tool_registry=tool_registry,
        decision=decision,
        observation=observation,
        blocking_findings=blocking_findings,
        executed_actions=executed_actions,
        verification=verification,
        delegations=[],
    )


def _run_operational_chain(goal: Goal, *, run: OrchestrationRun, task: AgentTask, actor, base_dir: str | Path | None) -> ExecutionResult:
    from ventas.models import EventoVenta

    event = EventoVenta.objects.prefetch_related("projection_artifacts", "financials").get(pk=goal.entity_id)
    observation = {
        "goal": goal.goal_type,
        "event_id": goal.entity_id,
        "event_status": event.status,
        "sequence": [
            "sales_event_publication_guard",
            "production_readiness_guard",
            "purchase_review_guard",
        ],
    }
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=2,
            phase=AgentLoopCheckpoint.PHASE_OBSERVE,
            title="Cadena operativa preparada para delegación.",
            details=observation,
        ),
    )

    child_goals = [
        Goal(
            goal_type="sales_event_publication_guard",
            objective=f"Revisar publicabilidad comercial del evento {goal.entity_id}",
            agent_code="agente_publicacion_eventos_ventas",
            entity_type=goal.entity_type,
            entity_id=goal.entity_id,
            requested_action="review",
            metadata={"delegated_by": task.agent.code},
        ),
        Goal(
            goal_type="production_readiness_guard",
            objective=f"Revisar readiness productiva del evento {goal.entity_id}",
            agent_code="agente_produccion",
            entity_type=goal.entity_type,
            entity_id=goal.entity_id,
            requested_action="review",
            metadata={"delegated_by": task.agent.code},
        ),
        Goal(
            goal_type="purchase_review_guard",
            objective=f"Revisar readiness de compras del evento {goal.entity_id}",
            agent_code="agente_compras",
            entity_type=goal.entity_type,
            entity_id=goal.entity_id,
            requested_action="review",
            metadata={"delegated_by": task.agent.code},
        ),
    ]
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=3,
            phase=AgentLoopCheckpoint.PHASE_THINK,
            title="El orquestador decidió delegar a agentes especializados.",
            details={"child_goals": [child.goal_type for child in child_goals]},
        ),
    )

    child_results: list[ExecutionResult] = []
    for index, child_goal in enumerate(child_goals, start=1):
        child_results.append(
            run_agent_goal(
                child_goal,
                actor=actor,
                base_dir=base_dir,
                parent_run=run,
                parent_task=task,
                delegation_order=index,
            )
        )

    if _should_delegate_reconciliation(event=event, child_results=child_results):
        reconciliation_goal = Goal(
            goal_type="reconciliation_guard",
            objective=f"Conciliar consistencia operativa del evento {goal.entity_id}",
            agent_code="agente_conciliacion",
            entity_type=goal.entity_type,
            entity_id=goal.entity_id,
            requested_action="review",
            metadata={"delegated_by": task.agent.code},
        )
        child_goals.append(reconciliation_goal)
        child_results.append(
            run_agent_goal(
                reconciliation_goal,
                actor=actor,
                base_dir=base_dir,
                parent_run=run,
                parent_task=task,
                delegation_order=len(child_goals),
            )
        )

    delegations = [
        {
            "goal_type": child_goal.goal_type,
            "run_id": result.run_id,
            "task_id": result.task_id,
            "status": result.status,
            "decision": result.decision,
            "blocking_findings": [finding.as_dict() for finding in result.blocking_findings],
        }
        for child_goal, result in zip(child_goals, child_results)
    ]
    executed_actions = [
        {
            "action": "delegate_goal",
            **delegation,
        }
        for delegation in delegations
    ]

    blocking_findings = [
        finding
        for result in child_results
        for finding in result.blocking_findings
    ]
    decision = "chain_blocked" if blocking_findings else "complete_review"
    verification = _verify_goal_outcome(goal, decision=decision)
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=4,
            phase=AgentLoopCheckpoint.PHASE_VERIFY,
            title="Validación consolidada de la cadena operativa.",
            details={
                "decision": decision,
                "delegations": delegations,
                "verification": verification,
            },
        ),
    )
    return _finalize_run(
        run=run,
        task=task,
        goal=goal,
        context=build_agent_context(goal, base_dir=base_dir),
        memory=load_agent_memory(base_dir=base_dir),
        tool_registry=resolve_tool_registry(goal),
        decision=decision,
        observation=observation,
        blocking_findings=blocking_findings,
        executed_actions=executed_actions,
        verification=verification,
        delegations=delegations,
    )


def _finalize_run(
    *,
    run: OrchestrationRun,
    task: AgentTask,
    goal: Goal,
    context: AgentContext,
    memory: AgentMemory,
    tool_registry: ToolRegistry,
    decision: str,
    observation: dict[str, Any],
    blocking_findings: list[BlockingFinding],
    executed_actions: list[dict[str, Any]],
    verification: dict[str, Any],
    delegations: list[dict[str, Any]],
) -> ExecutionResult:
    final_phase = AgentLoopCheckpoint.PHASE_COMPLETE if not blocking_findings else AgentLoopCheckpoint.PHASE_BLOCK
    if decision == "chain_blocked":
        final_phase = AgentLoopCheckpoint.PHASE_BLOCK
    final_status = OrchestrationRun.STATUS_SUCCESS if not blocking_findings else OrchestrationRun.STATUS_PARTIAL
    if decision == "chain_blocked":
        final_status = OrchestrationRun.STATUS_PARTIAL

    task.status = AgentTask.STATUS_RESOLVED if final_status == OrchestrationRun.STATUS_SUCCESS else AgentTask.STATUS_BLOCKED
    task.output_payload = {
        "decision": decision,
        "observation": observation,
        "blocking_findings": [finding.as_dict() for finding in blocking_findings],
        "executed_actions": executed_actions,
        "verification": verification,
        "delegations": delegations,
    }
    task.resolution_note = verification.get("summary", "")
    task.save(update_fields=["status", "output_payload", "resolution_note", "updated_at"])

    run.status = final_status
    run.finished_at = timezone.localtime()
    run.result_summary_json = {
        "decision": decision,
        "goal": goal.as_dict(),
        "blocking_findings": [finding.as_dict() for finding in blocking_findings],
        "executed_actions": executed_actions,
        "verification": verification,
        "delegations": delegations,
    }
    run.save(update_fields=["status", "finished_at", "result_summary_json"])
    _create_checkpoint(
        run=run,
        step=ExecutionStep(
            iteration=6,
            phase=final_phase,
            title="Loop finalizado.",
            details={
                "run_status": run.status,
                "task_status": task.status,
                "decision": decision,
            },
        ),
    )

    return ExecutionResult(
        run_id=run.id,
        task_id=task.id,
        status=run.status,
        decision=decision,
        context=context,
        memory=memory,
        tool_registry=tool_registry,
        blocking_findings=blocking_findings,
        executed_actions=executed_actions,
        observation=observation,
        delegations=delegations,
        limits=[
            "El runtime multi-agente actual aún se limita a goals sobre ventas.EventoVenta.",
            "Las skills siguen siendo contexto documental; el binding MCP nativo aún no está integrado al runtime.",
            "Todavía no existe scheduler persistente ni memoria de largo plazo autoregenerada.",
        ],
    )


def _should_delegate_reconciliation(*, event, child_results: list[ExecutionResult]) -> bool:
    if any(result.blocking_findings for result in child_results):
        return True
    if event.projection_artifacts.filter(forecast_version=event.version).exists():
        return True
    if not event.financials.filter(scenario=event.scenario_focus).exists():
        return True
    if event.status in {"ENVIADO_A_COMPRAS", "EN_EJECUCION", "CERRADO", "EVALUADO_POST_EVENTO"}:
        return True
    return False


def _create_checkpoint(*, run: OrchestrationRun, step: ExecutionStep) -> AgentLoopCheckpoint:
    return AgentLoopCheckpoint.objects.create(
        run=run,
        iteration=step.iteration,
        phase=step.phase,
        title=step.title,
        details_json=step.details,
    )


def _verify_goal_outcome(goal: Goal, *, decision: str) -> dict[str, Any]:
    from ventas.models import EventoVenta
    from ventas.views import _projection_artifact_dir

    event = EventoVenta.objects.get(pk=goal.entity_id)
    artifact_dir = _projection_artifact_dir(event)
    active_rows = event.projection_artifacts.filter(forecast_version=event.version).count()
    active_files = len([path for path in artifact_dir.glob("*") if path.is_file()]) if artifact_dir.exists() else 0
    summary = "Loop revisado sin publicación."
    if decision == "publish":
        summary = "Artifacts publicados y verificados."
    elif decision in {"block", "chain_blocked"}:
        summary = "Loop bloqueado con evidencia."
    return {
        "event_status": event.status,
        "event_version": event.version,
        "active_artifact_rows": active_rows,
        "active_artifact_files": active_files,
        "production_plans": event.production_plans.count(),
        "confirmed_production_plans": event.production_plans.filter(status="CONFIRMADO").count(),
        "input_requirements": event.input_requirements.count(),
        "purchase_requirements": event.purchase_requirements.count(),
        "summary": summary,
    }


def _parse_markdown_sections(raw_markdown: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current_key: str | None = None
    for raw_line in raw_markdown.splitlines():
        line = raw_line.rstrip()
        if line.startswith("## "):
            current_key = line[3:].strip().lower()
            sections.setdefault(current_key, [])
            continue
        if current_key is None:
            continue
        stripped = line.strip()
        if stripped.startswith("- "):
            sections[current_key].append(stripped[2:].strip())
        elif stripped and not stripped.startswith("1."):
            sections[current_key].append(stripped)
    return sections


def _log_runtime_audit(*, run: OrchestrationRun, task: AgentTask, goal: Goal, decision: str, actor) -> AuditLog:
    return AuditLog.objects.create(
        user=actor,
        action="CREATE",
        model="orquestacion.AgentRuntimeExecution",
        object_id=str(run.id),
        payload={
            "run_key": run.run_key,
            "task_id": task.id,
            "goal": goal.as_dict(),
            "decision": decision,
            "result_summary": run.result_summary_json,
        },
    )


def _goal_handlers() -> dict[str, GoalHandlerDefinition]:
    from orquestacion.services.multi_agent_guards import (
        observe_production_readiness,
        observe_purchase_review,
        observe_reconciliation_guard,
    )
    from orquestacion.services.sales_event_publication_guard import (
        execute_sales_event_publication,
        observe_sales_event_publication,
    )

    return {
        "sales_event_publication_guard": GoalHandlerDefinition(
            goal_type="sales_event_publication_guard",
            agent_code="agente_publicacion_eventos_ventas",
            observer=observe_sales_event_publication,
            executor=execute_sales_event_publication,
            blocking_rules=[
                "workflow_gate_not_ready",
                "unknown_base_methods",
                "mature_branch_without_base",
                "financials_missing",
            ],
            handoff_targets=["agente_produccion"],
            tool_hints=[
                {
                    "tool_key": "python.sales_event_publication_guard",
                    "description": "Observa taxonomía, workflow y artifacts del evento comercial.",
                    "kind": "python_callable",
                    "executable": True,
                    "source": "orquestacion.services.sales_event_publication_guard.observe_sales_event_publication",
                },
                {
                    "tool_key": "python.persist_projection_artifacts",
                    "description": "Publica artifacts si el guardrail ya validó el evento.",
                    "kind": "python_callable",
                    "executable": True,
                    "source": "ventas.views._persist_projection_artifacts",
                },
            ],
        ),
        "production_readiness_guard": GoalHandlerDefinition(
            goal_type="production_readiness_guard",
            agent_code="agente_produccion",
            observer=observe_production_readiness,
            blocking_rules=["event_not_ready_for_production", "production_plan_missing"],
            handoff_targets=["agente_compras"],
            tool_hints=[
                {
                    "tool_key": "python.production_readiness_guard",
                    "description": "Valida readiness productiva sobre planes y workflow del evento.",
                    "kind": "python_callable",
                    "executable": True,
                    "source": "orquestacion.services.multi_agent_guards.observe_production_readiness",
                }
            ],
        ),
        "purchase_review_guard": GoalHandlerDefinition(
            goal_type="purchase_review_guard",
            agent_code="agente_compras",
            observer=observe_purchase_review,
            blocking_rules=["event_not_validated_by_production", "input_requirements_missing"],
            handoff_targets=["agente_conciliacion"],
            tool_hints=[
                {
                    "tool_key": "python.purchase_review_guard",
                    "description": "Valida readiness de compras e insumos del evento.",
                    "kind": "python_callable",
                    "executable": True,
                    "source": "orquestacion.services.multi_agent_guards.observe_purchase_review",
                }
            ],
        ),
        "reconciliation_guard": GoalHandlerDefinition(
            goal_type="reconciliation_guard",
            agent_code="agente_conciliacion",
            observer=observe_reconciliation_guard,
            blocking_rules=["artifact_disk_db_mismatch", "financial_publication_mismatch"],
            tool_hints=[
                {
                    "tool_key": "python.reconciliation_guard",
                    "description": "Valida consistencia DB/disco y cobertura financiera del evento.",
                    "kind": "python_callable",
                    "executable": True,
                    "source": "orquestacion.services.multi_agent_guards.observe_reconciliation_guard",
                }
            ],
        ),
        "operational_chain_review": GoalHandlerDefinition(
            goal_type="operational_chain_review",
            agent_code="director_operativo",
            observer=lambda goal: (None, {"goal": goal.goal_type}, []),
            handoff_targets=[
                "agente_publicacion_eventos_ventas",
                "agente_produccion",
                "agente_compras",
            ],
            tool_hints=[
                {
                    "tool_key": "python.operational_chain_review",
                    "description": "Orquesta la cadena publicación -> producción -> compras.",
                    "kind": "python_callable",
                    "executable": True,
                    "source": "orquestacion.services.agent_runtime._run_operational_chain",
                }
            ],
        ),
    }


def _map_run_status_to_delegation(status: str) -> str:
    if status == OrchestrationRun.STATUS_SUCCESS:
        return AgentGoalDelegation.STATUS_SUCCESS
    if status == OrchestrationRun.STATUS_PARTIAL:
        return AgentGoalDelegation.STATUS_BLOCKED
    if status == OrchestrationRun.STATUS_FAILED:
        return AgentGoalDelegation.STATUS_FAILED
    return AgentGoalDelegation.STATUS_RUNNING


def _infer_tool_kind(tool_key: str) -> tuple[str, bool]:
    if tool_key.startswith("skill."):
        return "skill", False
    if tool_key.startswith("erp."):
        return "gateway", True
    if tool_key.startswith("api.") or tool_key.startswith("orm."):
        return "integration", True
    if tool_key.startswith("python."):
        return "python_callable", True
    return "tool", True


def _load_gateway_tool_definitions() -> dict[str, Any]:
    try:
        from api.ai_gateway_services import TOOLS
    except Exception:
        return {}
    return TOOLS


def _resolve_goal_agent(goal: Goal) -> AgentDefinition:
    handler = _goal_handlers().get(goal.goal_type)
    agent_code = goal.agent_code or (handler.agent_code if handler else "")
    if not agent_code:
        raise ValueError(f"No fue posible resolver agent_code para goal '{goal.goal_type}'.")
    return AgentDefinition.objects.get(code=agent_code)


def _dedupe_preserving_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered
