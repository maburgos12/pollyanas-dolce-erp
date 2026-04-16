from __future__ import annotations

from uuid import uuid4

from django.conf import settings
from django.db import models
from django.utils import timezone


class AgentDefinition(models.Model):
    STATUS_DRAFT = "draft"
    STATUS_ACTIVE = "active"
    STATUS_PAUSED = "paused"
    STATUS_RETIRED = "retired"
    STATUS_CHOICES = [
        (STATUS_DRAFT, "Borrador"),
        (STATUS_ACTIVE, "Activo"),
        (STATUS_PAUSED, "Pausado"),
        (STATUS_RETIRED, "Retirado"),
    ]

    code = models.CharField(max_length=80, unique=True)
    name = models.CharField(max_length=120)
    domain = models.CharField(max_length=80)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_DRAFT)
    description = models.TextField(blank=True, default="")
    owner_department = models.ForeignKey(
        "core.Departamento",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="orchestration_agents",
    )
    system_prompt_version = models.CharField(max_length=40, blank=True, default="")
    allowed_tools_json = models.JSONField(default=list, blank=True)
    allowed_actions_json = models.JSONField(default=list, blank=True)
    supported_goal_types_json = models.JSONField(default=list, blank=True)
    context_files_json = models.JSONField(default=list, blank=True)
    blocking_rules_json = models.JSONField(default=list, blank=True)
    handoff_targets_json = models.JSONField(default=list, blank=True)
    requires_human_approval_default = models.BooleanField(default=True)
    priority_order = models.PositiveIntegerField(default=100)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Agente"
        verbose_name_plural = "Agentes"
        ordering = ["priority_order", "name", "id"]
        indexes = [
            models.Index(fields=["status", "priority_order"]),
            models.Index(fields=["domain", "status"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.code})"


class AgentCapability(models.Model):
    SCOPE_READ = "read"
    SCOPE_ANALYZE = "analyze"
    SCOPE_RECOMMEND = "recommend"
    SCOPE_REQUEST_APPROVAL = "request_approval"
    SCOPE_EXECUTE_SAFE = "execute_safe_action"
    SCOPE_EXECUTE_SENSITIVE = "execute_sensitive_action"
    SCOPE_CHOICES = [
        (SCOPE_READ, "Lectura"),
        (SCOPE_ANALYZE, "Analisis"),
        (SCOPE_RECOMMEND, "Recomendacion"),
        (SCOPE_REQUEST_APPROVAL, "Solicitud de aprobacion"),
        (SCOPE_EXECUTE_SAFE, "Ejecucion segura"),
        (SCOPE_EXECUTE_SENSITIVE, "Ejecucion sensible"),
    ]

    agent = models.ForeignKey(AgentDefinition, on_delete=models.CASCADE, related_name="capabilities")
    capability_key = models.CharField(max_length=120)
    scope_type = models.CharField(max_length=32, choices=SCOPE_CHOICES, default=SCOPE_READ)
    resource_key = models.CharField(max_length=255)
    branch_scope = models.JSONField(default=list, blank=True)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Capacidad de agente"
        verbose_name_plural = "Capacidades de agentes"
        ordering = ["agent__priority_order", "capability_key", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["agent", "capability_key", "resource_key"],
                name="orquestacion_agent_capability_unique_resource",
            )
        ]
        indexes = [
            models.Index(fields=["scope_type", "active"]),
            models.Index(fields=["capability_key", "active"]),
        ]

    def __str__(self) -> str:
        return f"{self.agent.code}: {self.capability_key}"


class OrchestrationRule(models.Model):
    TRIGGER_EVENT = "event"
    TRIGGER_SCHEDULE = "schedule"
    TRIGGER_MANUAL = "manual"
    TRIGGER_THRESHOLD = "threshold"
    TRIGGER_CHOICES = [
        (TRIGGER_EVENT, "Evento"),
        (TRIGGER_SCHEDULE, "Programado"),
        (TRIGGER_MANUAL, "Manual"),
        (TRIGGER_THRESHOLD, "Umbral"),
    ]

    ACTION_OBSERVE = "observe"
    ACTION_RECOMMEND = "recommend"
    ACTION_APPROVAL_REQUIRED = "approval_required"
    ACTION_CHOICES = [
        (ACTION_OBSERVE, "Observar"),
        (ACTION_RECOMMEND, "Recomendar"),
        (ACTION_APPROVAL_REQUIRED, "Requiere aprobacion"),
    ]

    code = models.CharField(max_length=80, unique=True)
    name = models.CharField(max_length=140)
    trigger_type = models.CharField(max_length=24, choices=TRIGGER_CHOICES, default=TRIGGER_EVENT)
    source_event = models.CharField(max_length=120, blank=True, default="")
    condition_json = models.JSONField(default=dict, blank=True)
    primary_agent = models.ForeignKey(
        AgentDefinition,
        on_delete=models.PROTECT,
        related_name="primary_rules",
    )
    secondary_agent = models.ForeignKey(
        AgentDefinition,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="secondary_rules",
    )
    action_mode = models.CharField(max_length=24, choices=ACTION_CHOICES, default=ACTION_OBSERVE)
    cooldown_minutes = models.PositiveIntegerField(default=0)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Regla de orquestacion"
        verbose_name_plural = "Reglas de orquestacion"
        ordering = ["name", "id"]
        indexes = [
            models.Index(fields=["is_active", "trigger_type"]),
            models.Index(fields=["source_event", "is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.code})"


class OrchestrationRun(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_PARTIAL = "partial"
    STATUS_FAILED = "failed"
    STATUS_CANCELLED = "cancelled"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_RUNNING, "En ejecucion"),
        (STATUS_SUCCESS, "Exitoso"),
        (STATUS_PARTIAL, "Parcial"),
        (STATUS_FAILED, "Fallido"),
        (STATUS_CANCELLED, "Cancelado"),
    ]

    run_key = models.CharField(max_length=120, unique=True)
    trigger_source = models.CharField(max_length=80)
    rule = models.ForeignKey(
        OrchestrationRule,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="runs",
    )
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    context_json = models.JSONField(default=dict, blank=True)
    result_summary_json = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="orchestration_runs",
    )

    class Meta:
        verbose_name = "Corrida de orquestacion"
        verbose_name_plural = "Corridas de orquestacion"
        ordering = ["-started_at", "-id"]
        indexes = [
            models.Index(fields=["status", "started_at"]),
            models.Index(fields=["trigger_source", "started_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.run_key} [{self.status}]"


class AgentTask(models.Model):
    PRIORITY_LOW = "low"
    PRIORITY_MEDIUM = "medium"
    PRIORITY_HIGH = "high"
    PRIORITY_CRITICAL = "critical"
    PRIORITY_CHOICES = [
        (PRIORITY_LOW, "Baja"),
        (PRIORITY_MEDIUM, "Media"),
        (PRIORITY_HIGH, "Alta"),
        (PRIORITY_CRITICAL, "Critica"),
    ]

    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_BLOCKED = "blocked"
    STATUS_RESOLVED = "resolved"
    STATUS_CANCELLED = "cancelled"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_RUNNING, "En ejecucion"),
        (STATUS_BLOCKED, "Bloqueada"),
        (STATUS_RESOLVED, "Resuelta"),
        (STATUS_CANCELLED, "Cancelada"),
    ]

    run = models.ForeignKey(OrchestrationRun, on_delete=models.CASCADE, related_name="tasks")
    agent = models.ForeignKey(AgentDefinition, on_delete=models.PROTECT, related_name="tasks")
    title = models.CharField(max_length=200)
    task_type = models.CharField(max_length=80)
    priority = models.CharField(max_length=16, choices=PRIORITY_CHOICES, default=PRIORITY_MEDIUM)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    input_payload = models.JSONField(default=dict, blank=True)
    output_payload = models.JSONField(default=dict, blank=True)
    assigned_branch = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="orchestration_tasks",
    )
    due_at = models.DateTimeField(null=True, blank=True)
    resolution_note = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Tarea de agente"
        verbose_name_plural = "Tareas de agentes"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["status", "priority", "due_at"]),
            models.Index(fields=["agent", "status"]),
        ]

    def __str__(self) -> str:
        return f"{self.agent.code}: {self.title}"


class AgentSuggestion(models.Model):
    SEVERITY_INFO = "info"
    SEVERITY_WARNING = "warning"
    SEVERITY_HIGH = "high"
    SEVERITY_CRITICAL = "critical"
    SEVERITY_CHOICES = [
        (SEVERITY_INFO, "Info"),
        (SEVERITY_WARNING, "Advertencia"),
        (SEVERITY_HIGH, "Alta"),
        (SEVERITY_CRITICAL, "Critica"),
    ]

    DECISION_PENDING = "pending"
    DECISION_APPROVED = "approved"
    DECISION_REJECTED = "rejected"
    DECISION_EXECUTED = "executed"
    DECISION_CHOICES = [
        (DECISION_PENDING, "Pendiente"),
        (DECISION_APPROVED, "Aprobada"),
        (DECISION_REJECTED, "Rechazada"),
        (DECISION_EXECUTED, "Ejecutada"),
    ]

    task = models.ForeignKey(AgentTask, on_delete=models.CASCADE, related_name="suggestions")
    suggestion_type = models.CharField(max_length=80)
    domain = models.CharField(max_length=80)
    severity = models.CharField(max_length=16, choices=SEVERITY_CHOICES, default=SEVERITY_INFO)
    summary = models.CharField(max_length=255)
    details_json = models.JSONField(default=dict, blank=True)
    recommended_action = models.CharField(max_length=255, blank=True, default="")
    requires_approval = models.BooleanField(default=True)
    decision_status = models.CharField(max_length=16, choices=DECISION_CHOICES, default=DECISION_PENDING)
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="approved_agent_suggestions",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    rejected_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="rejected_agent_suggestions",
    )
    rejected_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Sugerencia de agente"
        verbose_name_plural = "Sugerencias de agentes"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["decision_status", "severity"]),
            models.Index(fields=["domain", "decision_status"]),
        ]

    def __str__(self) -> str:
        return self.summary


class AgentExecutionLink(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_CANCELLED = "cancelled"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_RUNNING, "En ejecucion"),
        (STATUS_SUCCESS, "Exitosa"),
        (STATUS_FAILED, "Fallida"),
        (STATUS_CANCELLED, "Cancelada"),
    ]

    suggestion = models.ForeignKey(AgentSuggestion, on_delete=models.CASCADE, related_name="executions")
    execution_mode = models.CharField(max_length=40)
    target_reference = models.CharField(max_length=255, blank=True, default="")
    execution_status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    audit_log = models.ForeignKey(
        "core.AuditLog",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="orchestration_execution_links",
    )
    executed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="orchestration_executions",
    )
    executed_at = models.DateTimeField(null=True, blank=True)
    execution_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Ejecucion ligada a sugerencia"
        verbose_name_plural = "Ejecuciones ligadas a sugerencias"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["execution_status", "created_at"]),
            models.Index(fields=["execution_mode", "execution_status"]),
        ]

    def __str__(self) -> str:
        return f"{self.execution_mode} -> {self.target_reference or 'sin destino'}"


class AgentGap(models.Model):
    STATUS_OPEN = "open"
    STATUS_UNDER_REVIEW = "under_review"
    STATUS_ACCEPTED = "accepted"
    STATUS_REJECTED = "rejected"
    STATUS_IMPLEMENTED = "implemented"
    STATUS_CHOICES = [
        (STATUS_OPEN, "Abierto"),
        (STATUS_UNDER_REVIEW, "En revision"),
        (STATUS_ACCEPTED, "Aceptado"),
        (STATUS_REJECTED, "Rechazado"),
        (STATUS_IMPLEMENTED, "Implementado"),
    ]

    gap_type = models.CharField(max_length=80)
    detected_by_rule = models.ForeignKey(
        OrchestrationRule,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="gaps",
    )
    summary = models.CharField(max_length=255)
    evidence_json = models.JSONField(default=dict, blank=True)
    suggested_agent_name = models.CharField(max_length=140)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_OPEN)
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reviewed_agent_gaps",
    )
    reviewed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Hueco de cobertura de agentes"
        verbose_name_plural = "Huecos de cobertura de agentes"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["gap_type", "status"]),
        ]

    def __str__(self) -> str:
        return self.summary


class MemoryProposal(models.Model):
    STATUS_PROPOSED = "proposed"
    STATUS_APPROVED = "approved"
    STATUS_REJECTED = "rejected"
    STATUS_APPLIED = "applied"
    STATUS_CHOICES = [
        (STATUS_PROPOSED, "Propuesta"),
        (STATUS_APPROVED, "Aprobada"),
        (STATUS_REJECTED, "Rechazada"),
        (STATUS_APPLIED, "Aplicada"),
    ]

    SECTION_FACT = "fact"
    SECTION_ERROR = "error"
    SECTION_GAP = "gap"
    SECTION_CHOICES = [
        (SECTION_FACT, "Hecho"),
        (SECTION_ERROR, "Error"),
        (SECTION_GAP, "Gap"),
    ]

    SOURCE_AGENT_RUNTIME = "agent_runtime"
    SOURCE_RULE_RUNNER = "rule_runner"
    SOURCE_GATEWAY = "gateway"
    SOURCE_TEST_VALIDATION = "test_validation"
    SOURCE_QUALITY_GUARD = "quality_guard"
    SOURCE_MANUAL = "manual"
    SOURCE_CHOICES = [
        (SOURCE_AGENT_RUNTIME, "Runtime de agente"),
        (SOURCE_RULE_RUNNER, "Rule runner"),
        (SOURCE_GATEWAY, "AI Gateway"),
        (SOURCE_TEST_VALIDATION, "Validación"),
        (SOURCE_QUALITY_GUARD, "Quality guard"),
        (SOURCE_MANUAL, "Manual"),
    ]

    APPROVAL_MODE_MANUAL = "manual"
    APPROVAL_MODE_AUTO = "auto"
    APPROVAL_MODE_CHOICES = [
        (APPROVAL_MODE_MANUAL, "Manual"),
        (APPROVAL_MODE_AUTO, "Automática"),
    ]

    proposal_key = models.CharField(max_length=120, unique=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PROPOSED)
    section = models.CharField(max_length=16, choices=SECTION_CHOICES)
    category = models.CharField(max_length=64, blank=True, default="")
    summary = models.CharField(max_length=255)
    statement = models.TextField()
    confidence_score = models.FloatField(default=0.0)
    source_type = models.CharField(max_length=32, choices=SOURCE_CHOICES, default=SOURCE_MANUAL)
    source_reference = models.CharField(max_length=255, blank=True, default="")
    approval_mode = models.CharField(max_length=16, choices=APPROVAL_MODE_CHOICES, default=APPROVAL_MODE_MANUAL)
    proposed_by_agent = models.ForeignKey(
        AgentDefinition,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="memory_proposals",
    )
    run = models.ForeignKey(
        OrchestrationRun,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="memory_proposals",
    )
    task = models.ForeignKey(
        AgentTask,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="memory_proposals",
    )
    suggestion = models.ForeignKey(
        AgentSuggestion,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="memory_proposals",
    )
    evidence_refs_json = models.JSONField(default=list, blank=True)
    detected_count = models.PositiveIntegerField(default=1)
    first_detected_at = models.DateTimeField(default=timezone.now)
    last_detected_at = models.DateTimeField(default=timezone.now)
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reviewed_memory_proposals",
    )
    reviewed_at = models.DateTimeField(null=True, blank=True)
    rejection_reason = models.TextField(blank=True, default="")
    auto_approval_reason = models.TextField(blank=True, default="")
    auto_approved_at = models.DateTimeField(null=True, blank=True)
    applied_at = models.DateTimeField(null=True, blank=True)
    applied_result_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Propuesta de memoria"
        verbose_name_plural = "Propuestas de memoria"
        ordering = ["status", "-last_detected_at", "-id"]
        indexes = [
            models.Index(fields=["status", "section"]),
            models.Index(fields=["source_type", "last_detected_at"]),
            models.Index(fields=["proposed_by_agent", "status"]),
        ]

    def __str__(self) -> str:
        return f"{self.get_section_display()}: {self.summary}"


class QualityFinding(models.Model):
    CATEGORY_ARCHITECTURE_VIOLATION = "architecture_violation"
    CATEGORY_TEST_REGRESSION = "test_regression"
    CATEGORY_PUBLICATION_GAP = "publication_gap"
    CATEGORY_RUNTIME_GAP = "runtime_gap"
    CATEGORY_CHOICES = [
        (CATEGORY_ARCHITECTURE_VIOLATION, "Violacion arquitectonica"),
        (CATEGORY_TEST_REGRESSION, "Regresion de pruebas"),
        (CATEGORY_PUBLICATION_GAP, "Gap de publicacion"),
        (CATEGORY_RUNTIME_GAP, "Gap de runtime"),
    ]

    SEVERITY_INFO = "info"
    SEVERITY_WARNING = "warning"
    SEVERITY_HIGH = "high"
    SEVERITY_CRITICAL = "critical"
    SEVERITY_CHOICES = [
        (SEVERITY_INFO, "Info"),
        (SEVERITY_WARNING, "Advertencia"),
        (SEVERITY_HIGH, "Alta"),
        (SEVERITY_CRITICAL, "Critica"),
    ]

    STATUS_OPEN = "open"
    STATUS_RESOLVED = "resolved"
    STATUS_SUPPRESSED = "suppressed"
    STATUS_CHOICES = [
        (STATUS_OPEN, "Abierto"),
        (STATUS_RESOLVED, "Resuelto"),
        (STATUS_SUPPRESSED, "Suprimido"),
    ]

    SOURCE_GUARD = "guard"
    SOURCE_TEST = "test"
    SOURCE_RUNTIME = "runtime"
    SOURCE_MANUAL = "manual"
    SOURCE_CHOICES = [
        (SOURCE_GUARD, "Guard"),
        (SOURCE_TEST, "Prueba"),
        (SOURCE_RUNTIME, "Runtime"),
        (SOURCE_MANUAL, "Manual"),
    ]

    finding_key = models.CharField(max_length=120, unique=True)
    code = models.CharField(max_length=80)
    category = models.CharField(max_length=40, choices=CATEGORY_CHOICES)
    severity = models.CharField(max_length=16, choices=SEVERITY_CHOICES, default=SEVERITY_WARNING)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_OPEN)
    source_type = models.CharField(max_length=24, choices=SOURCE_CHOICES, default=SOURCE_GUARD)
    source_reference = models.CharField(max_length=255, blank=True, default="")
    statement = models.TextField()
    evidence_refs_json = models.JSONField(default=list, blank=True)
    detected_count = models.PositiveIntegerField(default=1)
    first_seen_at = models.DateTimeField(default=timezone.now)
    last_seen_at = models.DateTimeField(default=timezone.now)
    resolved_at = models.DateTimeField(null=True, blank=True)
    is_blocking = models.BooleanField(default=True)
    details_json = models.JSONField(default=dict, blank=True)
    memory_proposal = models.ForeignKey(
        "MemoryProposal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="quality_findings",
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Hallazgo de calidad"
        verbose_name_plural = "Hallazgos de calidad"
        ordering = ["status", "-last_seen_at", "-id"]
        indexes = [
            models.Index(fields=["code", "status"]),
            models.Index(fields=["category", "status"]),
            models.Index(fields=["source_type", "last_seen_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.code} [{self.status}]"


class RemediationProposal(models.Model):
    STATUS_PROPOSED = "proposed"
    STATUS_ACCEPTED = "accepted"
    STATUS_IMPLEMENTED = "implemented"
    STATUS_VALIDATED = "validated"
    STATUS_REJECTED = "rejected"
    STATUS_CHOICES = [
        (STATUS_PROPOSED, "Propuesta"),
        (STATUS_ACCEPTED, "Aceptada"),
        (STATUS_IMPLEMENTED, "Implementada"),
        (STATUS_VALIDATED, "Validada"),
        (STATUS_REJECTED, "Rechazada"),
    ]

    RISK_LOW = "low"
    RISK_MEDIUM = "medium"
    RISK_HIGH = "high"
    RISK_CHOICES = [
        (RISK_LOW, "Bajo"),
        (RISK_MEDIUM, "Medio"),
        (RISK_HIGH, "Alto"),
    ]

    remediation_key = models.CharField(max_length=120, unique=True)
    finding = models.ForeignKey(QualityFinding, on_delete=models.CASCADE, related_name="remediation_proposals")
    remediation_type = models.CharField(max_length=80)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PROPOSED)
    summary = models.CharField(max_length=255)
    suggested_fix = models.TextField(blank=True, default="")
    target_files_json = models.JSONField(default=list, blank=True)
    suggested_tests_json = models.JSONField(default=list, blank=True)
    risk_level = models.CharField(max_length=16, choices=RISK_CHOICES, default=RISK_LOW)
    details_json = models.JSONField(default=dict, blank=True)
    validated_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Propuesta de remediacion"
        verbose_name_plural = "Propuestas de remediacion"
        ordering = ["status", "-created_at", "-id"]
        indexes = [
            models.Index(fields=["status", "risk_level"]),
            models.Index(fields=["remediation_type", "status"]),
            models.Index(fields=["finding", "status"]),
        ]

    def __str__(self) -> str:
        return self.summary


class AgentLoopCheckpoint(models.Model):
    PHASE_LOAD = "load"
    PHASE_OBSERVE = "observe"
    PHASE_THINK = "think"
    PHASE_ACT = "act"
    PHASE_VERIFY = "verify"
    PHASE_BLOCK = "block"
    PHASE_COMPLETE = "complete"
    PHASE_CHOICES = [
        (PHASE_LOAD, "Carga"),
        (PHASE_OBSERVE, "Observa"),
        (PHASE_THINK, "Piensa"),
        (PHASE_ACT, "Actua"),
        (PHASE_VERIFY, "Verifica"),
        (PHASE_BLOCK, "Bloquea"),
        (PHASE_COMPLETE, "Cierra"),
    ]

    run = models.ForeignKey(OrchestrationRun, on_delete=models.CASCADE, related_name="loop_checkpoints")
    iteration = models.PositiveIntegerField(default=1)
    phase = models.CharField(max_length=16, choices=PHASE_CHOICES, default=PHASE_LOAD)
    title = models.CharField(max_length=200)
    details_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Checkpoint de loop de agente"
        verbose_name_plural = "Checkpoints de loop de agentes"
        ordering = ["run_id", "iteration", "id"]
        indexes = [
            models.Index(fields=["run", "iteration"]),
            models.Index(fields=["phase", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.run.run_key} · iter {self.iteration} · {self.phase}"


class AgentGoalDelegation(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_BLOCKED = "blocked"
    STATUS_FAILED = "failed"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_RUNNING, "En ejecucion"),
        (STATUS_SUCCESS, "Exitosa"),
        (STATUS_BLOCKED, "Bloqueada"),
        (STATUS_FAILED, "Fallida"),
    ]

    parent_run = models.ForeignKey(
        OrchestrationRun,
        on_delete=models.CASCADE,
        related_name="delegations_sent",
    )
    parent_task = models.ForeignKey(
        AgentTask,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="delegations_sent",
    )
    child_run = models.ForeignKey(
        OrchestrationRun,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="delegations_received",
    )
    child_task = models.ForeignKey(
        AgentTask,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="delegations_received",
    )
    from_agent = models.ForeignKey(
        AgentDefinition,
        on_delete=models.PROTECT,
        related_name="delegations_from",
    )
    to_agent = models.ForeignKey(
        AgentDefinition,
        on_delete=models.PROTECT,
        related_name="delegations_to",
    )
    goal_type = models.CharField(max_length=120)
    sequence_order = models.PositiveIntegerField(default=1)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    details_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Delegacion de goal entre agentes"
        verbose_name_plural = "Delegaciones de goals entre agentes"
        ordering = ["parent_run_id", "sequence_order", "id"]
        indexes = [
            models.Index(fields=["parent_run", "sequence_order"]),
            models.Index(fields=["goal_type", "status"]),
        ]

    def __str__(self) -> str:
        return f"{self.from_agent.code} -> {self.to_agent.code} · {self.goal_type}"


class ChatConversation(models.Model):
    STATUS_ACTIVE = "active"
    STATUS_ARCHIVED = "archived"
    STATUS_CHOICES = [
        (STATUS_ACTIVE, "Activa"),
        (STATUS_ARCHIVED, "Archivada"),
    ]

    public_id = models.UUIDField(default=uuid4, unique=True, editable=False)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="erp_chat_conversations",
    )
    title = models.CharField(max_length=200, default="Nueva conversación")
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_ACTIVE)
    model_name = models.CharField(max_length=80, blank=True, default="")
    session_key = models.CharField(max_length=80, blank=True, default="")
    last_message_at = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Conversación ERP"
        verbose_name_plural = "Conversaciones ERP"
        ordering = ["-last_message_at", "-updated_at", "-id"]
        indexes = [
            models.Index(fields=["owner", "status", "last_message_at"]),
            models.Index(fields=["status", "updated_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.owner_id} · {self.title}"


class ChatConversationState(models.Model):
    conversation = models.OneToOneField(
        ChatConversation,
        on_delete=models.CASCADE,
        related_name="state",
    )
    summary = models.TextField(blank=True, default="")
    token_estimate = models.PositiveIntegerField(default=0)
    context_window_json = models.JSONField(default=dict, blank=True)
    last_compacted_at = models.DateTimeField(null=True, blank=True)
    metadata_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Estado de conversación ERP"
        verbose_name_plural = "Estados de conversación ERP"

    def __str__(self) -> str:
        return f"Estado {self.conversation_id}"


class ChatMessage(models.Model):
    ROLE_SYSTEM = "system"
    ROLE_USER = "user"
    ROLE_ASSISTANT = "assistant"
    ROLE_TOOL = "tool"
    ROLE_CHOICES = [
        (ROLE_SYSTEM, "Sistema"),
        (ROLE_USER, "Usuario"),
        (ROLE_ASSISTANT, "Asistente"),
        (ROLE_TOOL, "Herramienta"),
    ]

    STATUS_PENDING = "pending"
    STATUS_STREAMING = "streaming"
    STATUS_COMPLETE = "complete"
    STATUS_ERROR = "error"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_STREAMING, "Transmitiendo"),
        (STATUS_COMPLETE, "Completo"),
        (STATUS_ERROR, "Error"),
    ]

    public_id = models.UUIDField(default=uuid4, unique=True, editable=False)
    conversation = models.ForeignKey(
        ChatConversation,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    sequence = models.PositiveIntegerField(default=1)
    role = models.CharField(max_length=16, choices=ROLE_CHOICES)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_COMPLETE)
    content = models.TextField(blank=True, default="")
    tool_name = models.CharField(max_length=120, blank=True, default="")
    tool_display_name = models.CharField(max_length=160, blank=True, default="")
    metadata_json = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="erp_chat_messages",
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Mensaje de conversación ERP"
        verbose_name_plural = "Mensajes de conversación ERP"
        ordering = ["conversation_id", "sequence", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["conversation", "sequence"],
                name="orquestacion_chat_message_unique_sequence",
            )
        ]
        indexes = [
            models.Index(fields=["conversation", "sequence"]),
            models.Index(fields=["role", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.conversation_id} · {self.role} · {self.sequence}"


class ChatToolCall(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_COMPLETE = "complete"
    STATUS_APPROVAL_REQUESTED = "approval_requested"
    STATUS_ERROR = "error"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_RUNNING, "En ejecución"),
        (STATUS_COMPLETE, "Completo"),
        (STATUS_APPROVAL_REQUESTED, "Aprobación solicitada"),
        (STATUS_ERROR, "Error"),
    ]

    public_id = models.UUIDField(default=uuid4, unique=True, editable=False)
    conversation = models.ForeignKey(
        ChatConversation,
        on_delete=models.CASCADE,
        related_name="tool_calls",
    )
    request_message = models.ForeignKey(
        ChatMessage,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="requested_tool_calls",
    )
    assistant_message = models.ForeignKey(
        ChatMessage,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="tool_calls",
    )
    tool_key = models.CharField(max_length=120)
    tool_name = models.CharField(max_length=120)
    tool_display_name = models.CharField(max_length=160, blank=True, default="")
    arguments_json = models.JSONField(default=dict, blank=True)
    status = models.CharField(max_length=24, choices=STATUS_CHOICES, default=STATUS_PENDING)
    requires_approval = models.BooleanField(default=False)
    approval_suggestion = models.ForeignKey(
        AgentSuggestion,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="chat_tool_calls",
    )
    metadata_json = models.JSONField(default=dict, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Invocación de herramienta en chat ERP"
        verbose_name_plural = "Invocaciones de herramientas en chat ERP"
        ordering = ["conversation_id", "created_at", "id"]
        indexes = [
            models.Index(fields=["conversation", "created_at"]),
            models.Index(fields=["tool_key", "status"]),
        ]

    def __str__(self) -> str:
        return f"{self.conversation_id} · {self.tool_key} · {self.status}"


class ChatToolResult(models.Model):
    tool_call = models.OneToOneField(
        ChatToolCall,
        on_delete=models.CASCADE,
        related_name="result",
    )
    is_error = models.BooleanField(default=False)
    summary = models.TextField(blank=True, default="")
    result_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Resultado de herramienta en chat ERP"
        verbose_name_plural = "Resultados de herramientas en chat ERP"

    def __str__(self) -> str:
        return f"Resultado {self.tool_call_id}"


class ChatMemoryPin(models.Model):
    STATUS_ACTIVE = "active"
    STATUS_ARCHIVED = "archived"
    STATUS_CHOICES = [
        (STATUS_ACTIVE, "Activo"),
        (STATUS_ARCHIVED, "Archivado"),
    ]

    conversation = models.ForeignKey(
        ChatConversation,
        on_delete=models.CASCADE,
        related_name="memory_pins",
    )
    message = models.ForeignKey(
        ChatMessage,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="memory_pins",
    )
    memory_proposal = models.ForeignKey(
        MemoryProposal,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="chat_memory_pins",
    )
    label = models.CharField(max_length=120)
    content = models.TextField()
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_ACTIVE)
    metadata_json = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="erp_chat_memory_pins",
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Memoria fijada en conversación ERP"
        verbose_name_plural = "Memorias fijadas en conversaciones ERP"
        ordering = ["conversation_id", "-created_at", "-id"]
        indexes = [
            models.Index(fields=["conversation", "status"]),
        ]

    def __str__(self) -> str:
        return f"{self.conversation_id} · {self.label}"
