from django.contrib import admin

from .models import (
    AgentCapability,
    AgentDefinition,
    AgentExecutionLink,
    AgentGap,
    AgentGoalDelegation,
    AgentLoopCheckpoint,
    AgentSuggestion,
    AgentTask,
    MemoryProposal,
    QualityFinding,
    OrchestrationRule,
    OrchestrationRun,
    RemediationProposal,
)


@admin.register(AgentDefinition)
class AgentDefinitionAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "domain", "status", "owner_department", "requires_human_approval_default", "priority_order")
    list_filter = ("status", "domain", "requires_human_approval_default", "owner_department")
    search_fields = ("code", "name", "domain", "description")


@admin.register(AgentCapability)
class AgentCapabilityAdmin(admin.ModelAdmin):
    list_display = ("agent", "capability_key", "scope_type", "resource_key", "active")
    list_filter = ("scope_type", "active", "agent__domain")
    search_fields = ("agent__code", "capability_key", "resource_key")


@admin.register(OrchestrationRule)
class OrchestrationRuleAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "trigger_type", "action_mode", "primary_agent", "secondary_agent", "is_active")
    list_filter = ("trigger_type", "action_mode", "is_active")
    search_fields = ("code", "name", "source_event")


@admin.register(OrchestrationRun)
class OrchestrationRunAdmin(admin.ModelAdmin):
    list_display = ("run_key", "trigger_source", "rule", "status", "started_at", "finished_at", "created_by")
    list_filter = ("status", "trigger_source", "rule")
    search_fields = ("run_key", "trigger_source", "created_by__username")
    readonly_fields = ("started_at", "finished_at", "context_json", "result_summary_json")


@admin.register(AgentTask)
class AgentTaskAdmin(admin.ModelAdmin):
    list_display = ("title", "agent", "task_type", "priority", "status", "assigned_branch", "due_at")
    list_filter = ("priority", "status", "agent__domain", "assigned_branch")
    search_fields = ("title", "task_type", "agent__code", "resolution_note")


@admin.register(AgentSuggestion)
class AgentSuggestionAdmin(admin.ModelAdmin):
    list_display = ("summary", "task", "domain", "severity", "decision_status", "requires_approval", "created_at")
    list_filter = ("domain", "severity", "decision_status", "requires_approval")
    search_fields = ("summary", "recommended_action", "task__title")


@admin.register(AgentExecutionLink)
class AgentExecutionLinkAdmin(admin.ModelAdmin):
    list_display = ("suggestion", "execution_mode", "target_reference", "execution_status", "executed_by", "executed_at")
    list_filter = ("execution_mode", "execution_status")
    search_fields = ("target_reference", "suggestion__summary", "executed_by__username")


@admin.register(AgentGap)
class AgentGapAdmin(admin.ModelAdmin):
    list_display = ("gap_type", "summary", "suggested_agent_name", "status", "detected_by_rule", "reviewed_by")
    list_filter = ("gap_type", "status")
    search_fields = ("summary", "suggested_agent_name")


@admin.register(AgentLoopCheckpoint)
class AgentLoopCheckpointAdmin(admin.ModelAdmin):
    list_display = ("run", "iteration", "phase", "title", "created_at")
    list_filter = ("phase",)
    search_fields = ("run__run_key", "title")
    readonly_fields = ("details_json", "created_at")


@admin.register(AgentGoalDelegation)
class AgentGoalDelegationAdmin(admin.ModelAdmin):
    list_display = ("parent_run", "from_agent", "to_agent", "goal_type", "sequence_order", "status", "child_run")
    list_filter = ("status", "goal_type", "from_agent", "to_agent")
    search_fields = ("parent_run__run_key", "goal_type", "from_agent__code", "to_agent__code")
    readonly_fields = ("details_json", "created_at", "updated_at")


@admin.register(MemoryProposal)
class MemoryProposalAdmin(admin.ModelAdmin):
    list_display = ("summary", "category", "section", "status", "approval_mode", "proposed_by_agent", "source_type", "confidence_score", "detected_count", "last_detected_at")
    list_filter = ("status", "approval_mode", "section", "category", "source_type", "proposed_by_agent")
    search_fields = ("summary", "statement", "source_reference", "proposal_key")
    readonly_fields = ("proposal_key", "applied_result_json", "created_at", "updated_at")


@admin.register(QualityFinding)
class QualityFindingAdmin(admin.ModelAdmin):
    list_display = ("code", "category", "severity", "status", "source_type", "is_blocking", "detected_count", "last_seen_at")
    list_filter = ("category", "severity", "status", "source_type", "is_blocking")
    search_fields = ("code", "statement", "source_reference", "finding_key")
    readonly_fields = ("finding_key", "details_json", "created_at", "updated_at")


@admin.register(RemediationProposal)
class RemediationProposalAdmin(admin.ModelAdmin):
    list_display = ("summary", "finding", "remediation_type", "status", "risk_level", "validated_at", "updated_at")
    list_filter = ("status", "risk_level", "remediation_type")
    search_fields = ("summary", "suggested_fix", "remediation_key", "finding__code", "finding__source_reference")
    readonly_fields = ("remediation_key", "details_json", "created_at", "updated_at", "validated_at")
