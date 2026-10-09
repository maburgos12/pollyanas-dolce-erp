from __future__ import annotations

from rest_framework import serializers


class AIToolInvokeSerializer(serializers.Serializer):
    arguments = serializers.JSONField(required=False, default=dict)

    def validate_arguments(self, value):
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise serializers.ValidationError("arguments debe ser un objeto JSON.")
        return value


class AIToolApprovalDecisionSerializer(serializers.Serializer):
    comment = serializers.CharField(required=False, allow_blank=True, default="")


class AIToolApprovalRequestSerializer(serializers.Serializer):
    arguments = serializers.JSONField(required=False, default=dict)
    summary = serializers.CharField(required=False, allow_blank=True, default="")
    rationale = serializers.CharField(required=False, allow_blank=True, default="")

    def validate_arguments(self, value):
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise serializers.ValidationError("arguments debe ser un objeto JSON.")
        return value


from api.ai_gateway_assets import StrictArguments, StrictIntegerField, StrictStringField


class ReadLimitArguments(StrictArguments):
    operation = serializers.ChoiceField(choices=['READ', 'CREATE', 'UPDATE', 'ACTION', 'DELETE', 'UNKNOWN'])


class WorkflowPrepareArguments(StrictArguments):
    query = StrictStringField(required=False, allow_blank=True, max_length=180)
    asset_id = StrictIntegerField(required=False, min_value=1, max_value=9223372036854775807)

    def validate(self, attrs):
        if len(attrs) > 1:
            raise serializers.ValidationError('Usa query o asset_id.')
        return attrs


class WorkflowPendingArguments(StrictArguments):
    pass


class WorkflowResumeArguments(WorkflowPrepareArguments):
    workflow_id = serializers.UUIDField()
    expected_version = StrictIntegerField(min_value=1)
    option_position = StrictIntegerField(required=False, min_value=1, max_value=50)

    def validate(self, attrs):
        if len(set(attrs) & {'query', 'asset_id', 'option_position'}) > 1:
            raise serializers.ValidationError('Usa query, asset_id o option_position.')
        return attrs


class WorkflowCreateSerializer(StrictArguments):
    kind = serializers.ChoiceField(choices=['CONSULT_ASSET_MAINTENANCE'])
    origin_request_id = serializers.UUIDField()
    conversation_id = serializers.UUIDField()
    payload = serializers.JSONField(default=dict)


class WorkflowUpdateSerializer(StrictArguments):
    expected_version = StrictIntegerField(min_value=1)
    request_id = serializers.UUIDField()
    payload = serializers.JSONField(default=dict)


class WorkflowResumeSerializer(WorkflowUpdateSerializer):
    conversation_id = serializers.UUIDField()
