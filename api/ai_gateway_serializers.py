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
