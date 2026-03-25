from rest_framework import serializers

from pos_bridge.models import PointSyncJob


class PointSyncJobSerializer(serializers.ModelSerializer):
    triggered_by_username = serializers.CharField(source="triggered_by.username", default="", read_only=True)

    class Meta:
        model = PointSyncJob
        fields = (
            "id",
            "job_type",
            "status",
            "started_at",
            "finished_at",
            "error_message",
            "parameters",
            "result_summary",
            "artifacts",
            "attempt_count",
            "triggered_by_username",
            "created_at",
        )


class TriggerSyncSerializer(serializers.Serializer):
    JOB_CHOICES = [
        (PointSyncJob.JOB_TYPE_INVENTORY, "Inventario"),
        (PointSyncJob.JOB_TYPE_SALES, "Ventas"),
        (PointSyncJob.JOB_TYPE_RECIPES, "Recetas"),
    ]

    job_type = serializers.ChoiceField(choices=JOB_CHOICES)
    branch_filter = serializers.CharField(required=False, allow_blank=True, default="")
    days = serializers.IntegerField(required=False, default=3, min_value=1, max_value=365)
    lag_days = serializers.IntegerField(required=False, default=1, min_value=0, max_value=30)


class AgentQuerySerializer(serializers.Serializer):
    query = serializers.CharField(max_length=500)
    context = serializers.JSONField(required=False, default=dict)


class AgentResponseSerializer(serializers.Serializer):
    answer = serializers.CharField()
    data = serializers.JSONField(required=False, default=dict)
    query_type = serializers.CharField()
