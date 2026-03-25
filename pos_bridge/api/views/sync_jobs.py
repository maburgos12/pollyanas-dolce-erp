from __future__ import annotations

from rest_framework import status
from rest_framework import filters as drf_filters
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from pos_bridge.api.pagination import StandardPagination
from pos_bridge.api.permissions import IsPosAdminUser
from pos_bridge.api.serializers.sync_jobs import PointSyncJobSerializer, TriggerSyncSerializer
from pos_bridge.models import PointSyncJob
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from pos_bridge.tasks.run_product_recipe_sync import run_product_recipe_sync


class SyncJobsViewSet(ReadOnlyModelViewSet):
    serializer_class = PointSyncJobSerializer
    permission_classes = [IsPosAdminUser]
    pagination_class = StandardPagination
    filter_backends = [drf_filters.SearchFilter, drf_filters.OrderingFilter]
    search_fields = ["job_type", "status", "triggered_by__username", "error_message"]
    ordering_fields = ["started_at", "finished_at", "job_type", "status"]
    ordering = ["-started_at", "-id"]

    def get_queryset(self):
        return PointSyncJob.objects.select_related("triggered_by")

    @action(detail=False, methods=["post"])
    def trigger(self, request):
        serializer = TriggerSyncSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        payload = serializer.validated_data
        job_type = payload["job_type"]
        branch_filter = payload.get("branch_filter") or None

        if job_type == PointSyncJob.JOB_TYPE_INVENTORY:
            sync_job = run_inventory_sync(triggered_by=request.user, branch_filter=branch_filter)
        elif job_type == PointSyncJob.JOB_TYPE_SALES:
            sync_job = run_daily_sales_sync(
                triggered_by=request.user,
                branch_filter=branch_filter,
                lookback_days=payload.get("days", 3),
                lag_days=payload.get("lag_days", 1),
            )
        elif job_type == PointSyncJob.JOB_TYPE_RECIPES:
            sync_job = run_product_recipe_sync(triggered_by=request.user, branch_hint=branch_filter)
        else:
            return Response({"detail": "Tipo de job no soportado."}, status=status.HTTP_400_BAD_REQUEST)

        response = PointSyncJobSerializer(sync_job)
        return Response(response.data, status=status.HTTP_201_CREATED)
