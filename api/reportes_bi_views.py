from __future__ import annotations

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_view_reportes
from reportes.bi_utils import compute_bi_snapshot, serialize_bi_for_api


class ReportesBIDashboardView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _bounded_int(value, *, default: int, min_value: int, max_value: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(min_value, min(parsed, max_value))

    def get(self, request):
        if not can_view_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar reportes."}, status=status.HTTP_403_FORBIDDEN)

        period_days = self._bounded_int(request.query_params.get("period_days"), default=90, min_value=7, max_value=365)
        months_window = self._bounded_int(request.query_params.get("months"), default=6, min_value=3, max_value=24)
        snapshot = compute_bi_snapshot(period_days=period_days, months_window=months_window)
        return Response(serialize_bi_for_api(snapshot), status=status.HTTP_200_OK)
