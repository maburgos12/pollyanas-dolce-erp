from __future__ import annotations

from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_view_reportes
from reportes.services_dashboard_charts import build_dashboard_charts_payload


class ReportesDashboardChartsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar reportes."}, status=status.HTTP_403_FORBIDDEN)
        raw_year = request.query_params.get("año") or request.query_params.get("year") or timezone.localdate().year
        try:
            year = int(raw_year)
        except (TypeError, ValueError):
            return Response({"detail": "Año inválido."}, status=status.HTTP_400_BAD_REQUEST)
        if year < 2020 or year > 2100:
            return Response({"detail": "Año fuera de rango."}, status=status.HTTP_400_BAD_REQUEST)
        return Response(build_dashboard_charts_payload(year=year), status=status.HTTP_200_OK)
