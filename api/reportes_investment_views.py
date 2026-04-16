from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_view_reportes
from reportes.models import ProyectoInversion, ProyectoInversionEscenario
from reportes.services_investment_projects import (
    ProyectoInversionDashboardService,
    ProyectoInversionRefreshService,
    ProyectoInversionScenarioService,
)


def _serialize(value):
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "pk") and hasattr(value, "__class__"):
        return getattr(value, "pk")
    return value


class InvestmentProjectDashboardView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, project_id: int):
        if not can_view_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar reportes."}, status=status.HTTP_403_FORBIDDEN)
        project = ProyectoInversion.objects.filter(pk=project_id).first()
        if project is None:
            return Response({"detail": "Proyecto no encontrado."}, status=status.HTTP_404_NOT_FOUND)
        if (request.query_params.get("refresh") or "").strip() == "1":
            ProyectoInversionRefreshService().refresh_project(project, user=request.user)
        context = ProyectoInversionDashboardService().build_detail_context(project)
        return Response(_serialize(context), status=status.HTTP_200_OK)


class InvestmentProjectScenarioSimulationView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, project_id: int, scenario_id: int):
        if not can_view_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar reportes."}, status=status.HTTP_403_FORBIDDEN)
        project = ProyectoInversion.objects.filter(pk=project_id).first()
        scenario = ProyectoInversionEscenario.objects.filter(pk=scenario_id, proyecto_id=project_id).first()
        if project is None or scenario is None:
            return Response({"detail": "Escenario no encontrado."}, status=status.HTTP_404_NOT_FOUND)
        payload = ProyectoInversionScenarioService().compute(project, scenario)
        return Response(_serialize(payload), status=status.HTTP_200_OK)
