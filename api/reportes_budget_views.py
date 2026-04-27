from __future__ import annotations

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_view_reportes
from reportes.services_budget_vs_actual import BudgetVsActualSnapshotService, parse_period


class BudgetVsActualView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, periodo: str):
        if not can_view_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar reportes."}, status=status.HTTP_403_FORBIDDEN)
        try:
            period_start = parse_period(periodo)
        except Exception:
            return Response({"detail": "Periodo inválido. Usa formato YYYY-MM."}, status=status.HTTP_400_BAD_REQUEST)

        summary = BudgetVsActualSnapshotService().build_snapshot(period_start=period_start, dry_run=True)
        return Response(
            {
                "period": summary.period.isoformat(),
                "has_budget": summary.has_budget,
                "has_actual": summary.has_actual,
                "persisted": False,
                "rows": [
                    {
                        "concept": row["concept"],
                        "label": row["label"],
                        "type": row["type"],
                        "budget": str(row["budget"]),
                        "actual": str(row["actual"]),
                        "variance": str(row["variance"]),
                        "variance_pct": str(row["variance_pct"]),
                        "tone": row["tone"],
                    }
                    for row in summary.rows
                ],
            },
            status=status.HTTP_200_OK,
        )
