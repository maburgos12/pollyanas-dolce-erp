from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from django.db import transaction
from django.db.models import Count, Sum

from reportes.models import PresupuestoLineaMensual, PresupuestoResumenMensual


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


@dataclass
class PresupuestoResumenSnapshot:
    rows_created: int
    rows_updated: int
    periods: list[str]


class BudgetMonitoringSnapshotService:
    def _is_master_global_source(self, *, source_name: str, title: str) -> bool:
        source_upper = (source_name or "").upper()
        title_upper = (title or "").upper()
        return "PRESUPUESTO GENERAL" in title_upper or (
            "ADMINISTRACIÓN" in source_upper and "GENERAL" in title_upper
        )

    @transaction.atomic
    def build_snapshot(self, period_start: date | None = None) -> PresupuestoResumenSnapshot:
        lineas = PresupuestoLineaMensual.objects.select_related("importacion")
        if period_start is not None:
            lineas = lineas.filter(period=period_start)

        period_values = list(lineas.order_by().values_list("period", flat=True).distinct())
        rows_created = 0
        rows_updated = 0

        for period in period_values:
            period_lineas = lineas.filter(period=period)
            fuente_rows = list(
                period_lineas.values("importacion__fuente_nombre", "importacion__titulo")
                .annotate(
                    total_budget=Sum("monthly_budget"),
                    total_actual=Sum("monthly_actual"),
                    line_count=Count("id"),
                )
            )
            total_budget = Decimal("0")
            total_actual = Decimal("0")
            total_lines = 0

            for row in fuente_rows:
                fuente_nombre = row["importacion__fuente_nombre"] or ""
                fuente_budget = row["total_budget"] or Decimal("0")
                fuente_actual = row["total_actual"] or Decimal("0")
                fuente_variance = _safe_ratio(fuente_actual, fuente_budget)
                _, created = PresupuestoResumenMensual.objects.update_or_create(
                    period=period,
                    tipo=PresupuestoResumenMensual.TIPO_FUENTE,
                    fuente_nombre=fuente_nombre,
                    defaults={
                        "total_budget": fuente_budget,
                        "total_actual": fuente_actual,
                        "total_variance": fuente_variance,
                        "line_count": row["line_count"] or 0,
                        "metadata": {},
                    },
                )
                rows_created += int(created)
                rows_updated += int(not created)

            master_rows = [
                row
                for row in fuente_rows
                if self._is_master_global_source(
                    source_name=row["importacion__fuente_nombre"] or "",
                    title=row["importacion__titulo"] or "",
                )
            ]
            global_rows = master_rows or fuente_rows
            for row in global_rows:
                total_budget += row["total_budget"] or Decimal("0")
                total_actual += row["total_actual"] or Decimal("0")
                total_lines += row["line_count"] or 0

            total_variance = _safe_ratio(total_actual, total_budget)
            _, created = PresupuestoResumenMensual.objects.update_or_create(
                period=period,
                tipo=PresupuestoResumenMensual.TIPO_GLOBAL,
                fuente_nombre="",
                defaults={
                    "total_budget": total_budget,
                    "total_actual": total_actual,
                    "total_variance": total_variance,
                    "line_count": total_lines,
                    "metadata": {
                        "fuentes": [row["importacion__fuente_nombre"] or "" for row in global_rows],
                        "global_mode": "master_source" if master_rows else "sum_sources",
                    },
                },
            )
            rows_created += int(created)
            rows_updated += int(not created)

        return PresupuestoResumenSnapshot(
            rows_created=rows_created,
            rows_updated=rows_updated,
            periods=[period.isoformat() for period in period_values],
        )
