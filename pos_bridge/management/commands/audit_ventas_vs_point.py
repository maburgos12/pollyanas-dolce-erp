from __future__ import annotations

import csv
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.utils.text import slugify

from core.branch_catalog import resolver_sucursal_por_texto
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointMonthlySummary
from reportes.models import FactVentaDiaria


ZERO = Decimal("0")


def decimalize(value) -> Decimal:
    if value in (None, ""):
        return ZERO
    return Decimal(str(value))


class Command(BaseCommand):
    help = "Compara PointMonthlySummary contra FactVentaDiaria y genera auditoría CSV/SQL de referencia."

    OUTPUT_DIR = Path("output/auditoria_ventas_historicas")
    CSV_PATH = OUTPUT_DIR / "comparacion_point_vs_erp.csv"
    SQL_PATH = OUTPUT_DIR / "correccion_necesaria.sql"

    def _resolve_branch(self, summary: PointMonthlySummary) -> Sucursal | None:
        branch_code = str(summary.branch_code or "").strip()
        point_branch = None
        if branch_code:
            point_branch = (
                PointBranch.objects.filter(external_id=branch_code).select_related("erp_branch").order_by("id").first()
            )
        if point_branch and point_branch.erp_branch_id:
            return point_branch.erp_branch
        normalized_branch = slugify(summary.branch or "")
        for candidate in PointBranch.objects.select_related("erp_branch").filter(erp_branch__isnull=False):
            if slugify(candidate.name or "") == normalized_branch:
                return candidate.erp_branch
        return resolver_sucursal_por_texto(summary.branch)

    def _classify(self, pct_diff: Decimal | None) -> str:
        if pct_diff is None:
            return "CRITICAL"
        if pct_diff < Decimal("2"):
            return "OK"
        if pct_diff <= Decimal("15"):
            return "WARNING"
        return "CRITICAL"

    def handle(self, *args, **options):
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        rows = []
        critical_labels: list[str] = []
        status_counter: Counter[str] = Counter()
        sql_lines = [
            "-- REFERENCIA SOLAMENTE. NO EJECUTAR SIN REVISION HUMANA.",
            "-- Generado por audit_ventas_vs_point",
            "",
        ]

        summaries = PointMonthlySummary.objects.filter(year__gte=2022, year__lte=2024).order_by("year", "month", "branch")
        for summary in summaries:
            sucursal = self._resolve_branch(summary)
            point_real = decimalize(summary.total_revenue)
            erp_fact = ZERO
            if sucursal is not None:
                erp_fact = decimalize(
                    FactVentaDiaria.objects.filter(
                        fecha__year=summary.year,
                        fecha__month=summary.month,
                        sucursal=sucursal,
                    ).aggregate(total=Sum("venta_total"))["total"]
                )
            diferencia = point_real - erp_fact
            pct_diferencia = None
            if point_real > ZERO:
                pct_diferencia = (abs(diferencia) / point_real) * Decimal("100")
            elif erp_fact == ZERO:
                pct_diferencia = Decimal("0")
            status = self._classify(pct_diferencia)
            status_counter[status] += 1
            label = f"{summary.year}-{summary.month:02d} {summary.branch}"
            if status == "CRITICAL":
                critical_labels.append(label)
                sql_lines.append(f"-- {label}")
                if sucursal is None:
                    sql_lines.append("-- Sucursal no mapeada en ERP; revisar manualmente.")
                elif erp_fact == ZERO:
                    sql_lines.append("-- ERP fact es 0; no se puede calcular factor automático. Revisar manualmente.")
                else:
                    factor = point_real / erp_fact
                    month_start = date(summary.year, summary.month, 1).isoformat()
                    if summary.month == 12:
                        next_month = date(summary.year + 1, 1, 1)
                    else:
                        next_month = date(summary.year, summary.month + 1, 1)
                    sql_lines.extend(
                        [
                            f"-- point_real={point_real:.2f} erp_fact={erp_fact:.2f} factor={factor:.8f}",
                            "UPDATE reportes_factventadiaria",
                            f"SET venta_bruta = ROUND((venta_bruta * {factor:.8f})::numeric, 2),",
                            f"    descuento = ROUND((descuento * {factor:.8f})::numeric, 2),",
                            f"    venta_total = ROUND((venta_total * {factor:.8f})::numeric, 2),",
                            f"    venta_neta = ROUND((venta_neta * {factor:.8f})::numeric, 2),",
                            f"    margen = ROUND((margen * {factor:.8f})::numeric, 2),",
                            "    actualizado_en = NOW()",
                            f"WHERE fecha >= DATE '{month_start}'",
                            f"  AND fecha < DATE '{next_month.isoformat()}'",
                            f"  AND sucursal_id = {sucursal.id};",
                        ]
                    )
                sql_lines.append("")

            rows.append(
                {
                    "year": summary.year,
                    "month": summary.month,
                    "branch": summary.branch,
                    "point_real": f"{point_real:.2f}",
                    "erp_fact": f"{erp_fact:.2f}",
                    "diferencia": f"{diferencia:.2f}",
                    "pct_diferencia": "" if pct_diferencia is None else f"{pct_diferencia:.4f}",
                    "status": status,
                }
            )

        with self.CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["year", "month", "branch", "point_real", "erp_fact", "diferencia", "pct_diferencia", "status"],
            )
            writer.writeheader()
            writer.writerows(rows)

        self.SQL_PATH.write_text("\n".join(sql_lines) + "\n", encoding="utf-8")

        self.stdout.write("RESUMEN AUDITORÍA 2022-2024")
        self.stdout.write(
            f"OK: {status_counter.get('OK', 0)} meses | WARNING: {status_counter.get('WARNING', 0)} meses | CRITICAL: {status_counter.get('CRITICAL', 0)} meses"
        )
        self.stdout.write(f"CSV: {self.CSV_PATH}")
        self.stdout.write(f"SQL: {self.SQL_PATH}")
        self.stdout.write(f"Meses críticos: {critical_labels}")
