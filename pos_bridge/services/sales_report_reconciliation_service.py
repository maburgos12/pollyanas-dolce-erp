from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
from django.db.models import Sum

from pos_bridge.models import PointDailySale
from recetas.models import VentaHistorica

MANUAL_Q1_2026_START = date(2026, 1, 1)
MANUAL_Q1_2026_END = date(2026, 3, 13)
MANUAL_Q1_2026_SOURCE = "POINT_HIST_2026_Q1"
POINT_BRIDGE_SOURCE = "POINT_BRIDGE_SALES"


@dataclass
class SalesReconciliationResult:
    summary: dict
    rows: list[dict]
    report_path: str


class SalesReportReconciliationService:
    def __init__(self, reports_dir: Path | None = None):
        self.reports_dir = Path(reports_dir or "storage/pos_bridge/reports")
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def reconcile(self, *, report_path: str, start_date: date, end_date: date) -> SalesReconciliationResult:
        excel_rows, declared_summary = self._parse_excel_report(report_path)
        canonical_rows = self._load_canonical_rows(start_date=start_date, end_date=end_date)
        point_rows = self._load_point_rows(start_date=start_date, end_date=end_date)

        all_codes = sorted(set(excel_rows) | set(canonical_rows) | set(point_rows))
        rows: list[dict] = []
        for code in all_codes:
            excel = excel_rows.get(code, self._empty_row())
            canonical = canonical_rows.get(code, self._empty_row())
            point = point_rows.get(code, self._empty_row())
            rows.append(
                {
                    "codigo": code,
                    "producto_excel": excel["producto"],
                    "categoria_excel": excel["categoria"],
                    "excel_qty": excel["qty"],
                    "excel_venta": excel["venta"],
                    "canon_qty": canonical["qty"],
                    "canon_venta": canonical["venta"],
                    "canon_qty_diff": canonical["qty"] - excel["qty"],
                    "canon_venta_diff": canonical["venta"] - excel["venta"],
                    "point_qty": point["qty"],
                    "point_venta": point["venta"],
                    "point_qty_diff": point["qty"] - excel["qty"],
                    "point_venta_diff": point["venta"] - excel["venta"],
                    "canon_producto": canonical["producto"],
                    "point_producto": point["producto"],
                }
            )

        rows.sort(key=lambda item: abs(item["canon_venta_diff"]), reverse=True)

        summary = {
            "excel_declared_total_venta": declared_summary.get("venta", Decimal("0")),
            "excel_declared_total_neta": declared_summary.get("venta_neta", Decimal("0")),
            "excel_declared_total_qty": sum(row["excel_qty"] for row in rows),
            "canonical_total_venta": sum(row["canon_venta"] for row in rows),
            "canonical_total_qty": sum(row["canon_qty"] for row in rows),
            "point_total_venta": sum(row["point_venta"] for row in rows),
            "point_total_qty": sum(row["point_qty"] for row in rows),
        }
        summary["canonical_vs_excel_venta_diff"] = summary["canonical_total_venta"] - summary["excel_declared_total_venta"]
        summary["canonical_vs_excel_qty_diff"] = summary["canonical_total_qty"] - summary["excel_declared_total_qty"]
        summary["point_vs_excel_venta_diff"] = summary["point_total_venta"] - summary["excel_declared_total_venta"]
        summary["point_vs_excel_qty_diff"] = summary["point_total_qty"] - summary["excel_declared_total_qty"]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = self.reports_dir / f"{timestamp}_sales_report_reconciliation.csv"
        self._write_csv(csv_path, rows)
        return SalesReconciliationResult(summary=summary, rows=rows, report_path=str(csv_path))

    def _parse_excel_report(self, report_path: str) -> tuple[dict[str, dict], dict[str, Decimal]]:
        path = Path(report_path)
        sheet1 = pd.read_excel(path, sheet_name="Sheet1", header=None)
        sheet2 = pd.read_excel(path, sheet_name="Sheet2", header=None)

        data = defaultdict(self._empty_row)
        current_category = ""
        for _, row in sheet1.iterrows():
            category, code, product, qty, venta = row[0], row[3], row[4], row[5], row[9]
            if isinstance(category, str) and category.strip() and category != "CATEGORÍA":
                current_category = category.strip()
            if isinstance(product, str) and ("Total de la categoría" in product or product == "PRODUCTO"):
                continue
            try:
                if pd.notna(code) and pd.notna(product) and pd.notna(qty) and pd.notna(venta):
                    code_str = str(code).strip()
                    qty_dec = Decimal(str(qty))
                    venta_dec = Decimal(str(venta))
                    data[code_str]["producto"] = str(product).strip()
                    data[code_str]["categoria"] = current_category
                    data[code_str]["qty"] += qty_dec
                    data[code_str]["venta"] += venta_dec
            except Exception:
                continue

        declared = {"venta": Decimal("0"), "venta_neta": Decimal("0")}
        for _, row in sheet2.iterrows():
            values = [value for value in row.tolist() if pd.notna(value)]
            if values and values[0] == "Total Ventas":
                declared["venta"] = Decimal(str(values[3]))
                declared["venta_neta"] = Decimal(str(values[5]))
                break
        return data, declared

    def _load_canonical_rows(self, *, start_date: date, end_date: date) -> dict[str, dict]:
        rows = defaultdict(self._empty_row)

        manual_end = min(end_date, MANUAL_Q1_2026_END)
        if start_date <= MANUAL_Q1_2026_END:
            manual_qs = (
                VentaHistorica.objects.filter(
                    fecha__gte=start_date,
                    fecha__lte=manual_end,
                    fuente=MANUAL_Q1_2026_SOURCE,
                )
                .values("receta__codigo_point", "receta__nombre")
                .annotate(qty=Sum("cantidad"), venta=Sum("monto_total"))
            )
            for row in manual_qs:
                code = (row["receta__codigo_point"] or "").strip()
                rows[code]["producto"] = row["receta__nombre"] or ""
                rows[code]["qty"] += row["qty"] or Decimal("0")
                rows[code]["venta"] += row["venta"] or Decimal("0")

        bridge_start = max(start_date, MANUAL_Q1_2026_END + timedelta(days=1))
        if bridge_start <= end_date:
            bridge_qs = (
                VentaHistorica.objects.filter(
                    fecha__gte=bridge_start,
                    fecha__lte=end_date,
                    fuente=POINT_BRIDGE_SOURCE,
                )
                .values("receta__codigo_point", "receta__nombre")
                .annotate(qty=Sum("cantidad"), venta=Sum("monto_total"))
            )
            for row in bridge_qs:
                code = (row["receta__codigo_point"] or "").strip()
                rows[code]["producto"] = row["receta__nombre"] or ""
                rows[code]["qty"] += row["qty"] or Decimal("0")
                rows[code]["venta"] += row["venta"] or Decimal("0")

        return rows

    def _load_point_rows(self, *, start_date: date, end_date: date) -> dict[str, dict]:
        rows = defaultdict(self._empty_row)
        queryset = (
            PointDailySale.objects.filter(sale_date__gte=start_date, sale_date__lte=end_date)
            .values("product__sku", "product__name")
            .annotate(qty=Sum("quantity"), venta=Sum("total_amount"))
        )
        for row in queryset:
            code = (row["product__sku"] or "").strip()
            rows[code]["producto"] = row["product__name"] or ""
            rows[code]["qty"] += row["qty"] or Decimal("0")
            rows[code]["venta"] += row["venta"] or Decimal("0")
        return rows

    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        dataframe = pd.DataFrame(rows)
        dataframe.to_csv(path, index=False)

    @staticmethod
    def _empty_row() -> dict:
        return {"producto": "", "categoria": "", "qty": Decimal("0"), "venta": Decimal("0")}
