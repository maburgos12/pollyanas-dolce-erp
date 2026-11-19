from datetime import date
from decimal import Decimal as D
from django.test import TestCase
from reportes.models import (AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual,
    EmpresaResultadoMensual, PresupuestoImport, PresupuestoLineaMensual)
from reportes.executive_panels import build_budget_operating_panel
from reportes.services_budget_vs_actual import BudgetVsActualSnapshotService


class BudgetSourcePresenceTests(TestCase):
    def setUp(self):
        area = AreaPresupuesto.objects.create(codigo="ventas", nombre="Ingresos")
        active = RubroPresupuesto.objects.create(area=area, concepto="Plan activo", tipo="INGRESO")
        inactive = RubroPresupuesto.objects.create(area=area, concepto="Plan retirado", tipo="INGRESO", activo=False)
        self.line = LineaPresupuestoMensual.objects.create(rubro=active, periodo=date(2026, 10, 1), monto_presupuesto=100)
        self.inactive = LineaPresupuestoMensual.objects.create(rubro=inactive, periodo=date(2026, 10, 1), monto_presupuesto=20)
        EmpresaResultadoMensual.objects.create(periodo=date(2026, 10, 1), venta_total=80)

    def row(self):
        return build_budget_operating_panel(year=2026, selected_month=10)["selected_row"]

    def test_original_api_cohort_and_types_are_preserved_until_full_pnl_is_reviewed(self):
        summary = BudgetVsActualSnapshotService().build_snapshot(period_start=date(2026, 10, 1), dry_run=True)
        sales = next(r for r in summary.rows if r["concept"] == "ventas")
        self.assertEqual(sales["budget"], D("120"))
        self.assertIsInstance(sales["budget"], D)

    def test_bi_reuses_canonical_sales_without_inventing_costs_or_profit(self):
        row = self.row()
        self.assertEqual(row["sales_budget_total"], D("100"))
        for key in ("product_cost_budget_total", "recurrent_expense_budget_total", "budget_result_total"):
            self.assertIsNone(row[key])
        self.assertEqual(row["sales_gap_total"], D("-20"))
        panel = build_budget_operating_panel(year=2026, selected_month=10)
        self.assertIsNone(panel["ytd_budget_result"])
        from django.template.loader import render_to_string
        from django.test import RequestFactory
        from django.contrib.auth.models import AnonymousUser
        request = RequestFactory().get("/reportes/bi/?detalle=1&budget_month=10")
        request.user = AnonymousUser()
        html = render_to_string("reportes/bi.html", {"request": request, "bi_detail_loaded": True, "budget_operating_panel": panel})
        self.assertIn("rubros de ingreso activos", html)
        self.assertIn("El presupuesto original de resultados incluye otras partidas", html)

    def test_zero_with_source_is_known_and_absence_is_not_zero(self):
        self.line.monto_presupuesto = 0
        self.line.save(update_fields=["monto_presupuesto"])
        self.assertEqual(self.row()["sales_budget_total"], D("0"))
        self.assertTrue(self.row()["has_budget"])
        self.line.delete()
        self.assertIsNone(self.row()["sales_budget_total"])
        self.assertIsNone(self.row()["sales_gap_total"])

    def test_explicit_general_zero_is_not_replaced_by_master_fallback(self):
        imported = PresupuestoImport.objects.create(tipo=PresupuestoImport.TIPO_GENERAL, fuente_nombre="PRESUPUESTO ADMINISTRACIÓN 2026.xlsx", sheet_name="GENERAL")
        PresupuestoLineaMensual.objects.create(importacion=imported, external_key="known-zero", period=date(2026, 10, 1), concept="INGRESOS", monthly_budget=0)
        self.assertEqual(self.row()["sales_budget_total"], D("0"))
        PresupuestoLineaMensual.objects.create(importacion=imported, external_key="known-cost-zero", period=date(2026, 10, 1), concept="COSTOS INSUMOS/PRODUCTOS", monthly_budget=0)
        detail = PresupuestoImport.objects.create(tipo=PresupuestoImport.TIPO_DETALLE, fuente_nombre="Detalle verificado", sheet_name="ADMON", metadata={"kind": "admin_recurrente"})
        PresupuestoLineaMensual.objects.create(importacion=detail, external_key="known-expense-zero", period=date(2026, 10, 1), concept="Renta", monthly_budget=0, audit_status="OK")
        self.assertEqual(self.row()["budget_result_total"], D("0"))

    def test_ytd_does_not_hide_a_financial_month_without_budget_source(self):
        EmpresaResultadoMensual.objects.create(periodo=date(2026, 1, 1), venta_total=50)
        panel = build_budget_operating_panel(year=2026, selected_month=10)
        self.assertEqual(panel["selected_row"]["sales_budget_total"], D("100"))
        self.assertIsNone(panel["ytd_sales_budget"])

    def test_partial_ytd_structured_budget_renders_nd_instead_of_empty_currency(self):
        detail = PresupuestoImport.objects.create(tipo=PresupuestoImport.TIPO_DETALLE,
            fuente_nombre="Detalle parcial", sheet_name="ADMON", metadata={"kind": "admin_recurrente"})
        PresupuestoLineaMensual.objects.create(importacion=detail, external_key="partial-ytd",
            period=date(2026, 10, 1), concept="Renta", monthly_budget=10, audit_status="OK")
        panel = build_budget_operating_panel(year=2026, selected_month=10)
        self.assertTrue(panel["show_budget_primary"])
        self.assertIsNone(panel["ytd_structured_budget_total"])
        from django.template.loader import render_to_string
        from django.test import RequestFactory
        from django.contrib.auth.models import AnonymousUser
        request = RequestFactory().get("/reportes/bi/?detalle=1&budget_month=10")
        request.user = AnonymousUser()
        html = render_to_string("reportes/bi.html", {"request": request,
            "bi_detail_loaded": True, "budget_operating_panel": panel})
        import re
        amount = re.search(r'Costo \+ gasto verificado</strong>\s*<div class="bi-source-amount">([^<]*)</div>', html)
        self.assertIsNotNone(amount)
        self.assertEqual(amount.group(1), "N/D")
