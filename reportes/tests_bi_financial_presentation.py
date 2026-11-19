from decimal import Decimal
from datetime import date

from django.contrib.auth.models import AnonymousUser
from django.template.loader import render_to_string
from django.test import SimpleTestCase, RequestFactory


class BIFinancialPresentationTests(SimpleTestCase):
    def render_panel(self, *, finance=True, amounts_present=True, budget_primary=True):
        healthy = {"tone": "success", "label": "Sano"}
        row = {
            "period": date(2026, 10, 1), "month_label": "Octubre",
            "has_finance": finance, "finance_close_complete": amounts_present,
            "sales_total": Decimal("1000"), "manufacturing_total": Decimal("443"),
            "gross_margin_total": Decimal("557"), "gross_margin_pct": Decimal("55.7"),
            "actual_product_cost_mix_pct": Decimal("44.3"),
            "operating_expense_total": Decimal("100"),
            "actual_expense_mix_pct": Decimal("10"),
            "operating_profit_total": Decimal("457"), "operating_margin_pct": Decimal("45.7"),
            **{name: healthy for name in (
                "health_signal", "gross_margin_signal", "product_cost_signal",
                "expense_signal", "margin_signal",
            )},
        }
        request = RequestFactory().get("/reportes/bi/?detalle=1&budget_month=10")
        request.user = AnonymousUser()
        return render_to_string("reportes/bi.html", {
            "request": request, "bi_detail_loaded": True,
            "budget_operating_panel": {
                "available": True, "selected_row": row, "rows_summary": [row],
                "show_budget_primary": budget_primary, "year": 2026, "selected_month": 10,
                "ytd_expense_signal": healthy, "ytd_margin_signal": healthy,
                "ytd_health_signal": healthy, "ytd_product_cost_signal": healthy,
            },
        })

    def test_recorded_costs_and_expenses_do_not_certify_financial_close(self):
        html = self.render_panel()
        self.assertIn("costo de producción registrado pesa", html)
        self.assertNotIn("costo de producto y merma registrado pesa", html)
        self.assertIn("Margen bruto provisional", html)
        self.assertIn("Cierre no acreditado", html)
        self.assertIn("55.7%", html)
        self.assertIn("$557.00", html)
        self.assertNotIn("Cierre completo", html)
        self.assertNotIn("Mes cerrado y comparable", html)
        self.assertNotIn("Margen bruto real", html)
        self.assertNotIn(">Sano<", html)

    def test_missing_expenses_stay_pending(self):
        html = self.render_panel(amounts_present=False)
        self.assertIn("Cierre pendiente", html)
        self.assertIn("Lectura parcial", html)
        self.assertNotIn(">Sano<", html)

    def test_budget_only_does_not_claim_recorded_financial_data(self):
        html = self.render_panel(finance=False, amounts_present=False)
        self.assertIn("Solo presupuesto", html)
        self.assertNotIn("Cierre no acreditado", html)
        self.assertNotIn(">Sano<", html)

    def test_financial_pressure_warning_is_preserved_as_provisional(self):
        html = render_to_string("reportes/partials/bi_financial_signal.html", {
            "signal": {"tone": "danger", "label": "Presión"},
        })
        self.assertIn('bi-tone danger', html)
        self.assertIn("Presión · provisional", html)

    def test_missing_signal_stays_without_reading(self):
        html = render_to_string("reportes/partials/bi_financial_signal.html", {})
        self.assertIn("Sin lectura", html)
        self.assertNotIn("provisional", html)

    def test_operating_ytd_stays_provisional_with_budget_under_audit(self):
        html = self.render_panel(budget_primary=False)
        self.assertIn("Costo de fabricación registrado", html)
        self.assertIn("Utilidad operativa provisional YTD", html)
        self.assertIn("Acumulado provisional", html)
