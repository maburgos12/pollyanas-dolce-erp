from datetime import date
from decimal import Decimal as D

from django.test import TestCase
from reportes.models import EmpresaResultadoMensual


class ComponentesFinancierosVerificadosTests(TestCase):
    def setUp(self):
        self.snapshot = EmpresaResultadoMensual.objects.create(periodo=date(2026, 10, 1),
            venta_total=D('3648207.58'), costo_materia_prima_total=D('1566669.92'),
            costo_reventa_total=D('50191.39'), costo_fabricacion_total=D('1566669.92'),
            margen_bruto_total=D('2019689.29'), gasto_comercial_total=D('554984.76'),
            gasto_corporativo_total=D('617164.33'), utilidad_operativa_total=D('847540.20'),
            metadata={'ajustes_verificados_octubre_2026': True, 'merma_registrada': '11656.98',
                      'fuente_corte': '2026-10-31', 'alcance_resultado': 'PARCIAL_COSTOS_CENTRALES_SIN_CERTIFICAR'})

    def test_bi_mismo_importe_y_porcentaje_y_merma_una_sola_vez(self):
        from reportes.executive_panels import build_budget_operating_panel
        row = build_budget_operating_panel(year=2026, selected_month=10)['selected_row']
        self.assertEqual(row['product_variable_cost_total'], D('1628518.29'))
        self.assertEqual(row['recorded_waste_total'], D('11656.98'))
        self.assertEqual(row['gross_margin_total'], D('2019689.29'))
        self.assertEqual(row['operating_profit_total'], D('847540.20'))
        self.assertTrue(row['financial_amounts_available'])
        self.assertFalse(row['finance_close_complete'])
        self.assertAlmostEqual(row['actual_product_cost_mix_pct'], D('44.6389'), places=4)

    def test_otro_mes_conserva_semantica_previa(self):
        from reportes.executive_panels import build_budget_operating_panel
        self.snapshot.periodo = date(2026, 9, 1)
        self.snapshot.save(update_fields=['periodo'])
        row = build_budget_operating_panel(year=2026, selected_month=9)['selected_row']
        self.assertEqual(row['product_variable_cost_total'], D('1616861.31'))
        self.assertEqual(row['gross_margin_total'], D('2031346.27'))

    def test_presupuesto_y_export_usan_margen_ya_conciliado(self):
        from reportes.services_budget_vs_actual import BudgetVsActualSnapshotService
        row = BudgetVsActualSnapshotService()._actual_by_concept(self.snapshot)
        self.assertEqual(row['utilidad_bruta'], D('2019689.29'))
        self.assertEqual(row['utilidad_operativa'], D('847540.20'))

    def test_pantalla_muestra_componentes_y_resultado_aunque_el_cierre_siga_parcial(self):
        from reportes.executive_panels import build_budget_operating_panel
        from django.template.loader import render_to_string
        from django.test import RequestFactory
        from django.contrib.auth.models import AnonymousUser
        panel = build_budget_operating_panel(year=2026, selected_month=10)
        panel['show_budget_primary'] = True
        request = RequestFactory().get('/reportes/bi/?detalle=1&budget_month=10')
        request.user = AnonymousUser()
        html = render_to_string('reportes/bi.html', dict(request=request, bi_detail_loaded=True,
            budget_operating_panel=panel))
        for importe in ['1,628,518.29', '11,656.98', '2,019,689.29', '847,540.20']:
            self.assertIn(importe, html)
        self.assertIn('Costo producto y merma', html)
        self.assertIn('44.6%', html)
        self.assertIn('Lectura parcial', html)
        self.assertNotIn('Cierre completo', html)

    def test_api_lee_merma_financiera_conciliada_sin_alterar_captura_operativa(self):
        from control.models import MermaMensualSucursal
        from reportes.services_dashboard_charts import build_dashboard_charts_payload
        merma = MermaMensualSucursal.objects.create(periodo=date(2026, 10, 1),
            nombre_producto='Sintético', unidades_merma=1, costo_merma=5)
        data = build_dashboard_charts_payload(year=2026)
        self.assertEqual(data['charts']['merma_mensual']['costo_merma'], ['11656.98'])
        self.assertEqual(data['charts']['utilidad_operativa']['real'], ['847540.20'])
        merma.refresh_from_db()
        self.assertEqual(merma.costo_merma, D('5'))

    def test_cobertura_no_se_convierte_en_cero_ni_en_ytd(self):
        from reportes.executive_panels import build_budget_operating_panel
        panel = build_budget_operating_panel(year=2026, selected_month=10)
        for key in ('costed_sales_total', 'non_recipe_total', 'costed_coverage_pct'):
            self.assertIsNone(panel['selected_row'][key])
        self.assertIsNone(panel['ytd_costed_sales'])
        self.assertIsNone(panel['ytd_costed_coverage_pct'])

    def test_venta_neta_no_es_reemplazada_por_venta_comercial(self):
        from reportes.executive_panels import build_budget_operating_panel
        from unittest.mock import patch
        from types import SimpleNamespace
        with patch('reportes.executive_panels.PointMonthlySalesOfficial.objects') as objects:
            objects.filter.return_value = [SimpleNamespace(month_start=date(2026, 10, 1), total_amount=D('4000000'))]
            row = build_budget_operating_panel(year=2026, selected_month=10)['selected_row']
        self.assertEqual(row['sales_total'], D('3648207.58'))
        self.assertEqual(row['gross_margin_total'], D('2019689.29'))

    def test_metadatos_invalidos_no_acreditan_merma_cero(self):
        from reportes.services_budget_vs_actual import merma_financiera_verificada
        from reportes.executive_panels import build_budget_operating_panel
        from reportes.services_dashboard_charts import build_dashboard_charts_payload
        for invalid in (None, True, 'no es monto', 'NaN', 'Infinity', '-1', '0'):
            with self.subTest(invalid=invalid):
                self.snapshot.metadata['merma_registrada'] = invalid
                self.snapshot.save(update_fields=['metadata'])
                self.assertIsNone(merma_financiera_verificada(self.snapshot))
                row = build_budget_operating_panel(year=2026, selected_month=10)['selected_row']
                self.assertFalse(row['financial_projection_verified'])
                self.assertTrue(row['financial_projection_invalid'])
                self.assertIsNone(row['recorded_waste_total'])
                self.assertFalse(row['financial_amounts_available'])
                data = build_dashboard_charts_payload(year=2026)
                self.assertEqual(data['sources']['waste_reconciled_periods'], {})
