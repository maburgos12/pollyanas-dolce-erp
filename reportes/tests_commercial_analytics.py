from datetime import date
from decimal import Decimal as D
from unittest.mock import patch

from django.test import TestCase, RequestFactory
from django.template.loader import render_to_string

from core.models import Sucursal
from recetas.models import Receta
from reportes.models import FactVentaDiaria
from reportes.commercial_analytics import _build_panel, commercial_panel_from_request, decompose_products


class CommercialAnalyticsTests(TestCase):
    def setUp(self):
        self.closed = Sucursal.objects.create(codigo='HISTORY', nombre='Crucero', activa=False, fecha_apertura=date(2026,7,14))
        self.open = Sucursal.objects.create(codigo='NEW', nombre='Bamoa', activa=True)
        self.recipe = Receta.objects.create(nombre='Pastel mediano', hash_contenido='commercial-test')
        self.factory = RequestFactory()

    def fact(self, year=2025, month=9, key='1', branch=None, source='AUTHORITATIVE', amount=116, quantity=1, **kwargs):
        return FactVentaDiaria.objects.create(fecha=date(year,month,10), sucursal=branch or self.closed,
            producto_clave=key, producto_nombre='Pastel', categoria='Mediano', source_kind=source,
            cantidad=quantity, venta_total=amount, venta_neta=D(str(amount))/D('1.16'), venta_bruta=D(str(amount))+10,
            **kwargs)

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_entire_2025_includes_all_twelve_months_and_inactive_preopening_history(self, today):
        for month in range(1,13):
            self.fact(month=month)
        self.fact(2026, branch=self.open, amount=232)
        panel = _build_panel(2026,9,None)
        self.assertEqual(panel['annual_previous'], D(1392))
        self.assertEqual(panel['annual_previous_months'],12)
        self.assertEqual(panel['previous'],D(116))
        self.assertEqual(panel['current'],D(232))
        self.assertEqual(len(panel['branches']),2)
        self.assertEqual(sum(r['delta'] for r in panel['branches']),panel['delta'])
        self.assertIsNone(panel['monthly'][11]['current'])
        self.assertEqual(panel['monthly'][11]['previous'],D(116))
        annual = _build_panel(2025,0,None)
        self.assertEqual(annual['current'],D(1392))
        self.assertEqual(annual['end'],date(2025,12,31))
        self.assertIsNone(annual['previous'])
        self.assertTrue(all(r['delta'] is None for r in annual['branches']))
        self.assertEqual(annual['components'],[])
        self.closed.refresh_from_db()
        self.assertFalse(self.closed.activa)

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_tax_and_discounts_not_removed_and_alternative_sources_not_added(self,today):
        self.fact(amount=116)
        self.fact(key='0001',amount=58)
        self.fact(source='V2_FACT',key='receta:65',amount=900)
        self.fact(2026,branch=self.open,amount=200)
        panel=_build_panel(2026,9,None)
        self.assertEqual(panel['previous'],D(174))
        self.assertEqual(sum(r['previous'] or 0 for r in panel['products']),D(174))
        self.assertEqual(len([r for r in panel['products'] if r['previous'] is not None]),2)
        self.assertEqual(sum(r['delta'] for r in panel['categories']),D(26))
        self.assertEqual(sum(r['amount'] for r in panel['components']),D(26))

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_realized_price_and_quantity_bridge_reconciles_with_new_and_return_rows(self,today):
        self.fact(amount=100,quantity=10,receta=self.recipe)
        self.fact(2026,source='V2_FACT',key='receta:1',amount=180,quantity=12,receta=self.recipe)
        self.fact(2026,key='new',source='V2_FACT',amount=50,quantity=1)
        self.fact(2026,key='return',source='V2_FACT',amount=-20,quantity=-1)
        panel=_build_panel(2026,9,None)
        components=panel['components']
        self.assertEqual(components[0]['amount'],D(60))
        self.assertEqual(components[1]['amount'],D(20))
        self.assertEqual(components[2]['amount'],D(30))
        self.assertEqual(sum(r['amount'] for r in components),panel['delta'])

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_branch_drilldown_does_not_change_historical_identity(self,today):
        self.fact(amount=100)
        self.fact(branch=self.open,amount=200)
        panel=_build_panel(2025,0,self.closed.id)
        self.assertEqual(panel['current'],D(100))
        self.assertEqual(len(panel['branches']),1)
        self.assertEqual(len(panel['branch_options']),2)
        self.assertEqual(panel['branch_id'],self.closed.id)

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_missing_data_is_not_zero_and_invalid_filters_are_bounded(self,today):
        panel=commercial_panel_from_request(self.factory.get('/',{'sales_year':'bad','sales_month':'99','sales_branch':'x'}))
        self.assertEqual(panel['year'],2026)
        self.assertEqual(panel['month'],9)
        self.assertIsNone(panel['current'])
        self.assertIsNone(panel['previous'])
        self.assertIsNone(panel['growth'])
        self.assertTrue(all(r['current'] is None for r in panel['monthly']))

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_visible_tables_contain_closed_branch_and_all_2025_months(self,today):
        for month in range(1,13): self.fact(month=month)
        panel=_build_panel(2025,0,None)
        html=render_to_string('reportes/partials/commercial_analytics.html',{'commercial_analytics':panel})
        self.assertIn('Venta total con IVA',html)
        self.assertIn('Crucero',html)
        self.assertIn('historia conservada',html)
        self.assertIn('Dic',html)
        self.assertIn('1,392',html)
        self.assertIn('ca-monthly',html)
        self.assertIn('ca-branches',html)

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_same_export_derived_import_is_not_a_second_sale_and_other_files_are_kept(self,today):
        from ventas.models import VentaAutoritativaPoint
        for code,sheet,file in [('01','category_report','one.xls'),('1','PointSalesDailyProductFact','one.xls'),('2','PointSalesDailyProductFact','other.xls')]:
            VentaAutoritativaPoint.objects.create(branch=self.closed,sale_date=date(2026,3,10),product_code=code,
                source_sheet=sheet,source_file=file,total_amount=100)
            self.fact(2026,3,key=code,amount=100)
        panel=_build_panel(2026,3,None)
        self.assertEqual(panel['current'],D(200))
        self.assertEqual(VentaAutoritativaPoint.objects.count(),3)
        self.assertEqual(FactVentaDiaria.objects.count(),3)

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_current_partial_month_uses_same_observed_last_date_and_full_prior_year_remains(self,today):
        for day,year,amount in [(3,2026,100),(3,2025,80),(4,2025,400)]:
            FactVentaDiaria.objects.create(fecha=date(year,10,day),sucursal=self.closed,producto_clave='1',
                cantidad=1,venta_total=amount,source_kind='AUTHORITATIVE')
        panel=_build_panel(2026,10,None)
        self.assertEqual(panel['end'],date(2026,10,3))
        self.assertEqual(panel['previous_end'],date(2025,10,3))
        self.assertEqual(panel['previous'],D(80))
        self.assertEqual(panel['annual_reference'],D(480))
        self.assertIsNone(panel['monthly'][9]['growth'])

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_monthly_control_is_displayed_without_overwriting_branch_details(self,today):
        from pos_bridge.models import PointMonthlySalesOfficial
        self.fact(2026,4,amount=100)
        PointMonthlySalesOfficial.objects.create(month_start=date(2026,4,1),month_end=date(2026,4,30),total_amount=D('100.10'))
        panel=_build_panel(2026,4,None)
        self.assertEqual(panel['current'],D(100))
        self.assertEqual(panel['monthly'][3]['control'],D('100.10'))
        self.assertEqual(panel['monthly'][3]['control_difference'],D('-0.10'))
        filtered=_build_panel(2026,4,self.closed.id)
        self.assertFalse(filtered['has_controls'])

    @patch('reportes.commercial_analytics.timezone.localdate', return_value=date(2026,10,4))
    def test_without_any_matched_sku_price_and_volume_are_not_reported_as_zero_effects(self,today):
        self.fact(2024,key='old-code',source='LEGACY',amount=100)
        self.fact(2025,key='new-code',source='AUTHORITATIVE',amount=150)
        panel=_build_panel(2025,0,None)
        self.assertEqual(panel['delta'],D(50))
        self.assertNotIn('Precio medio realizado',[r['label'] for r in panel['components']])
        self.assertNotIn('Cantidad por producto',[r['label'] for r in panel['components']])
        self.assertEqual(sum(r['amount'] for r in panel['components']),D(50))
