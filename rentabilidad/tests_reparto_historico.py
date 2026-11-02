from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from pos_bridge.models import PointDailySale
from reportes.models import LineaPresupuestoMensual
from .services_resultado import leer_resultado_mensual


class RepartoRentabilidadTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from reportes.tests_reparto_historico import RepartoHistoricoTests
        RepartoHistoricoTests.setUpTestData.__func__(cls)

    def test_same_report_and_csv_without_writes(self):
        self.client.force_login(self.user)
        with CaptureQueriesContext(connection) as queries:
            pantalla = self.client.get('/rentabilidad/?tab=reparto')
            csv_rent = self.client.get('/rentabilidad/?export=reparto')
            csv_er = self.client.get('/reportes/estado-resultados/?year=2026&export=reparto')
        self.assertContains(pantalla, 'Total conciliado enero–agosto')
        self.assertEqual(pantalla.context['reparto']['total'], Decimal('631'))
        self.assertEqual(csv_rent.status_code, 200)
        self.assertEqual(csv_rent.content, csv_er.content)
        self.assertContains(pantalla, '?tab=reparto&amp;export=reparto')
        self.assertFalse([q for q in queries if q['sql'].split()[0] in {'INSERT', 'UPDATE', 'DELETE'}])

    def test_discrepancy_blocks_both_exports(self):
        self.client.force_login(self.user)
        LineaPresupuestoMensual.objects.filter(rubro__concepto='Ingresos', periodo=date(2026,2,1)).update(monto_real=101)
        self.assertEqual(self.client.get('/rentabilidad/?export=reparto').status_code,409)
        response=self.client.get('/rentabilidad/?tab=reparto')
        self.assertContains(response,'Febrero')
        self.assertNotContains(response,'id="reparto-table"')

    def test_read_and_export_permissions(self):
        self.assertEqual(self.client.get('/rentabilidad/?export=reparto').status_code,302)
        user=get_user_model().objects.create_user('sin_rentabilidad',password='test')
        self.client.force_login(user)
        self.assertEqual(self.client.get('/rentabilidad/?tab=reparto').status_code,403)
        self.assertEqual(self.client.get('/rentabilidad/?export=reparto').status_code,403)

    def test_historical_sales_identity_preserves_tax_basis_and_source(self):
        PointDailySale.objects.filter(sale_date=date(2026,6,15)).update(tax_amount=5)
        before=list(PointDailySale.objects.values())
        june=leer_resultado_mensual(date(2026,6,1))['sucursales']
        july=leer_resultado_mensual(date(2026,7,1))['sucursales']
        self.assertNotIn(self.bamoa.pk,june)
        self.assertEqual(june[self.crucero.pk]['ventas_brutas']-june[self.crucero.pk]['descuentos'],Decimal(95))
        self.assertNotIn(self.crucero.pk,july)
        self.assertEqual(july[self.bamoa.pk]['ventas_brutas']-july[self.bamoa.pk]['descuentos'],Decimal(100))
        self.assertEqual(list(PointDailySale.objects.values()),before)

    def test_fiscal_reconciliation_combines_historical_ids(self):
        self.client.force_login(self.user)
        fiscal=[{'sucursal_id': self.crucero.pk,'sucursal__nombre':'Crucero','total':Decimal(60)},
                {'sucursal_id': self.bamoa.pk,'sucursal__nombre':'Bamoa','total':Decimal(40)}]
        with patch('rentabilidad.views_rentabilidad.selected_sales_facts') as facts:
            facts.return_value.values.return_value.annotate.return_value=fiscal
            response=self.client.get('/rentabilidad/?periodo=2026-06&tab=gastos')
        self.assertEqual(response.status_code,200)
        self.assertTrue(response.context['fuente_estado']['cuadra'])
        self.assertIn('Crucero', [r['centro_costo__sucursal__nombre'] for r in response.context['gastos_panel']['por_sucursal']])
        self.assertEqual(response.context['fuente_estado']['total'],Decimal(100))

    def test_missing_crucero_is_unassigned_not_bamoa(self):
        self.crucero.codigo='OTRO'
        self.crucero.save(update_fields=['codigo'])
        june=leer_resultado_mensual(date(2026,6,1))['sucursales']
        self.assertIn(None,june)
        self.assertNotIn(self.bamoa.pk,june)

    def test_monthly_export_keeps_closed_crucero_separate(self):
        from .services_reporte_mensual import build_reporte_mensual_consolidado
        report=build_reporte_mensual_consolidado(hasta=date(2026,6,1),meses=1,sucursal_id=self.crucero.pk)
        june=report['filas'][0]
        self.assertEqual(june['ingresos'],Decimal(100))
        self.assertEqual([r['sucursal_id'] for r in june['sucursales']],[self.crucero.pk])
        PointDailySale.objects.create(branch=self.point,product=self.product,sale_date=date(2025,6,15),gross_amount=10)
        old=leer_resultado_mensual(date(2025,6,1))['sucursales']
        self.assertIn(self.bamoa.pk,old)
        self.assertNotIn(self.crucero.pk,old)

    def test_primary_monthly_view_uses_closed_sources_without_partial_model(self):
        self.client.force_login(self.user)
        with patch('rentabilidad.views_rentabilidad.leer_costos_mensuales', side_effect=AssertionError('No mezclar bases')):
            with CaptureQueriesContext(connection) as queries:
                response = self.client.get('/rentabilidad/?periodo=2026-06')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['total']['ventas'], Decimal(100))
        self.assertEqual(response.context['total']['utilidad'], Decimal(79))
        self.assertEqual(response.context['total']['margen'], Decimal(89))
        self.assertContains(response, 'Crucero')
        self.assertContains(response, 'id="cierre-table"')
        self.assertTemplateUsed(response, 'rentabilidad/dashboard.html')
        self.assertContains(response, 'class="rent-shell"')
        self.assertContains(response, 'class="kpi-grid"')
        self.assertContains(response, 'class="rent-table"')
        self.assertContains(response, 'aria-label="Pestañas de rentabilidad"')
        self.assertContains(response, 'data-local-view-tabs')
        self.assertContains(response, 'id="periodo-select"')
        for tab in ('resumen', 'diagnostico', 'productos', 'gastos', 'alertas'):
            self.assertContains(response, f'&tab={tab}')
        self.assertEqual(response.context['total']['gastos_operativos'], Decimal(10))
        self.assertEqual(response.context['total']['margen'] - response.context['total']['gastos_operativos'], response.context['total']['utilidad'])
        self.assertNotContains(response, 'Sin datos suficientes')
        self.assertNotContains(response, 'Analizar con IA')
        self.assertFalse([q for q in queries if q['sql'].split()[0] in {'INSERT', 'UPDATE', 'DELETE'}])

    def test_report_export_and_branch_detail_reconcile_including_preopening(self):
        from .services_reporte_mensual import build_reporte_mensual_consolidado, reporte_a_json
        from .models_rentabilidad import SucursalRentabilidad
        rent = SucursalRentabilidad.objects.create(sucursal=self.bamoa, periodo=date(2026,1,1))
        self.client.force_login(self.user)
        response = self.client.get(f'/rentabilidad/{rent.pk}/')
        self.assertContains(response, f"params.set('sucursal', '{self.bamoa.pk}')")
        self.assertEqual(response.context['total']['ventas'], 0)
        self.assertEqual(response.context['total']['utilidad'], -1)
        self.assertIsNone(response.context['total']['margen_pct'])
        report = build_reporte_mensual_consolidado(hasta=date(2026,8,1), meses=8)
        self.assertEqual(report['totales']['utilidad_neta'], Decimal(631))
        for row in report['filas']:
            self.assertEqual(row['alcance'], 'REPARTO_CONCILIADO')
            self.assertIsNone(row['central_registrado'])
            self.assertEqual(sum(s['utilidad_neta'] for s in row['sucursales']), row['utilidad_neta'])
        self.assertEqual(reporte_a_json(report)['filas'][0]['alcance'], 'REPARTO_CONCILIADO')
        csv = self.client.get('/rentabilidad/reporte-mensual/?hasta=2026-08&formato=csv')
        self.assertEqual(csv.status_code, 200)
        self.assertIn(b'REPARTO_CONCILIADO', csv.content)

    def test_unreconciled_close_never_falls_back_to_partial_profit(self):
        self.client.force_login(self.user)
        PointDailySale.objects.filter(sale_date=date(2026,2,15)).update(gross_amount=111)
        for url in ('/rentabilidad/?periodo=2026-02', '/rentabilidad/reporte-mensual/?hasta=2026-08&formato=json'):
            self.assertEqual(self.client.get(url).status_code, 409)

    def test_decimal_costs_and_fractional_margin_are_not_truncated(self):
        from .services_resultado import resumen_cierre_mensual
        cierre = {'errores': [], 'detalle': [{'mes': 8, 'sucursal_id': 1, 'ventas': Decimal('100.50'),
            'costos': Decimal('10.20'), 'produccion': Decimal('1.35'), 'gasto_directo': Decimal('4.10'),
            'administracion': Decimal('2.01'), 'logistica': Decimal('3.04'), 'utilidad': Decimal('79.80')}]}
        data = resumen_cierre_mensual(cierre, date(2026,8,1))
        self.assertEqual(data['total']['costos_produccion'], Decimal('11.55'))
        self.assertEqual(data['total']['margen'], Decimal('88.95'))

    def test_diagnosis_retains_original_navigation_and_uses_closed_funds(self):
        self.client.force_login(self.user)
        response = self.client.get(f'/rentabilidad/?periodo=2026-06&tab=diagnostico&sucursal={self.crucero.pk}')
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'rentabilidad/dashboard.html')
        self.assertEqual(response.context['active_tab'], 'diagnostico')
        self.assertEqual(response.context['total']['utilidad'], Decimal(79))
        self.assertContains(response, 'Puente de resultado')
        self.assertContains(response, 'Interpretación ejecutiva')
        self.assertContains(response, 'Escenario integral del mes')
        self.assertContains(response, 'Gastos directos por concepto')
        self.assertContains(response, 'Ver desglose de Crucero')
        self.assertNotContains(response, 'onclick="analizarSucursal')
        self.assertNotContains(response, 'Resultado provisional:')
        self.assertContains(response, 'value="2026-06" selected')

    def test_break_even_separates_fuel_and_preserves_closed_amounts(self):
        from reportes.models import RubroPresupuesto
        from .services_resultado import leer_cierre_conciliado, resumen_cierre_mensual
        before = leer_cierre_conciliado()
        logistics = RubroPresupuesto.objects.get(concepto='logistica')
        fuel = RubroPresupuesto.objects.create(area=logistics.area, concepto='Diésel', tipo='EGRESO')
        period = date(2026, 8, 1)
        LineaPresupuestoMensual.objects.filter(rubro=logistics, periodo=period).update(monto_real=1)
        source = LineaPresupuestoMensual.objects.create(rubro=fuel, periodo=period, monto_real=2, fuente_real='AUTO:COMBUSTIBLE_UNIDAD')
        # Un importe presupuestado sin fuente real nunca forma parte del combustible.
        LineaPresupuestoMensual.objects.create(rubro=RubroPresupuesto.objects.create(
            area=logistics.area, concepto='Gasolina', tipo='EGRESO'), periodo=period,
            monto_real=999, monto_presupuesto=999)
        with CaptureQueriesContext(connection) as queries:
            close = leer_cierre_conciliado()
            data = resumen_cierre_mensual(close, period)
        self.assertFalse([q for q in queries if q['sql'].split()[0] in {'INSERT', 'UPDATE', 'DELETE'}])
        fields = ('ventas', 'costos', 'produccion', 'gasto_directo', 'administracion', 'logistica', 'utilidad')
        self.assertEqual([[f[k] for k in fields] for f in before['detalle']],
                         [[f[k] for k in fields] for f in close['detalle']])
        row = data['filas'][0]
        self.assertEqual(row['combustible'], Decimal(2))
        self.assertEqual(row['logistica_restante'], Decimal(1))
        self.assertEqual(row['costo_variable'], Decimal(12))
        self.assertEqual(row['gasto_fijo_escenario'], Decimal(9))
        self.assertEqual(row['punto_equilibrio'], Decimal('10.23'))
        self.assertEqual(row['ventas'] - row['costo_variable'] - row['gasto_fijo_escenario'], row['utilidad'])
        self.assertEqual(sum(g['monto'] for g in row['desglose_directo']), row['gasto_directo'])
        self.assertEqual(data['total']['punto_equilibrio'], row['punto_equilibrio'])
        # Combustible mayor que todo logística no permite publicar un PE engañoso.
        source.monto_real = 4
        source.save(update_fields=['monto_real'])
        LineaPresupuestoMensual.objects.filter(rubro=logistics, periodo=period).update(monto_real=-1)
        invalid = resumen_cierre_mensual(leer_cierre_conciliado(), period)['filas'][0]
        self.assertIsNone(invalid['punto_equilibrio'])
        self.assertIn('Revisar combustible', invalid['motivo_pe'])

    def test_break_even_handles_preopening_nonpositive_contribution_and_zero_fixed(self):
        from copy import deepcopy
        from .services_resultado import resumen_cierre_mensual
        fields = ('ventas', 'costos', 'produccion', 'gasto_directo', 'administracion', 'logistica', 'utilidad', 'combustible')
        for sales, variable, fixed, expected in ((0, 0, 1, None), (100, 100, 5, None),
                                                (100, 110, 5, None), (100, 50, 0, Decimal('0.00'))):
            with self.subTest(sales=sales, variable=variable, fixed=fixed):
                row = {k: Decimal(0) for k in fields}
                row.update(mes=8, sucursal_id=1, ventas=Decimal(sales), costos=Decimal(variable),
                           gasto_directo=Decimal(fixed), utilidad=Decimal(sales-variable-fixed))
                close = {'errores': [], 'detalle': [row]}
                original = deepcopy(close)
                result = resumen_cierre_mensual(close, date(2026,8,1))
                self.assertEqual(result['filas'][0]['punto_equilibrio'], expected)
                self.assertEqual(close, original)
