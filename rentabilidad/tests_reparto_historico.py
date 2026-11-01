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
            response=self.client.get('/rentabilidad/?periodo=2026-06')
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
