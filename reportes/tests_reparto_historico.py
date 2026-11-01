import csv
from datetime import date
from decimal import Decimal as D
from io import StringIO

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.models import AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual
from reportes.services_reparto_historico import reparto_historico, etapa_sucursal, FONDOS


class RepartoHistoricoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_superuser('reparto_dg', 'reparto@test.mx', 'test')
        cls.crucero = Sucursal.objects.create(codigo='CRUCERO', nombre='Crucero', activa=False)
        cls.bamoa = Sucursal.objects.create(codigo='BAMOA', nombre='Bamoa', fecha_apertura=date(2026, 7, 14))
        cls.point = PointBranch.objects.create(external_id='2', name='Bamoa', erp_branch=cls.bamoa)
        cls.product = PointProduct.objects.create(external_id='reparto-product', name='Pastel')
        areas = {k: AreaPresupuesto.objects.create(codigo=k, nombre=k) for k in ('resultados', 'gastos-venta', 'produccion', 'administracion', 'logistica')}
        rubros = {
            'ingresos': RubroPresupuesto.objects.create(area=areas['resultados'], concepto='Ingresos', tipo='INGRESO'),
            'costos': RubroPresupuesto.objects.create(area=areas['resultados'], concepto='Costos', tipo='EGRESO'),
        }
        for k in FONDOS[1:]:
            rubros[k] = RubroPresupuesto.objects.create(area=areas[k], concepto=k, tipo='EGRESO')
        directos = {s.pk: RubroPresupuesto.objects.create(area=areas['gastos-venta'], concepto='Gasto '+s.codigo, sucursal=s, tipo='EGRESO') for s in (cls.crucero, cls.bamoa)}
        cls.buckets = {k: {} for k in ('ingresos', 'gastos-venta', *FONDOS)}
        for m in range(1, 9):
            PointDailySale.objects.create(branch=cls.point, product=cls.product, sale_date=date(2026, m, 15), gross_amount=110, discount_amount=10)
            amounts = {'ingresos': 100, 'costos': 10, 'produccion': 1, 'administracion': 2, 'logistica': 3}
            for k, amount in amounts.items():
                LineaPresupuestoMensual.objects.create(rubro=rubros[k], periodo=date(2026,m,1), monto_real=amount, fuente_real='AUTO:LEGADO')
                cls.buckets[k][m] = {'real': D(amount), 'con_real': True}
            sid = cls.crucero.pk if m<=6 else cls.bamoa.pk
            LineaPresupuestoMensual.objects.create(rubro=directos[sid], periodo=date(2026,m,1), monto_real=5, fuente_real='AUTO:LEGADO')
            cls.buckets['gastos-venta'][m] = {'real': D(6 if m==1 else 5), 'con_real': True}
        LineaPresupuestoMensual.objects.create(rubro=directos[cls.bamoa.pk], periodo=date(2026,1,1), monto_real=1, fuente_real='AUTO:LEGADO')

    def calculate(self):
        return reparto_historico(self.buckets, LineaPresupuestoMensual.objects.select_related('rubro__area'))

    def test_historical_identity_conservation_and_preopening(self):
        before = list(PointDailySale.objects.values())
        result = self.calculate()
        self.assertEqual(result['errores'], [])
        self.assertEqual(result['total'], D(79*8-1))
        for m in range(1,9):
            month=[r for r in result['detalle'] if r['mes']==m]
            self.assertEqual(sum(r['ventas'] for r in month),D(100))
            for k in FONDOS:
                self.assertEqual(sum(r[k] for r in month),self.buckets[k][m]['real'])
            seller=next(r for r in month if r['ventas'])
            self.assertEqual(seller['sucursal_id'],self.crucero.pk if m<=6 else self.bamoa.pk)
        pre=next(r for r in result['detalle'] if r['mes']==1 and r['sucursal_id']==self.bamoa.pk)
        self.assertEqual(pre['utilidad'],D(-1))
        self.assertEqual(pre['etapa'],'Registro anterior a apertura')
        self.assertEqual(before,list(PointDailySale.objects.values()))
        self.assertEqual(etapa_sucursal('GUAMUCHIL',2),'Preapertura')
        self.assertIn('24/03',etapa_sucursal('GUAMUCHIL',3))

    def test_source_difference_hides_incomplete_results(self):
        PointDailySale.objects.filter(sale_date=date(2026,2,15)).update(gross_amount=111)
        result=self.calculate()
        self.assertIn('Febrero',str(result['errores']))
        self.assertEqual(result['filas'],[])
        self.client.force_login(self.user)
        self.assertEqual(self.client.get('/reportes/estado-resultados/?year=2026&export=reparto').status_code,409)

    def test_unmapped_expense_is_not_dropped(self):
        line=LineaPresupuestoMensual.objects.filter(rubro__sucursal=self.crucero).first()
        line.rubro.sucursal=None
        line.rubro.save(update_fields=['sucursal'])
        self.assertIn('sin sucursal',str(self.calculate()['errores']))

    def test_sales_in_transition_gap_are_not_guessed(self):
        PointDailySale.objects.filter(sale_date=date(2026,7,15)).update(sale_date=date(2026,7,1))
        self.assertIn('fuera del período',str(self.calculate()['errores']))

    def test_authenticated_surface_and_csv_use_same_result(self):
        url='/reportes/estado-resultados/?year=2026'
        self.assertEqual(self.client.get(url).status_code,302)
        self.client.force_login(self.user)
        response=self.client.get(url)
        self.assertContains(response,'id="reparto-table"')
        self.assertEqual(response.context['reparto']['total'],D(631))
        exported=self.client.get(url+'&export=reparto')
        self.assertEqual(exported.status_code,200)
        rows=list(csv.DictReader(StringIO(exported.content.decode('utf-8-sig'))))
        self.assertEqual(sum(D(r['Utilidad operativa asignada']) for r in rows),D(631))
        self.assertEqual(len(rows),9)
        self.assertEqual(self.client.get('/reportes/estado-resultados/?year=2025&export=reparto').status_code,409)
        self.assertNotContains(self.client.get('/reportes/estado-resultados/?year=2025'),'id="reparto-table"')
        self.assertEqual(self.client.get(url+'&version=REVISADO&export=reparto').status_code,409)

    def test_user_without_report_permission_cannot_export(self):
        user=get_user_model().objects.create_user('sin_reportes',password='test')
        self.client.force_login(user)
        self.assertEqual(self.client.get('/reportes/estado-resultados/?year=2026&export=reparto').status_code,403)
