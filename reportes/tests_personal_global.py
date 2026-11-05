from datetime import date
from decimal import Decimal as D
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase, override_settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from rrhh.models import Empleado, NominaConceptoLinea, NominaLinea, NominaPeriodo
from .models import AreaPresupuesto, CategoriaGasto, CentroCosto, GastoOperativoMensual, ObligacionGasto, RubroPresupuesto
from .services_personal_global import resumen_personal_global
from .services_personal_registrado import leer_personal_registrado


@override_settings(STORAGES={'default': {'BACKEND': 'django.core.files.storage.InMemoryStorage'}, 'staticfiles': {'BACKEND': 'django.contrib.staticfiles.storage.StaticFilesStorage'}})
class PersonalGlobalTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.month = date(2026, 10, 1)
        a = Sucursal.objects.create(codigo='GLOBAL-A', nombre='A')
        b = Sucursal.objects.create(codigo='GLOBAL-B', nombre='B')
        first = NominaPeriodo.objects.create(fecha_inicio=cls.month, fecha_fin=date(2026, 10, 15), estatus='PAGADA')
        second = NominaPeriodo.objects.create(fecha_inicio=date(2026, 10, 16), fecha_fin=date(2026, 10, 31), estatus='PAGADA')
        cashier = Empleado.objects.create(codigo='GLOBAL-C', nombre='Caja', departamento='VENTAS', puesto_operativo='CAJAS', sucursal_ref=a)
        production = Empleado.objects.create(codigo='GLOBAL-P', nombre='Produccion', departamento='PRODUCCION', puesto_operativo='EMBETUNADO', sucursal_ref=a)
        admin = Empleado.objects.create(codigo='GLOBAL-D', nombre='Admin', departamento='ADMINISTRACION', sucursal_ref=a)
        for period, employee, branch in [(first, cashier, a), (second, cashier, b), (second, production, a), (second, admin, a)]:
            line = NominaLinea.objects.create(periodo=period, empleado=employee, salario_base=100)
            NominaLinea.objects.filter(pk=line.pk).update(sucursal_snapshot=branch)
            for code, amount in [('1', 100), ('32', 10)]:
                NominaConceptoLinea.objects.create(linea=line, tipo='PERCEPCION', codigo_concepto=code, nombre=code, importe=amount)
        area = AreaPresupuesto.objects.create(codigo='gastos-venta', nombre='Gastos de venta')
        category = CategoriaGasto.objects.create(codigo='RENTA', nombre='Renta', capa_objetivo='SUCURSAL')
        center = CentroCosto.objects.create(codigo='GLOBAL', nombre='Global', tipo='SUCURSAL_VENTA', sucursal=a)
        rubro = RubroPresupuesto.objects.create(area=area, concepto='Arrendamiento local', tipo='EGRESO', sucursal=a)
        expense = GastoOperativoMensual.objects.create(periodo=cls.month, centro_costo=center, categoria_gasto=category, monto=100, archivo_soporte='test/renta.pdf')
        ObligacionGasto.objects.create(origen='VARIABLE', area=area, rubro=rubro, centro_costo=center,
            categoria_gasto=category, concepto='Renta', periodo=cls.month, fecha_gasto=cls.month,
            fecha_vencimiento=cls.month, monto_reconocido=100, gasto_operativo=expense)

    def setUp(self):
        default_storage.save('test/renta.pdf', ContentFile(b'Comprobante de prueba'))
        self.addCleanup(default_storage.delete, 'test/renta.pdf')

    def test_nombre_de_archivo_inexistente_no_acredita_importe(self):
        default_storage.delete('test/renta.pdf')
        result = resumen_personal_global(self.selected())
        self.assertIsNone(result['gastos_total'])
        self.assertEqual(result['gastos_parciales'], 1)
        self.assertEqual(result['subtotal'], D(462))

    def test_honorarios_corporativos_aparecen_una_vez_en_administracion(self):
        from .models import ReglaFuenteRubro
        area = AreaPresupuesto.objects.create(codigo='administracion', nombre='Administración')
        category = CategoriaGasto.objects.create(codigo='ADMIN_CORP', nombre='Administración corporativa',
            capa_objetivo='EMPRESA', bucket='CORPORATIVO')
        center = CentroCosto.objects.create(codigo='GLOBAL-CORP', nombre='Corporativo', tipo='CORPORATIVO')
        rubro = RubroPresupuesto.objects.create(area=area, concepto='Honorarios personas fisicas', tipo='EGRESO')
        ReglaFuenteRubro.objects.create(rubro=rubro, tipo_fuente='OBLIGACION_GASTO',
            filtros={'desde': '2026-10'})
        expense = GastoOperativoMensual.objects.create(periodo=self.month, centro_costo=center,
            categoria_gasto=category, monto=D('122999.97'), archivo_soporte='test/renta.pdf')
        ObligacionGasto.objects.create(origen='VARIABLE', area=area, rubro=rubro, centro_costo=center,
            categoria_gasto=category, concepto='Honorarios octubre', periodo=self.month,
            fecha_gasto=self.month, fecha_vencimiento=self.month, monto_reconocido=expense.monto,
            gasto_operativo=expense)
        result = resumen_personal_global(self.selected())
        admin = next(row for row in result['gastos_areas'] if row['codigo'] == 'administracion')
        self.assertEqual(admin['total'], D('122999.97'))
        self.assertEqual(admin['registros'], 1)
        self.assertEqual(result['gastos_total'], D('123099.97'))
        self.assertEqual(result['nomina'], D(440))

    def selected(self):
        return dict(month=self.month, sales=D(1000),
            registered=leer_personal_registrado(self.month, date(2026, 11, 1))[self.month],
            imss=D(20), rcv=None, isn=D(0), fees=D(2), ordinary=D(999), extraordinary=D(50),
            payroll_difference=D(609))

    def cargas_mensuales(self):
        for employee in Empleado.objects.all():
            line = employee.lineas_nomina.order_by('-periodo__fecha_fin').first()
            for column, amount in {'AU':2, 'AW':3, 'AZ':4, 'BC':1,
                                   'AV':5, 'AX':2, 'BB':3, 'AY':999, 'BD':0}.items():
                NominaConceptoLinea.objects.create(linea=line, tipo='OBLIGACION',
                    codigo_concepto=f'M202610:{column}', nombre=column, importe=amount)

    def test_cargas_mensuales_e_isn_devengado_concilian_departamentos_sin_duplicar(self):
        self.cargas_mensuales()
        bases = {emp.pk:D(100) for emp in Empleado.objects.all()}
        selected = self.selected()
        selected.update(imss=None, rcv=None, isn=None)
        with patch('reportes.services_personal_global.bases_gravadas_empleados', return_value=bases):
            result = resumen_personal_global(selected)
        components = {c['clave']:c['monto'] for c in result['componentes']}
        self.assertEqual(components['imss'], D(30))
        self.assertEqual(components['rcv'], D(30))
        self.assertEqual(components['isn'], D('7.20'))
        self.assertEqual(result['costo_departamentos'], D('507.20'))
        self.assertEqual(result['personal_subtotal'], D('509.20'))
        self.assertEqual(result['cargas_total'], D('67.20'))
        self.assertEqual(result['costo_departamentos_porcentaje'], D('50.72'))
        self.assertEqual(result['cargas_error'], '')
        self.assertEqual(sum(g['costo_personal'] for g in result['departamentos']), D('507.20'))
        self.assertEqual(result['nomina'], D(440))
        self.assertTrue(result['isn_calculado'])

    def test_cargas_mensuales_incompletas_no_se_presentan_como_costo_completo(self):
        self.cargas_mensuales()
        NominaConceptoLinea.objects.filter(codigo_concepto='M202610:BD').first().delete()
        selected = self.selected()
        selected.update(imss=None, rcv=None, isn=None)
        self.assertFalse(selected['registered']['cargas_cubiertas'])
        result = resumen_personal_global(selected)
        self.assertIsNone(result['costo_departamentos'])
        self.assertTrue(all(g['costo_personal'] is None for g in result['departamentos']))

    def test_control_patronal_distinto_no_se_reparte_como_si_conciliara(self):
        self.cargas_mensuales()
        selected = self.selected()
        bases = {emp.pk:D(100) for emp in Empleado.objects.all()}
        with patch('reportes.services_personal_global.bases_gravadas_empleados', return_value=bases):
            result = resumen_personal_global(selected)
        self.assertIsNone(result['costo_departamentos'])
        self.assertIn('no concilian', result['cargas_error'])
        self.assertEqual(next(c['monto'] for c in result['componentes'] if c['clave']=='imss'), D(20))

    def test_cargas_no_completan_un_mes_al_que_le_falta_una_quincena(self):
        self.cargas_mensuales()
        NominaPeriodo.objects.get(fecha_fin=date(2026,10,15)).delete()
        selected = self.selected()
        self.assertFalse(selected['registered']['mes_cubierto'])
        self.assertFalse(selected['registered']['cargas_cubiertas'])
        self.assertIsNone(resumen_personal_global(selected)['costo_departamentos'])

    def test_transfer_counts_once_and_departments_reconcile(self):
        selected = self.selected()
        result = resumen_personal_global(selected)
        departments = {g['destino']: g for g in result['departamentos']}
        self.assertEqual(departments['tienda']['personas'], 1)
        self.assertEqual(result['personas'], 3)
        self.assertEqual(departments['tienda']['total'], D(220))
        self.assertEqual(sum(g['total'] for g in departments.values()), selected['registered']['total'])
        self.assertEqual(result['nomina'], D(440))
        self.assertEqual(result['nomina_porcentaje'], D('44.00'))

    def test_source_obligation_and_fiscal_comparison_do_not_duplicate(self):
        result = resumen_personal_global(self.selected())
        self.assertEqual(result['gastos_total'], D(100))
        self.assertEqual(len(result['fuentes_gastos']), 1)
        self.assertEqual(result['personal_subtotal'], D(462))
        self.assertEqual(result['subtotal'], D(562))
        self.assertEqual(result['porcentaje'], D('56.20'))
        self.assertIsNone(result['costo_integral'])
        components = {c['clave']: c for c in result['componentes']}
        self.assertIsNone(components['rcv']['monto'])
        self.assertIsNone(components['rcv']['porcentaje'])
        self.assertEqual(components['isn']['monto'], D(0))
        self.assertEqual(components['isn']['porcentaje'], D('0.00'))
        self.assertIsNone(components['provisiones']['monto'])

    def test_missing_sales_payroll_and_expenses_never_claim_zero_complete_cost(self):
        selected = self.selected()
        selected.update(sales=None, registered=None)
        result = resumen_personal_global(selected)
        self.assertIsNone(result['subtotal'])
        self.assertIsNone(result['porcentaje'])
        self.assertEqual(result['gastos_total'], D(100))
        selected['month'] = date(2025, 1, 1)
        result = resumen_personal_global(selected)
        self.assertIsNone(result['gastos_total'])
        self.assertEqual(result['departamentos'], [])

    def test_read_only_and_pending_expense_is_not_summed(self):
        GastoOperativoMensual.objects.create(periodo=self.month,
            centro_costo=CentroCosto.objects.create(codigo='AMB', nombre='Compartido', tipo='COMPARTIDO'),
            categoria_gasto=CategoriaGasto.objects.get(codigo='RENTA'), monto=9999)
        with CaptureQueriesContext(connection) as queries:
            result = resumen_personal_global(self.selected())
        self.assertTrue(result['pendientes_gastos'])
        self.assertEqual(result['gastos_total'], D(100))
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))

    @patch('reportes.services_planeacion_personal.timezone.localdate', return_value=date(2026, 11, 4))
    def test_html_json_csv_share_global_and_preserve_incomplete_month_warning(self, _):
        NominaPeriodo.objects.filter(fecha_inicio=self.month).update(estatus='BORRADOR')
        self.client.force_login(get_user_model().objects.create_superuser('global', password='test-only'))
        url = '/reportes/planeacion-personal/?mes=2026-10'
        page = self.client.get(url)
        self.assertContains(page, 'Global por departamento')
        self.assertContains(page, 'Resumen final · peso sobre las ventas totales')
        self.assertContains(page, 'La nómina no cubre el mes completo')
        self.assertContains(page, 'Costo integral cerrado')
        data = self.client.get(url + '&formato=json').json()['selected']['global']
        self.assertEqual(D(data['nomina']), D(330))
        self.assertEqual(D(data['subtotal']), D(430))
        self.assertIsNone(data['porcentaje'])
        csv = self.client.get(url + '&formato=csv')
        self.assertContains(csv, 'Global por departamento')
        self.assertContains(csv, 'Subtotal personal y gastos identificados,430.00')
        self.assertContains(csv, 'Periodos cubren mes,False')
