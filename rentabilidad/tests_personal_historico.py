from datetime import date
from decimal import Decimal as D
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from core.models import Sucursal
from rrhh.models import Empleado, NominaPeriodo, NominaLinea, NominaConceptoLinea
from reportes.models import (AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual,
    ExpedienteCedulaIMSS, DocumentoCedulaIMSS, FactVentaDiaria)
from reportes.services_rentabilidad_personal import leer_personal_mensual
from .services_resultado import leer_resultado_mensual


class PersonalHistoricoResultadoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.month = date(2025, 9, 1)
        cls.branch = Sucursal.objects.create(codigo='LAB-HIST', nombre='Sucursal histórica')
        cls.period = NominaPeriodo.objects.create(fecha_inicio=cls.month,
            fecha_fin=date(2025, 9, 30), estatus='PAGADA')
        cls.lines = []
        for n, dep in enumerate(('VENTAS', 'PRODUCCION')):
            emp = Empleado.objects.create(codigo=f'LAB-H{n}', nombre=f'Persona privada {n}',
                departamento='VENTAS', sucursal_ref=cls.branch)
            line = NominaLinea.objects.create(periodo=cls.period, empleado=emp,
                salario_base=100, bonos=10, snapshot_origen='HISTORICA',
                departamento_snapshot=dep, puesto_operativo_snapshot='CAJAS' if n==0 else 'HORNOS')
            for code, amount in [('1', 100), ('32', 10)]:
                NominaConceptoLinea.objects.create(linea=line, tipo='PERCEPCION',
                    codigo_concepto=code, nombre=code, importe=amount)
            cls.lines.append(line)
        FactVentaDiaria.objects.create(fecha=cls.month, sucursal=cls.branch,
            producto_clave='LAB-P', producto_nombre='Producto', cantidad=1,
            venta_bruta=1000, venta_neta=1000, venta_total=1000, source_kind='AUTHORITATIVE')

    def cedula(self, kind, month, cost, paid=None):
        sha=('c' if kind=='MENSUAL' else 'd')*64
        paid=paid or cost
        data=dict(version=1, importe_pagado=str(paid), gasto_empresa=str(cost),
            retenciones=str(D(str(paid))-D(str(cost))), cuentas_por_cobrar='0.00',
            fecha_pago='2025-11-17', soporte_pago_sha256='a'*64,
            soporte_conciliacion_sha256='b'*64, sua_sha256=sha)
        e=ExpedienteCedulaIMSS.objects.create(tipo=kind, periodo=month,
            registro_patronal='E5240157100', estado='APLICADO', total_patronal=cost,
            metadata={'pago_conciliado':data})
        DocumentoCedulaIMSS.objects.create(expediente=e, clase='SUA_PDF',
            nombre_original='SUA.pdf', archivo='fixture.pdf', sha256=sha, tamano=1, mime_type='application/pdf')
        return e

    def test_empresa_reconoce_nomina_sin_inventar_sucursal_ni_repetir_despensa(self):
        with CaptureQueriesContext(connection) as queries:
            result=leer_resultado_mensual(self.month)
        self.assertEqual(result['central_registrado'], D(220))
        self.assertEqual(sum(f['monto'] for f in result['centrales'] if not f['pendiente']), D(220))
        self.assertEqual(leer_personal_mensual(self.month)['filas'], [])
        self.assertTrue(all(f['pendiente'] for f in result['centrales'] if f['concepto']!='Nómina real de empresa'))
        self.assertNotIn('Persona privada', str(result['centrales']))
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for q in queries))
        self.lines[0].refresh_from_db()
        self.assertIsNone(self.lines[0].sucursal_snapshot_id)

    def test_nomina_ya_asignada_se_resta_del_costo_de_empresa_sin_repartir_el_resto(self):
        NominaLinea.objects.filter(pk=self.lines[0].pk).update(sucursal_snapshot=self.branch)
        result=leer_resultado_mensual(self.month)
        direct=leer_personal_mensual(self.month)
        self.assertEqual(sum(f['monto_mensual'] for f in direct['filas']), D(110))
        self.assertEqual(result['central_registrado'], D(110))

    def test_control_presupuestal_de_nomina_no_se_suma_sobre_los_recibos(self):
        area=AreaPresupuesto.objects.create(codigo='produccion', nombre='Producción')
        for source, amount in [('AUTO:NOMINA',220),('AUTO:NOMINA_CONCEPTO',20)]:
            r=RubroPresupuesto.objects.create(area=area, concepto=source, tipo='EGRESO')
            LineaPresupuestoMensual.objects.create(periodo=self.month, rubro=r, monto_real=amount, fuente_real=source)
        rent=RubroPresupuesto.objects.create(area=area, concepto='Arrendamiento de fábrica', tipo='EGRESO')
        LineaPresupuestoMensual.objects.create(periodo=self.month, rubro=rent, monto_real=50, fuente_real='AUTO:GASTO_OPERATIVO')
        self.assertEqual(leer_resultado_mensual(self.month)['central_registrado'], D(270))

    def test_cedulas_mensual_y_bimestral_usan_gasto_sin_retenciones_ni_doble_bimestre(self):
        self.cedula('MENSUAL', self.month, D('30.00'), D('35.00'))
        self.cedula('BIMESTRAL', date(2025,10,1), D('20.01'), D('22.01'))
        september=leer_resultado_mensual(self.month)
        october=leer_resultado_mensual(date(2025,10,1))
        self.assertEqual(september['central_registrado'], D('260.00'))
        self.assertEqual(october['central_registrado'], D('10.01'))

    def test_control_invalido_no_convierte_faltante_en_costo_cero_confirmado(self):
        e=self.cedula('MENSUAL',self.month,D('30.00'))
        e.metadata['pago_conciliado']['gasto_empresa']='999.00';e.save(update_fields=['metadata'])
        result=leer_resultado_mensual(self.month)
        self.assertEqual(result['central_registrado'],D(220))
        self.assertTrue(any(f['pendiente'] and 'IMSS' in f['concepto'] for f in result['centrales']))

    def test_fuera_de_2025_conserva_el_lector_actual(self):
        self.assertEqual(leer_resultado_mensual(date(2026,9,1))['centrales'],[])

    def test_dashboard_muestra_importe_y_traza_sin_afirmar_cierre_integral(self):
        self.client.force_login(get_user_model().objects.create_superuser('lab-hist',password='test-only'))
        page=self.client.get('/rentabilidad/',{'periodo':'2025-09'})
        self.assertEqual(page.status_code,200)
        self.assertEqual(page.context['totales']['central_registrado'],D(220))
        self.assertContains(page,'Nómina real de empresa')
        self.assertContains(page,'Sin distribución histórica')
        self.assertNotContains(page,'Persona privada')

    def test_cargas_ya_en_sucursal_no_se_reconocen_de_nuevo_en_empresa(self):
        self.cedula('MENSUAL',self.month,D(30))
        area=AreaPresupuesto.objects.create(codigo='gastos-venta',nombre='Gastos de venta')
        rubro=RubroPresupuesto.objects.create(area=area,concepto='IMSS',tipo='EGRESO',sucursal=self.branch)
        LineaPresupuestoMensual.objects.create(periodo=self.month,rubro=rubro,
            monto_real=5,fuente_real='MANUAL:captura')
        self.assertEqual(leer_resultado_mensual(self.month)['central_registrado'],D(245))

    def test_control_mixto_no_se_borra_ni_se_duplica_como_nomina(self):
        area=AreaPresupuesto.objects.create(codigo='produccion',nombre='Producción')
        rubro=RubroPresupuesto.objects.create(area=area,concepto='Mixto',tipo='EGRESO')
        LineaPresupuestoMensual.objects.create(periodo=self.month,rubro=rubro,monto_real=150,
            fuente_real='AUTO:GASTO_OPERATIVO+NOMINA',
            metadata={'real_breakdown':[{'tipo_fuente':'NOMINA'},{'tipo_fuente':'GASTO_OPERATIVO'}]})
        result=leer_resultado_mensual(self.month)
        self.assertEqual(result['central_registrado'],D(150))
        self.assertTrue(any(f['concepto']=='Nómina real de empresa' and f['pendiente'] for f in result['centrales']))

    def test_reporte_encuentra_mes_historico_sin_snapshot_y_descontara_empresa_una_vez(self):
        from .services_reporte_mensual import build_reporte_mensual_consolidado
        report=build_reporte_mensual_consolidado(hasta=self.month,meses=1)
        self.assertEqual(report['filas'][0]['ingresos'],D(1000))
        self.assertEqual(report['filas'][0]['central_registrado'],D(220))

    def test_reporte_de_una_sucursal_no_carga_el_remanente_de_toda_la_empresa(self):
        from .services_reporte_mensual import build_reporte_mensual_consolidado
        row=build_reporte_mensual_consolidado(hasta=self.month,meses=1,sucursal_id=self.branch.pk)['filas'][0]
        self.assertIsNone(row['resultado_empresa_parcial'])
        self.assertIsNone(row['central_registrado'])
