from datetime import date
from decimal import Decimal as D
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.test import TestCase
from core.models import Sucursal
from rrhh.models import Empleado, NominaPeriodo, NominaLinea, NominaConceptoLinea


class PersonalRegistradoTests(TestCase):
    def test_rrhh_muestra_funcion_existente_cuando_no_hay_descriptivo(self):
        from rrhh.services_catalogos import puesto_operativo_choices
        e=Empleado.objects.create(codigo='visible',nombre='Persona',departamento='VENTAS',
            puesto_operativo='AUXILIAR_CAJAS')
        self.client.force_login(get_user_model().objects.create_superuser('rrhh-test',password='test-only'))
        response=self.client.get('/rrhh/empleados/')
        expected=dict(puesto_operativo_choices())['AUXILIAR_CAJAS']
        self.assertEqual(response.context['empleados'][0].puesto_presentacion,expected)
        self.assertContains(response,f'<td class="cell-wrap">{expected}</td>',html=True)
        e.refresh_from_db()
        self.assertEqual(e.puesto,'')

    def test_dos_quincenas_despensa_y_funciones_conservan_total(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        suc = Sucursal.objects.create(codigo='LAB', nombre='Matriz')
        people = [
            ('caja', 'CAJAS', 'COLABORADOR'),
            ('encargado', 'CAJAS', 'ENCARGADA'),
            ('call', 'CALL_CENTER', 'COLABORADOR'),
            ('reparto', 'REPARTIDOR', 'COLABORADOR'),
            ('jefe', '', 'JEFATURA'),
        ]
        employees = [Empleado.objects.create(codigo=code, nombre=code, departamento='VENTAS',
            sucursal_ref=suc, puesto_operativo=puesto, nivel_organizacional=nivel)
            for code, puesto, nivel in people]
        for start, end in [(1, 15), (16, 31)]:
            period = NominaPeriodo.objects.create(fecha_inicio=date(2026,10,start), fecha_fin=date(2026,10,end), estatus='PAGADA')
            for emp in employees:
                line = NominaLinea.objects.create(periodo=period, empleado=emp, salario_base=100)
                for code, amount in [('1',100), ('32',10)]:
                    NominaConceptoLinea.objects.create(linea=line,tipo='PERCEPCION',codigo_concepto=code,nombre=code,importe=amount)
        row = leer_personal_registrado(date(2026,10,1),date(2026,11,1))[date(2026,10,1)]
        self.assertEqual(row['total'], D(1100))
        self.assertEqual(row['percepciones'], D(1000))
        self.assertEqual(row['despensa'], D(100))
        self.assertTrue(row['mes_cubierto'])
        self.assertEqual(len(row['periodos']),2)
        buckets = {x['destino']:x for x in row['grupos']}
        self.assertEqual(buckets['tienda']['total'],D(440))
        self.assertEqual(buckets['logistica']['total'],D(220))
        self.assertEqual(buckets['administracion']['total'],D(220))
        self.assertEqual(buckets['call_center']['total'],D(220))
        self.assertEqual(sum(x['total'] for x in row['grupos']),row['total'])

    def test_despensa_ya_incluida_no_se_duplica_y_mes_incompleto_se_advierte(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        p=NominaPeriodo.objects.create(fecha_inicio=date(2026,10,1),fecha_fin=date(2026,10,15),estatus='PAGADA')
        e=Empleado.objects.create(codigo='x',nombre='x',departamento='VENTAS')
        l=NominaLinea.objects.create(periodo=p,empleado=e,salario_base=110)
        for code,amount in [('1',100),('32',10)]:
            NominaConceptoLinea.objects.create(linea=l,tipo='PERCEPCION',codigo_concepto=code,nombre=code,importe=amount)
        r=leer_personal_registrado(date(2026,10,1),date(2026,11,1))[date(2026,10,1)]
        self.assertEqual(r['total'],D(110))
        self.assertEqual(r['despensa'],D(0))
        self.assertFalse(r['mes_cubierto'])
        self.assertEqual(r['grupos'][0]['destino'],'ventas_pendiente')
        self.assertTrue(r['pendientes'])

    @patch('reportes.services_planeacion_personal.timezone.localdate', return_value=date(2026,11,4))
    def test_reporte_y_exportaciones_muestran_fuentes_sin_sumarlas(self, _):
        from reportes.tests_planeacion_personal import payroll_xml
        from reportes.services_planeacion_personal import RFC
        from sat_client.models import CfdiDescargado
        from datetime import datetime, timezone
        p=NominaPeriodo.objects.create(fecha_inicio=date(2026,10,1),fecha_fin=date(2026,10,31),estatus='PAGADA')
        e=Empleado.objects.create(codigo='export',nombre='Privado',departamento='ADMINISTRACION')
        NominaLinea.objects.create(periodo=p,empleado=e,salario_base=100)
        CfdiDescargado.objects.create(uuid='test-export',rfc_emisor=RFC,rfc_receptor='PERSONA',
            subtotal=120,total=120,tipo_cfdi='emitido',tipo_comprobante='N',
            xml_raw=payroll_xml('2026-10-31','O','120'),
            fecha_emision=datetime(2026,11,1,tzinfo=timezone.utc))
        user=get_user_model().objects.create_superuser('report-test',password='test-only')
        self.client.force_login(user)
        url='/reportes/planeacion-personal/?mes=2026-10'
        response=self.client.get(url)
        self.assertContains(response,'Nómina registrada')
        self.assertNotContains(response,'Privado')
        row=self.client.get(url+'&formato=json').json()['selected']
        self.assertEqual(row['registered']['total'],'100.00')
        self.assertEqual(row['payroll_difference'],'20.00')
        self.assertEqual(row['documented'],'120')
        csv=self.client.get(url+'&formato=csv')
        self.assertContains(csv,'Despensa adicional')
        self.assertContains(csv,'100.00')

    def test_desacuerdo_no_se_convierte_en_costo_completo(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        p=NominaPeriodo.objects.create(fecha_inicio=date(2026,10,1),fecha_fin=date(2026,10,31),estatus='PAGADA')
        e=Empleado.objects.create(codigo='z',nombre='z',departamento='ADMINISTRACION')
        l=NominaLinea.objects.create(periodo=p,empleado=e,salario_base=100)
        NominaConceptoLinea.objects.create(linea=l,tipo='PERCEPCION',codigo_concepto='1',nombre='Sueldo',importe=90)
        r=leer_personal_registrado(date(2026,10,1),date(2026,11,1))[date(2026,10,1)]
        self.assertEqual(r['total'],D(100))
        self.assertTrue(r['pendientes'])

    def test_sucursal_con_puesto_pendiente_y_apoyos_concilia_sin_inventar_cajas(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        suc = Sucursal.objects.create(codigo='MES', nombre='Sucursal')
        period = NominaPeriodo.objects.create(fecha_inicio=date(2026,10,1),fecha_fin=date(2026,10,31),estatus='PAGADA')
        people = [('VENTAS','CAJAS'),('VENTAS',''),('ADMINISTRACION','LIMPIEZA'),('ADMINISTRACION','ALMACEN')]
        for i, (dep, fun) in enumerate(people):
            e = Empleado.objects.create(nombre=str(i), departamento=dep, puesto_operativo=fun, sucursal_ref=suc)
            line = NominaLinea.objects.create(periodo=period,empleado=e,salario_base=100)
            NominaConceptoLinea.objects.create(linea=line,tipo='PERCEPCION',codigo_concepto='1',nombre='Sueldo',importe=100)
        row = leer_personal_registrado(date(2026,10,1),date(2026,11,1))[date(2026,10,1)]
        self.assertEqual(row['sucursales_ventas'][0]['personas'], 2)
        self.assertEqual(row['sucursales_ventas'][0]['puestos_pendientes'], 1)
        self.assertEqual(row['total_ventas'], D(200))
        self.assertEqual(row['total_otros'], D(200))
        self.assertEqual(row['total'], row['total_ventas'] + row['total_otros'])
        self.assertEqual({g['destino'] for g in row['otros_costos']}, {'limpieza','almacen'})
        self.assertEqual(Empleado.objects.filter(departamento='VENTAS',puesto_operativo='').count(), 1)
