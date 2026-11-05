from datetime import date
from decimal import Decimal as D
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.test import TestCase
from core.models import Sucursal
from rrhh.models import Empleado, NominaPeriodo, NominaLinea, NominaConceptoLinea


class PersonalRegistradoTests(TestCase):
    def test_mantenimiento_registrado_no_se_marca_sin_funcion_ni_como_personal_de_tienda(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        sucursal = Sucursal.objects.create(codigo='MANT', nombre='Matriz')
        empleado = Empleado.objects.create(codigo='mantenimiento', nombre='Persona de mantenimiento',
            departamento='MANTENIMIENTO', puesto='Mantenimiento', puesto_operativo='MANTENIMIENTO',
            nivel_organizacional='COLABORADOR', sucursal_ref=sucursal, activo=False)
        for inicio, fin in [(1,15), (16,30)]:
            periodo = NominaPeriodo.objects.create(fecha_inicio=date(2026,9,inicio),
                fecha_fin=date(2026,9,fin), estatus='PAGADA')
            linea = NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=100)
            NominaConceptoLinea.objects.create(linea=linea, tipo='PERCEPCION',
                codigo_concepto='1', nombre='Sueldo', importe=100)
        antes = list(NominaLinea.objects.filter(empleado=empleado).order_by('pk').values())
        registro = leer_personal_registrado(date(2026,9,1), date(2026,10,1))[date(2026,9,1)]
        self.assertEqual(registro['pendientes'], [])
        self.assertEqual(registro['total'], D(200))
        self.assertEqual(registro['departamentos'][0]['destino'], 'mantenimiento')
        self.assertEqual(registro['departamentos'][0]['destino_label'], 'Mantenimiento')
        self.assertEqual(registro['departamentos'][0]['personas'], 1)
        self.assertEqual(registro['departamentos'][0]['total'], D(200))
        self.assertEqual(registro['sucursales_ventas'], [])
        self.assertEqual(list(NominaLinea.objects.filter(empleado=empleado).order_by('pk').values()), antes)
        empleado.refresh_from_db()
        self.assertFalse(empleado.activo)
        self.assertEqual(empleado.departamento, 'MANTENIMIENTO')

    def test_fabricacion_no_asume_etapa_actual_para_un_documento_historico(self):
        from reportes.services_mano_obra_mensual import componente
        row = {'snapshot_origen':'HISTORICA', 'puesto_operativo_snapshot':'',
            'empleado__puesto_operativo':'HORNOS', 'empleado__puesto':'Jefe de producción'}
        self.assertIsNone(componente(row))
        row['puesto_operativo_snapshot'] = 'ENVIO_SUCURSAL'
        self.assertEqual(componente(row), 'despacho')

    def test_consumidores_no_reconstruyen_asignacion_historica_con_rrhh_actual(self):
        from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
        from reportes.services_rentabilidad_personal import leer_personal_mensual
        from reportes.services_costeo_etapas import leer_logistica
        suc = Sucursal.objects.create(codigo='HOY', nombre='Actual')
        emp = Empleado.objects.create(codigo='movido', nombre='Persona', departamento='VENTAS',
            puesto_operativo='CAJAS', nivel_organizacional='JEFATURA', sucursal_ref=suc)
        period = NominaPeriodo.objects.create(fecha_inicio=date(2025,1,1),
            fecha_fin=date(2025,1,31), estatus='CERRADA')
        line = NominaLinea.objects.create(periodo=period, empleado=emp, salario_base=100,
            snapshot_origen=NominaLinea.SNAPSHOT_HISTORICA, departamento_snapshot='LOGISTICA',
            puesto_operativo_snapshot='REPARTIDOR', nivel_organizacional_snapshot='')
        NominaConceptoLinea.objects.create(linea=line, tipo='PERCEPCION',
            codigo_concepto='1', nombre='Sueldo', importe=100)
        index = PresupuestoRealConsolidacionService._build_nomina_index(date(2025,1,1))
        concepts = PresupuestoRealConsolidacionService._build_nomina_concepto_index(date(2025,1,1))
        for row, prefix in [(index[0], ''), (concepts[0], 'linea__')]:
            self.assertEqual(row[prefix+'empleado__departamento'], 'LOGISTICA')
            self.assertEqual(row[prefix+'empleado__puesto_operativo'], 'REPARTIDOR')
            self.assertEqual(row[prefix+'empleado__nivel_organizacional'], '')
            self.assertIsNone(row[prefix+'empleado__sucursal_ref_id'])
        self.assertFalse([r for r in leer_personal_mensual(date(2025,1,1))['filas'] if r['familia']=='nomina'])
        logistics = leer_logistica(date(2025,1,1), date(2025,1,31), {'personas': []})
        self.assertEqual(len(logistics['personal']), 1)
        self.assertEqual(logistics['personal'][0]['monto'], D(100))

    def test_otro_pago_conserva_neto_sin_sumarlo_al_costo_salarial(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        from django.core.management import call_command
        from io import StringIO
        emp = Empleado.objects.create(codigo='prestamo', nombre='Persona',
            departamento='VENTAS', puesto_operativo='CAJAS', salario_diario=500)
        period = NominaPeriodo.objects.create(fecha_inicio=date(2025,1,1),
            fecha_fin=date(2025,1,31), estatus='CERRADA',
            tipo_periodo=NominaPeriodo.TIPO_EXTRAORDINARIA)
        line = NominaLinea.objects.create(periodo=period, empleado=emp,
            snapshot_origen=NominaLinea.SNAPSHOT_HISTORICA, salario_base=100,
            otros_pagos=3000, descuentos=10)
        for tipo, codigo, importe in [('PERCEPCION','1',100), ('OTRO_PAGO','132',3000)]:
            NominaConceptoLinea.objects.create(linea=line, tipo=tipo,
                codigo_concepto=codigo, nombre=codigo, importe=importe)
        call_command('capturar_snapshots_nomina', periodo='2025-01', apply=True, stdout=StringIO())
        line.refresh_from_db()
        self.assertEqual(line.snapshot_origen, NominaLinea.SNAPSHOT_HISTORICA)
        self.assertEqual(line.departamento_snapshot, '')
        self.assertEqual(line.neto_calculado, D(3090))
        row = leer_personal_registrado(date(2025,1,1), date(2025,2,1))[date(2025,1,1)]
        self.assertEqual(row['total'], D(100))
        self.assertEqual(row['grupos'][0]['destino'], 'sin_clasificar')

    def test_puesto_del_periodo_no_cambia_con_la_ficha_actual(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        suc = Sucursal.objects.create(codigo='HIS', nombre='Sucursal histórica')
        emp = Empleado.objects.create(codigo='hist', nombre='Persona', departamento='VENTAS',
            puesto_operativo='CAJAS', nivel_organizacional='COLABORADOR', sucursal_ref=suc)
        period = NominaPeriodo.objects.create(fecha_inicio=date(2025,1,1),
            fecha_fin=date(2025,1,31), estatus='CERRADA')
        line = NominaLinea.objects.create(periodo=period, empleado=emp, salario_base=100)
        NominaConceptoLinea.objects.create(linea=line, tipo='PERCEPCION',
            codigo_concepto='1', nombre='Sueldo', importe=100)
        Empleado.objects.filter(pk=emp.pk).update(puesto_operativo='REPARTIDOR',
            nivel_organizacional='JEFATURA', departamento='LOGISTICA', sucursal_ref=None)
        line.puesto_operativo_snapshot = 'REPARTIDOR'
        line.save()
        row = leer_personal_registrado(date(2025,1,1), date(2025,2,1))[date(2025,1,1)]
        self.assertEqual(row['total_ventas'], D(100))
        self.assertEqual(row['grupos'][0]['destino'], 'tienda')
        self.assertEqual(row['grupos'][0]['sucursal_id'], suc.pk)
        self.assertEqual(line.puesto_operativo_snapshot, 'CAJAS')

    def test_historico_incompleto_no_inventa_puesto_sucursal_ni_sueldo_actual(self):
        from reportes.services_personal_registrado import leer_personal_registrado
        suc = Sucursal.objects.create(codigo='ACT', nombre='Sucursal actual')
        emp = Empleado.objects.create(codigo='actual', nombre='Persona', departamento='VENTAS',
            puesto_operativo='CAJAS', nivel_organizacional='JEFATURA', sucursal_ref=suc,
            salario_diario=500)
        period = NominaPeriodo.objects.create(fecha_inicio=date(2025,1,1),
            fecha_fin=date(2025,1,31), estatus='CERRADA')
        line = NominaLinea.objects.create(periodo=period, empleado=emp,
            snapshot_origen=NominaLinea.SNAPSHOT_HISTORICA, departamento_snapshot='VENTAS',
            dias_trabajados=15, salario_base=0, bonos=100)
        NominaConceptoLinea.objects.create(linea=line, tipo='PERCEPCION',
            codigo_concepto='17', nombre='Ajuste comprobado', importe=100)
        row = leer_personal_registrado(date(2025,1,1), date(2025,2,1))[date(2025,1,1)]
        self.assertIsNone(line.sucursal_snapshot_id)
        self.assertEqual(line.salario_base, D(0))
        self.assertEqual(row['total'], D(100))
        self.assertEqual(row['grupos'][0]['destino'], 'ventas_pendiente')
        self.assertTrue(row['pendientes'])

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
        self.assertEqual(e.puesto,expected)

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
