from datetime import date
from decimal import Decimal as D

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import AuditLog, Sucursal
from rrhh.models import Empleado, NominaConceptoLinea, NominaImportacion, NominaLinea, NominaPeriodo
from .models import AreaPresupuesto, LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto
from .services_nomina_pagos_reales import CLAVE, ajustes_pagos_reales
from .services_personal_global import resumen_personal_global
from .services_personal_registrado import leer_personal_registrado
from .services_presupuesto_real import PresupuestoRealConsolidacionService


class PagosRealesNominaTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sep, cls.oct, cls.nov = date(2026, 9, 1), date(2026, 10, 1), date(2026, 11, 1)
        cls.branch = Sucursal.objects.create(codigo='PR-M', nombre='Matriz')
        cls.emp = Empleado.objects.create(codigo='307', nombre='Persona de prueba', departamento='VENTAS',
            sucursal_ref=cls.branch, puesto_operativo='CAJAS', nivel_organizacional='COLABORADOR', activo=False)
        cls.periods = []
        for month, last in [(9, 30), (10, 31)]:
            period = NominaPeriodo.objects.create(fecha_inicio=date(2026, month, 1),
                fecha_fin=date(2026, month, last), estatus='PAGADA')
            cls.periods.append(period)
            line = NominaLinea.objects.create(periodo=period, empleado=cls.emp, salario_base=100)
            NominaConceptoLinea.objects.create(linea=line, tipo='PERCEPCION', codigo_concepto='1', nombre='Sueldo', importe=100)
        other = Empleado.objects.create(codigo='308', nombre='Convenio de prueba', departamento='VENTAS',
            sucursal_ref=cls.branch, puesto_operativo='CAJAS', nivel_organizacional='COLABORADOR', activo=False)
        cls.line = NominaLinea.objects.create(periodo=cls.periods[1], empleado=other,
            salario_base=D('21369.32'), descuentos=D('184.66'))
        NominaConceptoLinea.objects.create(linea=cls.line, tipo='PERCEPCION', codigo_concepto='22',
            nombre='Finiquito', importe=D('21369.32'))
        cls.audit = AuditLog.objects.create(action='DOCUMENT', model='rrhh.NominaLinea', object_id=str(cls.line.pk),
            payload={'empleado_id':other.pk, 'codigo':other.codigo, 'importe_bancario':'21000.00',
                     'fecha_bancaria':'2026-09-07', 'referencia_bancaria':'0081534016',
                     'fuentes':[{'sha256':'a'*64, 'pagina':2}]})
        cls.item = dict(linea_id=cls.line.pk, soporte_audit_id=cls.audit.pk, estado='APLICADO',
            criterio='PAGO_REAL_CONFIRMADO_DG', bruto_fiscal='21369.32', neto_fiscal='21184.66',
            autorizacion='Dirección autoriza reconocer el pago en septiembre una sola vez.')
        cls.imp = NominaImportacion.objects.create(periodo=cls.periods[1], estatus='IMPORTADA',
            archivo_hash='b'*64, archivo_nombre='nomina.xlsx', resumen={CLAVE:[cls.item], 'original':'conservar'})
        cls.rules = {}
        for code in ('gastos-venta', 'nomina'):
            area = AreaPresupuesto.objects.create(codigo=code, nombre=code)
            rubro = RubroPresupuesto.objects.create(area=area, sucursal=cls.branch, concepto='Sueldo', tipo='EGRESO')
            cls.rules[code] = ReglaFuenteRubro.objects.create(rubro=rubro, tipo_fuente='NOMINA',
                filtros={'departamento':'VENTAS', 'campo_monto':'total_percepciones'})
            for month in (cls.sep, cls.oct):
                LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=month, monto_presupuesto=100)

    def test_consolidacion_repetida_cuenta_neto_en_septiembre_y_conserva_control_fiscal(self):
        before = list(NominaLinea.objects.order_by('pk').values())
        for _ in range(2):
            for month in (self.sep, self.oct):
                result = PresupuestoRealConsolidacionService().consolidar(periodo=month)
                self.assertEqual(result.errores, [])
        values = {(l.rubro.area.codigo, l.periodo):l.monto_real for l in LineaPresupuestoMensual.objects.select_related('rubro__area')}
        self.assertEqual(values[('gastos-venta', self.sep)], D('21100.00'))
        self.assertEqual(values[('gastos-venta', self.oct)], D('100.00'))
        self.assertEqual(values[('nomina', self.sep)], D('100.00'))
        self.assertEqual(values[('nomina', self.oct)], D('21469.32'))
        self.assertEqual(list(NominaLinea.objects.order_by('pk').values()), before)
        self.imp.refresh_from_db()
        self.assertEqual(self.imp.resumen['original'], 'conservar')
        self.line.empleado.refresh_from_db()
        self.assertFalse(self.line.empleado.activo)

    def test_manual_se_conserva_y_otros_campos_no_se_convierten_en_pago_bruto(self):
        lp = LineaPresupuestoMensual.objects.get(rubro=self.rules['gastos-venta'].rubro, periodo=self.sep)
        lp.monto_real, lp.fuente_real = D('321'), 'MANUAL:persona'
        lp.save()
        PresupuestoRealConsolidacionService().consolidar(periodo=self.sep)
        lp.refresh_from_db()
        self.assertEqual(lp.monto_real, D('321'))
        rule = self.rules['gastos-venta']
        rule.filtros = {'campo_monto':'neto_calculado'}
        monto, _ = PresupuestoRealConsolidacionService()._monto_regla(rule, self.oct, {})
        self.assertEqual(monto, D('21284.66'))

    def test_lista_fiscal_y_desglose_global_no_suman_convenio_dos_veces(self):
        rows = leer_personal_registrado(self.sep, self.nov)
        for month, fiscal, delta, real in [(self.sep,D(100),D(21000),D(21100)),
                                          (self.oct,D('21469.32'),D('-21369.32'),D(100))]:
            row = rows[month]
            self.assertEqual(row['total'], fiscal)
            self.assertEqual(row['ajuste_pago_real'], delta)
            self.assertEqual(row['departamentos'][0]['ajuste_pago_real'], delta)
            self.assertEqual(row['sucursales_ventas'][0]['ajuste_pago_real'], delta)
            result = resumen_personal_global(dict(month=month, sales=D(100000), registered=row,
                imss=D(0), rcv=D(0), isn=D(0), fees=D(0)))
            self.assertEqual(result['personal_subtotal'], real)
            self.assertEqual(result['pagos_reales'][0]['diferencia_neta'], D('184.66'))
        self.assertEqual(ajustes_pagos_reales(date(2025,1,1), date(2026,1,1)), [])

    def test_fuente_modificada_o_control_duplicado_no_se_aceptan(self):
        self.imp.resumen[CLAVE].append(self.item)
        self.imp.save(update_fields=['resumen'])
        with self.assertRaisesMessage(ValueError, 'más de una conciliación'):
            ajustes_pagos_reales(self.sep, self.nov)
        row = leer_personal_registrado(self.sep, self.nov)[self.oct]
        self.assertEqual(row['ajuste_pago_real'], D(0))
        self.assertTrue(any('conciliación válida' in p for p in row['pendientes']))
        self.imp.resumen[CLAVE] = [self.item]
        self.imp.save(update_fields=['resumen'])
        NominaLinea.objects.filter(pk=self.line.pk).update(total_percepciones=D(22000))
        with self.assertRaisesMessage(ValueError, 'fuentes vigentes'):
            ajustes_pagos_reales(self.sep, self.nov)

    def test_pago_mismo_mes_solo_produce_la_diferencia_y_rechaza_importe_invalido(self):
        self.audit.payload['fecha_bancaria'] = '2026-10-07'
        self.audit.save(update_fields=['payload'])
        ajustes = ajustes_pagos_reales(self.sep, self.nov)
        self.assertEqual(len(ajustes), 1)
        self.assertEqual(ajustes[0]['ajuste'], D('-369.32'))
        self.audit.payload['importe_bancario'] = '-21000.00'
        self.audit.save(update_fields=['payload'])
        with self.assertRaises(ValueError):
            ajustes_pagos_reales(self.sep, self.nov)

    def test_pantalla_y_csv_exponen_ambas_fechas_y_no_exportan_cuentas(self):
        user = get_user_model().objects.create_superuser(username='pagos-real', password='test')
        self.client.force_login(user)
        url = '/reportes/planeacion-personal/?mes=2026-10'
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Conciliación por fecha de pago real')
        self.assertContains(response, '07/09/2026')
        csv = self.client.get(url + '&formato=csv')
        self.assertContains(csv, '2026-09-07')
        self.assertContains(csv, '-21369.32')
        self.assertNotContains(csv, '0081534016')
