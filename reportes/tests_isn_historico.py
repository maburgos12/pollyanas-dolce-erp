from datetime import date
from decimal import Decimal as D
from io import StringIO
from unittest.mock import patch
from uuid import uuid4

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from rrhh.models import Empleado, NominaPeriodo, NominaLinea, NominaConceptoLinea, NominaImportacion
from sat_client.models import CfdiDescargado
from reportes.models import ExpedienteISN, DistribucionISNEmpleado
from reportes.services_isn_historico import preparar_isn_empresa_2025, aplicar_isn_empresa_2025
from reportes.services_planeacion_personal import build_personnel_plan


class ISNEmpresaHistoricoTests(TestCase):
    def setUp(self):
        self.persona = Empleado.objects.create(codigo='HIST-ISN', nombre='Persona histórica')
        self.periodo = date(2025, 1, 1)
        self.lineas = []
        self.crear_nomina(date(2025, 1, 1), date(2025, 1, 15), [('1', 'Sueldo', '10000', '0')])
        self.crear_nomina(date(2025, 1, 16), date(2025, 1, 31), [('1', 'Sueldo', '10000', '0')])
        self.crear_nomina(date(2025, 1, 20), date(2025, 1, 20), [
            ('24', 'Aguinaldo', '5000', '3257.10'), ('25', 'PTU', '2000', '2000'),
            ('32', 'Despensa', '500', '300')], extraordinaria=True)
        self.cfdi = CfdiDescargado.objects.create(uuid=str(uuid4()).upper(),
            rfc_emisor='GES8101015I7', rfc_receptor='GEF211230KR2', tipo_cfdi='recibido',
            tipo_comprobante='I', estatus='VIGENTE', fecha_emision=timezone.now(),
            subtotal=D('526.63'), total=D('526.63'), xml_raw='''
            <cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4"><cfdi:Conceptos>
            <cfdi:Concepto NoIdentificacion="202501 2-003" Importe="526.63"/>
            </cfdi:Conceptos></cfdi:Comprobante>''')

    def crear_nomina(self, inicio, fin, conceptos, extraordinaria=False):
        p = NominaPeriodo.objects.create(folio=str(uuid4()), fecha_inicio=inicio, fecha_fin=fin,
            tipo_periodo='EXTRAORDINARIA' if extraordinaria else 'QUINCENAL', estatus='CERRADA')
        clave = str(uuid4()).upper()
        tipos = {'24': '002', '25': '003', '32': '029'}
        fiscal = CfdiDescargado.objects.create(uuid=clave, rfc_emisor='GEF211230KR2',
            rfc_receptor='PERSONA', tipo_cfdi='emitido', tipo_comprobante='N', estatus='VIGENTE',
            fecha_emision=timezone.now(), subtotal=0, total=0, xml_raw=(
                '<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" xmlns:n="http://www.sat.gob.mx/nomina12">'
                f'<cfdi:Complemento><n:Nomina FechaFinalPago="{fin}"><n:Percepciones>' + ''.join(
                    f'<n:Percepcion TipoPercepcion="{tipos.get(c,"001")}" Clave="{c}" Concepto="{n}" ImporteGravado="{D(m)-D(e)}" ImporteExento="{e}"/>'
                    for c,n,m,e in conceptos) + '</n:Percepciones></n:Nomina></cfdi:Complemento></cfdi:Comprobante>'))
        l = NominaLinea.objects.create(periodo=p, empleado=self.persona, snapshot_origen='HISTORICA',
            observaciones=f'CFDI {clave}', salario_base=sum((D(c[2]) for c in conceptos), D(0)))
        NominaConceptoLinea.objects.bulk_create([NominaConceptoLinea(linea=l,tipo='PERCEPCION',
            codigo_concepto=c,nombre=n,importe=D(m)) for c,n,m,e in conceptos])
        imp = NominaImportacion.objects.create(periodo=p,archivo_hash='a'*64,archivo_nombre='historico',
            estatus='IMPORTADA',resumen={'uuids':[clave], 'ajustes_por_uuid':{}})
        self.lineas.append((l,fiscal,imp))
        return l, fiscal, imp

    def test_concilia_empresa_extraordinarias_y_prevision_sin_distribuir(self):
        preview = preparar_isn_empresa_2025(self.periodo)
        self.assertEqual(preview['base_calculada'], '21942.90')
        self.assertEqual(preview['estado'], 'APLICADO')
        expediente = aplicar_isn_empresa_2025(preview)
        self.assertEqual(aplicar_isn_empresa_2025(preview).pk, expediente.pk)
        self.assertEqual(ExpedienteISN.objects.count(), 1)
        self.assertEqual(DistribucionISNEmpleado.objects.count(), 0)
        self.assertFalse(NominaLinea.objects.exclude(sucursal_snapshot=None).exists())
        with patch('reportes.services_planeacion_personal.timezone.localdate', return_value=date(2026,11,5)):
            row = build_personnel_plan(date(2025,1,31))['selected']
        self.assertEqual(row['isn'], D('526.63'))
        self.assertEqual(len([s for s in row['sources'] if s['kind'].startswith('ISN')]), 1)

    def test_discrepancia_no_es_gasto_conciliado_ni_reparto(self):
        self.cfdi.xml_raw = self.cfdi.xml_raw.replace('526.63','600.00')
        self.cfdi.save(update_fields=['xml_raw'])
        preview = preparar_isn_empresa_2025(self.periodo)
        e = aplicar_isn_empresa_2025(preview)
        self.assertEqual(e.estado, 'DISCREPANCIA')
        self.assertIsNone(e.aplicado_en)
        self.assertFalse(e.distribuciones.exists())
        row = build_personnel_plan(date(2025,1,31))['selected']
        self.assertTrue(any('diferencia de 73.37' in error for error in row['errors']))
        self.assertTrue(any(s.get('reconciled') is False for s in row['sources'] if s['kind'].startswith('ISN')))

    def test_preview_alterado_o_fuente_cambiada_se_rechaza(self):
        preview = preparar_isn_empresa_2025(self.periodo)
        for key, value in [('estado','DISCREPANCIA'),('base_calculada','1.00'),('huella_fuentes','f'*64)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                aplicar_isn_empresa_2025(dict(preview, **{key:value}))
        fiscal = self.lineas[0][1]
        fiscal.estatus='CANCELADO';fiscal.save(update_fields=['estatus'])
        with self.assertRaises(ValueError): aplicar_isn_empresa_2025(preview)
        self.assertFalse(ExpedienteISN.objects.exists())

    def test_rechaza_calendario_incompleto_y_solapado(self):
        NominaPeriodo.objects.filter(pk=self.lineas[0][0].periodo_id).update(fecha_inicio=date(2025,1,2))
        with self.assertRaisesMessage(ValueError,'cobertura'): preparar_isn_empresa_2025(self.periodo)
        NominaPeriodo.objects.filter(pk=self.lineas[0][0].periodo_id).update(fecha_inicio=date(2025,1,1))
        self.crear_nomina(date(2025,1,2),date(2025,1,3),[('1','Sueldo','100','0')])
        with self.assertRaisesMessage(ValueError,'solapada'): preparar_isn_empresa_2025(self.periodo)

    def test_ajustes_documentados_se_conservan_y_sin_soporte_se_rechazan(self):
        l,fiscal,imp=self.lineas[0]
        imp.resumen['ajustes_por_uuid']={fiscal.uuid:[dict(concepto='Ajuste en sueldos',importe='-100.00',
            estado='PAGO_CONCILIADO_CON_LISTA',soporte='documento') ]}
        imp.save(update_fields=['resumen'])
        # Un código agregado negativo no es una percepción válida: no netearlo con otro código.
        NominaConceptoLinea.objects.create(linea=l,tipo='PERCEPCION',codigo_concepto='17',importe=-100)
        with self.assertRaisesMessage(ValueError,'conceptos reales'): preparar_isn_empresa_2025(self.periodo)
        imp.resumen['ajustes_por_uuid'][fiscal.uuid][0].update(concepto='Sueldo')
        imp.save(update_fields=['resumen'])
        NominaConceptoLinea.objects.filter(linea=l,codigo_concepto='17').update(codigo_concepto='1')
        NominaLinea.objects.filter(pk=l.pk).update(total_percepciones=D('9900'))
        self.assertEqual(preparar_isn_empresa_2025(self.periodo)['base_calculada'],'21842.90')
        imp.resumen['ajustes_por_uuid'][fiscal.uuid][0]['soporte']='';imp.save(update_fields=['resumen'])
        with self.assertRaisesMessage(ValueError,'sin conciliación'): preparar_isn_empresa_2025(self.periodo)

    def test_codigo_desconocido_no_se_estima(self):
        l,f,imp=self.lineas[0]
        f.xml_raw=f.xml_raw.replace('Clave="1"','Clave="999"');f.save(update_fields=['xml_raw'])
        NominaConceptoLinea.objects.filter(linea=l).update(codigo_concepto='999')
        with self.assertRaisesMessage(ValueError,'conceptos reales'): preparar_isn_empresa_2025(self.periodo)

    def test_uuid_repetido_o_fuente_ajena_no_se_acepta(self):
        l,f,imp=self.lineas[0]
        NominaLinea.objects.filter(pk=l.pk).update(snapshot_origen='CREACION')
        with self.assertRaisesMessage(ValueError,'origen'): preparar_isn_empresa_2025(self.periodo)
        NominaLinea.objects.filter(pk=l.pk).update(snapshot_origen='HISTORICA')
        otro=self.lineas[1][2];otro.resumen['uuids']=[f.uuid];otro.save(update_fields=['resumen'])
        with self.assertRaisesMessage(ValueError,'varias importaciones'): preparar_isn_empresa_2025(self.periodo)

    def test_no_sobrescribe_expediente_ni_admite_2026(self):
        preview=preparar_isn_empresa_2025(self.periodo);e=aplicar_isn_empresa_2025(preview)
        ExpedienteISN.objects.filter(pk=e.pk).update(metadata={})
        with self.assertRaisesMessage(ValueError,'no se sobrescribe'): aplicar_isn_empresa_2025(preview)
        with self.assertRaises(ValueError): preparar_isn_empresa_2025(date(2026,1,1))
        with self.assertRaises(ValueError): aplicar_isn_empresa_2025(dict(preview,periodo='2026-01-01'))

    def test_comando_preview_y_prohibicion_de_aceptar_diferencias(self):
        out=StringIO()
        call_command('materializar_isn',periodo='2025-01',empresa_2025=True,stdout=out)
        self.assertIn('DRY-RUN',out.getvalue());self.assertFalse(ExpedienteISN.objects.exists())
        with self.assertRaises(CommandError):
            call_command('materializar_isn',periodo='2025-01',empresa_2025=True,aceptar_diferencia_isn=True)
        call_command('materializar_isn',periodo='2025-01',empresa_2025=True,apply=True,stdout=StringIO())
        self.assertEqual(ExpedienteISN.objects.count(),1)

    def test_aguinaldo_acumulado_no_repite_exencion_en_febrero(self):
        self.crear_nomina(date(2025,2,1),date(2025,2,15),[('1','Sueldo','10000','0')])
        self.crear_nomina(date(2025,2,16),date(2025,2,28),[('1','Sueldo','10000','0')])
        self.crear_nomina(date(2025,2,20),date(2025,2,20),[('24','Aguinaldo','2000','0')],True)
        self.cfdi.xml_raw=self.cfdi.xml_raw.replace('202501','202502').replace('526.63','528.00')
        self.cfdi.save(update_fields=['xml_raw'])
        self.assertEqual(preparar_isn_empresa_2025(date(2025,2,1))['base_calculada'],'22000.00')

    def test_tipo_fiscal_exento_incompatible_se_rechaza(self):
        f=self.lineas[2][1]
        f.xml_raw=f.xml_raw.replace('TipoPercepcion="003"','TipoPercepcion="001"')
        f.save(update_fields=['xml_raw'])
        with self.assertRaisesMessage(ValueError,'tipo fiscal'): preparar_isn_empresa_2025(self.periodo)
