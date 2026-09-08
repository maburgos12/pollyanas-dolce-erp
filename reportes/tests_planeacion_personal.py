from datetime import date, datetime, timezone as tz
from decimal import Decimal as D
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase

from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from rrhh.models import Empleado
from sat_client.models import CfdiDescargado
from reportes.models import AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual
from reportes.services_planeacion_personal import budget_policy, build_personnel_plan, parse_payroll, RFC


def payroll_xml(end='2026-08-31', kind='O', amount='100'):
    return ('<c:Comprobante xmlns:c="http://www.sat.gob.mx/cfd/4" '
            'xmlns:n="http://www.sat.gob.mx/nomina12"><c:Complemento>'
            f'<n:Nomina FechaFinalPago="{end}" TipoNomina="{kind}" '
            f'TotalPercepciones="{amount}"/></c:Complemento></c:Comprobante>')


class BudgetPolicyTests(SimpleTestCase):
    def test_real_recent_quarter(self):
        result = budget_policy([D('3506661.80'), D('3171945.93'), D('3455069.62')])
        self.assertEqual(result['average'], D('3377892.45'))
        self.assertEqual(result['target'], D('844473.11'))
        self.assertEqual(result['ceiling'], D('912030.96'))

    def test_low_month_limits_fixed_budget(self):
        self.assertEqual(budget_policy([D(100), D(200), D(300)])['target'], D('27'))

    def test_missing_month_never_becomes_zero(self):
        self.assertIsNone(budget_policy([D(100), None, D(300)]))
        self.assertIsNone(budget_policy([D(100), D(200)]))

    def test_extraordinary_classified_by_period_not_emission(self):
        month, amount, kind, _ = parse_payroll(payroll_xml('2026-05-15', 'E', '178281.35'))
        self.assertEqual((month, amount, kind), (date(2026, 5, 1), D('178281.35'), 'E'))


@patch('reportes.services_planeacion_personal.timezone.localdate', return_value=date(2026, 9, 8))
class PersonnelPlanTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(external_id='test-plan', name='Sucursal A')
        self.product = PointProduct.objects.create(external_id='test-plan', name='Pastel')
        for month, amount in [(6, 300), (7, 200), (8, 400), (9, 9000)]:
            PointDailySale.objects.create(branch=self.branch, product=self.product,
                                         sale_date=date(2026, month, 1), total_amount=amount)

    def invoice(self, uuid, xml, **kwargs):
        values = dict(uuid=uuid, rfc_emisor=RFC, rfc_receptor='PERSONA', subtotal=100,
                      total=100, tipo_cfdi='emitido', tipo_comprobante='N', xml_raw=xml,
                      fecha_emision=datetime(2026, 9, 2, tzinfo=tz.utc))
        values.update(kwargs)
        return CfdiDescargado.objects.create(**values)

    def test_future_cut_excludes_partial_month_and_keeps_current_staff(self, _):
        Empleado.objects.create(codigo='ACTIVE', nombre='Activa', activo=True)
        Empleado.objects.create(codigo='LEFT', nombre='Baja', activo=False)
        self.invoice('ordinary', payroll_xml())
        self.invoice('canceled', payroll_xml(), estatus='cancelado')
        self.invoice('other-entity', payroll_xml(), rfc_emisor='OTHER')
        result = build_personnel_plan(date(2026, 12, 1))
        self.assertEqual(result['cutoff'], date(2026, 8, 31))
        self.assertEqual(result['sales_total'], D(900))
        self.assertEqual(result['people'], 1)
        self.assertEqual(result['months'][-1]['ordinary'], D(100))
        self.assertIsNone(result['months'][-1]['imss'])
        self.assertFalse(result['months'][-1]['reconciled_components'])
        self.assertEqual(result['branches'][0]['recent_average'], D(300))

    def test_edenred_balance_is_not_added_again(self, _):
        def xml(desc):
            return ('<c:Comprobante xmlns:c="http://www.sat.gob.mx/cfd/4">'
                    f'<c:Conceptos><c:Concepto Descripcion="{desc}" Importe="100"/>'
                    '</c:Conceptos></c:Comprobante>')
        for name in ('COMISION', 'MANEJO DE CUENTA', 'TARJETAS REPOSICION', 'CARGO POR ENVIO', 'CARGA DE SALDO'):
            self.invoice(name, xml(name), tipo_cfdi='recibido', tipo_comprobante='I',
                         rfc_emisor='EDENRED', nombre_emisor='EDENRED MEXICO',
                         rfc_receptor=RFC, total=116,
                         fecha_emision=datetime(2026, 8, 15, tzinfo=tz.utc))
        self.assertEqual(build_personnel_plan()['months'][-1]['fees'], D(464))

    def test_isn_uses_obligation_period(self, _):
        xml = ('<c:Comprobante xmlns:c="http://www.sat.gob.mx/cfd/4"><c:Conceptos>'
               '<c:Concepto Descripcion="Empresarial decl.Nomina" '
               'NoIdentificacion="202607 2-003" Importe="16737"/>'
               '</c:Conceptos></c:Comprobante>')
        self.invoice('isn', xml, tipo_cfdi='recibido', tipo_comprobante='I',
                     rfc_emisor='GES8101015I7', rfc_receptor=RFC)
        result = build_personnel_plan()
        self.assertEqual(result['months'][6]['isn'], D(16737))
        self.assertIsNone(result['months'][7]['isn'])

    def test_sipare_corporate_not_department_double_count(self, _):
        for area_code in ('nomina', 'gastos-venta'):
            area, _ = AreaPresupuesto.objects.get_or_create(codigo=area_code, defaults={'nombre': area_code})
            rubro = RubroPresupuesto.objects.create(area=area, concepto='Infonavit')
            LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=date(2026, 8, 1),
                monto_real=200, fuente_real='AUTO:SIPARE',
                metadata={'cedula_imss': {'registro_patronal': ''},
                          'cedula_imss_documento': {'registro_patronal': 'E52-40157-10-0'}})
        self.assertEqual(build_personnel_plan()['months'][-1]['rcv'], D(200))

    def test_page_permissions_csv_and_read_only(self, _):
        path = '/reportes/planeacion-personal/'
        self.assertEqual(self.client.get(path).status_code, 302)
        user = get_user_model().objects.create_superuser(username='plan-admin', password='local-test')
        self.client.force_login(user)
        self.assertEqual(self.client.post(path).status_code, 405)
        self.assertEqual(self.client.get(path + '?mes=no').status_code, 400)
        self.assertContains(self.client.get(path), 'Presupuesto de personal')
        response = self.client.get(path + '?formato=csv')
        self.assertContains(response, 'Ventas Point')
        self.assertNotContains(response, 'local-test')
        reader = get_user_model().objects.create_user(username='without-report-access')
        self.client.force_login(reader)
        self.assertEqual(self.client.get(path).status_code, 403)
        self.assertEqual(self.client.get(path + '?formato=csv').status_code, 403)
