from datetime import date, datetime, timezone as tz
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase

from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from rrhh.models import Empleado
from sat_client.models import CfdiDescargado
from reportes.models import AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual
from reportes.services_planeacion_personal import budget_policy, build_personnel_plan, parse_payroll, RFC
from orquestacion.services.pointdailysale_guard import scan_pointdailysale_usage
from ventas.services.sales_canonical_source import OFFICIAL_POINT_SOURCE, RECENT_POINT_SOURCE


def payroll_xml(end='2026-08-31', kind='O', amount='100'):
    return ('<c:Comprobante xmlns:c="http://www.sat.gob.mx/cfd/4" '
            'xmlns:n="http://www.sat.gob.mx/nomina12"><c:Complemento>'
            f'<n:Nomina FechaFinalPago="{end}" TipoNomina="{kind}" '
            f'TotalPercepciones="{amount}"/></c:Complemento></c:Comprobante>')


class BudgetPolicyTests(SimpleTestCase):
    def test_personnel_plan_respects_sales_read_boundary(self):
        scan = scan_pointdailysale_usage(base_dir=Path(__file__).resolve().parents[1])
        self.assertEqual([v for v in scan.violations
                          if v.relative_path == 'reportes/services_planeacion_personal.py'], [])

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
                                         sale_date=date(2026, month, 1), total_amount=amount,
                                         source_endpoint=OFFICIAL_POINT_SOURCE)

    def test_official_sales_preserve_tax_amount_dates_and_branch_averages(self, _):
        other = PointBranch.objects.create(external_id='plan-b', name='Sucursal B')
        for month in (6, 7, 8):
            PointDailySale.objects.create(
                branch=other, product=self.product, sale_date=date(2026, month, 28),
                total_amount=116, gross_amount=100, source_endpoint=OFFICIAL_POINT_SOURCE)
        PointDailySale.objects.create(
            branch=self.branch, product=self.product, sale_date=date(2026, 8, 31),
            total_amount=116, gross_amount=100, source_endpoint=OFFICIAL_POINT_SOURCE)
        PointDailySale.objects.create(
            branch=other, product=self.product, sale_date=date(2025, 12, 31),
            total_amount=9000, source_endpoint=OFFICIAL_POINT_SOURCE)
        # Category and unknown staging rows must not inflate the official report.
        for source, day in ((RECENT_POINT_SOURCE, 2), ('unknown', 3)):
            PointDailySale.objects.create(
                branch=self.branch, product=self.product, sale_date=date(2026, 8, day),
                total_amount=9000, source_endpoint=source)
        result = build_personnel_plan()
        self.assertEqual(result['sales_total'], D(1364))
        self.assertIsNone(result['months'][0]['sales'])
        self.assertEqual(result['months'][-1]['sales'], D(632))
        self.assertEqual(result['months'][-1]['target'], D(158))
        branches = {r['branch_id']: r for r in result['branches']}
        self.assertEqual(branches[self.branch.pk]['recent_average'], D('338.67'))
        self.assertEqual(branches[other.pk]['recent_average'], D(116))
        self.assertEqual(sum(r['amount'] for r in result['branches']), result['sales_total'])

    def test_legacy_only_month_remains_missing(self, _):
        PointDailySale.objects.filter(sale_date__month=8).update(source_endpoint=RECENT_POINT_SOURCE)
        result = build_personnel_plan()
        self.assertIsNone(result['months'][-1]['sales'])
        self.assertIsNone(result['months'][-1]['target'])
        self.assertIsNone(result['policy'])
        self.assertIsNone(result['branches'][0]['recent_average'])

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

    def test_unrecognized_service_is_not_silently_treated_as_zero(self, _):
        invoice = self.invoice('unknown-service',
            '<c:Comprobante xmlns:c="http://www.sat.gob.mx/cfd/4"><c:Conceptos>'
            '<c:Concepto Descripcion="SERVICIO NUEVO" Importe="100"/>'
            '</c:Conceptos></c:Comprobante>',
            tipo_cfdi='recibido', tipo_comprobante='I', rfc_emisor='EDENRED',
            nombre_emisor='EDENRED MEXICO', rfc_receptor=RFC,
            fecha_emision=datetime(2026, 8, 15, tzinfo=tz.utc))
        result = build_personnel_plan()
        self.assertEqual(result['unparsed'], [invoice.pk])
        self.assertIsNone(result['months'][-1]['documented'])

    def test_zero_despensa_complement_is_not_a_service_charge(self, _):
        self.invoice('informative',
            '<c:Comprobante xmlns:c="http://www.sat.gob.mx/cfd/4" '
            'xmlns:v="http://www.sat.gob.mx/valesdedespensa"><c:Conceptos>'
            '<c:Concepto Descripcion="S E R V I C I O" Importe="0.01"/>'
            '</c:Conceptos><c:Complemento><v:ValesDeDespensa/></c:Complemento></c:Comprobante>',
            tipo_cfdi='recibido', tipo_comprobante='I', rfc_emisor='EDENRED', total=0,
            nombre_emisor='EDENRED MEXICO', rfc_receptor=RFC,
            fecha_emision=datetime(2026, 8, 15, tzinfo=tz.utc))
        self.assertEqual(build_personnel_plan()['unparsed'], [])

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
        self.assertContains(response, '2026-08-01,400.00,100.00,108.00')
        data = self.client.get(path + '?formato=json').json()
        self.assertEqual(data['sales_total'], '900.00')
        self.assertEqual(data['months'][-1]['sales'], '400.00')
        reader = get_user_model().objects.create_user(username='without-report-access')
        self.client.force_login(reader)
        self.assertEqual(self.client.get(path).status_code, 403)
        self.assertEqual(self.client.get(path + '?formato=csv').status_code, 403)
