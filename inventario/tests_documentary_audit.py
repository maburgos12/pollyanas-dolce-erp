from datetime import date, timedelta
from decimal import Decimal
from hashlib import sha256
from io import StringIO
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointProductionLine, PointTransferLine
from recetas.models import LineaReceta, Receta, VentaHistorica
from .models import InsumoAuditReview, InsumoAuditSnapshot, MovimientoInventario
from .services_documentary_audit import build_report, bounds, convert, refresh_report
from pos_bridge.services.inventory_baseline import point_transfer_origin_exit_hash


class DocumentaryAuditTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.period = (timezone.localdate().replace(day=1)-timedelta(days=1)).replace(day=1)
        cls.start, cls.end = bounds(cls.period)
        cls.gram = UnidadMedida.objects.create(codigo='g', nombre='Gramo')
        cls.piece = UnidadMedida.objects.create(codigo='pza', nombre='Pieza')
        cls.item = Insumo.objects.create(nombre='HARINA', codigo_point='H1', unidad_base=cls.gram)
        cls.origin = PointBranch.objects.create(external_id='audit-a', name='Almacen')
        cls.destination = PointBranch.objects.create(external_id='audit-c', name='CEDIS')

    def row(self, payload, loc='ALMACEN'):
        return next(r for r in payload['rows'] if r['insumo_id'] == self.item.pk and r['location'] == loc)

    def move(self, quantity, kind, key=None, location='ALMACEN_1', **kwargs):
        return MovimientoInventario.objects.create(insumo=self.item, cantidad=quantity, tipo=kind,
            fecha=self.start+timedelta(days=2), almacen=location, source_hash=key, **kwargs)

    def purchase(self, qty='150', unit='KG', key='purchase'):
        cost = CostoInsumo.objects.create(insumo=self.item, fecha=self.period, costo_unitario=1,
            source_hash=key, raw={'source': 'POINT_COMPRAS_HISTORICAS', 'purchase_id': '100',
            'folio': 'A100', 'branch': 'Almacen', 'article_name': 'HARINA', 'quantity': qty, 'unit': unit})
        self.move(150000, 'ENTRADA', sha256(f'point_purchase|{key}'.encode()).hexdigest(), trazabilidad={'costo_insumo_id': cost.pk})
        return cost

    def test_purchase_counted_once_and_physical_unknown(self):
        self.purchase()
        payload = build_report(self.period)
        row = self.row(payload)
        self.assertEqual(Decimal(row['totals']['purchase']), 150000)
        self.assertEqual(Decimal(row['totals']['other_in']), 0)
        self.assertEqual(payload['coverage']['purchase_quantity_matches'], 1)
        self.assertIsNone(row['physical_consumption'])
        self.assertIsNone(row['difference'])
        self.assertIsNone(row['inventory_now']['quantity'])
        self.assertEqual(MovimientoInventario.objects.count(), 1)
        self.assertEqual(InsumoAuditSnapshot.objects.count(), 0)

    def test_presentation_and_duplicate_not_silently_reconciled(self):
        self.purchase(unit='FARDO')
        self.purchase(key='revised')
        row = self.row(build_report(self.period))
        codes = {i['code'] for i in row['issues']}
        self.assertIn('UNIDAD_NO_CONVERTIBLE', codes)
        self.assertIn('COMPRA_REVISION_O_DUPLICADO', codes)
        self.assertEqual(Decimal(row['ledger_net']), 300000)  # observed ledger, not certified receipts
        self.assertIsNone(convert(15, 'FARDO', 'kg'))
        self.assertIsNone(convert('NaN', 'kg', 'g'))
        self.assertEqual(convert(150, 'KG', 'g'), 150000)

    def transfer(self, **overrides):
        data = dict(origin_branch=self.origin, destination_branch=self.destination, insumo=self.item,
            transfer_external_id='T1', detail_external_id='L1', source_hash='transfer',
            registered_at=self.start+timedelta(days=2), sent_at=self.start+timedelta(days=2),
            received_at=self.start+timedelta(days=2, minutes=20), item_name='HARINA', item_code='H1',
            unit='KG', sent_quantity=20, received_quantity=18, is_insumo=True, is_received=True)
        data.update(overrides)
        return PointTransferLine.objects.create(**data)

    def test_transfer_difference_not_consumption_or_double_entry(self):
        self.transfer()
        self.move(20000, 'SALIDA', point_transfer_origin_exit_hash('transfer'))
        self.move(18000, 'ENTRADA', 'transfer', 'CUARTO_FRIO')
        payload = build_report(self.period)
        self.assertEqual(Decimal(self.row(payload)['totals']['transfer_out']), 20000)
        destination = self.row(payload, 'CEDIS')
        self.assertEqual(Decimal(destination['totals']['transfer_in']), 18000)
        self.assertEqual(Decimal(destination['totals']['other_in']), 0)
        self.assertIn('DIFERENCIA_TRANSFERENCIA', {i['code'] for i in destination['issues']})
        self.assertIsNone(destination['physical_consumption'])

    def test_cross_cut_and_pending_receipt(self):
        self.transfer(sent_at=self.start-timedelta(minutes=5), received_quantity=20)
        self.move(20000, 'ENTRADA', 'transfer', 'CUARTO_FRIO')
        payload = build_report(self.period)
        self.assertEqual(Decimal(self.row(payload)['totals']['transfer_out']), 0)
        self.assertIn('TRANSFERENCIA_CRUZA_CORTE', {i['code'] for i in self.row(payload, 'CEDIS')['issues']})
        PointTransferLine.objects.update(is_received=False)
        destination = self.row(build_report(self.period), 'CEDIS')
        self.assertEqual(Decimal(destination['totals']['transfer_in']), 0)
        self.assertEqual(Decimal(destination['totals']['other_in']), 20000)

    def test_superseded_and_cancelled_never_double_effect(self):
        self.transfer(is_current_snapshot=False)
        self.transfer(source_hash='current', is_cancelled=True)
        self.move(-500, 'AJUSTE')
        payload = build_report(self.period)
        self.assertEqual(payload['coverage']['transfer_superseded'], 1)
        self.assertEqual(Decimal(self.row(payload)['ledger_net']), -500)
        self.assertEqual(payload['coverage']['transfer_pairs_quantity_verified'], 0)

    def recipe(self):
        recipe = Receta.objects.create(nombre='Pan de prueba', rendimiento_cantidad=2, rendimiento_unidad=self.piece)
        LineaReceta.objects.create(receta=recipe, insumo=self.item, insumo_texto='HARINA', cantidad=100, unidad=self.gram, match_status=LineaReceta.STATUS_AUTO)
        return recipe

    def test_bom_provenance_and_yield_are_not_physical(self):
        recipe = self.recipe()
        PointProductionLine.objects.create(branch=self.destination, receta=recipe, production_external_id='P1',
            detail_external_id='1', source_hash='prod', production_date=self.period, item_name=recipe.nombre,
            produced_quantity=10, unit='pza')
        self.move(1000, 'CONSUMO', sha256(f'BOM|P1|{self.item.pk}'.encode()).hexdigest(), 'CUARTO_FRIO')
        self.move(25, 'CONSUMO', 'unknown', 'CUARTO_FRIO')
        row = self.row(build_report(self.period), 'CEDIS')
        self.assertEqual(Decimal(row['totals']['calculated']), 1000)
        self.assertEqual(Decimal(row['theoretical_current']), 500)
        self.assertEqual(Decimal(row['totals']['unverified']), 25)
        self.assertIsNone(row['difference'])

    def test_sale_calculation_reused_without_writing_consumption(self):
        recipe = self.recipe()
        VentaHistorica.objects.create(receta=recipe, fecha=self.period, cantidad=3)
        key = sha256(f'VENTA_SERV|{self.period}|{recipe.pk}|{self.item.pk}'.encode()).hexdigest()
        self.move(300, 'CONSUMO', key, 'CUARTO_FRIO')
        row = self.row(build_report(self.period), 'CEDIS')
        self.assertEqual(Decimal(row['totals']['calculated_sale']), 300)
        self.assertEqual(MovimientoInventario.objects.count(), 1)

    def test_revision_is_idempotent_and_preserves_old_evidence(self):
        first = refresh_report(self.period)
        self.assertEqual(refresh_report(self.period).pk, first.pk)
        self.move(25, 'AJUSTE')
        second = refresh_report(self.period)
        self.assertNotEqual(second.pk, first.pk)
        self.assertEqual(Decimal(self.row(first.payload)['ledger_net']), 0)
        with self.assertRaises(ValidationError):
            first.save()
        output = StringIO()
        call_command('auditar_compras_produccion', mes=self.period.strftime('%Y-%m'), stdout=output)
        self.assertEqual(InsumoAuditSnapshot.objects.count(), 2)
        self.assertIn('NO_DETERMINADA', output.getvalue())


@override_settings(STORAGES={'default': {'BACKEND': 'django.core.files.storage.FileSystemStorage'}, 'staticfiles': {'BACKEND': 'django.contrib.staticfiles.storage.StaticFilesStorage'}})
class DocumentaryAuditViewTests(TestCase):
    setUpTestData = classmethod(DocumentaryAuditTests.setUpTestData.__func__)
    move = DocumentaryAuditTests.move

    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(username='audit-admin', password='synthetic-only')
        self.other = get_user_model().objects.create_superuser(username='audit-other', password='synthetic-only')
        self.snapshot = refresh_report(self.period)
        self.key = f'{self.item.pk}:ALMACEN'
        self.url = reverse('reportes:auditoria_documental')
        self.review_url = reverse('reportes:auditoria_documental_revision', args=[self.snapshot.pk])
        self.client.force_login(self.admin)

    def post(self, action='EXPLICAR', **kwargs):
        data = dict(caso=self.key, accion=action, nota='Documento sintético de revisión.', request_id=str(uuid4()))
        data.update(kwargs)
        return self.client.post(self.review_url, data, HTTP_ACCEPT='application/json')

    def test_html_export_and_unknown_case(self):
        response = self.client.get(self.url, {'mes': self.period.strftime('%Y-%m'), 'caso': self.key})
        self.assertContains(response, 'Consumo físico inconcluso')
        response = self.client.get(self.url, {'mes': self.period.strftime('%Y-%m'), 'ubicacion': 'CEDIS', 'export': 'json'})
        self.assertEqual(len(response.json()['rows']), 1)
        self.assertEqual(response.json()['rows'][0]['location'], 'CEDIS')
        self.assertEqual(self.client.get(self.url, {'mes': 'bad'}).status_code, 400)
        self.assertEqual(self.client.get(self.url, {'caso': 'invalid'}).status_code, 404)

    def test_no_self_approval_and_idempotent_retry(self):
        req = str(uuid4())
        self.assertEqual(self.post(request_id=req).status_code, 200)
        self.assertEqual(self.post(request_id=req).status_code, 200)
        explanation = InsumoAuditReview.objects.get()
        self.assertEqual(self.post('APROBAR', explicacion=str(explanation.pk)).status_code, 409)
        self.client.force_login(self.other)
        self.assertEqual(self.post('APROBAR', explicacion=str(explanation.pk)).status_code, 200)
        self.assertEqual(InsumoAuditReview.objects.count(), 2)

    def test_changed_evidence_rejects_old_form(self):
        self.move(1, 'AJUSTE')
        refresh_report(self.period)
        self.assertEqual(self.post().status_code, 409)
        self.assertFalse(InsumoAuditReview.objects.exists())
        old = self.client.get(self.url, {'mes': self.period.strftime('%Y-%m'), 'revision': self.snapshot.pk, 'caso': self.key})
        self.assertContains(old, 'Revisión histórica')
        self.assertNotContains(old, 'data-async-action')

    def test_permissions_and_validation(self):
        self.assertEqual(self.post(nota='corta').status_code, 400)
        self.assertEqual(self.post(request_id='invalid').status_code, 400)
        user = get_user_model().objects.create_user(username='audit-denied')
        self.client.force_login(user)
        with patch('reportes.views_documentary_audit.can_view_reportes', return_value=False):
            self.assertEqual(self.client.get(self.url).status_code, 403)
            self.assertEqual(self.post().status_code, 403)
        with patch('reportes.views_documentary_audit.can_view_reportes', return_value=True), patch('reportes.views_documentary_audit.can_manage_module', return_value=False):
            self.assertEqual(self.post().status_code, 403)

    def test_csv_preserves_negative_numbers_without_allowing_formulas(self):
        import csv
        self.move(-25, 'AJUSTE')
        refresh_report(self.period)
        response = self.client.get(self.url, {'mes': self.period.strftime('%Y-%m'), 'export': 'csv', 'ubicacion': 'ALMACEN'})
        row = next(csv.DictReader(StringIO(response.content.decode('utf-8-sig'))))
        self.assertEqual(Decimal(row['ajustes']), -25)
        self.assertEqual(Decimal(row['neto_libro']), -25)
        from reportes.views_documentary_audit import _csv_cell
        self.assertEqual(_csv_cell('-1+SUM(A1:A2)'), "'-1+SUM(A1:A2)")

    def test_csv_formula_injection_and_full_filtered_export(self):
        Insumo.objects.filter(pk=self.item.pk).update(nombre='=HYPERLINK("bad")')
        refresh_report(self.period)
        response = self.client.get(self.url, {'mes': self.period.strftime('%Y-%m'), 'export': 'csv', 'ubicacion': 'ALMACEN'})
        self.assertIn("'=HYPERLINK", response.content.decode())
        self.assertNotIn('CEDIS', response.content.decode())


class DocumentaryAuditTaskTests(TestCase):
    def test_schedule_and_previous_month_refresh(self):
        from django_celery_beat.models import PeriodicTask
        from inventario.tasks import auditar_compras_produccion
        from types import SimpleNamespace
        task = PeriodicTask.objects.get(task='inventario.auditar_compras_produccion')
        self.assertTrue(task.enabled)
        self.assertEqual(task.crontab.hour, '9')
        self.assertEqual(str(task.crontab.timezone), 'America/Mazatlan')
        with patch('inventario.services_documentary_audit.refresh_report', return_value=SimpleNamespace(pk=1)) as refresh:
            result = auditar_compras_produccion.run()
        self.assertEqual(len(result), 2)
        current = timezone.localdate().replace(day=1)
        self.assertEqual([c.args[0] for c in refresh.call_args_list], [(current-timedelta(days=1)).replace(day=1), current])

    def test_latest_running_job_defers_but_old_unclosed_is_manifested(self):
        from celery.exceptions import Retry
        from pos_bridge.models import PointSyncJob
        from inventario.tasks import auditar_compras_produccion
        from types import SimpleNamespace
        job = PointSyncJob.objects.create(job_type='inventory', status='RUNNING')
        with patch.object(auditar_compras_produccion, 'retry', side_effect=Retry), patch('inventario.services_documentary_audit.refresh_report') as refresh:
            with self.assertRaises(Retry):
                auditar_compras_produccion.run()
            refresh.assert_not_called()
        PointSyncJob.objects.create(job_type='inventory', status='SUCCESS', finished_at=timezone.now())
        with patch('inventario.services_documentary_audit.refresh_report', return_value=SimpleNamespace(pk=1)):
            self.assertEqual(len(auditar_compras_produccion.run()), 2)
        payload = build_report(timezone.localdate().replace(day=1))
        self.assertIn(job.pk, [j['id'] for j in payload['coverage']['unfinished_source_jobs']])
