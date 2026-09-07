from decimal import Decimal
from datetime import timedelta
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from django.utils import timezone
from core.models import Sucursal
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointProduct, PointSyncJob, PointInventorySnapshot, PointInsumoInventorySnapshot
from inventario.models_conteos import ConteoSucursal, LineaConteoSucursal
from inventario import conteos_point


class PointRawReferenceTests(SimpleTestCase):
    def test_hidden_columns_preserve_code_quantity_and_unit(self):
        raw = {'headers': ['Código', 'Producto', 'Cantidad', 'Unidad', 'Costo Unitario', 'Costo Total', 'Último Movimiento', 'Opciones'], 'row': ['1048', '4358', 'Dot Cake', 'Vasos', '0', 'PZA', '7', '0', '', 'false']}
        self.assertEqual(conteos_point.leer_fila_point(raw, codigo='4358'), (Decimal('0'), 'PZA'))

    def test_blank_is_not_zero_and_wrong_code_rejected(self):
        raw = {'headers': ['Código', 'Producto', 'Cantidad', 'Unidad'], 'row': ['4358', 'Dot Cake', '', 'PZA']}
        self.assertIsNone(conteos_point.leer_fila_point(raw, codigo='4358'))
        raw['row'][2] = '2'
        self.assertIsNone(conteos_point.leer_fila_point(raw, codigo='8734'))

    def test_nan_and_misaligned_unknown_rows_rejected(self):
        for value in ['NaN', 'Infinity', '1,2', '']:
            raw = {'headers': ['Código', 'Producto', 'Cantidad', 'Unidad'], 'row': ['4358', 'Dot Cake', value, 'PZA']}
            self.assertIsNone(conteos_point.leer_fila_point(raw, codigo='4358'))
        self.assertIsNone(conteos_point.leer_fila_point({'headers': [], 'row': ['1', '2']}, codigo='1'))

    def test_only_unit_aliases_no_conversion(self):
        self.assertTrue(conteos_point.misma_unidad('piezas', 'PZA'))
        self.assertTrue(conteos_point.misma_unidad('kg', 'kilogramos'))
        self.assertFalse(conteos_point.misma_unidad('caja', 'PZA'))
        self.assertFalse(conteos_point.misma_unidad('kg', 'g'))


class PointCountReferenceIntegrationTests(TestCase):
    def setUp(self):
        self.cutoff = timezone.now()
        self.user = get_user_model().objects.create_user('point-count-reference')
        self.branch = Sucursal.objects.create(codigo='REF1', nombre='Referencia exacta')
        self.point_branch = PointBranch.objects.create(external_id='REF1', name='Referencia exacta', erp_branch=self.branch)
        self.product = PointProduct.objects.create(external_id='99001', sku='REF-P', name='Producto referencia')
        unit = UnidadMedida.objects.create(codigo='g', nombre='Gramo', tipo=UnidadMedida.TIPO_MASA, factor_to_base=1)
        self.insumo = Insumo.objects.create(codigo_point='REF-I', nombre='Insumo referencia', categoria='PRUEBA', unidad_base=unit)
        self.count = ConteoSucursal.objects.create(sucursal=self.branch, responsable=self.user, creado_por=self.user,
            fecha=self.cutoff.date(), titulo='Referencia', request_id=uuid4(), payload_hash='a' * 64, iniciado_en=self.cutoff)
        self.product_line = LineaConteoSucursal.objects.create(conteo=self.count, producto=self.product,
            codigo='REF-P', nombre=self.product.name, unidad='pza', fuente_unidad='Evidencia Point')
        self.insumo_line = LineaConteoSucursal.objects.create(conteo=self.count, insumo=self.insumo,
            codigo='REF-I', nombre=self.insumo.nombre, unidad='g', fuente_unidad='Evidencia Point')
        self.job = self.make_job()

    def make_job(self, *, minutes=-10, status=PointSyncJob.STATUS_SUCCESS, job_type=PointSyncJob.JOB_TYPE_INVENTORY):
        return PointSyncJob.objects.create(job_type=job_type, status=status,
            started_at=self.cutoff + timedelta(minutes=minutes - 1), finished_at=self.cutoff + timedelta(minutes=minutes))

    def snapshot(self, *, insumo=False, job=None, branch=None, amount='4', raw=None, captured_at=None):
        job = job or self.job
        code, unit = ('REF-I', 'g') if insumo else ('REF-P', 'PZA')
        payload = raw if raw is not None else {'headers': ['Código', 'Producto', 'Cantidad', 'Unidad'],
            'row': [code, 'Referencia', amount, unit]}
        kwargs = dict(branch=branch or self.point_branch, sync_job=job,
            captured_at=captured_at or job.finished_at, raw_payload=payload)
        if insumo:
            return PointInsumoInventorySnapshot.objects.create(**kwargs, insumo=self.insumo, point_code=code,
                point_name=self.insumo.nombre, point_quantity=amount, point_unit=unit, quantity_base=amount)
        return PointInventorySnapshot.objects.create(**kwargs, product=self.product, stock=amount)

    def reference(self):
        return conteos_point.referencia_conteo(self.count, now=self.cutoff + timedelta(hours=2))

    def test_exact_branch_mixed_cycle_and_evidenced_zero(self):
        product = self.snapshot(amount='0')
        insumo = self.snapshot(insumo=True, amount='3')
        other = PointBranch.objects.create(external_id='OTHER', name='Otra sucursal')
        self.snapshot(branch=other, amount='99', job=self.make_job(minutes=-1))
        result = self.reference()
        self.assertEqual(result['estado'], 'REFERENCIA')
        self.assertFalse(result['corte_verificado'])
        self.assertEqual(result['sucursal_point_id'], self.point_branch.pk)
        for line, snapshot, amount in [(self.product_line, product, '0'), (self.insumo_line, insumo, '3')]:
            reading = result['lineas'][str(line.pk)]
            self.assertEqual(Decimal(reading['cantidad']), Decimal(amount))
            self.assertEqual(reading['snapshot_id'], snapshot.pk)
            self.assertEqual(reading['sync_job_id'], self.job.pk)

    def test_missing_insumo_in_selected_cycle_never_borrows_another_cycle(self):
        self.snapshot()
        self.snapshot(insumo=True, job=self.make_job(minutes=-2), amount='91')
        result = self.reference()
        self.assertEqual(result['estado'], 'PARCIAL')
        reading = result['lineas'][str(self.insumo_line.pk)]
        self.assertIsNone(reading['cantidad'])
        self.assertEqual(reading['sync_job_id'], self.job.pk)

    def test_malformed_unit_identity_and_amount_mismatch_are_missing_not_zero(self):
        snapshot = self.snapshot(amount='0')
        self.snapshot(insumo=True)
        invalid_rows = [None, {}, {'headers': [], 'row': []}]
        for code, amount, unit in [('REF-P', '', 'PZA'), ('OTHER', '0', 'PZA'), ('REF-P', '0', 'kg'),
                                   ('REF-P', '0', ''), ('REF-P', 'NaN', 'PZA'), ('REF-P', '1', 'PZA')]:
            invalid_rows.append({'headers': ['Código', 'Producto', 'Cantidad', 'Unidad'], 'row': [code, 'Producto', amount, unit]})
        for raw in invalid_rows:
            with self.subTest(raw=raw):
                snapshot.raw_payload = raw if raw is not None else []
                snapshot.save(update_fields=['raw_payload'])
                result = self.reference()
                self.assertEqual(result['estado'], 'PARCIAL')
                self.assertIsNone(result['lineas'][str(self.product_line.pk)]['cantidad'])

    def test_post_start_capture_or_completion_excluded_even_when_read_later(self):
        original = self.snapshot()
        self.snapshot(insumo=True)
        self.snapshot(job=self.make_job(minutes=1), amount='81')
        self.snapshot(job=self.make_job(minutes=-1), captured_at=self.cutoff + timedelta(seconds=1), amount='82')
        late_finish = self.make_job(minutes=2)
        self.snapshot(job=late_finish, captured_at=self.cutoff - timedelta(minutes=1), amount='83')
        result = self.reference()
        self.assertEqual(result['lineas'][str(self.product_line.pk)]['snapshot_id'], original.pk)

    def test_insumo_raw_identity_and_unit_must_match_count_line(self):
        self.snapshot()
        snapshot = self.snapshot(insumo=True, amount='0')
        for code, unit, amount in [('WRONG', 'g', '0'), ('REF-I', 'kg', '0'), ('REF-I', 'g', '')]:
            with self.subTest(code=code, unit=unit, amount=amount):
                snapshot.raw_payload = {'headers': ['Código', 'Producto', 'Cantidad', 'Unidad'],
                    'row': [code, 'Insumo', amount, unit]}
                snapshot.save(update_fields=['raw_payload'])
                result = self.reference()
                self.assertEqual(result['estado'], 'PARCIAL')
                self.assertIsNone(result['lineas'][str(self.insumo_line.pk)]['cantidad'])

    def test_failed_partial_running_and_other_job_types_are_not_reference_cycles(self):
        original = self.snapshot()
        self.snapshot(insumo=True)
        for status in [PointSyncJob.STATUS_FAILED, PointSyncJob.STATUS_PARTIAL, PointSyncJob.STATUS_RUNNING, PointSyncJob.STATUS_PENDING]:
            self.snapshot(job=self.make_job(minutes=-1, status=status), amount='88')
        self.snapshot(job=self.make_job(minutes=-1, job_type=PointSyncJob.JOB_TYPE_SALES), amount='89')
        self.assertEqual(self.reference()['lineas'][str(self.product_line.pk)]['snapshot_id'], original.pk)
        self.job.status = PointSyncJob.STATUS_FAILED
        self.job.save(update_fields=['status'])
        result = self.reference()
        self.assertEqual(result['estado'], 'SIN_REFERENCIA')
        self.assertTrue(all(row['cantidad'] is None for row in result['lineas'].values()))

    def test_duplicate_product_snapshots_are_ambiguous(self):
        self.snapshot()
        self.snapshot(amount='5')
        self.snapshot(insumo=True)
        result = self.reference()
        self.assertEqual(result['estado'], 'PARCIAL')
        self.assertIsNone(result['lineas'][str(self.product_line.pk)]['cantidad'])

    def test_two_active_point_branches_for_same_erp_branch_are_ambiguous(self):
        self.snapshot()
        PointBranch.objects.create(external_id='REF-ALIAS', name='Referencia duplicada', erp_branch=self.branch)
        result = self.reference()
        self.assertEqual(result['estado'], 'SIN_REFERENCIA')
        self.assertFalse(result['corte_verificado'])
        self.assertEqual(result['lineas'], {})
