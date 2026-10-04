from decimal import Decimal
from io import StringIO
from unittest.mock import patch

from django.test import TestCase

from control.models import MermaPOS
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointSyncJob, PointWasteLine
from recetas.models import Receta

from scripts.recover_point_waste_1683114 import recover_original_waste


class OriginalWasteRecoveryTests(TestCase):
    def setUp(self):
        self.branch, _ = Sucursal.objects.update_or_create(pk=1, defaults={'codigo': 'MATRIZ', 'nombre': 'Matriz'})
        PointBranch.objects.create(pk=24, external_id='Matriz', name='Matriz', erp_branch=self.branch)
        Receta.objects.create(pk=16, nombre='Empanada de Manzana', codigo_point='0135', hash_contenido='recovery-original')
        PointSyncJob.objects.create(pk=79066, job_type='WASTE', status='SUCCESS')
        self.waste = (
            '1676\t1683114\t9f27f6de945bbc8b19cd\t2026-09-27 19:01:00.49+00\tOperación\tEmpanada de Manzana\t\t5.000\tPZA\t2.067315\t10.34\tMerma desde la caja\t/Mermas/get_mermas\t'
            '{"movement":{"PK_Movimiento":1683114,"Sucursal":"Matriz","Fecha":"2026-09-27T19:01:00.49"},"details":[{"Articulo":"Empanada de Manzana","Cantidad":5.0,"Unidad":"PZA"}]}\t'
            '2026-09-28 09:45:41+00\t2026-10-02 09:45:39+00\t24\t1\t\\N\t16\t79066\n'
        )
        self.merma = '1676\t2026-09-27\t0135\tEmpanada de Manzana\t5.000\tMerma desde la caja\tPOINT_BRIDGE_WASTE\t2026-09-28 09:45:41+00\t2026-10-02 09:45:39+00\t16\t1\t9f27f6de945bbc8b19cd\tOperación\n'

    def backup(self, waste=None, merma=None):
        return StringIO(
            'COPY public.control_mermapos (id, fecha, codigo_point, producto_texto, cantidad, motivo, fuente, creado_en, actualizado_en, receta_id, sucursal_id, source_hash, responsable_texto) FROM stdin;\n'
            + (self.merma if merma is None else merma) + '\\.' + '\n'
            + 'COPY public.pos_bridge_waste_lines (id, movement_external_id, source_hash, movement_at, responsible, item_name, item_code, quantity, unit, unit_cost, total_cost, justification, source_endpoint, raw_payload, created_at, updated_at, branch_id, erp_branch_id, insumo_id, receta_id, sync_job_id) FROM stdin;\n'
            + (self.waste if waste is None else waste) + '\\.' + '\n'
        )

    def recover(self, **kwargs):
        with patch('scripts.recover_point_waste_1683114.gzip.open', return_value=self.backup()):
            return recover_original_waste(**kwargs)

    def test_dry_run_does_not_restore_operational_rows(self):
        result = self.recover()
        self.assertEqual(result['would_restore'], 2)
        self.assertFalse(PointWasteLine.objects.exists())
        self.assertFalse(MermaPOS.objects.exists())

    def test_restores_original_ids_timestamps_and_writer_and_second_is_noop(self):
        with patch('requests.Session.request', side_effect=AssertionError('HTTP forbidden')):
            first = self.recover(apply=True)
            original = list(PointWasteLine.objects.values())
            ledger = list(MermaPOS.objects.values())
            second = self.recover(apply=True)
        self.assertEqual(first['restored'], 2)
        self.assertEqual(second['restored'], 0)
        self.assertEqual(original, list(PointWasteLine.objects.values()))
        self.assertEqual(ledger, list(MermaPOS.objects.values()))
        self.assertEqual(original[0]['id'], 1676)
        self.assertEqual(original[0]['sync_job_id'], 79066)
        self.assertEqual(original[0]['quantity'], Decimal('5'))
        self.assertEqual(original[0]['created_at'].year, 2026)

    def test_conflicting_original_pk_aborts_both_rows(self):
        MermaPOS.objects.create(pk=1676, fecha='2026-09-27', cantidad=1, source_hash='other-original')
        with self.assertRaises(ValueError):
            self.recover(apply=True)
        self.assertFalse(PointWasteLine.objects.exists())
        self.assertEqual(MermaPOS.objects.get(pk=1676).cantidad, Decimal('1'))

    def test_inconsistent_backup_quantity_cannot_be_restored(self):
        with patch('scripts.recover_point_waste_1683114.gzip.open', return_value=self.backup(waste=self.waste.replace('\t5.000\t', '\t6.000\t'))):
            with self.assertRaises(ValueError):
                recover_original_waste(apply=True)
        self.assertFalse(PointWasteLine.objects.exists())
        self.assertFalse(MermaPOS.objects.exists())

    def test_missing_evidence_pair_aborts_without_partial_restore(self):
        with patch('scripts.recover_point_waste_1683114.gzip.open', return_value=self.backup(merma='')):
            with self.assertRaises(ValueError):
                recover_original_waste(apply=True)
        self.assertFalse(PointWasteLine.objects.exists())

    def test_conflict_in_second_table_rolls_back_first_insert(self):
        PointWasteLine.objects.create(pk=1676, branch_id=24, receta_id=16,
            source_hash='different-original', movement_external_id='other',
            movement_at='2026-09-27T19:01:00+00:00', item_name='Other', quantity=1)
        with self.assertRaises(ValueError):
            self.recover(apply=True)
        self.assertFalse(MermaPOS.objects.exists())
        self.assertEqual(PointWasteLine.objects.get(pk=1676).quantity, Decimal('1'))

    def test_original_hash_at_other_pk_cannot_be_duplicated(self):
        MermaPOS.objects.create(pk=1677, fecha='2026-09-27', cantidad=5, source_hash='9f27f6de945bbc8b19cd')
        with self.assertRaises(ValueError):
            self.recover(apply=True)
        self.assertFalse(PointWasteLine.objects.exists())
        self.assertEqual(MermaPOS.objects.count(), 1)

    def test_one_missing_member_recovers_only_that_member(self):
        self.recover(apply=True)
        original = list(PointWasteLine.objects.values())
        MermaPOS.objects.filter(pk=1676).delete()
        self.assertEqual(self.recover(apply=True)['restored'], 1)
        self.assertEqual(original, list(PointWasteLine.objects.values()))
        self.assertEqual(MermaPOS.objects.get(pk=1676).cantidad, Decimal('5'))
