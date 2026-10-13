from copy import deepcopy
from datetime import datetime, timedelta, timezone as utc_timezone
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase
from pos_bridge.models import PointSyncJob
from inventario.conteos_conciliacion import calcular_cierre_linea

MZ = ZoneInfo('America/Mazatlan')


def movement(ident, when, kind, previous, new, *, cancelled=False):
    return {
        'FK_Movimiento': ident, 'FK_Tipo_Movimiento': ident,
        'Movimiento': kind, 'Fecha': when.astimezone(utc_timezone.utc).isoformat(),
        'Cantidad': str(abs(Decimal(new)-Decimal(previous))),
        'Existencia_anterior': str(previous), 'Existencia_nueva': str(new),
        'Cancelado': cancelled,
    }


class DailyCountCalculationTests(SimpleTestCase):
    def setUp(self):
        self.when = datetime(2026, 10, 12, 20, tzinfo=MZ)
        self.cut = datetime(2026, 10, 13, tzinfo=MZ)
        self.now = self.cut + timedelta(hours=2, minutes=5)
        self.line = SimpleNamespace(pk=1, producto_id=1, insumo_id=None, codigo='C100', unidad='PZA')
        self.reading = SimpleNamespace(linea=self.line, linea_id=1, ronda=1,
                                       cantidad=Decimal('3'), contado_en=self.when)
        self.snapshot = SimpleNamespace(
            pk=10, product_id=1, branch_id=1, stock=Decimal('3'),
            captured_at=self.cut+timedelta(minutes=30),
            raw_payload={'headers':['Codigo','Cantidad','Unidad'],'row':['C100','3','PZA']},
            sync_job=SimpleNamespace(pk=5, status=PointSyncJob.STATUS_SUCCESS,
                job_type=PointSyncJob.JOB_TYPE_INVENTORY,
                started_at=self.cut+timedelta(minutes=29),
                finished_at=self.cut+timedelta(minutes=31)),
        )
        self.batch = {
            'branch_id':1, 'product_id':1,
            'unique_rows':[
                movement(101, self.when+timedelta(minutes=15), 'VENTA', '3', '2'),
                movement(102, self.when+timedelta(minutes=30), 'ENTRADA POR TRANSFERENCIA', '2', '4'),
                movement(103, self.cut+timedelta(minutes=15), 'VENTA', '4', '3'),
            ],
            'stock_chain_gaps':(),
            'evidence':{'import_id':7, 'retrieved_at':self.now.isoformat(),
                        'original_count':3,'history_limit':500,'raw_sha256':'a'*64},
        }

    def calculate(self):
        return calcular_cierre_linea(self.reading, snapshot=self.snapshot,
                                      batch=self.batch, cierre=self.cut, branch_id=1, now=self.now)

    def test_aligns_physical_and_point_without_mixing_post_close_sale(self):
        result = self.calculate()
        self.assertEqual(result['estado'], 'CALCULADA')
        self.assertEqual(Decimal(result['fisico_al_cierre']), Decimal('4'))
        self.assertEqual(Decimal(result['point_al_cierre']), Decimal('4'))
        self.assertEqual(Decimal(result['diferencia']), Decimal('0'))
        self.assertEqual(result['movimientos_fisico'], [101,102])
        self.assertEqual(result['movimientos_point'], [103])
        self.assertEqual(result['posteriores_cierre'], [103])
        self.assertEqual(self.reading.cantidad, Decimal('3'))

    def test_verified_empty_history_and_explicit_zero_are_calculable(self):
        self.reading.cantidad = Decimal('0')
        self.snapshot.stock = Decimal('0')
        self.snapshot.raw_payload['row'][1] = '0'
        self.batch['unique_rows'] = []
        self.batch['evidence']['original_count'] = 0
        result = self.calculate()
        self.assertEqual(result['estado'], 'CALCULADA')
        self.assertEqual(result['diferencia'], '0')

    def test_absent_time_or_source_never_becomes_zero(self):
        for attr, value in [('contado_en',None), ('cantidad',None)]:
            with self.subTest(attr=attr):
                original=getattr(self.reading,attr)
                setattr(self.reading,attr,value)
                result=self.calculate()
                self.assertEqual(result['estado'],'PENDIENTE')
                self.assertIsNone(result['diferencia'])
                setattr(self.reading,attr,original)
        result=calcular_cierre_linea(self.reading,snapshot=None,batch=None,cierre=self.cut,branch_id=1,now=self.now)
        self.assertEqual(result['estado'],'PENDIENTE')
        self.assertIsNone(result['point_al_cierre'])

    def test_snapshot_amount_without_consistent_raw_unit_is_pending(self):
        for raw in [
            {'headers':['Codigo','Cantidad','Unidad'],'row':['C100','','PZA']},
            {'headers':['Codigo','Cantidad','Unidad'],'row':['C100','3','KG']},
            {'headers':['Codigo','Cantidad','Unidad'],'row':['OTHER','3','PZA']},
        ]:
            with self.subTest(raw=raw):
                self.snapshot.raw_payload=raw
                self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_failed_cycle_and_old_receipt_are_pending(self):
        self.snapshot.sync_job.status=PointSyncJob.STATUS_FAILED
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')
        self.snapshot.sync_job.status=PointSyncJob.STATUS_SUCCESS
        self.batch['evidence']['retrieved_at']=(self.cut-timedelta(minutes=1)).isoformat()
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_saturated_response_without_lower_boundary_is_pending(self):
        self.batch['evidence']['original_count']=500
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_stock_gap_or_snapshot_extraction_movement_is_pending(self):
        original=deepcopy(self.batch)
        self.batch['stock_chain_gaps']=({'movement_at':(self.when+timedelta(minutes=30)).isoformat()},)
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')
        self.batch=original
        self.snapshot.sync_job.started_at=self.cut+timedelta(minutes=10)
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_book_adjustment_does_not_invent_physical_movement(self):
        self.batch['unique_rows'][1]['Movimiento']='ENTRADA POR AJUSTE INVENTARIO'
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')
        self.assertIsNone(self.calculate()['diferencia'])

    def test_reading_after_close_is_reconstructed_backwards(self):
        self.reading.contado_en=self.cut+timedelta(minutes=20)
        self.reading.cantidad=Decimal('3')
        result=self.calculate()
        self.assertEqual(result['estado'],'CALCULADA')
        self.assertEqual(result['fisico_al_cierre'],'4')
        self.assertEqual(result['point_al_cierre'],'4')

    def test_wrong_product_or_missing_domain_is_pending(self):
        self.snapshot.product_id=2
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')
        self.snapshot.product_id=1
        self.line.producto_id=None
        self.line.insumo_id=1
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_negative_physical_projection_is_not_reported_as_stock(self):
        self.reading.cantidad=Decimal('0')
        self.batch['unique_rows'][1]=movement(102,self.when+timedelta(minutes=30),'VENTA','2','1')
        self.batch['unique_rows'][2]=movement(103,self.cut+timedelta(minutes=15),'VENTA','1','0')
        self.snapshot.stock=Decimal('0')
        self.snapshot.raw_payload['row'][1]='0'
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_cancelled_inert_movement_does_not_replace_effective_anchor(self):
        self.batch['unique_rows'].append(movement(104,self.cut+timedelta(minutes=20),
            'VENTA','8','8',cancelled=True))
        self.assertEqual(self.calculate()['estado'],'CALCULADA')

    def test_saturated_boundary_at_exact_close_is_not_proven(self):
        self.reading.contado_en=self.cut+timedelta(minutes=20)
        self.reading.cantidad=Decimal('0')
        self.snapshot.stock=Decimal('0')
        self.snapshot.raw_payload['row'][1]='0'
        self.batch['unique_rows']=[movement(101,self.cut,'VENTA','1','0')]
        self.batch['evidence']['original_count']=500
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_snapshot_or_batch_from_other_branch_is_pending(self):
        self.snapshot.branch_id=2
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')
        self.snapshot.branch_id=1
        self.batch['branch_id']=2
        self.assertEqual(self.calculate()['estado'],'PENDIENTE')

    def test_point_naive_date_is_utc_and_waste_cancellation_is_effective(self):
        debit=movement(101,self.when+timedelta(minutes=15),'MERMA','3','2',cancelled=True)
        debit.update(FK_Tipo_Movimiento=5,isCargo=True)
        undo=movement(102,self.when+timedelta(minutes=30),'CANCELACION DE MERMA','2','3')
        undo.update(FK_Tipo_Movimiento=15,isCargo=False)
        debit['Fecha']=datetime.fromisoformat(debit['Fecha']).replace(tzinfo=None).isoformat()
        self.batch['unique_rows']=[debit,undo]
        result=self.calculate()
        self.assertEqual(result['estado'],'CALCULADA')
        self.assertEqual(result['fisico_al_cierre'],'3')
        self.assertEqual(result['movimientos_fisico'],[101,102])
