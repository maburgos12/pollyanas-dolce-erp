from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from unittest.mock import patch
from types import SimpleNamespace
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointTransferLine
from seguimiento.models import SeguimientoItem, SeguimientoComentario
from inventario.models import InsumoAuditSnapshot, InsumoReconciliationCase, InsumoReconciliationEvent, MovimientoInventario
from inventario.services_documentary_audit import bounds, convert
from inventario.services_reconciliation_agent import InsumoReconciliationAgent, documentary_quantity
from pos_bridge.services.inventory_baseline import point_transfer_origin_exit_hash


class ReconciliationAgentTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.period=(timezone.localdate().replace(day=1)-timedelta(days=1)).replace(day=1)
        cls.start,cls.end=bounds(cls.period)
        cls.unit=UnidadMedida.objects.create(codigo='pza',nombre='Pieza')
        cls.item=Insumo.objects.create(nombre='CUBREBOCAS',codigo_point='MASK',unidad_base=cls.unit)
        cls.owner=get_user_model().objects.create_user(username='responsable',password='local-test')
        cls.other=get_user_model().objects.create_user(username='otro',password='local-test')
        cls.admin=get_user_model().objects.create_superuser(username='auditor',password='local-test')
        cls.origin=PointBranch.objects.create(external_id='rec-a',name='Almacen')
        cls.destination=PointBranch.objects.create(external_id='rec-c',name='CEDIS')
        cls.snapshot=InsumoAuditSnapshot.objects.create(periodo=cls.period,fingerprint='test',payload={'rows':[], 'coverage':{}})

    def purchase(self,unit='U',qty=10,key='one'):
        c=CostoInsumo.objects.create(insumo=self.item,fecha=self.period,costo_unitario=1,source_hash=key,
            raw={'source':'POINT_COMPRAS_HISTORICAS','purchase_id':key,'folio':key,'branch':'Almacen',
                 'quantity':qty,'unit':unit,'article_name':'CUBREBOCAS'})
        m=MovimientoInventario.objects.create(insumo=self.item,fecha=self.start+timedelta(hours=12),tipo='ENTRADA',
            almacen='ALMACEN_1',cantidad=qty,source_hash=sha256(f'point_purchase|{key}'.encode()).hexdigest(),
            trazabilidad={'costo_insumo_id':c.pk})
        return c,m

    def run_agent(self,dry_run=False):
        with patch('reportes.services_inventory_audit_agent.InventoryAuditAgent._resolve_assignee',return_value=(self.owner.pk,'Jefatura verificada')):
            return InsumoReconciliationAgent().run(self.period,dry_run=dry_run)

    def test_existing_aliases_and_same_units_without_pack_inference(self):
        for source,target in [('U','pza'),('GLI','GLI'),('Gfn','Gfn'),('kg','g')]:
            expected=Decimal('10000') if source=='kg' else Decimal('10')
            self.assertEqual(documentary_quantity(10,source,target)[0],expected)
            self.assertEqual(convert(10,source,target),expected)
        self.assertIsNone(documentary_quantity(10,'CJA','pza')[0])
        self.assertIsNone(convert(10,'CJA','pza'))
        self.assertIsNone(documentary_quantity('NaN','U','pza')[0])
        self.assertIsNone(documentary_quantity(-1,'U','pza')[0])

    def test_reconciles_quantity_without_operational_writes(self):
        c,m=self.purchase()
        with CaptureQueriesContext(connection) as queries:
            self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        self.assertEqual(case.estado,'CONCILIADO')
        self.assertIsNone(case.solicitud_id)
        self.assertIn('10',case.conclusion)
        writes=[q['sql'].lower() for q in queries if q['sql'].lstrip().upper().startswith(('INSERT','UPDATE','DELETE'))]
        self.assertTrue(writes)
        for sql in writes:
            self.assertTrue(any(table in sql for table in ['inventario_insumoreconciliation','seguimiento_seguimientoitem']),sql)
        self.assertEqual(MovimientoInventario.objects.get(pk=m.pk).cantidad,10)

    def test_request_is_filled_and_idempotent_history_stable(self):
        self.purchase('CJA')
        self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        request=case.solicitud
        self.assertEqual(request.responsable_user,self.owner)
        self.assertIn('cuántas pza contiene cada CJA',request.entregable_esperado)
        self.assertIn('10 CJA',request.descripcion)
        self.assertIn('10.000 pza',request.descripcion)
        self.assertIn('Compra one',request.descripcion)
        self.assertEqual(request.metadata['caso_id'],case.pk)
        self.assertIn('movimiento',case.evidencia['investigacion'])
        counts=(SeguimientoItem.objects.count(),InsumoReconciliationEvent.objects.count())
        self.run_agent()
        self.assertEqual(counts,(SeguimientoItem.objects.count(),InsumoReconciliationEvent.objects.count()))

    def test_response_is_read_not_accepted_as_inventory_correction(self):
        self.purchase('CJA');self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        SeguimientoComentario.objects.create(seguimiento=case.solicitud,usuario=self.owner,comentario='Cada caja contiene 50 piezas; pendiente factura.')
        self.run_agent();case.refresh_from_db()
        self.assertEqual(case.estado,'RESPUESTA')
        self.assertIn('50 piezas',case.evidencia['respuesta']['comentarios'][0]['comentario'])
        self.assertEqual(MovimientoInventario.objects.get().cantidad,10)
        self.assertEqual(case.solicitud.entregable_esperado.count('cuántas'),1)
        self.assertEqual(case.eventos.count(),2)

    def test_new_source_evidence_resolves_case_and_preserves_request(self):
        c,m=self.purchase('CJA');self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA');request_id=case.solicitud_id
        # Simula corrección externa autorizada; el agente no escribe esta fuente.
        c.raw={**c.raw,'unit':'U'};c.save(update_fields=['raw'])
        self.run_agent();case.refresh_from_db()
        self.assertEqual(case.estado,'CONCILIADO');self.assertEqual(case.solicitud_id,request_id)
        self.assertEqual(case.solicitud.estatus,'EN_REVISION')
        self.assertTrue(case.solicitud.requiere_aprobacion)
        self.assertEqual(case.eventos.count(),2)

    def test_zero_imports_creates_specific_source_request(self):
        self.run_agent()
        case=InsumoReconciliationCase.objects.get(key__endswith='coverage:purchases')
        self.assertEqual(case.estado,'ACLARACION')
        self.assertIn('exportación de compras Point',case.solicitud.entregable_esperado)
        self.assertIn('no prueba ausencia',case.conclusion)

    def test_source_removal_does_not_silently_close_case(self):
        c,m=self.purchase('CJA');self.run_agent()
        c.raw={**c.raw,'source':'OTHER'};c.save(update_fields=['raw'])
        self.run_agent();case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        self.assertEqual(case.estado,'ACLARACION')
        self.assertIn('ya no aparece',case.conclusion)

    def test_dry_run_never_creates_cases_requests_or_events(self):
        self.purchase('CJA')
        result=self.run_agent(dry_run=True)
        self.assertGreater(len(result['cases']),0)
        self.assertEqual(InsumoReconciliationCase.objects.count(),0)
        self.assertEqual(SeguimientoItem.objects.count(),0)

    def test_other_user_cannot_read_case_or_export_but_assignee_can(self):
        self.purchase('CJA');self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        url=reverse('reportes:conciliacion_insumos')+f'?caso={case.pk}'
        with patch('reportes.views_reconciliation_agent.can_view_reportes',return_value=False):
            self.client.force_login(self.other)
            self.assertEqual(self.client.get(url).status_code,404)
            self.assertEqual(self.client.get(url+'&export=json').status_code,404)
            self.client.force_login(self.owner)
            self.assertEqual(self.client.get(url).status_code,200)
            self.assertEqual(self.client.get(url+'&export=json').json()[0]['id'],case.pk)

    def test_repeated_transfer_is_grouped_and_cross_cut_explained(self):
        for index in (1,2):
            key=f'transfer-{index}'
            line=PointTransferLine.objects.create(origin_branch=self.origin,destination_branch=self.destination,
                insumo=self.item,transfer_external_id='T1',detail_external_id=str(index),source_hash=key,
                registered_at=self.start,sent_at=self.start-timedelta(hours=1),received_at=self.start+timedelta(hours=1),
                item_name=self.item.nombre,item_code='MASK',unit='U',sent_quantity=10,received_quantity=10,is_insumo=True,is_received=True)
            for kind,location,h,stamp in [('SALIDA','ALMACEN_1',point_transfer_origin_exit_hash(key),line.sent_at),('ENTRADA','CUARTO_FRIO',key,line.received_at)]:
                MovimientoInventario.objects.create(insumo=self.item,tipo=kind,almacen=location,source_hash=h,cantidad=10,fecha=stamp)
        self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='TRANSFERENCIA')
        self.assertEqual(case.estado,'EXPLICADO')
        self.assertIsNone(case.solicitud_id)
        self.assertEqual(len(case.evidencia['investigacion']['lineas']),2)

    def test_unverified_consumption_creates_document_specific_request(self):
        self.snapshot.payload={'coverage':{},'rows':[{'key':f'{self.item.pk}:CEDIS','insumo_id':self.item.pk,
            'name':'CUBREBOCAS','point_code':'MASK','unit':'pza','location':'CEDIS','totals':{'unverified':'2'},
            'inventory_now':{'state':'FRESH'},'evidence':[{'model':'inventario.MovimientoInventario','id':123,'category':'unverified','quantity':'2'}]}]}
        InsumoAuditSnapshot.objects.filter(pk=self.snapshot.pk).update(payload=self.snapshot.payload)
        self.run_agent();case=InsumoReconciliationCase.objects.get(kind='CONSUMO')
        self.assertEqual(case.estado,'ACLARACION')
        self.assertEqual(case.evidencia['investigacion']['movimientos'][0]['id'],123)
        self.assertIn('medición de origen',case.solicitud.entregable_esperado)

    def test_transfer_missing_origin_respects_importer_scope_and_cutover(self):
        branch=PointBranch.objects.create(external_id='centro',name='Centro')
        for document,origin,destination,sent_at in [
            ('OUTSIDE',self.destination,branch,self.start+timedelta(days=1)),
            ('BEFORE',self.origin,self.destination,self.start-timedelta(hours=1)),
            ('EXPECTED',self.origin,self.destination,self.start+timedelta(days=1)),
        ]:
            line=PointTransferLine.objects.create(origin_branch=origin,destination_branch=destination,
                insumo=self.item,transfer_external_id=document,detail_external_id=document,source_hash=document,
                registered_at=sent_at,sent_at=sent_at,received_at=self.start+timedelta(days=2),
                item_name=self.item.nombre,item_code='MASK',unit='U',sent_quantity=10,received_quantity=10,is_insumo=True,is_received=True)
            if destination==self.destination:
                MovimientoInventario.objects.create(insumo=self.item,tipo='ENTRADA',almacen='CUARTO_FRIO',
                    source_hash=document,cantidad=10,fecha=line.received_at)
        with patch('inventario.services_reconciliation_agent.latest_point_almacen_baseline_run',
                   return_value=SimpleNamespace(pk=77,finished_at=self.start)):
            self.run_agent()
        for document in ('OUTSIDE','BEFORE'):
            case=InsumoReconciliationCase.objects.get(titulo='Transferencia '+document)
            self.assertEqual(case.estado,'EXPLICADO')
            self.assertIsNone(case.solicitud_id)
            self.assertIn('No es prueba de faltante',case.conclusion)
        expected=InsumoReconciliationCase.objects.get(titulo='Transferencia EXPECTED')
        self.assertEqual(expected.estado,'ACLARACION')
        self.assertIn('SALIDA',expected.solicitud.entregable_esperado)

    def test_old_open_period_is_revisited_by_scheduled_task(self):
        from inventario.tasks import conciliar_documentos_insumos
        self.purchase('CJA');self.run_agent()
        old=(self.period-timedelta(days=1)).replace(day=1)
        InsumoReconciliationCase.objects.filter(kind='COMPRA').update(periodo=old)
        with patch.object(InsumoReconciliationAgent,'run',return_value={'status':'investigated'}) as run:
            conciliar_documentos_insumos.run()
        self.assertIn(old,[c.args[0] for c in run.call_args_list])

    def test_approved_request_does_not_approve_missing_source(self):
        self.purchase('CJA');self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        SeguimientoItem.objects.filter(pk=case.solicitud_id).update(estatus='COMPLETADO',aprobado_por=self.admin,aprobado_at=timezone.now())
        self.run_agent();case.refresh_from_db()
        self.assertNotEqual(case.estado,'CONCILIADO')
        self.assertEqual(MovimientoInventario.objects.get().cantidad,10)

    def test_manual_request_text_is_preserved_on_new_investigation(self):
        c,m=self.purchase('CJA');self.run_agent()
        case=InsumoReconciliationCase.objects.get(kind='COMPRA')
        SeguimientoItem.objects.filter(pk=case.solicitud_id).update(entregable_esperado='Texto específico añadido por Dirección.')
        m.cantidad=9;m.save(update_fields=['cantidad'])
        self.run_agent();case.refresh_from_db()
        self.assertEqual(case.solicitud.entregable_esperado,'Texto específico añadido por Dirección.')
        self.assertEqual(case.evidencia['investigacion']['movimiento']['cantidad'],'9.000')
