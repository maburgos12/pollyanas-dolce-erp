from datetime import date
from decimal import Decimal
from uuid import uuid4
from unittest.mock import patch
from django.test import TestCase
from django.contrib.auth import get_user_model
from core.models import Sucursal, UserProfile
from pos_bridge.models import PointProduct


class ConteosDomainTests(TestCase):
    def setUp(self):
        from inventario import services_conteos as service
        from inventario import models_conteos as models
        self.service, self.models = service, models
        self.admin = get_user_model().objects.create_superuser('count_admin', password='test')
        self.user = get_user_model().objects.create_user('count_user', password='test')
        self.branch = Sucursal.objects.create(codigo='CS1', nombre='Conteos')
        UserProfile.objects.update_or_create(user=self.user, defaults={'sucursal': self.branch})
        self.products = [PointProduct.objects.create(external_id=str(i+1000), sku=str(i), name=f'Producto {i}') for i in (901,902)]
        self.args = dict(actor=self.admin, sucursal=self.branch, responsable=self.user, fecha=date(2026,9,7), titulo='Cierre', items=[{'producto_id':p.pk,'unidad':'pza','fuente_unidad':'catálogo verificado'} for p in self.products], request_id=uuid4())
        self.count = service.preparar_conteo(**self.args)

    def act(self, action, payload=None, actor=None, version=None, request_id=None):
        self.count.refresh_from_db()
        return self.service.ejecutar_accion(conteo_id=self.count.pk, actor=actor or self.user, action=action, version=version or self.count.version, request_id=request_id or uuid4(), payload=payload or {})

    def readings(self, value='0'):
        return {'lecturas':{str(l.pk):{'cantidad':value,'incidencia':''} for l in self.count.lineas.all()}}

    def test_creation_replay_and_overlap(self):
        self.assertEqual(self.service.preparar_conteo(**self.args).pk, self.count.pk)
        with self.assertRaises(self.service.ConteoConflict):
            self.service.preparar_conteo(**{**self.args,'titulo':'Otro'})
        with self.assertRaises(self.service.ConteoConflict):
            self.service.preparar_conteo(**{**self.args,'request_id':uuid4()})

    def test_zero_is_present_missing_is_not(self):
        self.act('iniciar')
        with self.assertRaises(self.service.ConteoError): self.act('enviar')
        self.act('enviar', self.readings())
        self.count.refresh_from_db()
        self.assertEqual(self.count.estado, 'ENVIADO')
        self.assertTrue(all(r.cantidad == Decimal('0') for r in self.models.LecturaConteoSucursal.objects.all()))

    def test_invalid_quantities_roll_back(self):
        self.act('iniciar')
        for value in ['NaN','Infinity','-1','0.0000001','1000000000000']:
            with self.subTest(value=value), self.assertRaises(self.service.ConteoError):
                self.act('guardar', self.readings(value))
        self.assertEqual(self.models.OperacionConteoSucursal.objects.count(), 1)

    def test_operation_replay_and_stale_version(self):
        self.act('iniciar')
        key = uuid4()
        first = self.act('guardar', self.readings(), request_id=key, version=2)
        self.assertEqual(first, self.act('guardar', self.readings(), request_id=key, version=2))
        with self.assertRaises(self.service.ConteoConflict): self.act('guardar', self.readings('2'), request_id=key, version=2)
        with self.assertRaises(self.service.ConteoConflict): self.act('guardar', self.readings(), version=2)

    def test_recount_preserves_previous_and_requires_new_reading(self):
        self.act('iniciar')
        self.act('enviar', self.readings('3'))
        line = self.count.lineas.first()
        self.act('reconteo', {'linea_ids':[line.pk],'motivo':'Verificar'}, actor=self.admin)
        with self.assertRaises(self.service.ConteoError): self.act('enviar')
        self.act('iniciar')
        self.act('enviar', {'lecturas':{str(line.pk):{'cantidad':'2'}}})
        self.act('aceptar', {'motivo':'Conteo revisado'}, actor=self.admin)
        self.assertEqual(list(line.lecturas.order_by('ronda').values_list('cantidad',flat=True)),[Decimal('3'),Decimal('2')])

    def test_revocation_precedes_replay(self):
        self.act('iniciar')
        key=uuid4()
        self.act('guardar', self.readings(), request_id=key, version=2)
        UserProfile.objects.filter(user=self.user).update(sucursal=None)
        with self.assertRaises(self.service.ConteoError): self.act('guardar',self.readings(),request_id=key,version=2)

    def test_reviewer_and_capture_grants_are_separate(self):
        self.act('iniciar')
        outsider = get_user_model().objects.create_user('outsider')
        with self.assertRaises(self.service.ConteoError): self.act('guardar',self.readings(),actor=outsider)
        self.models.AccesoConteoSucursal.objects.create(user=outsider,sucursal=self.branch,capturar=True)
        self.act('guardar',self.readings(),actor=outsider)
        self.act('enviar',self.readings())
        with self.assertRaises(self.service.ConteoError): self.act('aceptar',{'motivo':'x'},actor=outsider)

    def test_event_failure_rolls_back_readings_and_receipt(self):
        self.act('iniciar')
        with patch.object(self.models.EventoConteoSucursal.objects, 'create', side_effect=RuntimeError('audit unavailable')):
            with self.assertRaises(RuntimeError): self.act('guardar',self.readings())
        self.count.refresh_from_db()
        self.assertEqual(self.count.version,2)
        self.assertEqual(self.models.OperacionConteoSucursal.objects.count(),1)
        self.assertFalse(self.models.LecturaConteoSucursal.objects.exclude(cantidad=None).exists())

    def test_reference_confirmation_requires_fresh_complete_evidence(self):
        self.act('iniciar')
        from datetime import timedelta
        self.act('enviar', self.readings())
        self.count.refresh_from_db()
        reference=self.fresh_reference()
        with patch('inventario.conteos_point.referencia_conteo',return_value=reference):
            self.act('referencia',actor=self.admin)
        with self.assertRaises(self.service.ConteoError): self.act('validar_referencia',{'motivo':'Revisado'},actor=self.admin)
        self.act('validar_referencia',{'motivo':'Ventana controlada comprobada','confirmado':True},actor=self.admin)
        self.count.refresh_from_db()
        self.assertTrue(self.count.referencia['corte_verificado'])
        self.assertEqual(self.count.referencia['validado_por'],self.admin.pk)

    def test_reference_confirmation_rejects_stale_source(self):
        self.act('iniciar')
        from datetime import timedelta
        self.act('enviar', self.readings())
        self.count.refresh_from_db()
        reference={'estado':'REFERENCIA','corte_verificado':False,'lineas':{str(line.pk):{'cantidad':'1','unidad':'pza','capturado_en':(self.count.iniciado_en-timedelta(hours=3)).isoformat()} for line in self.count.lineas.all()}}
        with patch('inventario.conteos_point.referencia_conteo',return_value=reference): self.act('referencia',actor=self.admin)
        with self.assertRaises(self.service.ConteoError): self.act('validar_referencia',{'motivo':'Revisado','confirmado':True},actor=self.admin)

    def test_unknown_or_prior_round_line_is_rejected(self):
        self.act('iniciar')
        with self.assertRaises(self.service.ConteoError): self.act('guardar',{'lecturas':{'999999':{'cantidad':'2'}}})
        self.act('enviar',self.readings())
        line=self.count.lineas.first()
        self.act('reconteo',{'linea_ids':[line.pk],'motivo':'Verificar'},actor=self.admin)
        self.act('iniciar')
        other=self.count.lineas.exclude(pk=line.pk).get()
        with self.assertRaises(self.service.ConteoError): self.act('guardar',{'lecturas':{str(other.pk):{'cantidad':'2'}}})

    def test_incident_is_valid_evidence_without_invented_zero(self):
        self.act('iniciar')
        values={'lecturas':{str(l.pk):{'cantidad':'','incidencia':'Producto inaccesible'} for l in self.count.lineas.all()}}
        self.act('enviar',values)
        self.assertFalse(self.models.LecturaConteoSucursal.objects.exclude(cantidad=None).exists())

    def test_no_inventory_or_point_mutations(self):
        import re
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        from inventario.models import MovimientoInventario, ExistenciaInsumo
        from pos_bridge.models import PointInventorySnapshot, PointInsumoInventorySnapshot, PointSyncJob
        models=[MovimientoInventario, ExistenciaInsumo,PointInventorySnapshot,PointInsumoInventorySnapshot,PointSyncJob]
        before=[m.objects.count() for m in models]
        allowed_tables={model._meta.db_table for model in (
            self.models.ConteoSucursal, self.models.LineaConteoSucursal,
            self.models.LecturaConteoSucursal, self.models.EventoConteoSucursal,
            self.models.OperacionConteoSucursal,
        )}
        # Row totals alone would miss accidental UPDATEs to existing stock.
        # Inspect all issued SQL, including indirect writes from model signals.
        with CaptureQueriesContext(connection) as queries:
            self.act('iniciar')
            self.act('enviar',self.readings())
            self.act('referencia',actor=self.admin)
            self.act('aceptar',{'motivo':'Revisado'},actor=self.admin)
        writes=[]
        for query in queries.captured_queries:
            sql=query['sql']
            if not re.search(r'\b(?:INSERT|UPDATE|DELETE|TRUNCATE|MERGE|ALTER|CREATE|DROP)\b',sql,re.IGNORECASE):
                continue
            # SELECT ... FOR UPDATE is a lock, never a data mutation.
            if re.match(r'^\s*SELECT\b',sql,re.IGNORECASE):
                self.assertNotRegex(sql,r'(?i)\b(?:INSERT|DELETE|TRUNCATE|MERGE|ALTER|CREATE|DROP)\b')
                continue
            match=re.match(r'^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"?([a-z_][a-z_0-9]*)"?',sql,re.IGNORECASE)
            self.assertIsNotNone(match, f'Unexpected mutating SQL: {sql}')
            self.assertIn(match.group(1),allowed_tables, f'Write outside count evidence tables: {sql}')
            writes.append(match.group(1))
        self.assertTrue(writes, 'The guard must inspect actual writes during the count workflow.')
        self.assertEqual(before,[m.objects.count() for m in models])

    def test_freezes_point_sku_and_rejects_missing_code(self):
        self.assertEqual(set(self.count.lineas.values_list('codigo',flat=True)),{'901','902'})
        p=PointProduct.objects.create(external_id='uncoded',name='Sin código')
        with self.assertRaises(self.service.ConteoError):
            self.service.preparar_conteo(**{**self.args,'request_id':uuid4(),'items':[{'producto_id':p.pk,'unidad':'pza','fuente_unidad':'catálogo'}]})

    def test_rejects_duplicate_physical_code(self):
        a=PointProduct.objects.create(external_id='duplicate-a',sku='888',name='A')
        b=PointProduct.objects.create(external_id='duplicate-b',sku='888',name='B')
        with self.assertRaises(self.service.ConteoError):
            self.service.preparar_conteo(**{**self.args,'request_id':uuid4(),'items':[{'producto_id':p.pk,'unidad':'pza','fuente_unidad':'catálogo'} for p in (a,b)]})

    def test_capture_visibility_is_branch_specific_and_revocable(self):
        from inventario.conteos_access import conteos_visibles
        self.assertTrue(conteos_visibles(self.user).filter(pk=self.count.pk).exists())
        UserProfile.objects.filter(user=self.user).update(sucursal=None)
        self.assertFalse(conteos_visibles(self.user).exists())
        access=self.models.AccesoConteoSucursal.objects.create(user=self.user,sucursal=self.branch,capturar=True)
        self.assertTrue(conteos_visibles(self.user).exists())
        access.activo=False
        access.save()
        self.assertFalse(conteos_visibles(self.user).exists())

    def test_nonoverlapping_partial_is_permitted(self):
        product=PointProduct.objects.create(external_id='new',sku='new-code',name='Nuevo')
        count=self.service.preparar_conteo(**{**self.args,'request_id':uuid4(),'items':[{'producto_id':product.pk,'unidad':'pza','fuente_unidad':'catálogo'}]})
        self.assertNotEqual(count.pk,self.count.pk)

    def test_inactive_user_or_branch_cannot_capture(self):
        self.act('iniciar')
        get_user_model().objects.filter(pk=self.user.pk).update(is_active=False)
        with self.assertRaises(self.service.ConteoError): self.act('guardar',self.readings())
        get_user_model().objects.filter(pk=self.user.pk).update(is_active=True)
        Sucursal.objects.filter(pk=self.branch.pk).update(activa=False)
        with self.assertRaises(self.service.ConteoError): self.act('guardar',self.readings())

    def test_missing_insumo_point_code_rejected_even_with_internal_code(self):
        from maestros.models import Insumo
        insumo=Insumo.objects.create(codigo='internal',nombre='Sin Point')
        with self.assertRaises(self.service.ConteoError):
            self.service.preparar_conteo(**{**self.args,'request_id':uuid4(),'items':[{'insumo_id':insumo.pk,'unidad':'kg','fuente_unidad':'catálogo'}]})


    def fresh_reference(self, start_minutes=5, end_minutes=2):
        from datetime import timedelta
        from pos_bridge.models import PointSyncJob
        self.count.refresh_from_db()
        job=PointSyncJob.objects.create(status='SUCCESS',job_type='inventory',started_at=self.count.iniciado_en-timedelta(minutes=start_minutes),finished_at=self.count.iniciado_en-timedelta(minutes=end_minutes))
        return {'estado':'REFERENCIA','corte_verificado':False,'lineas':{str(line.pk):{'cantidad':'1','unidad':'pza','sync_job_id':job.pk,'inicio_extraccion':job.started_at.isoformat(),'fin_extraccion':job.finished_at.isoformat(),'capturado_en':(self.count.iniciado_en-timedelta(minutes=3)).isoformat()} for line in self.count.lineas.all()}}

    def test_guardar_enviar_require_explicit_start(self):
        for action in ('guardar','enviar'):
            with self.subTest(action=action), self.assertRaises(self.service.ConteoError):
                self.act(action,self.readings())
        self.count.refresh_from_db()
        self.assertIsNone(self.count.iniciado_en)
        self.assertEqual(self.count.version,1)

    def test_start_is_server_timestamp_audited_idempotent(self):
        key=uuid4()
        result=self.act('iniciar',request_id=key)
        self.assertEqual(result,self.act('iniciar',version=1,request_id=key))
        self.count.refresh_from_db()
        self.assertIsNotNone(self.count.iniciado_en)
        event=self.count.eventos.get(action='iniciar')
        self.assertEqual(event.payload['ronda'],1)
        self.assertEqual(event.payload['iniciado_en'],self.count.iniciado_en.isoformat())
        with self.assertRaises(self.service.ConteoConflict): self.act('iniciar')
        self.act('enviar',self.readings())
        with self.assertRaises(self.service.ConteoConflict): self.act('iniciar')

    def test_start_rejects_client_timestamp_and_reviewer_only(self):
        with self.assertRaises(self.service.ConteoError): self.act('iniciar',{'iniciado_en':'2020-01-01'})
        reviewer=get_user_model().objects.create_user('review_only')
        self.models.AccesoConteoSucursal.objects.create(user=reviewer,sucursal=self.branch,revisar=True)
        with self.assertRaises(self.service.ConteoError): self.act('iniciar',actor=reviewer)

    def test_recount_resets_start_reference_and_preserves_evidence(self):
        self.act('iniciar')
        self.act('enviar',self.readings())
        reference=self.fresh_reference()
        with patch('inventario.conteos_point.referencia_conteo',return_value=reference): self.act('referencia',actor=self.admin)
        self.act('reconteo',{'linea_ids':[self.count.lineas.first().pk],'motivo':'Revisar'},actor=self.admin)
        self.count.refresh_from_db()
        self.assertIsNone(self.count.iniciado_en)
        self.assertEqual(self.count.referencia,{})
        self.assertEqual(self.count.eventos.get(action='referencia').payload['referencia'],reference)
        with self.assertRaises(self.service.ConteoError): self.act('guardar',{})
        self.act('iniciar')
        self.assertEqual(self.count.eventos.filter(action='iniciar').count(),2)

    def test_reference_rejects_cycle_started_too_early_or_finishes_after_start(self):
        self.act('iniciar')
        self.act('enviar',self.readings())
        for start,end in ((30,2),(5,-1)):
            reference=self.fresh_reference(start,end)
            with patch('inventario.conteos_point.referencia_conteo',return_value=reference): self.act('referencia',actor=self.admin)
            with self.assertRaises(self.service.ConteoError): self.act('validar_referencia',{'motivo':'Revisado','confirmado':True},actor=self.admin)

    def test_reference_rejects_mixed_or_unproven_sync_jobs(self):
        self.act('iniciar')
        self.act('enviar',self.readings())
        for changed_id in (None,999999):
            reference=self.fresh_reference()
            next(iter(reference['lineas'].values()))['sync_job_id']=changed_id
            with patch('inventario.conteos_point.referencia_conteo',return_value=reference): self.act('referencia',actor=self.admin)
            with self.assertRaises(self.service.ConteoError): self.act('validar_referencia',{'motivo':'Revisado','confirmado':True},actor=self.admin)

    def test_reference_rejects_mixed_rounds(self):
        self.act('iniciar')
        self.act('enviar',self.readings())
        line=self.count.lineas.first()
        self.act('reconteo',{'linea_ids':[line.pk],'motivo':'Revisar'},actor=self.admin)
        self.act('iniciar')
        self.act('enviar',{'lecturas':{str(line.pk):{'cantidad':'4'}}})
        reference=self.fresh_reference()
        with patch('inventario.conteos_point.referencia_conteo',return_value=reference): self.act('referencia',actor=self.admin)
        with self.assertRaises(self.service.ConteoError): self.act('validar_referencia',{'motivo':'Revisado','confirmado':True},actor=self.admin)


from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from django.db import close_old_connections, connections
from django.test import TransactionTestCase


class ConteosConcurrencyTests(TransactionTestCase):
    def setUp(self):
        from inventario import services_conteos as service
        from inventario import models_conteos as models
        self.service, self.models = service, models
        self.admin=get_user_model().objects.create_superuser('parallel_admin',password='test')
        self.user=get_user_model().objects.create_user('parallel_capture',password='test')
        self.branches=[Sucursal.objects.create(codigo=f'PAR{i}',nombre=f'Paralela {i}') for i in (1,2)]
        for branch in self.branches:
            self.models.AccesoConteoSucursal.objects.create(user=self.user,sucursal=branch,capturar=True)
        self.product=PointProduct.objects.create(external_id='par-id',sku='par-code',name='Paralelo')

    def create(self,branch=None,key=None):
        return self.service.preparar_conteo(actor=get_user_model().objects.get(pk=self.admin.pk),sucursal=branch or self.branches[0],responsable=self.user,fecha=date(2026,9,7),titulo='Paralelo',items=[{'producto_id':self.product.pk,'unidad':'pza','fuente_unidad':'catálogo'}],request_id=key or uuid4())

    def parallel(self,first,second):
        barrier=Barrier(2)
        def run(fn):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                return ('ok',fn())
            except self.service.ConteoConflict as exc:
                return ('conflict',str(exc))
            finally:
                connections.close_all()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(run,fn) for fn in (first,second)]
            return [f.result(timeout=20) for f in futures]

    def test_parallel_overlapping_creation_only_creates_one(self):
        results=self.parallel(lambda:self.create().pk,lambda:self.create().pk)
        self.assertCountEqual([r[0] for r in results],['ok','conflict'])
        self.assertEqual(self.models.ConteoSucursal.objects.count(),1)
        self.assertEqual(self.models.LineaConteoSucursal.objects.count(),1)
        self.assertEqual(self.models.EventoConteoSucursal.objects.count(),1)

    def test_parallel_creation_same_uuid_different_branches_is_conflict(self):
        key=uuid4()
        # Force both requests to pass their initial replay lookup before INSERT.
        insert_barrier=Barrier(2)
        original=self.models.ConteoSucursal.save
        def synchronized_save(instance,*args,**kwargs):
            if instance._state.adding: insert_barrier.wait(timeout=10)
            return original(instance,*args,**kwargs)
        with patch.object(self.models.ConteoSucursal,'save',synchronized_save):
            results=self.parallel(lambda:self.create(self.branches[0],key).pk,lambda:self.create(self.branches[1],key).pk)
        self.assertCountEqual([r[0] for r in results],['ok','conflict'])
        self.assertEqual(self.models.ConteoSucursal.objects.count(),1)
        self.assertEqual(self.models.LineaConteoSucursal.objects.count(),1)
        self.assertEqual(self.models.LecturaConteoSucursal.objects.count(),1)
        self.assertEqual(self.models.EventoConteoSucursal.objects.count(),1)

    def mutate(self,count,key):
        return self.service.ejecutar_accion(conteo_id=count.pk,actor=get_user_model().objects.get(pk=self.user.pk),action='guardar',version=2,request_id=key,payload={'lecturas':{str(count.lineas.get().pk):{'cantidad':'7'}}})

    def test_parallel_same_uuid_action_replays_one_mutation(self):
        count=self.create()
        self.service.ejecutar_accion(conteo_id=count.pk,actor=self.user,action='iniciar',version=1,request_id=uuid4(),payload={})
        key=uuid4()
        results=self.parallel(lambda:self.mutate(count,key),lambda:self.mutate(count,key))
        self.assertEqual([r[0] for r in results],['ok','ok'])
        self.assertEqual(results[0][1],results[1][1])
        count.refresh_from_db()
        self.assertEqual(count.version,3)
        self.assertEqual(count.operaciones.count(),2)
        self.assertEqual(count.eventos.filter(action='guardar').count(),1)
        self.assertEqual(self.models.LecturaConteoSucursal.objects.get().cantidad,Decimal('7'))

    def test_parallel_same_version_different_uuid_rejects_loser(self):
        count=self.create()
        self.service.ejecutar_accion(conteo_id=count.pk,actor=self.user,action='iniciar',version=1,request_id=uuid4(),payload={})
        results=self.parallel(lambda:self.mutate(count,uuid4()),lambda:self.mutate(count,uuid4()))
        self.assertCountEqual([r[0] for r in results],['ok','conflict'])
        count.refresh_from_db()
        self.assertEqual(count.version,3)
        self.assertEqual(count.operaciones.count(),2)
        self.assertEqual(count.eventos.filter(action='guardar').count(),1)
