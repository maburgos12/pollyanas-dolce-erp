from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied
from django.db import close_old_connections, connection
from django.http import Http404
from django.test import TransactionTestCase
from rest_framework.test import APIClient
from activos.models import Activo, PlanMantenimiento, OrdenMantenimiento
from core.models import AuditLog, Sucursal, UserModuleAccess, UserProfile
from mantenimiento.models import ComprobanteConfiguracionPlan
from mantenimiento.services_configuracion_planes import configurar_plan
from mantenimiento.services_capturas_equipos import CapturaEquipoError
from mantenimiento.views import _guardar_plan_desde_data


class ConfiguracionPlanesTests(TransactionTestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user('config-admin', is_superuser=True)
        self.branch = Sucursal.objects.create(codigo='CP1', nombre='Uno')
        self.other = Sucursal.objects.create(codigo='CP2', nombre='Dos')
        self.asset = Activo.objects.create(codigo='CP1-1', nombre='Horno', sucursal=self.branch)
        self.foreign = Activo.objects.create(codigo='CP2-1', nombre='Horno dos', sucursal=self.other)
        self.user = get_user_model().objects.create_user('config-branch')
        UserProfile.objects.update_or_create(user=self.user, defaults={'sucursal': self.branch})
        UserModuleAccess.objects.create(user=self.user, module='mantenimiento.app', access='manage')
        self.key = uuid4()
        self.data = {'activo_id': self.asset.pk, 'nombre': 'Limpieza', 'frecuencia_dias': 10, 'clave_captura': str(self.key)}

    def config(self, user=None, data=None, op='plan_create', plan_id=None):
        return configurar_plan(usuario=user or self.user, operacion=op, data=self.data if data is None else data,
            guardar=_guardar_plan_desde_data, plan_id=plan_id)

    def api(self, user=None):
        client = APIClient(); client.force_authenticate(user=user or self.user); return client

    def test_scoped_routes_view_no_profile_and_global_modes(self):
        plan, _ = self.config()
        other = PlanMantenimiento.objects.create(activo_ref=self.foreign, nombre='Fuera')
        client = self.api()
        self.assertEqual([p['id'] for p in client.get('/api/mantenimiento/planes/').json()['items']], [plan.pk])
        self.assertEqual(client.patch(f'/api/mantenimiento/planes/{other.pk}/', {'nombre':'Cruce'}, format='json').status_code, 404)
        self.assertEqual(client.delete(f'/api/mantenimiento/planes/{other.pk}/').status_code, 404)
        self.assertEqual(client.post('/api/mantenimiento/planes/', {**self.data,'activo_id':self.foreign.pk}, format='json').status_code, 404)
        access = UserModuleAccess.objects.get(user=self.user); access.access='view'; access.save()
        self.assertEqual(self.api().get('/api/mantenimiento/planes/').status_code, 200)
        self.assertEqual(self.api().post('/api/mantenimiento/planes/', self.data, format='json').status_code, 403)
        UserProfile.objects.filter(user=self.user).delete()
        self.assertEqual(self.api().get('/api/mantenimiento/planes/').json()['items'], [])
        for group in ['DG', 'mantenimiento']:
            self.user.groups.add(Group.objects.get_or_create(name=group)[0])
            self.assertEqual(self.api().get('/api/mantenimiento/planes/').json()['pagination']['count'], 2)
            self.user.groups.clear()
        for module in ['mantenimiento', 'mantenimiento.bandeja']:
            row = UserModuleAccess.objects.create(user=self.user, module=module, access='manage')
            self.assertEqual(self.api().get('/api/mantenimiento/planes/').json()['pagination']['count'], 2)
            row.delete()

    def test_pagination_filters_before_limit_and_scoped_asset_catalog(self):
        PlanMantenimiento.objects.bulk_create([PlanMantenimiento(activo_ref=self.foreign, nombre=f'Fuera {i}', proxima_ejecucion=date(2025,1,1)) for i in range(130)])
        PlanMantenimiento.objects.bulk_create([PlanMantenimiento(activo_ref=self.asset, nombre=f'Propio {i}', proxima_ejecucion=date(2026,1,1)) for i in range(125)])
        first = self.api().get('/api/mantenimiento/planes/').json()
        second = self.api().get('/api/mantenimiento/planes/?page=2').json()
        self.assertEqual(first['pagination']['count'],125)
        self.assertTrue(first['pagination']['has_next']); self.assertEqual(len(first['items']),120)
        self.assertEqual(len(second['items']),5); self.assertFalse(second['pagination']['has_next'])
        self.assertTrue(all(p['activo_id']==self.asset.pk for p in first['items']+second['items']))
        self.assertEqual([a['id'] for a in self.api().get('/api/mantenimiento/planes/?catalogo=activos').json()['items']], [self.asset.pk])
        self.assertEqual(self.api(self.admin).get('/api/mantenimiento/planes/').json()['pagination']['count'],255)
        summary = self.api().get('/api/mantenimiento/resumen/').json()
        scoped = [row for row in summary['agenda'] if row['tipo']=='plan']
        self.assertTrue(scoped)
        self.assertEqual(summary['planes_coverage'], {'total':125,'mostrados':30,'parcial':True})
        self.assertEqual(summary['agenda_counts']['vencidos'],30)
        self.assertTrue(all(row['codigo']==self.asset.codigo for row in scoped))

    def test_replay_changed_deleted_and_legitimate_duplicates(self):
        first, replay = self.config(); second, replay = self.config()
        self.assertTrue(replay); self.assertEqual(first.pk, second.pk)
        with self.assertRaises(CapturaEquipoError) as exc: self.config(data={**self.data,'nombre':'Cambió'})
        self.assertEqual(exc.exception.status_code,409)
        self.config(data={**self.data,'clave_captura':str(uuid4())})
        self.assertEqual(PlanMantenimiento.objects.count(),2)
        first.delete()
        with self.assertRaises(CapturaEquipoError) as exc: self.config()
        self.assertEqual(exc.exception.status_code,410)
        self.assertEqual(PlanMantenimiento.objects.count(),1)

    def test_uuid_changed_to_another_authorized_asset_conflicts_without_leaking_foreign_asset(self):
        self.config(user=self.admin)
        with self.assertRaises(CapturaEquipoError) as exc:
            self.config(user=self.admin, data={**self.data, 'activo_id': self.foreign.pk})
        self.assertEqual(exc.exception.status_code, 409)
        self.config()
        with self.assertRaises(Http404):
            self.config(data={**self.data, 'activo_id': self.foreign.pk})
        self.assertEqual(PlanMantenimiento.objects.count(), 2)

    def test_full_stale_configuration_cannot_overwrite_execution_and_exact_replay_ignores_revision(self):
        from mantenimiento.services_planes import registrar_ejecucion_plan
        from mantenimiento.views import _plan_payload
        plan, _ = self.config()
        plan.ultima_ejecucion = date(2026,9,1); plan.proxima_ejecucion = date(2026,9,11); plan.save()
        snapshot = _plan_payload(plan)
        registrar_ejecucion_plan(usuario=self.user,plan_id=plan.pk,clave=uuid4(),fecha=date(2026,10,1),movil=True)
        payload = {**snapshot,'nombre':'Editado desde formulario anterior','clave_captura':str(uuid4())}
        with self.assertRaises(CapturaEquipoError) as error:
            self.config(op='plan_update',data=payload,plan_id=plan.pk)
        self.assertEqual(error.exception.status_code,409)
        self.assertEqual(error.exception.detail['error_code'],'plan_revision_conflict')
        plan.refresh_from_db()
        self.assertEqual(plan.ultima_ejecucion,date(2026,10,1)); self.assertEqual(plan.proxima_ejecucion,date(2026,10,11))
        self.assertEqual(plan.nombre,'Limpieza');self.assertEqual(OrdenMantenimiento.objects.count(),1)
        self.assertEqual(ComprobanteConfiguracionPlan.objects.count(),1)
        with self.assertRaises(CapturaEquipoError):
            self.config(op='plan_delete',data={'revision_en':snapshot['revision_en'],'clave_captura':str(uuid4())},plan_id=plan.pk)
        plan.refresh_from_db(); self.assertTrue(plan.activo)
        current = _plan_payload(plan)
        update = {'nombre':'Nombre revisado','revision_en':current['revision_en'],'clave_captura':str(uuid4())}
        self.config(op='plan_update',data=update,plan_id=plan.pk)
        registrar_ejecucion_plan(usuario=self.user,plan_id=plan.pk,clave=uuid4(),fecha=date(2026,10,2),movil=True)
        result,replayed=self.config(op='plan_update',data=update,plan_id=plan.pk)
        self.assertTrue(replayed);self.assertEqual(result.ultima_ejecucion,date(2026,10,2))

    def test_update_delete_retry_and_deleted_create_tombstone(self):
        plan, _ = self.config()
        update = {'nombre':'Nuevo','clave_captura':str(uuid4())}
        updated, _ = self.config(op='plan_update', data=update, plan_id=plan.pk)
        replay, yes = self.config(op='plan_update', data=update, plan_id=plan.pk)
        self.assertTrue(yes); self.assertEqual(replay.nombre,updated.nombre)
        remove = {'clave_captura':str(uuid4())}
        self.config(op='plan_delete',data=remove,plan_id=plan.pk)
        _, yes = self.config(op='plan_delete',data=remove,plan_id=plan.pk); self.assertTrue(yes)
        for op, data, pk in [('plan_create',self.data,None),('plan_update',update,plan.pk)]:
            with self.assertRaises(CapturaEquipoError) as exc: self.config(op=op,data=data,plan_id=pk)
            self.assertEqual(exc.exception.status_code,410)

    def test_fresh_permission_and_move_before_write_and_on_replay(self):
        plan, _ = self.config()
        self.asset.sucursal=self.other; self.asset.save()
        with self.assertRaises(Http404): self.config()
        self.asset.sucursal=self.branch; self.asset.save()
        UserModuleAccess.objects.filter(user=self.user).update(access='view')
        with self.assertRaises(PermissionDenied): self.config()
        UserModuleAccess.objects.filter(user=self.user).update(access='manage')
        def move(plan, data):
            Activo.objects.filter(pk=self.asset.pk).update(sucursal=self.other)
            return _guardar_plan_desde_data(plan,data)
        # The lock protects outside transactions; an in-transaction move is detected too.
        with patch('mantenimiento.services_configuracion_planes._validar', wraps=__import__('mantenimiento.services_configuracion_planes',fromlist=['_validar'])._validar):
            with self.assertRaises(Http404):
                configurar_plan(usuario=self.user,operacion='plan_update',plan_id=plan.pk,data={'nombre':'Cambio'},guardar=move)
        plan.refresh_from_db(); self.assertEqual(plan.nombre,'Limpieza')

    def test_audit_failure_rolls_back_plan_receipt_and_legacy_optional_uuid(self):
        with patch('mantenimiento.services_configuracion_planes.log_event',side_effect=RuntimeError('audit')):
            with self.assertRaises(RuntimeError): self.config()
        self.assertEqual(PlanMantenimiento.objects.count(),0); self.assertEqual(ComprobanteConfiguracionPlan.objects.count(),0)
        self.assertEqual(AuditLog.objects.count(),0)
        client=self.api(self.admin)
        self.assertEqual(client.post('/api/mantenimiento/planes/', {'activo_id':self.asset.pk,'nombre':'Legacy'},format='json').status_code,201)

    def parallel(self, callbacks):
        barrier=Barrier(len(callbacks))
        def run(callback):
            close_old_connections()
            try:
                with connection.cursor() as cursor: cursor.execute("SET lock_timeout = '5s'")
                barrier.wait(timeout=10)
                return callback()
            finally: connection.close()
        with ThreadPoolExecutor(max_workers=len(callbacks)) as pool:
            return [f.result(timeout=20) for f in [pool.submit(run,c) for c in callbacks]]

    def test_concurrent_same_attempt_vs_two_legitimate_attempts(self):
        results=self.parallel([self.config,self.config])
        self.assertEqual(results[0][0].pk,results[1][0].pk)
        self.assertEqual(PlanMantenimiento.objects.count(),1)
        self.assertEqual(ComprobanteConfiguracionPlan.objects.count(),1)
        self.parallel([lambda:self.config(data={**self.data,'clave_captura':str(uuid4())}),lambda:self.config(data={**self.data,'clave_captura':str(uuid4())})])
        self.assertEqual(PlanMantenimiento.objects.count(),3)

    def test_config_parallel_execution_generation_and_closing(self):
        from mantenimiento.services_planes import registrar_ejecucion_plan
        from activos.services_planes import generar_ordenes_programadas
        from activos.services_ordenes import cambiar_estatus_orden
        plan,_=self.config(user=self.admin)
        def update(): return self.config(user=self.admin,op='plan_update',plan_id=plan.pk,data={'nombre':'Updated','clave_captura':str(uuid4())})
        self.parallel([update,lambda:registrar_ejecucion_plan(usuario=self.admin,plan_id=plan.pk,clave=uuid4(),fecha=date(2026,10,1))])
        plan.refresh_from_db(); self.assertEqual(plan.ultima_ejecucion,date(2026,10,1))
        plan.proxima_ejecucion=date(2020,1,1);plan.save()
        results = self.parallel([update,lambda:generar_ordenes_programadas(usuario=self.admin,today=date(2026,10,4))])
        self.assertIsNone(results[1]['failed_plan'])
        self.assertEqual(results[1]['created'], 1)
        order=OrdenMantenimiento.objects.filter(plan_ref=plan,estatus=OrdenMantenimiento.ESTATUS_PENDIENTE).get()
        self.parallel([lambda:self.config(user=self.admin,op='plan_delete',plan_id=plan.pk,data={'clave_captura':str(uuid4())}),lambda:cambiar_estatus_orden(order.pk,OrdenMantenimiento.ESTATUS_CERRADA,self.admin)])
        plan.refresh_from_db(); self.assertFalse(plan.activo); self.assertIsNotNone(plan.ultima_ejecucion)
