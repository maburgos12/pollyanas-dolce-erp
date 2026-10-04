from datetime import date, timedelta
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.test import TransactionTestCase
from django.db import close_old_connections, connection
from django.core.exceptions import PermissionDenied
from rest_framework.test import APIClient
from activos.models import Activo, PlanMantenimiento, OrdenMantenimiento, BitacoraMantenimiento
from core.models import AuditLog, Sucursal
from mantenimiento.models import ComprobanteCapturaEquipo
from mantenimiento.services_capturas_equipos import CapturaEquipoError


class PlanesTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('planes', is_superuser=True)
        self.branch = Sucursal.objects.create(codigo='PL', nombre='Planes')
        self.asset = Activo.objects.create(codigo='PL-1', nombre='Horno', sucursal=self.branch)
        self.plan = PlanMantenimiento.objects.create(activo_ref=self.asset, nombre='Limpieza', frecuencia_dias=10, proxima_ejecucion=date(2026, 10, 1))
        self.key = uuid4()

    def capture(self, **kwargs):
        from mantenimiento.services_planes import registrar_ejecucion_plan
        return registrar_ejecucion_plan(usuario=self.user, plan_id=self.plan.pk, clave=self.key, fecha=date(2026, 10, 2), notas='Hecho', **kwargs)

    def test_replay_conflict_deleted_and_two_jobs(self):
        first, replay = self.capture()
        second, replay = self.capture()
        self.assertTrue(replay)
        self.assertEqual(first.pk, second.pk)
        from mantenimiento.services_planes import registrar_ejecucion_plan
        with self.assertRaises(CapturaEquipoError):
            registrar_ejecucion_plan(usuario=self.user, plan_id=self.plan.pk, clave=self.key, fecha=date(2026, 10, 2), notas='Otro')
        self.key = uuid4()
        self.capture()
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)
        first.delete()
        self.key = ComprobanteCapturaEquipo.objects.filter(orden__isnull=True).get().clave
        with self.assertRaises(CapturaEquipoError) as error:
            self.capture()
        self.assertEqual(error.exception.status_code, 410)

    def test_active_order_requires_explicit_additional(self):
        pending = OrdenMantenimiento.objects.create(activo_ref=self.asset, plan_ref=self.plan, descripcion='Abierta')
        with self.assertRaises(CapturaEquipoError) as error:
            self.capture()
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)
        self.plan.refresh_from_db()
        self.assertIsNone(self.plan.ultima_ejecucion)
        with self.assertRaises(CapturaEquipoError):
            self.capture(adicional=True)
        self.capture(adicional=True, motivo_adicional='Trabajo independiente')
        pending.refresh_from_db()
        self.assertEqual(pending.estatus, OrdenMantenimiento.ESTATUS_PENDIENTE)
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)

    def test_rollback_all_rows_and_dates(self):
        with patch('mantenimiento.services_capturas_equipos.log_event', side_effect=RuntimeError('audit')):
            with self.assertRaises(RuntimeError):
                self.capture()
        self.plan.refresh_from_db()
        self.assertIsNone(self.plan.ultima_ejecucion)
        self.assertEqual(self.plan.proxima_ejecucion, date(2026, 10, 1))
        for model in (OrdenMantenimiento, BitacoraMantenimiento, AuditLog, ComprobanteCapturaEquipo):
            self.assertEqual(model.objects.count(), 0)

    def test_revoked_and_inactive_plan_before_replay(self):
        self.capture()
        with patch('mantenimiento.services_planes.authorized_branch_ids', return_value=[]):
            with self.assertRaises(PermissionDenied):
                self.capture(movil=True)
        # Web's original permission is global and does not inherit mobile scope.
        with patch('mantenimiento.services_planes.authorized_branch_ids', return_value=[]):
            self.capture()
        self.plan.activo = False
        self.plan.save()
        with self.assertRaises(PermissionDenied):
            self.capture()

    def parallel(self, call):
        barrier = Barrier(2)
        def run(_):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                return call()
            finally:
                connection.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(run, range(2)))

    def test_concurrent_capture_one_order_bit_receipt_audit(self):
        result = self.parallel(lambda: self.capture()[0].pk)
        self.assertEqual(result[0], result[1])
        for model in (OrdenMantenimiento, BitacoraMantenimiento, AuditLog, ComprobanteCapturaEquipo):
            self.assertEqual(model.objects.count(), 1)

    def test_generator_concurrency_cancelled_and_agenda_only(self):
        from activos.services_planes import generar_ordenes_programadas, actualizar_agenda_plan
        self.parallel(lambda: generar_ordenes_programadas(usuario=self.user, today=date(2026, 10, 2)))
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        order = OrdenMantenimiento.objects.get()
        order.estatus = OrdenMantenimiento.ESTATUS_CANCELADA
        order.save()
        result = generar_ordenes_programadas(usuario=self.user, today=date(2026, 10, 2), dry_run=True)
        self.assertEqual(result['created'], 1)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        generar_ordenes_programadas(usuario=self.user, today=date(2026, 10, 2))
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)
        actualizar_agenda_plan(usuario=self.user, plan_id=self.plan.pk, fecha=date(2026, 10, 3))
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)
        self.plan.refresh_from_db()
        self.assertEqual(self.plan.proxima_ejecucion, date(2026, 10, 13))

    def test_mobile_legacy_and_uuid_cost_visibility(self):
        api = APIClient()
        api.force_authenticate(self.user)
        url = f'/api/mantenimiento/resumen/planes/{self.plan.pk}/ejecutar/'
        self.assertEqual(api.post(url, {'notas': 'Legacy'}, format='json').status_code, 200)
        data = {'clave_captura': str(self.key), 'fecha_ejecucion': '2026-10-02', 'notas': 'UUID'}
        self.assertEqual(api.post(url, data, format='json').status_code, 201)
        with patch('mantenimiento.views.can_view_costs', return_value=False):
            replay = api.post(url, data, format='json')
        self.assertEqual(replay.status_code, 200)
        self.assertFalse({'costo_repuestos', 'costo_mano_obra', 'costo_otros', 'costo_total'} & replay.data['orden'].keys())

    def test_paused_but_active_plan_keeps_original_execution_permission(self):
        self.plan.estatus = PlanMantenimiento.ESTATUS_PAUSADO
        self.plan.save()
        self.capture()
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)

    def test_generator_revalidates_actor_after_candidate_query(self):
        from activos.services_planes import generar_ordenes_programadas
        get_user_model().objects.filter(pk=self.user.pk).update(is_active=False)
        result = generar_ordenes_programadas(usuario=self.user, today=date(2026, 10, 2))
        self.assertEqual(result['failed_plan'], self.plan.pk)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_web_reader_global_scope_and_mobile_scope_preserved(self):
        from core.models import UserModuleAccess, UserProfile
        reader = get_user_model().objects.create_user('plans-reader')
        other = Sucursal.objects.create(codigo='OTHER', nombre='Otra')
        UserProfile.objects.update_or_create(user=reader, defaults={'sucursal': other})
        UserModuleAccess.objects.create(user=reader, module='mantenimiento.app', access='view')
        self.client.force_login(reader)
        url = f'/mantenimiento/planes/{self.plan.pk}/ejecutar/'
        from django.urls import reverse
        url = reverse('mantenimiento:mant-plan-ejecutar', args=[self.plan.pk])
        data = {'clave_captura': str(uuid4()), 'fecha_ejecucion': '2026-10-02', 'notas': 'Lectura global original'}
        response = self.client.post(url, data, HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(self.client.post(url, data, HTTP_ACCEPT='application/json').status_code, 200)
        api = APIClient(); api.force_authenticate(reader)
        mobile = f'/api/mantenimiento/resumen/planes/{self.plan.pk}/ejecutar/'
        self.assertEqual(api.post(mobile, data, format='json').status_code, 403)
        UserModuleAccess.objects.filter(user=reader).update(access='manage')
        writer = get_user_model().objects.get(pk=reader.pk)
        api.force_authenticate(writer)
        self.assertEqual(api.post(mobile, data, format='json').status_code, 404)
        UserProfile.objects.filter(user=reader).update(sucursal=self.branch)
        api.force_authenticate(get_user_model().objects.get(pk=reader.pk))
        data['clave_captura'] = str(uuid4())
        self.assertEqual(api.post(mobile, data, format='json').status_code, 201)
        UserProfile.objects.filter(user=reader).update(sucursal=other)
        # The service refreshes scope even when authentication supplies a cached actor.
        self.assertEqual(api.post(mobile, data, format='json').status_code, 403)

    def test_generator_rereads_date_after_waiting_for_lock(self):
        from django.db import transaction
        from django.db.models.query import QuerySet
        from threading import Event
        from activos.services_planes import generar_ordenes_programadas
        arrived = Event(); original = QuerySet.select_for_update
        def locking(qs, *args, **kwargs):
            if qs.model is PlanMantenimiento: arrived.set()
            return original(qs, *args, **kwargs)
        def generate():
            close_old_connections()
            try: return generar_ordenes_programadas(usuario=self.user, today=date(2026, 10, 2))
            finally: connection.close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                plan = PlanMantenimiento.objects.select_for_update().get(pk=self.plan.pk)
                with patch.object(QuerySet, 'select_for_update', locking):
                    future = pool.submit(generate)
                    self.assertTrue(arrived.wait(timeout=10))
                    plan.proxima_ejecucion = date(2026, 11, 1); plan.save()
            result = future.result(timeout=10)
        self.assertEqual(result['created'], 0)
        self.assertEqual(result['skipped'], 1)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_generator_and_close_have_no_deadlock(self):
        from activos.services_planes import generar_ordenes_programadas
        from activos.services_ordenes import cambiar_estatus_orden
        order = OrdenMantenimiento.objects.create(activo_ref=self.asset, plan_ref=self.plan, fecha_programada=self.plan.proxima_ejecucion, descripcion='Programada')
        with patch('activos.services_ordenes.timezone.localdate', return_value=date(2026, 10, 2)):
            result = self.parallel(lambda: generar_ordenes_programadas(usuario=self.user, today=date(2026, 10, 2)) if __import__('threading').current_thread().name.endswith('_0') else cambiar_estatus_orden(order.pk, OrdenMantenimiento.ESTATUS_CERRADA, self.user))
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        order.refresh_from_db(); self.assertEqual(order.estatus, OrdenMantenimiento.ESTATUS_CERRADA)

    def test_partial_generation_failure_keeps_completed_plan_and_rolls_back_failed(self):
        from activos.services_planes import generar_ordenes_programadas
        second = PlanMantenimiento.objects.create(activo_ref=self.asset, nombre='Segundo', proxima_ejecucion=date(2026,10,2))
        from core.audit import log_event
        def audit(*args, **kwargs):
            if args[4]['plan_id'] == second.pk: raise RuntimeError('audit failed')
            return log_event(*args, **kwargs)
        with patch('activos.services_planes.log_event', side_effect=audit):
            result = generar_ordenes_programadas(usuario=self.user, today=date(2026,10,2))
        self.assertEqual(result, {'created':1, 'skipped':0, 'failed_plan':second.pk})
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
        self.assertEqual(AuditLog.objects.count(), 1)

    def test_web_async_definitive_rejection_is_json(self):
        from django.urls import reverse
        self.client.force_login(self.user)
        self.plan.activo = False; self.plan.save()
        response = self.client.post(reverse('mantenimiento:mant-plan-ejecutar', args=[self.plan.pk]), {'clave_captura':str(self.key)}, HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code,404)
        self.assertEqual(response.headers['Content-Type'],'application/json')

    def test_uuid_replay_keeps_original_date_but_returns_current_agenda_and_hides_edited_costs(self):
        from django.contrib.auth.models import Group
        from core.models import UserModuleAccess
        from activos.services_planes import actualizar_agenda_plan
        actor = get_user_model().objects.create_user('plan-cost-limited')
        actor.groups.add(Group.objects.get_or_create(name='mantenimiento')[0])
        UserModuleAccess.objects.create(user=actor, module='mantenimiento', access='view')
        api = APIClient(); api.force_authenticate(actor)
        url = f'/api/mantenimiento/resumen/planes/{self.plan.pk}/ejecutar/'
        data = {'clave_captura':str(self.key),'notas':'Original'}
        with patch('mantenimiento.services_planes.timezone.localdate',return_value=date(2026,10,2)):
            first = api.post(url,data,format='json')
        self.assertEqual(first.status_code,201)
        order_id = first.data['orden']['id']
        OrdenMantenimiento.objects.filter(pk=order_id).update(costo_repuestos='900',costo_mano_obra='800',costo_otros='700')
        actualizar_agenda_plan(usuario=self.user,plan_id=self.plan.pk,fecha=date(2026,10,5))
        with patch('mantenimiento.services_planes.timezone.localdate',return_value=date(2026,10,3)):
            replay = api.post(url,data,format='json')
        self.assertEqual(replay.status_code,200)
        self.assertEqual(replay.data['proxima_ejecucion'],'2026-10-15')
        self.assertEqual(OrdenMantenimiento.objects.get(pk=order_id).fecha_cierre,date(2026,10,2))
        self.assertFalse({'costo_repuestos','costo_mano_obra','costo_otros','costo_total'} & replay.data['orden'].keys())

    def test_summary_uses_mazatlan_date_when_utc_is_next_day(self):
        from datetime import datetime, timezone as dt_timezone
        api = APIClient(); api.force_authenticate(self.user)
        with patch('django.utils.timezone.now', return_value=datetime(2026,10,3,3,0,tzinfo=dt_timezone.utc)):
            response = api.get('/api/mantenimiento/resumen/')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.data['fecha'],'2026-10-02')

    def test_generation_and_additional_execution_have_no_deadlock(self):
        from activos.services_planes import generar_ordenes_programadas
        barrier=Barrier(2)
        def run(index):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                if index==0:
                    return generar_ordenes_programadas(usuario=self.user,today=date(2026,10,2))
                return self.capture(adicional=True,motivo_adicional='Trabajo independiente')[0].pk
            finally: connection.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(run,index) for index in range(2)]
            result=[future.result(timeout=10) for future in futures]
        self.assertIsNone(result[0]['failed_plan'])
        self.assertEqual(OrdenMantenimiento.objects.filter(estatus=OrdenMantenimiento.ESTATUS_CERRADA).count(),1)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(),1)
        self.assertIn(OrdenMantenimiento.objects.count(),[1,2])
