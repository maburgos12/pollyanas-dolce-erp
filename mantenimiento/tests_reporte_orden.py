from uuid import uuid4
from django.contrib.auth import get_user_model
from django.test import TransactionTestCase
from activos.models import Activo, OrdenMantenimiento, BitacoraMantenimiento
from core.models import Sucursal, AuditLog
from fallas.models import CategoriaFalla, ReporteFalla
from mantenimiento.models import ComprobanteCapturaEquipo, VinculoAtencionEquipo


class OrdenDesdeReporteTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('p3c2', is_superuser=True)
        self.branch = Sucursal.objects.create(codigo='P3', nombre='Prueba')
        self.asset = Activo.objects.create(codigo='P3-1', nombre='Horno', sucursal=self.branch)
        self.category = CategoriaFalla.objects.create(nombre='Horno', tipo='equipo')
        self.report = ReporteFalla.objects.create(sucursal=self.branch, activo_relacionado=self.asset,
            categoria=self.category, titulo='Falla', descripcion='Revisar horno', reportado_por=self.user,
            costo_estimado='123.00', costo_real='98.00', proveedor_servicio='Proveedor original', foto_evidencia='original.jpg')
        self.data = dict(descripcion='Inspeccionar', prioridad='ALTA', fecha_programada='2026-10-05', responsable='Técnico', clave_captura=str(uuid4()))

    def create(self, data=None, user=None):
        from mantenimiento.services_reporte_orden import crear_orden_desde_reporte
        return crear_orden_desde_reporte(usuario=user or self.user, reporte_id=self.report.pk, datos=data or self.data)

    def test_atomic_creation_preserves_source_and_replay(self):
        source = ReporteFalla.objects.filter(pk=self.report.pk).values().get()
        order, replay = self.create()
        self.assertFalse(replay)
        same, replay = self.create()
        self.assertTrue(replay)
        self.assertEqual(same.pk, order.pk)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
        self.assertEqual(VinculoAtencionEquipo.objects.count(), 1)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(model='mantenimiento.VinculoAtencionEquipo').count(), 1)
        self.assertEqual(AuditLog.objects.filter(model='activos.OrdenMantenimiento').count(), 1)
        self.assertEqual(ReporteFalla.objects.filter(pk=self.report.pk).values().get(), source)
        self.assertEqual((order.tipo, order.estatus, order.origen), ('CORRECTIVO', 'PENDIENTE', 'SOLICITUD'))
        self.assertIsNone(order.fecha_inicio)
        self.assertIsNone(order.fecha_cierre)
        self.assertEqual(order.costo_total, 0)
        self.assertFalse(order.factura_archivo)
        self.assertIsNone(order.proveedor_servicio_id)
        self.assertEqual(order.responsable, 'Técnico')

    def test_active_blocks_distinct_attempt_closed_allows_new_and_final_replay(self):
        from mantenimiento.services_capturas_equipos import CapturaEquipoError
        order, _ = self.create()
        with self.assertRaises(CapturaEquipoError):
            self.create({**self.data, 'clave_captura':str(uuid4())})
        order.estatus = 'CERRADA'
        order.save(update_fields=['estatus'])
        second, _ = self.create({**self.data, 'clave_captura':str(uuid4())})
        self.assertNotEqual(second.pk, order.pk)
        self.report.estatus = 'cerrado'
        self.report.save(update_fields=['estatus'])
        original, replay = self.create()
        self.assertEqual(original.pk, order.pk)
        self.assertTrue(replay)
        with self.assertRaises(CapturaEquipoError):
            self.create({**self.data, 'clave_captura':str(uuid4())})
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)

    def test_conflict_deleted_and_atomic_link_failure(self):
        from unittest.mock import patch
        from mantenimiento.services_capturas_equipos import CapturaEquipoError
        order, _ = self.create()
        with self.assertRaises(CapturaEquipoError) as error:
            self.create({**self.data, 'descripcion':'Otro'})
        self.assertEqual(error.exception.status_code, 409)
        VinculoAtencionEquipo.objects.all().delete()
        order.delete()
        with self.assertRaises(CapturaEquipoError) as error:
            self.create()
        self.assertEqual(error.exception.status_code, 410)
        with patch('mantenimiento.services_reporte_orden.guardar_vinculo', side_effect=RuntimeError('link failed')):
            with self.assertRaises(RuntimeError):
                self.create({**self.data, 'clave_captura':str(uuid4())})
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 0)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 1)

    def test_invalid_report_and_equipment_permission_reject_without_partials(self):
        from django.core.exceptions import PermissionDenied
        from mantenimiento.services_capturas_equipos import CapturaEquipoError
        for field, value in [('estatus','resuelto'), ('duplicado_de',self.report), ('tipo_objetivo','INSTALACION'), ('activo_relacionado',None)]:
            original = getattr(self.report, field)
            setattr(self.report, field, value)
            self.report.save(update_fields=[field])
            with self.assertRaises(CapturaEquipoError):
                self.create()
            setattr(self.report, field, original)
            self.report.save(update_fields=[field])
        other = Sucursal.objects.create(codigo='O', nombre='Otra')
        self.asset.sucursal = other
        self.asset.save(update_fields=['sucursal'])
        with self.assertRaises(CapturaEquipoError):
            self.create()
        self.asset.activo = False
        self.asset.save(update_fields=['activo'])
        with self.assertRaises(PermissionDenied):
            self.create()
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)

    def test_concurrent_same_distinct_keys_and_actors(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections, connection
        from mantenimiento.services_capturas_equipos import CapturaEquipoError
        second = get_user_model().objects.create_user('p3c2-other', is_superuser=True)
        for same_key, same_user in [(True,True), (False,True), (False,False)]:
            barrier = Barrier(2)
            def send(index):
                close_old_connections()
                try:
                    actor = get_user_model().objects.get(pk=self.user.pk if same_user or index == 0 else second.pk)
                    data = {**self.data, 'clave_captura':self.data['clave_captura'] if same_key else str(uuid4())}
                    barrier.wait(timeout=10)
                    try:
                        order, replay = self.create(data, actor)
                        return order.pk
                    except CapturaEquipoError:
                        return None
                finally:
                    connection.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                ids = list(pool.map(send, [0,1]))
            self.assertEqual(OrdenMantenimiento.objects.filter(estatus='PENDIENTE').count(), 1)
            if same_key:
                self.assertEqual(ids[0], ids[1])
            else:
                self.assertEqual(ids.count(None), 1)
            VinculoAtencionEquipo.objects.all().delete()
            ComprobanteCapturaEquipo.objects.all().delete()
            OrdenMantenimiento.objects.all().delete()

    def test_api_context_creation_and_bidirectional_history(self):
        from rest_framework.test import APIClient
        api = APIClient()
        api.force_authenticate(self.user)
        url = f'/api/mantenimiento/v2/reportes/{self.report.pk}/orden/'
        context = api.get(url)
        self.assertEqual(context.status_code, 200)
        self.assertTrue(context.data['puede_crear'])
        first = api.post(url, self.data, format='json')
        replay = api.post(url, self.data, format='json')
        self.assertEqual((first.status_code, replay.status_code), (201,200))
        self.assertEqual(first.data['folio'], replay.data['folio'])
        order_id = first.data['orden_id']
        links = api.get(f'/api/mantenimiento/v2/items/orden/{order_id}/vinculos/')
        self.assertEqual(links.data['results'][0]['documento']['id'], self.report.pk)
        detail = api.get(f'/api/mantenimiento/v2/items/falla/{self.report.pk}/')
        self.assertEqual(detail.status_code, 200)
        context = api.get(url)
        self.assertFalse(context.data['puede_crear'])
        self.assertEqual(context.data['activas'][0]['id'], order_id)
        history = api.get(f'/api/mantenimiento/v2/historial/?tipo=sin_reporte&activo={self.asset.pk}')
        self.assertEqual(history.data['pagination']['total'], 0)

    def test_payload_validation_and_permissions_before_replay(self):
        from rest_framework.test import APIClient
        from core.models import UserModuleAccess, UserProfile
        api = APIClient()
        api.force_authenticate(self.user)
        url = f'/api/mantenimiento/v2/reportes/{self.report.pk}/orden/'
        for payload in [[], {**self.data,'prioridad':[]}, {**self.data,'costo_otros':'100'}, {**self.data,'activo_id':self.asset.pk},
                {**self.data,'fecha_programada':''}, {**self.data,'clave_captura':'bad'}]:
            self.assertEqual(api.post(url,payload,format='json').status_code,400)
        self.create()
        self.user.is_active = False
        self.user.save(update_fields=['is_active'])
        self.assertEqual(api.post(url,self.data,format='json').status_code,403)
        writer = get_user_model().objects.create_user('scoped')
        UserModuleAccess.objects.create(user=writer,module='mantenimiento.app',access='manage')
        UserProfile.objects.update_or_create(user=writer, defaults={'sucursal':self.branch})
        api.force_authenticate(writer)
        self.assertEqual(api.get(url).status_code,200)
        other = Sucursal.objects.create(codigo='SC',nombre='Otra')
        UserProfile.objects.filter(user=writer).update(sucursal=other)
        writer = get_user_model().objects.get(pk=writer.pk)
        api.force_authenticate(writer)
        self.assertEqual(api.get(url).status_code,404)
        self.assertEqual(api.post(url,self.data,format='json').status_code,404)

    def test_web_async_native_and_preserved_invalid_fields(self):
        self.client.force_login(self.user)
        route = f'/mantenimiento/reportes/{self.report.pk}/orden/'
        initial = self.client.get(route)
        self.assertContains(initial, 'Crear orden de trabajo')
        self.assertContains(initial, 'data-capture-snapshot="true"')
        self.assertContains(initial, 'data-report-order-back')
        invalid = self.client.post(route, {**self.data, 'descripcion':''})
        self.assertEqual(invalid.status_code,400)
        self.assertContains(invalid,self.data['clave_captura'],status_code=400)
        self.assertContains(invalid,'2026-10-05',status_code=400)
        first = self.client.post(route, self.data, HTTP_ACCEPT='application/json')
        replay = self.client.post(route, self.data, HTTP_ACCEPT='application/json')
        self.assertEqual((first.status_code,replay.status_code),(201,200))
        self.assertEqual(first.json()['target'],f'#reporteOrdenResultado-{self.report.pk}')
        self.assertIn(OrdenMantenimiento.objects.get().folio,first.json()['html'])
        from html.parser import HTMLParser
        class Links(HTMLParser):
            def __init__(self):
                super().__init__()
                self.targets = []
            def handle_starttag(self, tag, attrs):
                values = dict(attrs)
                if tag == 'a' and '/mantenimiento/app/' in values.get('href',''):
                    self.targets.append(values)
        parsed = Links()
        parsed.feed(first.json()['html'])
        self.assertEqual(len(parsed.targets),3)
        for link in parsed.targets:
            self.assertEqual(link.get('target'),'_blank')
            self.assertIn('noopener',link.get('rel','').split())
        self.assertIn('Abrir en otra pestaña',first.json()['html'])
        self.assertEqual(OrdenMantenimiento.objects.count(),1)

    def test_uuid_other_report_conflicts(self):
        from mantenimiento.services_reporte_orden import crear_orden_desde_reporte
        from mantenimiento.services_capturas_equipos import CapturaEquipoError
        self.create()
        other = ReporteFalla.objects.create(sucursal=self.branch,activo_relacionado=self.asset,categoria=self.category,
            titulo='Otra falla',descripcion='Otra',reportado_por=self.user)
        with self.assertRaises(CapturaEquipoError) as error:
            crear_orden_desde_reporte(usuario=self.user,reporte_id=other.pk,datos=self.data)
        self.assertEqual(error.exception.status_code,409)
        self.assertEqual(OrdenMantenimiento.objects.count(),1)

    def test_cancelled_previous_allows_new_and_asset_stays_unchanged(self):
        original_asset = Activo.objects.filter(pk=self.asset.pk).values().get()
        first, _ = self.create()
        first.estatus = 'CANCELADA'
        first.save(update_fields=['estatus'])
        self.report.costo_real = None
        self.report.save(update_fields=['costo_real'])
        second, _ = self.create({**self.data,'clave_captura':str(uuid4())})
        self.assertNotEqual(second.pk,first.pk)
        self.assertEqual(VinculoAtencionEquipo.objects.count(),2)
        self.report.refresh_from_db()
        self.assertIsNone(self.report.costo_real)
        self.assertEqual(str(self.report.costo_estimado),'123.00')
        self.assertEqual(Activo.objects.filter(pk=self.asset.pk).values().get(),original_asset)

    def test_reader_and_permission_withdrawal_replay_have_no_writes(self):
        from core.models import UserModuleAccess, UserProfile
        from django.core.exceptions import PermissionDenied
        from mantenimiento.services_reporte_orden import crear_orden_desde_reporte
        from rest_framework.test import APIClient
        writer = get_user_model().objects.create_user('withdrawn')
        row = UserModuleAccess.objects.create(user=writer,module='mantenimiento.app',access='manage')
        UserProfile.objects.update_or_create(user=writer, defaults={'sucursal':self.branch})
        crear_orden_desde_reporte(usuario=writer,reporte_id=self.report.pk,datos=self.data)
        row.access = 'view'
        row.save(update_fields=['access'])
        writer = get_user_model().objects.get(pk=writer.pk)
        api = APIClient()
        api.force_authenticate(writer)
        url = f'/api/mantenimiento/v2/reportes/{self.report.pk}/orden/'
        self.assertEqual(api.post(url,self.data,format='json').status_code,403)
        self.assertEqual(api.get(url).status_code,200)
        self.assertEqual(OrdenMantenimiento.objects.count(),1)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(),1)

    def test_web_replay_closed_order_offers_valid_explicit_next_intervention(self):
        self.client.force_login(self.user)
        order, _ = self.create()
        order.estatus = 'CERRADA'
        order.save(update_fields=['estatus'])
        route = f'/mantenimiento/reportes/{self.report.pk}/orden/'
        response = self.client.post(route,self.data)
        self.assertEqual(response.status_code,200)
        self.assertTrue(response.context['puede_crear'])
        from uuid import UUID
        next_key = response.context.get('clave_captura')
        self.assertIsNotNone(next_key)
        UUID(next_key)
        self.assertNotEqual(next_key,self.data['clave_captura'])
        next_data = {**self.data,'clave_captura':next_key}
        second = self.client.post(route,next_data,HTTP_ACCEPT='application/json')
        self.assertEqual(second.status_code,201)
        self.assertEqual(OrdenMantenimiento.objects.count(),2)
