from uuid import uuid4
from django.contrib.auth import get_user_model
from django.test import TestCase, TransactionTestCase
from rest_framework.test import APIClient
from activos.models import Activo, OrdenMantenimiento, BitacoraMantenimiento
from core.models import Sucursal, AuditLog
from mantenimiento.models import ComprobanteCapturaEquipo
from mantenimiento.services_capturas_equipos import capturar_equipo, CapturaEquipoError


class CapturasEquiposTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('capturas', is_superuser=True)
        self.branch = Sucursal.objects.create(codigo='CAP', nombre='Capturas')
        self.asset = Activo.objects.create(codigo='CAP-1', nombre='Horno', sucursal=self.branch)
        self.key = uuid4()

    def capture(self, key=None, payload=None):
        def create(files):
            order = OrdenMantenimiento.objects.create(activo_ref=self.asset, tipo='CORRECTIVO', descripcion='Trabajo', creado_por=self.user)
            BitacoraMantenimiento.objects.create(orden=order, usuario=self.user, accion='CREATE')
            return order
        return capturar_equipo(usuario=self.user, activo=self.asset, operacion='orden_pwa', clave=key or self.key, contenido=payload or {'descripcion': 'Trabajo'}, crear=create)

    def test_replay_conflict_and_deleted_order(self):
        order, replay = self.capture()
        self.assertFalse(replay)
        same, replay = self.capture()
        self.assertTrue(replay)
        self.assertEqual(order.pk, same.pk)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(action='CREATE', model='activos.OrdenMantenimiento').count(), 1)
        with self.assertRaises(CapturaEquipoError):
            self.capture(payload={'descripcion': 'Distinto'})
        order.delete()
        with self.assertRaises(CapturaEquipoError) as error:
            self.capture()
        self.assertEqual(error.exception.status_code, 410)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 1)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_distinct_keys_and_legacy(self):
        self.capture()
        self.capture(key=uuid4())
        def create(files):
            return OrdenMantenimiento.objects.create(activo_ref=self.asset, tipo='CORRECTIVO', descripcion='Legacy')
        for _ in range(2):
            capturar_equipo(usuario=self.user, activo=self.asset, operacion='orden_pwa', clave=None, contenido={}, crear=create)
        self.assertEqual(OrdenMantenimiento.objects.count(), 4)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 2)

    def test_concurrent_retry(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections, connection
        barrier = Barrier(2)
        def send():
            close_old_connections()
            try:
                user = get_user_model().objects.get(pk=self.user.pk)
                asset = Activo.objects.get(pk=self.asset.pk)
                barrier.wait(timeout=10)
                def create(files):
                    order = OrdenMantenimiento.objects.create(activo_ref=asset, tipo='CORRECTIVO', descripcion='Trabajo')
                    BitacoraMantenimiento.objects.create(orden=order, usuario=user, accion='CREATE')
                    return order
                order, replay = capturar_equipo(usuario=user, activo=asset, operacion='orden_pwa', clave=self.key, contenido={'descripcion': 'Trabajo'}, crear=create)
                return order.pk, replay
            finally:
                connection.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: send(), range(2)))
        self.assertEqual(results[0][0], results[1][0])
        self.assertEqual(sorted(replay for pk, replay in results), [False, True])
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(action='CREATE', model='activos.OrdenMantenimiento').count(), 1)

    def test_rollback_new_file_and_rows(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from django.core.files.uploadedfile import SimpleUploadedFile
        from django.test import override_settings
        from mantenimiento.services_capturas_equipos import guardar_factura
        with tempfile.TemporaryDirectory() as root, override_settings(MEDIA_ROOT=root):
            Path(root, 'confirmed.pdf').write_bytes(b'keep')
            def create(files):
                order = OrdenMantenimiento.objects.create(activo_ref=self.asset, tipo='CORRECTIVO', descripcion='Trabajo')
                guardar_factura(order, SimpleUploadedFile('attempt.pdf', b'invoice'), files)
                BitacoraMantenimiento.objects.create(orden=order, usuario=self.user, accion='CREATE')
                return order
            with patch('mantenimiento.services_capturas_equipos.log_event', side_effect=RuntimeError('audit failed')):
                with self.assertRaises(RuntimeError):
                    capturar_equipo(usuario=self.user, activo=self.asset, operacion='servicio_web', clave=self.key, contenido={}, crear=create)
            self.assertEqual([p.relative_to(root).as_posix() for p in Path(root).rglob('*') if p.is_file()], ['confirmed.pdf'])
            self.assertEqual(OrdenMantenimiento.objects.count(), 0)
            self.assertEqual(BitacoraMantenimiento.objects.count(), 0)
            self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)

    def test_files_hash_bytes_preserves_position_and_money(self):
        from decimal import Decimal
        from django.core.files.uploadedfile import SimpleUploadedFile
        from mantenimiento.services_capturas_equipos import huella_captura
        a = SimpleUploadedFile('same.pdf', b'abc')
        b = SimpleUploadedFile('same.pdf', b'abd')
        a.seek(1)
        self.assertNotEqual(huella_captura({'file': a}), huella_captura({'file': b}))
        self.assertEqual(a.tell(), 1)
        self.assertEqual(huella_captura({'cost': Decimal('1.0')}), huella_captura({'cost': Decimal('1.00')}))

    def test_invalid_key_before_writes(self):
        with self.assertRaises(CapturaEquipoError) as error:
            self.capture(key='wrong')
        self.assertEqual(error.exception.status_code, 400)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_write_scope_before_replay(self):
        from django.core.exceptions import PermissionDenied
        from unittest.mock import patch
        self.capture()
        with patch('mantenimiento.services_capturas_equipos.authorized_branch_ids', return_value=[]):
            with self.assertRaises(PermissionDenied):
                self.capture()
        self.user.is_active = False
        with self.assertRaises(PermissionDenied):
            self.capture()


class CapturasEquiposEntradasTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('operator', is_superuser=True)
        self.branch = Sucursal.objects.create(codigo='INT', nombre='Integración')
        self.asset = Activo.objects.create(codigo='INT-1', nombre='Horno', sucursal=self.branch)
        self.api = APIClient()
        self.api.force_authenticate(self.user)
        self.client.force_login(self.user)

    def datos(self):
        return dict(alcance='activo', sucursal_id=self.branch.pk, activo_id=self.asset.pk, descripcion='Trabajo', costo_total='150.00', clave_captura=str(uuid4()))

    def test_order_api_first_replay_legacy_and_invalid(self):
        data = dict(activo_ref=self.asset.pk, tipo='CORRECTIVO', descripcion='Trabajo', costo_real='150', clave_captura=str(uuid4()))
        first = self.api.post('/api/mantenimiento/ordenes/', data, format='json')
        replay = self.api.post('/api/mantenimiento/ordenes/', data, format='json')
        self.assertEqual((first.status_code, replay.status_code), (201, 200))
        self.assertEqual(first.data, replay.data)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(action='CREATE', model='activos.OrdenMantenimiento').count(), 1)
        data['descripcion'] = 'Cambio'
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', data, format='json').status_code, 409)
        data['clave_captura'] = 'invalid'
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', data, format='json').status_code, 400)
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', [], format='json').status_code, 400)
        del data['clave_captura']
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', data, format='json').status_code, 201)
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', data, format='json').status_code, 201)

    def test_mobile_service_and_stable_server_date(self):
        from datetime import date
        from unittest.mock import patch
        data = self.datos()
        with patch('mantenimiento.views.timezone.localdate', return_value=date(2026, 10, 2)):
            first = self.api.post('/api/mantenimiento/servicios-puntuales/', data, format='json')
        with patch('mantenimiento.views.timezone.localdate', return_value=date(2026, 10, 3)):
            data['costo_total'] = '150.0'
            replay = self.api.post('/api/mantenimiento/servicios-puntuales/', data, format='json')
        self.assertEqual((first.status_code, replay.status_code), (201, 200))
        self.assertEqual(first.data, replay.data)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)
        self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(action='CREATE', model='activos.OrdenMantenimiento').count(), 1)
        for bad in ([], 'text', 1):
            self.assertEqual(self.api.post('/api/mantenimiento/servicios-puntuales/', bad, format='json').status_code, 400)
        data['clave_captura'] = 'invalid'
        self.assertEqual(self.api.post('/api/mantenimiento/servicios-puntuales/', data, format='json').status_code, 400)

    def test_replay_hides_updated_costs_from_limited_operator_both_endpoints(self):
        from decimal import Decimal
        from django.contrib.auth.models import Group
        from core.models import UserModuleAccess, UserProfile
        from mantenimiento.services_access import can_view_costs
        operator = get_user_model().objects.create_user('cost-limited')
        operator.groups.add(Group.objects.get_or_create(name='mantenimiento')[0])
        UserModuleAccess.objects.create(user=operator, module='mantenimiento', access='view')
        UserProfile.objects.update_or_create(user=operator, defaults={'sucursal': self.branch})
        self.assertFalse(can_view_costs(operator))
        cost_fields = {'costo_repuestos', 'costo_mano_obra', 'costo_otros', 'costo_total'}
        for route in ('/api/mantenimiento/ordenes/', '/api/mantenimiento/servicios-puntuales/'):
            for actor in (operator, self.user):
                with self.subTest(route=route, actor=actor.username):
                    self.api.force_authenticate(actor)
                    data = dict(activo_ref=self.asset.pk, tipo='CORRECTIVO', descripcion='Trabajo', costo_real='150.00', clave_captura=str(uuid4())) if route.endswith('/ordenes/') else self.datos()
                    first = self.api.post(route, data, format='json')
                    self.assertEqual(first.status_code, 201)
                    self.assertIn('costo_otros', first.data)
                    self.assertEqual(Decimal(first.data['costo_otros']), Decimal('150'))
                    # Representa una corrección posterior de costos por un gestor.
                    OrdenMantenimiento.objects.filter(pk=first.data['id']).update(costo_repuestos='900', costo_mano_obra='800', costo_otros='700')
                    replay = self.api.post(route, data, format='json')
                    self.assertEqual(replay.status_code, 200)
                    self.assertEqual(replay.data['id'], first.data['id'])
                    if actor == operator:
                        self.assertTrue(cost_fields.isdisjoint(replay.data))
                    else:
                        self.assertEqual(Decimal(replay.data['costo_repuestos']), Decimal('900'))
                        self.assertEqual(Decimal(replay.data['costo_mano_obra']), Decimal('800'))
                        self.assertEqual(Decimal(replay.data['costo_otros']), Decimal('700'))
                        if route.endswith('/servicios-puntuales/'):
                            self.assertEqual(Decimal(replay.data['costo_total']), Decimal('2400'))
        self.assertEqual(OrdenMantenimiento.objects.count(), 4)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 4)

    def test_web_invoice_replay_conflict_and_delete(self):
        import tempfile
        from pathlib import Path
        from django.core.files.uploadedfile import SimpleUploadedFile
        from django.test import override_settings
        data = self.datos()
        data.update(fecha_objetivo='2026-10-02', cerrar_servicio='1')
        with tempfile.TemporaryDirectory() as root, override_settings(MEDIA_ROOT=root):
            def send(content=b'invoice'):
                return self.client.post('/mantenimiento/servicios/crear/', {**data, 'factura_archivo': SimpleUploadedFile('receipt.pdf', content)}, HTTP_ACCEPT='application/json')
            first, replay = send(), send()
            self.assertEqual((first.status_code, replay.status_code), (201, 200))
            self.assertEqual(first.json()['orden_id'], replay.json()['orden_id'])
            self.assertEqual(first.json()['target'], '#ordenServicioResultado')
            self.assertEqual(OrdenMantenimiento.objects.count(), 1)
            self.assertEqual(BitacoraMantenimiento.objects.count(), 1)
            self.assertEqual(AuditLog.objects.filter(action='CREATE', model='activos.OrdenMantenimiento').count(), 1)
            files = [p for p in Path(root).rglob('*') if p.is_file()]
            self.assertEqual(len(files), 1)
            self.assertEqual(send(b'changed').status_code, 409)
            self.assertEqual(len([p for p in Path(root).rglob('*') if p.is_file()]), 1)
            OrdenMantenimiento.objects.get().delete()
            self.assertEqual(send().status_code, 410)
            self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_reader_and_scoped_writer_before_replay(self):
        from core.models import UserModuleAccess, UserProfile
        reader = get_user_model().objects.create_user('reader')
        UserModuleAccess.objects.create(user=reader, module='mantenimiento.app', access='view')
        UserProfile.objects.update_or_create(user=reader, defaults={'sucursal': self.branch})
        self.api.force_authenticate(reader)
        data = self.datos()
        self.assertEqual(self.api.post('/api/mantenimiento/servicios-puntuales/', data, format='json').status_code, 403)
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', dict(activo_ref=self.asset.pk, tipo='CORRECTIVO', descripcion='No', costo_real='12'), format='json').status_code, 403)
        writer = get_user_model().objects.create_user('writer')
        UserModuleAccess.objects.create(user=writer, module='mantenimiento.app', access='manage')
        UserProfile.objects.update_or_create(user=writer, defaults={'sucursal': self.branch})
        self.api.force_authenticate(writer)
        first = self.api.post('/api/mantenimiento/servicios-puntuales/', data, format='json')
        self.assertEqual(first.status_code, 201)
        other = Sucursal.objects.create(codigo='OTHER', nombre='Otra')
        self.asset.sucursal = other
        self.asset.save(update_fields=['sucursal'])
        self.assertEqual(self.api.post('/api/mantenimiento/servicios-puntuales/', data, format='json').status_code, 404)
        data2 = dict(activo_ref=self.asset.pk, tipo='CORRECTIVO', descripcion='Otra')
        self.assertEqual(self.api.post('/api/mantenimiento/ordenes/', data2, format='json').status_code, 403)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)

    def test_web_native_error_preserves_uuid_fields(self):
        data = self.datos()
        data['descripcion'] = ''
        response = self.client.post('/mantenimiento/servicios/crear/', data)
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, data['clave_captura'], status_code=400)
        self.assertEqual(response.context['captura_datos']['costo_total'], '150.00')
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_web_native_error_preserves_unchecked_close(self):
        data = self.datos()
        data['descripcion'] = ''
        response = self.client.post('/mantenimiento/servicios/crear/', data)
        self.assertEqual(response.status_code, 400)
        self.assertNotIn('cerrar_servicio', response.context['captura_datos'])
        self.assertEqual(response.context['clave_captura'], data['clave_captura'])
        self.assertContains(response, 'cerrar.checked = false;', status_code=400)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_flota_installation_legacy_no_receipt(self):
        from logistica.models import Unidad, ServicioRealizadoUnidad
        unit = Unidad.objects.create(codigo='CAR', descripcion='Unidad', sucursal=self.branch)
        for route, mobile in (('/api/mantenimiento/servicios-puntuales/', True), ('/mantenimiento/servicios/crear/', False)):
            for scope in ('unidad', 'instalacion'):
                data = self.datos()
                data.update(alcance=scope, unidad_id=unit.pk, instalacion_categoria='Plomería', fecha_objetivo='2026-10-02', proveedor_servicio='Técnico')
                response = self.api.post(route, data, format='json') if mobile else self.client.post(route, data, HTTP_ACCEPT="application/json")
                self.assertEqual(response.status_code, 201 if mobile else 302)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)
        self.assertEqual(ServicioRealizadoUnidad.objects.count(), 2)
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)
