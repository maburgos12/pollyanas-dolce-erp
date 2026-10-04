from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4
from pathlib import Path

from django.contrib import admin
from django.contrib.admin.models import LogEntry
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import close_old_connections
from django.test import Client, TransactionTestCase, override_settings
from django.urls import reverse

from core.models import AuditLog
from mantenimiento.models import ComprobanteCapturaEquipo
from .admin import OrdenMantenimientoAdmin
from .models import Activo, BitacoraMantenimiento, OrdenMantenimiento


class OrdenAdminCapturasTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser('admin-captura', 'a@test.com', 'test')
        self.asset = Activo.objects.create(nombre='Equipo Admin', activo=False)
        self.client.force_login(self.user)
        self.url = reverse('admin:activos_ordenmantenimiento_add')

    def payload(self, **extra):
        return {'clave_captura': str(uuid4()), 'activo_ref': str(self.asset.pk),
            'tipo': 'CORRECTIVO', 'prioridad': 'MEDIA', 'estatus': 'PENDIENTE',
            'fecha_programada': '2026-10-03', 'origen': 'INICIATIVA',
            'costo_repuestos': '0', 'costo_mano_obra': '0', 'costo_otros': '0',
            'bitacora-TOTAL_FORMS': '1', 'bitacora-INITIAL_FORMS': '0',
            'bitacora-MIN_NUM_FORMS': '0', 'bitacora-MAX_NUM_FORMS': '1000',
            'bitacora-0-fecha_0': '2026-10-03', 'bitacora-0-fecha_1': '10:00:00',
            'bitacora-0-accion': 'Alta nativa', 'bitacora-0-costo_adicional': '0', **extra}

    def assert_counts(self, n):
        for model in (OrdenMantenimiento, BitacoraMantenimiento, LogEntry, AuditLog, ComprobanteCapturaEquipo):
            self.assertEqual(model.objects.count(), n, model.__name__)

    def test_native_form_hidden_uuid_and_edit_without_key(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'type="hidden" name="clave_captura"')
        first = self.client.post(self.url, self.payload())
        self.assertEqual(first.status_code, 302)
        obj = OrdenMantenimiento.objects.get()
        url = reverse('admin:activos_ordenmantenimiento_change', args=[obj.pk])
        self.assertNotContains(self.client.get(url), 'name="clave_captura"')
        data = self.payload(folio=obj.folio, descripcion='Edición nativa', **{
            'bitacora-TOTAL_FORMS': '0', 'bitacora-INITIAL_FORMS': '0'})
        del data['clave_captura']
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302, response.content)
        obj.refresh_from_db()
        self.assertEqual(obj.descripcion, 'Edición nativa')
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 1)

    def test_sequential_replay_and_inline_conflict(self):
        data = self.payload()
        for _ in range(2):
            response = self.client.post(self.url, data)
            self.assertEqual(response.status_code, 302, response.content)
        self.assert_counts(1)
        with patch.object(OrdenMantenimientoAdmin, 'save_related', side_effect=AssertionError('No writes')):
            conflict = self.client.post(self.url, {**data, 'bitacora-0-comentario': 'Cambió'})
        self.assertEqual(conflict.status_code, 409)
        self.assertContains(conflict, data['clave_captura'], status_code=409)
        self.assertContains(conflict, 'Cambió', status_code=409)
        self.assert_counts(1)

    def test_missing_uuid_invalid_then_corrected_same_uuid(self):
        data = self.payload()
        missing = dict(data); del missing['clave_captura']
        self.assertEqual(self.client.post(self.url, missing).status_code, 200)
        invalid = self.client.post(self.url, {**data, 'bitacora-0-accion': ''})
        self.assertEqual(invalid.status_code, 200)
        self.assertContains(invalid, data['clave_captura'])
        self.assert_counts(0)
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        self.assert_counts(1)

    def test_deleted_receipt_returns_410(self):
        data = self.payload()
        self.client.post(self.url, data)
        OrdenMantenimiento.objects.get().delete()
        self.assertEqual(self.client.post(self.url, data).status_code, 410)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_file_replay_and_conflict(self):
        data = self.payload()
        with TemporaryDirectory() as directory, override_settings(MEDIA_ROOT=directory):
            def send(content):
                return self.client.post(self.url, {**data, 'factura_archivo': SimpleUploadedFile('nota.pdf', content)})
            self.assertEqual(send(b'original').status_code, 302)
            self.assertEqual(send(b'original').status_code, 302)
            self.assertEqual(send(b'cambio').status_code, 409)
            self.assert_counts(1)
            self.assertEqual(len([p for p in Path(directory).rglob('*') if p.is_file()]), 1)

    def test_inline_and_log_failure_roll_back_and_clean_new_file(self):
        for hook in ('save_formset', 'log_addition'):
            with self.subTest(hook=hook), TemporaryDirectory() as directory, override_settings(MEDIA_ROOT=directory):
                with patch.object(OrdenMantenimientoAdmin, hook, side_effect=RuntimeError('fallo posterior')):
                    with self.assertRaisesMessage(RuntimeError, 'fallo posterior'):
                        self.client.post(self.url, {**self.payload(), 'factura_archivo': SimpleUploadedFile('nota.pdf', b'bytes')})
                self.assert_counts(0)
                self.assertEqual([p for p in Path(directory).rglob('*') if p.is_file()], [])

    def test_staff_add_only_native_gate_without_inventory_and_replay_read_denied(self):
        staff = get_user_model().objects.create_user('solo-alta', is_staff=True)
        staff.user_permissions.add(Permission.objects.get(codename='add_ordenmantenimiento'),
            Permission.objects.get(codename='add_bitacoramantenimiento'))
        self.client.force_login(staff)
        data = self.payload()
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        self.assert_counts(1)
        self.assertEqual(self.client.post(self.url, data).status_code, 403)
        self.assertEqual(self.client.post(self.url, {**data, 'descripcion': 'Cambió'}).status_code, 403)
        staff.user_permissions.clear()
        self.assertEqual(self.client.post(self.url, data).status_code, 403)
        self.assert_counts(1)

    def test_popup_continue_and_preserved_filters(self):
        for button in ('_popup', '_continue', '_save'):
            data = self.payload(**{button: '1'})
            url = self.url + '?_changelist_filters=estatus%3DPENDIENTE'
            first = self.client.post(url, data)
            replay = self.client.post(url, data)
            self.assertEqual(first.status_code, replay.status_code)
            if button == '_popup':
                self.assertEqual(first.context['popup_response_data'], replay.context['popup_response_data'])
            else:
                self.assertEqual(first['Location'], replay['Location'])
                self.assertIn('estatus', replay['Location'])

    def test_crafted_saveasnew_requires_uuid(self):
        self.client.post(self.url, self.payload())
        obj = OrdenMantenimiento.objects.get()
        data = self.payload(_saveasnew='1', folio='MANUAL-COPY')
        del data['clave_captura']
        url = reverse('admin:activos_ordenmantenimiento_change', args=[obj.pk])
        self.assertEqual(self.client.post(url, data).status_code, 200)
        self.assert_counts(1)

    def test_concurrent_replay_one_complete_native_cycle(self):
        barrier = Barrier(2)
        data = self.payload()
        def send(_):
            close_old_connections()
            try:
                client = Client(); client.force_login(self.user)
                barrier.wait(timeout=10)
                return client.post(self.url, data).status_code
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as executor:
            self.assertEqual(list(executor.map(send, range(2))), [302, 302])
        self.assert_counts(1)

    def test_explicit_folio_replays_and_changed_content_conflicts(self):
        data = self.payload(folio='OM-MANUAL-RETRY')
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        self.assertEqual(self.client.post(self.url, {**data, 'descripcion': 'Otro contenido'}).status_code, 409)
        self.assert_counts(1)
        fresh = {**data, 'clave_captura': str(uuid4())}
        invalid = self.client.post(self.url, fresh)
        self.assertEqual(invalid.status_code, 200)
        self.assertContains(invalid, 'Ya existe')
        self.assert_counts(1)

    def test_failure_after_file_write_before_sql_cleans(self):
        from django.db import DatabaseError
        def fail_save(obj, *args, **kwargs):
            obj._meta.get_field('factura_archivo').pre_save(obj, True)
            raise DatabaseError('SQL rechazado')
        with TemporaryDirectory() as directory, override_settings(MEDIA_ROOT=directory):
            with patch.object(OrdenMantenimiento, 'save', fail_save):
                with self.assertRaisesMessage(DatabaseError, 'SQL rechazado'):
                    self.client.post(self.url, {**self.payload(), 'factura_archivo': SimpleUploadedFile('nota.pdf', b'bytes')})
            self.assert_counts(0)
            self.assertEqual([p for p in Path(directory).rglob('*') if p.is_file()], [])

    def test_shared_committed_reference_survives_failure(self):
        with TemporaryDirectory() as directory, override_settings(MEDIA_ROOT=directory):
            existing = OrdenMantenimiento.objects.create(activo_ref=self.asset,
                factura_archivo=SimpleUploadedFile('anterior.pdf', b'bytes anteriores'))
            original_save = OrdenMantenimientoAdmin.save_model
            def share_file(modeladmin, request, obj, form, change):
                obj.factura_archivo = existing.factura_archivo.name
                return original_save(modeladmin, request, obj, form, change)
            with patch.object(OrdenMantenimientoAdmin, 'save_model', share_file), \
                    patch.object(OrdenMantenimientoAdmin, 'log_addition', side_effect=RuntimeError('falla log')):
                with self.assertRaisesMessage(RuntimeError, 'falla log'):
                    self.client.post(self.url, self.payload())
            self.assertEqual(existing.factura_archivo.read(), b'bytes anteriores')
            self.assertEqual(OrdenMantenimiento.objects.count(), 1)
            self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)

    def test_context_and_actor_are_not_shared_between_requests(self):
        first = self.payload()
        self.client.post(self.url, first)
        other = get_user_model().objects.create_superuser('otro-actor', 'b@test.com', 'test')
        self.client.force_login(other)
        self.client.post(self.url, first)
        self.assert_counts(2)
        self.assertEqual(set(ComprobanteCapturaEquipo.objects.values_list('usuario_id', flat=True)), {self.user.pk, other.pk})
        self.assertFalse(hasattr(admin.site._registry[OrdenMantenimiento], '_orden_admin_captura'))

    def test_manual_folio_edited_reused_and_deleted_replay(self):
        data = self.payload(folio='ORIGINAL-OCUPADO')
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        result = OrdenMantenimiento.objects.get()
        result.folio = 'RESULTADO-EDITADO'; result.save()
        OrdenMantenimiento.objects.create(activo_ref=self.asset, folio=data['folio'])
        response = self.client.post(self.url, {**data, '_continue': '1'})
        self.assertEqual(response.status_code, 302)
        self.assertIn(f'/{result.pk}/change/', response['Location'])
        result.delete()
        self.assertEqual(self.client.post(self.url, data).status_code, 410)
        self.assertEqual(OrdenMantenimiento.objects.count(), 1)

    def test_new_inline_file_cleanup_on_late_failure(self):
        from .models import EvidenciaOrden
        class FileInline(admin.TabularInline):
            model = EvidenciaOrden
            extra = 0
        data = self.payload(**{'evidencias-TOTAL_FORMS': '1', 'evidencias-INITIAL_FORMS': '0',
            'evidencias-MIN_NUM_FORMS': '0', 'evidencias-MAX_NUM_FORMS': '1000',
            'evidencias-0-tipo': 'DOCUMENTO',
            'evidencias-0-archivo': SimpleUploadedFile('inline.pdf', b'bytes inline')})
        with TemporaryDirectory() as directory, override_settings(MEDIA_ROOT=directory), \
                patch.object(OrdenMantenimientoAdmin, 'inlines', [FileInline]), \
                patch.object(OrdenMantenimientoAdmin, 'log_addition', side_effect=RuntimeError('rollback inline')):
            with self.assertRaisesMessage(RuntimeError, 'rollback inline'):
                self.client.post(self.url, data)
            self.assertEqual(EvidenciaOrden.objects.count(), 0)
            self.assertEqual([p for p in Path(directory).rglob('*') if p.is_file()], [])
            for model in (OrdenMantenimiento, LogEntry, AuditLog, ComprobanteCapturaEquipo):
                self.assertEqual(model.objects.count(), 0)

    def test_manual_folio_receipt_becomes_visible_during_native_unique_check(self):
        from unittest.mock import Mock
        data = self.payload(folio='FOLIO-CONCURRENT-VISIBLE')
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        receipt = Mock()
        receipt.exists.side_effect = [False, True]
        with patch.object(ComprobanteCapturaEquipo.objects, 'filter', return_value=receipt):
            response = self.client.post(self.url, data)
        self.assertEqual(response.status_code, 302, response.content)
        self.assertEqual(receipt.exists.call_count, 2)
        self.assert_counts(1)

    def test_fresh_revoked_read_hides_conflict_and_deleted_attempt(self):
        staff = get_user_model().objects.create_user('lectura-revocada', is_staff=True)
        staff.user_permissions.add(*Permission.objects.filter(codename__in=(
            'add_ordenmantenimiento', 'view_ordenmantenimiento', 'add_bitacoramantenimiento')))
        self.client.force_login(staff)
        data = self.payload()
        self.assertEqual(self.client.post(self.url, data).status_code, 302)
        staff.user_permissions.remove(Permission.objects.get(codename='view_ordenmantenimiento'))
        response = self.client.post(self.url, {**data, 'descripcion': 'Cambió'})
        self.assertEqual(response.status_code, 403)
        self.assertNotContains(response, 'otros datos', status_code=403)
        self.assertEqual(self.client.post(self.url, data).status_code, 403)
        OrdenMantenimiento.objects.get().delete()
        self.assertEqual(self.client.post(self.url, data).status_code, 403)
        self.assertEqual(self.client.post(self.url, {**data, 'descripcion': 'Cambió'}).status_code, 403)
        staff.user_permissions.add(Permission.objects.get(codename='view_ordenmantenimiento'))
        self.assertEqual(self.client.post(self.url, data).status_code, 410)
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)

    def test_current_queryset_denies_conflict_without_result_details(self):
        data = self.payload()
        self.client.post(self.url, data)
        with patch.object(OrdenMantenimientoAdmin, 'get_queryset', return_value=OrdenMantenimiento.objects.none()):
            response = self.client.post(self.url, {**data, 'descripcion': 'Cambió'})
        self.assertEqual(response.status_code, 403)
        self.assertNotContains(response, 'otros datos', status_code=403)
        self.assert_counts(1)
