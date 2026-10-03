from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from tempfile import TemporaryDirectory
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import close_old_connections, connection
from django.test import Client, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import AuditLog, Sucursal, UserModuleAccess, UserProfile
from mantenimiento.models import ComprobanteCapturaEquipo
from .models import Activo, BitacoraMantenimiento, EvidenciaOrden, OrdenMantenimiento, PlanMantenimiento, SolicitudFalla
from .services_capturas import crear_captura_activos


class CapturasLegadasTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser('legacy-capture', 'capture@test.com', 'test')
        self.asset = Activo.objects.create(nombre='Equipo legado')
        self.client.force_login(self.user)
        self.api = APIClient()
        self.api.force_authenticate(self.user)

    def send(self, mode, data, *, native=False):
        if mode == 'api':
            return self.api.post(reverse('api_activos_ordenes'), data, format='json')
        route = {'orden': 'ordenes', 'reporte': 'reportes', 'rapido': 'registro_rapido'}[mode]
        return self.client.post(reverse('activos:' + route), data,
            **({} if native else {'HTTP_X_REQUESTED_WITH': 'XMLHttpRequest'}))

    def payload(self, **extra):
        return dict(activo_id=self.asset.pk, descripcion='Reparación legítima', clave_captura=str(uuid4()), **extra)

    def test_all_four_consumers_replay_conflict_deleted_and_distinct_attempts(self):
        for mode in ('orden', 'reporte', 'rapido', 'api'):
            with self.subTest(mode=mode):
                data = self.payload()
                first = self.send(mode, data)
                self.assertEqual(first.status_code, 201, first.content)
                pk = first.json()['id']
                same = self.send(mode, data)
                self.assertEqual(same.status_code, 200)
                self.assertEqual(same.json()['id'], pk)
                self.assertTrue(same.json()['replay'])
                self.assertEqual(BitacoraMantenimiento.objects.filter(orden_id=pk).count(), 1)
                audits = AuditLog.objects.filter(model='activos.OrdenMantenimiento', object_id=str(pk))
                self.assertEqual(audits.count(), 1)
                metadata = audits.get().payload
                if mode == 'rapido':
                    self.assertEqual(metadata['activo'], self.asset.nombre)
                    self.assertEqual(metadata['origen'], 'EMERGENCIA')
                    self.assertEqual(metadata['captura_origen'], 'activos_rapido')
                elif mode == 'reporte':
                    self.assertEqual(metadata['tipo'], 'REPORTE_FALLA')
                    self.assertEqual(metadata['tipo_orden'], 'CORRECTIVO')
                conflict = self.send(mode, {**data, 'descripcion': 'Cambio'})
                self.assertEqual(conflict.status_code, 409)
                different = self.send(mode, {**data, 'clave_captura': str(uuid4())})
                self.assertEqual(different.status_code, 201)
                self.assertNotEqual(different.json()['id'], pk)
                OrdenMantenimiento.objects.get(pk=pk).delete()
                self.assertEqual(self.send(mode, data).status_code, 410)


    def test_folio_survives_deletion_gap_numeric_boundary_and_manual_suffix(self):
        first = self.send('api', self.payload()).json()
        second = self.send('api', self.payload()).json()
        OrdenMantenimiento.objects.get(pk=first['id']).delete()
        third = self.send('api', self.payload())
        self.assertEqual(third.status_code, 201)
        self.assertNotEqual(third.json()['folio'], second['folio'])
        prefix = second['folio'].rsplit('-', 1)[0] + '-'
        for suffix in ('999', '1000', 'MANUAL'):
            OrdenMantenimiento.objects.create(activo_ref=self.asset, folio=prefix + suffix)
        next_order = self.send('api', self.payload())
        self.assertEqual(next_order.status_code, 201)
        self.assertEqual(next_order.json()['folio'], prefix + '1001')
        self.assertTrue(OrdenMantenimiento.objects.filter(folio=prefix+'MANUAL').exists())

    def test_web_requires_uuid_without_writes_and_retains_error_context(self):
        for mode in ('orden','reporte','rapido'):
            for key in (None, ''):
                data = self.payload()
                if key is None: del data['clave_captura']
                else: data['clave_captura'] = key
                response = self.send(mode, data)
                self.assertEqual(response.status_code, 400)
                native = self.send(mode, data, native=True)
                self.assertEqual(native.status_code, 400)
                self.assertTrue(native.context_data['clave_captura'])
                self.assertEqual(native.context_data['captura_borrador']['clave_captura'], [native.context_data['clave_captura']])
                self.assertEqual(native.context_data['captura_datos']['descripcion'], data['descripcion'])
        self.assertEqual(OrdenMantenimiento.objects.count(), 0)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)

    def test_api_without_uuid_stays_compatible_and_does_not_claim_idempotence(self):
        data = self.payload(); del data['clave_captura']
        first, second = self.send('api', data), self.send('api', data)
        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 201)
        self.assertNotEqual(first.json()['id'], second.json()['id'])
        self.assertFalse(first.json()['idempotente'])
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(), 0)

    def test_plan_is_validated_for_new_capture_but_not_reapplied_on_replay(self):
        plan = PlanMantenimiento.objects.create(activo_ref=self.asset, nombre='Plan inicial', frecuencia_dias=7)
        data = self.payload(plan_id=plan.pk)
        first = self.send('api', data)
        self.assertEqual(first.status_code, 201)
        other = Activo.objects.create(nombre='Equipo nuevo del plan')
        plan.activo_ref = other; plan.save(update_fields=['activo_ref'])
        same = self.send('api', data)
        self.assertEqual(same.status_code, 200)
        self.assertEqual(same.json()['id'], first.json()['id'])
        invalid_new = self.send('api', {**data,'clave_captura':str(uuid4())})
        self.assertEqual(invalid_new.status_code,404)
        self.assertEqual(OrdenMantenimiento.objects.count(),1)
        self.assertEqual(ComprobanteCapturaEquipo.objects.count(),1)

    def test_defaults_are_stable_across_midnight_and_user_name_change(self):
        for mode in ('reporte', 'rapido', 'api'):
            data = self.payload()
            first = self.send(mode, data)
            self.user.first_name = 'Nombre nuevo'; self.user.save(update_fields=['first_name'])
            with patch('activos.services_capturas.timezone.localdate', return_value=timezone.localdate() + timedelta(days=1)):
                replay = self.send(mode, data)
            self.assertEqual(replay.status_code, 200, replay.content)
            self.assertEqual(first.json()['id'], replay.json()['id'])

    def test_web_native_replay_and_validation_keep_context_and_uuid(self):
        for mode in ('orden','reporte','rapido'):
            route = {'orden':'ordenes','reporte':'reportes','rapido':'registro_rapido'}[mode]
            self.assertContains(self.client.get(reverse('activos:' + route)), 'data-capture-snapshot="true"')
            data = self.payload()
            first = self.send(mode, data, native=True)
            repeat = self.send(mode, data, native=True)
            self.assertEqual(first.status_code, 302)
            self.assertEqual(repeat.status_code, 302)
            bad = self.send(mode, {**data, 'descripcion':'Un cambio conservado'}, native=True)
            self.assertEqual(bad.status_code, 409)
            self.assertContains(bad, data['clave_captura'], status_code=409)
            self.assertContains(bad, 'Un cambio conservado', status_code=409)
            self.assertEqual(bad.context_data['captura_datos']['activo_id'], str(self.asset.pk))

    def test_permissions_global_scope_and_active_equipment_before_replay(self):
        actor = get_user_model().objects.create_user('legacy-inventory', password='test')
        branch_a = Sucursal.objects.create(codigo='LEG1', nombre='A')
        branch_b = Sucursal.objects.create(codigo='LEG2', nombre='B')
        UserProfile.objects.update_or_create(user=actor, defaults={'sucursal':branch_a})
        UserModuleAccess.objects.create(user=actor, module='inventario', access='manage')
        self.asset.sucursal = branch_b; self.asset.save(update_fields=['sucursal'])
        data = self.payload()
        # Inventario WRITE conserva acceso global sin depender del permiso mantenimiento.
        first, _ = crear_captura_activos(usuario=actor, datos=data, modo='orden')
        UserModuleAccess.objects.filter(user=actor).delete()
        if hasattr(actor, '_module_access_map_cache'): del actor._module_access_map_cache
        with self.assertRaises(PermissionDenied): crear_captura_activos(usuario=actor, datos=data, modo='orden')
        self.asset.activo = False; self.asset.save(update_fields=['activo'])
        self.assertEqual(self.send('api', data).status_code, 403)
        self.user.is_active = False
        with self.assertRaises(PermissionDenied): crear_captura_activos(usuario=self.user, datos=data, modo='orden')
        self.assertTrue(OrdenMantenimiento.objects.filter(pk=first.pk).exists())

    def test_legacy_report_view_permission_is_not_promoted_for_other_creators(self):
        actor = get_user_model().objects.create_user('report-reader', password='test')
        UserModuleAccess.objects.create(user=actor, module='inventario', access='view')
        client = Client(); client.force_login(actor)
        data = self.payload()
        self.assertEqual(client.post(reverse('activos:reportes'), data, HTTP_X_REQUESTED_WITH='XMLHttpRequest').status_code, 201)
        for route in ('ordenes','registro_rapido'):
            self.assertEqual(client.post(reverse('activos:' + route), data).status_code, 403)
        api = APIClient(); api.force_authenticate(actor)
        self.assertEqual(api.post(reverse('api_activos_ordenes'),data).status_code,403)

    def test_web_permission_rejections_are_json_and_native_permissions_stay_403(self):
        data = self.payload()
        self.asset.activo = False; self.asset.save(update_fields=['activo'])
        for mode in ('orden','reporte','rapido'):
            denied = self.send(mode, data)
            self.assertEqual(denied.status_code,403)
            self.assertEqual(denied['Content-Type'],'application/json')
            self.assertFalse(denied.json()['ok'])
            self.assertEqual(self.send(mode,data,native=True).status_code,403)
        actor = get_user_model().objects.create_user('no-legacy-access',password='test')
        client = Client(); client.force_login(actor)
        for route in ('ordenes','reportes','registro_rapido'):
            response = client.post(reverse('activos:'+route),data,HTTP_X_REQUESTED_WITH='XMLHttpRequest')
            self.assertEqual(response.status_code,403)
            self.assertEqual(response['Content-Type'],'application/json')
        self.assertEqual(OrdenMantenimiento.objects.count(),0)

    def test_native_request_draft_can_deselect_wrong_asset_solicitudes(self):
        requests = [SolicitudFalla.objects.create(activo_ref=self.asset,descripcion='Falla '+str(i)) for i in range(2)]
        other = Activo.objects.create(nombre='Otro equipo')
        data = self.payload(solicitud_id=[str(s.pk) for s in requests])
        data['activo_id'] = other.pk
        invalid = self.send('rapido', data, native=True)
        self.assertEqual(invalid.status_code,400)
        for row in requests:
            self.assertContains(invalid, 'name="solicitud_id" value="'+str(row.pk)+'" checked',status_code=400)
        self.assertEqual(invalid.context_data['captura_borrador']['solicitud_id'],data['solicitud_id'])
        # Navegación nativa sin JS: desmarcar las casillas elimina el campo del POST.
        del data['solicitud_id']
        corrected = self.send('rapido',data,native=True)
        self.assertEqual(corrected.status_code,302)
        self.assertEqual(OrdenMantenimiento.objects.count(),1)
        for row in requests:
            row.refresh_from_db(); self.assertIsNone(row.orden_atencion_id)

    def test_selected_solicitudes_replay_and_no_silent_reassignment(self):
        request = SolicitudFalla.objects.create(activo_ref=self.asset, descripcion='Falla')
        data = self.payload(solicitud_id=[str(request.pk)])
        first = self.send('rapido', data)
        self.assertEqual(first.status_code, 201, first.content)
        self.assertEqual(self.send('rapido', data).status_code, 200)
        conflict = self.send('rapido', {**data,'clave_captura':str(uuid4())})
        self.assertEqual(conflict.status_code,409)
        request.refresh_from_db(); self.assertEqual(request.orden_atencion_id,first.json()['id'])
        other = Activo.objects.create(nombre='Otro equipo')
        invalid = self.send('rapido',{**data,'clave_captura':str(uuid4()),'activo_id':other.pk})
        self.assertEqual(invalid.status_code,400)
        self.assertEqual(OrdenMantenimiento.objects.count(),1)

    def test_uploaded_evidence_and_receipt_roll_back_when_audit_fails(self):
        from pathlib import Path
        request = SolicitudFalla.objects.create(activo_ref=self.asset, descripcion='Falla')
        with TemporaryDirectory() as root, override_settings(MEDIA_ROOT=root):
            data = self.payload(solicitud_id=[str(request.pk)],
                factura_archivo=SimpleUploadedFile('factura.pdf',b'factura'),
                evidencias=[SimpleUploadedFile('foto.jpg',b'foto')])
            with patch('mantenimiento.services_capturas_equipos.log_event', side_effect=RuntimeError('audit fail')):
                with self.assertRaises(RuntimeError): self.send('rapido',data)
            self.assertEqual(OrdenMantenimiento.objects.count(),0)
            self.assertEqual(BitacoraMantenimiento.objects.count(),0)
            self.assertEqual(EvidenciaOrden.objects.count(),0)
            self.assertEqual(ComprobanteCapturaEquipo.objects.count(),0)
            request.refresh_from_db(); self.assertIsNone(request.orden_atencion_id)
            self.assertEqual([p for p in Path(root).rglob('*') if p.is_file()],[])

    def test_file_replay_checks_bytes_and_writes_only_once(self):
        with TemporaryDirectory() as root, override_settings(MEDIA_ROOT=root):
            key = str(uuid4())
            def data(content=b'foto'):
                return dict(activo_id=self.asset.pk, descripcion='Con evidencia', clave_captura=key,
                    evidencias=[SimpleUploadedFile('foto.jpg', content)], factura_archivo=SimpleUploadedFile('factura.pdf', b'factura'))
            first = self.send('rapido', data())
            self.assertEqual(first.status_code, 201)
            self.assertEqual(self.send('rapido', data()).status_code, 200)
            self.assertEqual(self.send('rapido', data(b'distinta')).status_code, 409)
            self.assertEqual(EvidenciaOrden.objects.count(), 1)
            self.assertEqual(OrdenMantenimiento.objects.count(), 1)

    def test_cleanup_failure_does_not_skip_other_files_or_mask_original_error(self):
        from django.core.files.storage import FileSystemStorage
        with TemporaryDirectory() as root, override_settings(MEDIA_ROOT=root):
            data = self.payload(factura_archivo=SimpleUploadedFile('factura.pdf',b'factura'),
                evidencias=[SimpleUploadedFile('foto.jpg',b'foto')])
            original = FileSystemStorage.delete
            calls = []
            def delete(storage, name):
                calls.append(name)
                if len(calls) == 1: raise OSError('cleanup fail')
                return original(storage, name)
            with patch('mantenimiento.services_capturas_equipos.log_event', side_effect=RuntimeError('audit fail')), patch.object(FileSystemStorage, 'delete', delete):
                with self.assertLogs('mantenimiento.services_capturas_equipos', level='ERROR'):
                    with self.assertRaisesRegex(RuntimeError, 'audit fail'): self.send('rapido',data)
            self.assertEqual(len(calls),2)
            self.assertEqual(OrdenMantenimiento.objects.count(),0)
            self.assertEqual(ComprobanteCapturaEquipo.objects.count(),0)

    def test_oversize_or_excess_files_are_not_silently_dropped(self):
        data = self.payload(evidencias=[SimpleUploadedFile(str(i)+'.jpg',b'x') for i in range(11)])
        self.assertEqual(self.send('rapido',data).status_code,400)
        self.assertEqual(OrdenMantenimiento.objects.count(),0)

    def test_concurrent_actual_api_and_web_retries(self):
        for mode in ('api','rapido'):
            data = self.payload(); barrier = Barrier(2)
            def send(_):
                close_old_connections()
                try:
                    actor = get_user_model().objects.get(pk=self.user.pk)
                    client = APIClient() if mode == 'api' else Client()
                    if mode == 'api':
                        client.force_authenticate(actor); url=reverse('api_activos_ordenes')
                    else:
                        client.force_login(actor); url=reverse('activos:registro_rapido')
                    barrier.wait(timeout=10)
                    response=client.post(url,data,HTTP_X_REQUESTED_WITH='XMLHttpRequest')
                    return response.status_code,response.json()['id']
                finally: connection.close()
            with ThreadPoolExecutor(max_workers=2) as pool: results=list(pool.map(send,range(2)))
            self.assertEqual(sorted(r[0] for r in results),[200,201])
            self.assertEqual(results[0][1],results[1][1])
