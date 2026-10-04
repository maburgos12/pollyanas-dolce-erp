from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import close_old_connections, transaction
from django.test import Client, TransactionTestCase, override_settings
from django.urls import reverse

from core.models import AuditLog, UserModuleAccess
from .models import Activo, BitacoraMantenimiento, EvidenciaOrden, OrdenMantenimiento
from .services_soportes import mutar_soporte


class SoportesSegurosTests(TransactionTestCase):
    def setUp(self):
        self.media = TemporaryDirectory(prefix='p7-soportes-test-')
        self.settings_media = override_settings(MEDIA_ROOT=self.media.name)
        self.settings_media.enable()
        self.addCleanup(self.media.cleanup)
        self.addCleanup(self.settings_media.disable)
        self.admin = get_user_model().objects.create_superuser('soportes_admin', password='test')
        self.lector = get_user_model().objects.create_user('soportes_lector')
        UserModuleAccess.objects.create(user=self.lector, module='inventario', access='view')
        self.orden = OrdenMantenimiento.objects.create(activo_ref=Activo.objects.create(nombre='Soportes'), tipo='CORRECTIVO')
        self.orden.factura_archivo.save('anterior.pdf', SimpleUploadedFile('anterior.pdf', b'anterior'))
        self.anterior = self.orden.factura_archivo.name
        self.storage = self.orden.factura_archivo.storage
        self.url = reverse('activos:orden_evidencias', args=[self.orden.pk])
        self.client.force_login(self.admin)

    def archivo(self):
        return SimpleUploadedFile('nuevo.pdf', b'nuevo')

    def factura(self):
        return mutar_soporte(orden_id=self.orden.pk, usuario=self.admin, accion='update_factura', datos={'numero_factura': 'F2'}, archivo=self.archivo())

    def test_fallos_sql_bitacora_y_auditoria_preservan_original_y_limpian_nuevo(self):
        for objetivo in ('activos.models.OrdenMantenimiento.save', 'activos.services_soportes.BitacoraMantenimiento.objects.create', 'activos.services_soportes.log_event'):
            with self.subTest(objetivo=objetivo), patch(objetivo, side_effect=RuntimeError('fallo original')):
                with self.assertRaisesRegex(RuntimeError, 'fallo original'):
                    self.factura()
            self.orden.refresh_from_db()
            self.assertEqual(self.orden.factura_archivo.name, self.anterior)
            self.assertEqual(self.orden.numero_factura, '')
            self.assertEqual(list(Path(self.media.name).rglob('*.pdf')), [Path(self.media.name) / self.anterior])
            self.assertFalse(self.orden.bitacora.exists())

    def test_reemplazo_elimina_solo_despues_commit_y_audita(self):
        real_delete = self.storage.delete
        def comprobar(nombre):
            self.assertFalse(transaction.get_connection().in_atomic_block and OrdenMantenimiento.objects.get(pk=self.orden.pk).factura_archivo.name == nombre)
            self.assertTrue(AuditLog.objects.filter(model='activos.OrdenMantenimiento', object_id=str(self.orden.pk)).exists())
            real_delete(nombre)
        with patch.object(self.storage, 'delete', side_effect=comprobar):
            self.assertEqual(self.factura(), [])
        self.orden.refresh_from_db()
        self.assertTrue(self.storage.exists(self.orden.factura_archivo.name))
        self.assertFalse(self.storage.exists(self.anterior))

    def test_limpieza_postcommit_falla_y_registra_advertencia_sin_rollback(self):
        with patch.object(self.storage, 'delete', side_effect=OSError('storage no disponible')):
            avisos = self.factura()
        self.orden.refresh_from_db()
        self.assertEqual(self.orden.numero_factura, 'F2')
        self.assertTrue(self.storage.exists(self.anterior))
        self.assertTrue(avisos)
        self.assertTrue(AuditLog.objects.filter(payload__soporte_limpieza_pendiente=self.anterior).exists())

    def test_referencia_compartida_con_evidencia_conserva_bytes(self):
        EvidenciaOrden.objects.create(orden=self.orden, archivo=self.anterior)
        self.factura()
        self.assertTrue(self.storage.exists(self.anterior))

    def test_storage_ruta_preexistente_no_se_borra(self):
        with patch.object(self.storage, 'save', return_value=self.anterior):
            with self.assertRaises(OSError):
                self.factura()
        self.assertTrue(self.storage.exists(self.anterior))
        self.orden.refresh_from_db()
        self.assertEqual(self.orden.factura_archivo.name, self.anterior)

    def test_storage_falla_sin_retorno_conserva_y_registra_candidato_incierto(self):
        real_save = self.storage.save
        def fallo(nombre, archivo, **kwargs):
            real_save(nombre, archivo, **kwargs)
            raise OSError('fallo de storage')
        with patch.object(self.storage, 'save', side_effect=fallo):
            with self.assertRaisesRegex(OSError, 'fallo de storage'):
                self.factura()
        self.assertTrue(self.storage.exists(self.anterior))
        self.assertEqual(len(list(Path(self.media.name).rglob('*.pdf'))), 2)
        self.assertTrue(AuditLog.objects.filter(payload__has_key='soporte_storage_revision').exists())

    def test_delete_get405_post_csrf_y_permisos(self):
        ev = EvidenciaOrden.objects.create(orden=self.orden, archivo=self.anterior)
        url = reverse('activos:eliminar_evidencia', args=[ev.pk])
        self.assertEqual(self.client.get(url).status_code, 405)
        csrf = Client(enforce_csrf_checks=True)
        csrf.force_login(self.admin)
        rechazo = csrf.post(url)
        self.assertEqual(rechazo.status_code, 302)  # CSRF_FAILURE_VIEW vigente retorna a login.
        self.assertEqual(rechazo.url, reverse('login'))
        self.assertTrue(EvidenciaOrden.objects.filter(pk=ev.pk).exists())
        self.assertTrue(self.storage.exists(self.anterior))
        self.client.force_login(self.lector)
        self.assertEqual(self.client.get(self.url).status_code, 200)
        self.assertEqual(self.client.post(self.url, {'archivo': self.archivo()}).status_code, 403)
        self.assertEqual(self.client.post(self.url, {'action': 'update_factura'}).status_code, 403)
        self.assertEqual(self.client.post(url).status_code, 403)
        self.assertTrue(EvidenciaOrden.objects.filter(pk=ev.pk).exists())

    def test_delete_rollback_preserva_fila_y_bytes(self):
        ev = EvidenciaOrden.objects.create(orden=self.orden, archivo=self.anterior)
        with patch('activos.services_soportes.log_event', side_effect=RuntimeError('auditoria')):
            with self.assertRaises(RuntimeError):
                mutar_soporte(orden_id=self.orden.pk, usuario=self.admin, accion='eliminar', datos={}, evidencia_id=ev.pk)
        self.assertTrue(EvidenciaOrden.objects.filter(pk=ev.pk).exists())
        self.assertTrue(self.storage.exists(self.anterior))

    def test_upload_audit_falla_no_deja_evidencia_ni_archivo(self):
        with patch('activos.services_soportes.log_event', side_effect=RuntimeError('audit upload')):
            with self.assertRaises(RuntimeError):
                mutar_soporte(orden_id=self.orden.pk, usuario=self.admin, accion='subir_evidencia', datos={}, archivo=self.archivo())
        self.assertFalse(self.orden.evidencias.exists())
        self.assertFalse(self.orden.bitacora.exists())
        self.assertEqual(list(Path(self.media.name).rglob('*.pdf')), [Path(self.media.name) / self.anterior])

    def test_delete_confirmado_retira_archivo_y_audita(self):
        ev = EvidenciaOrden.objects.create(orden=self.orden, archivo=self.archivo())
        nombre = ev.archivo.name
        response = self.client.post(reverse('activos:eliminar_evidencia', args=[ev.pk]), HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['target'], '#orden-evidencias')
        self.assertNotIn('name="nota_trabajo"', response.json()['html'])
        self.assertNotIn('name="archivo"', response.json()['html'])
        self.assertFalse(EvidenciaOrden.objects.filter(pk=ev.pk).exists())
        self.assertFalse(ev.archivo.storage.exists(nombre))
        self.assertTrue(AuditLog.objects.filter(action='DELETE', model='activos.EvidenciaOrden', object_id=str(ev.pk)).exists())
        self.assertTrue(self.storage.exists(self.anterior))

    def test_json_fragmento_html_contexto_y_mismo_negocio(self):
        datos = {'action': 'update_factura', 'numero_factura': 'F2', 'origen': 'reportes', 'return_query': 'q=texto&estatus=CERRADAS'}
        response = self.client.post(self.url, datos, HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['target'], '#orden-factura')
        self.assertNotIn('name="descripcion"', response.json()['html'])
        self.assertNotIn('id="orden-evidencias"', response.json()['html'])
        self.assertIn('name="origen" value="reportes"', response.json()['html'])
        self.assertIn('F2', response.json()['html'])
        self.assertNotIn('reload', response.json())
        response = self.client.post(self.url, {**datos, 'numero_factura': 'F3'})
        self.assertEqual(response.status_code, 302)
        self.assertIn('origen=reportes', response.url)
        self.orden.refresh_from_db()
        self.assertEqual(self.orden.numero_factura, 'F3')
        self.assertEqual(self.orden.bitacora.count(), 2)

    def test_upload_confirmado_y_segundo_envio_sin_archivo_no_duplica(self):
        datos = {'archivo': self.archivo(), 'descripcion': 'Soporte confirmado'}
        response = self.client.post(self.url, datos, HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['target'], '#orden-evidencias')
        self.assertEqual(self.orden.evidencias.count(), 1)
        response = self.client.post(self.url, {'descripcion': ''}, HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.orden.evidencias.count(), 1)

    def test_factura_desde_lista_preserva_original_ante_fallo_y_filtros(self):
        url = reverse('activos:ordenes') + '?estatus=CERRADA&enterprise_gap=SIN_RESPONSABLE'
        with patch('activos.services_soportes.log_event', side_effect=RuntimeError('audit lista')):
            response = self.client.post(url, {'action': 'update_factura', 'orden_id': self.orden.pk,
                                            'numero_factura': 'LISTA', 'factura_archivo': self.archivo()})
        self.assertEqual(response.url, url)
        self.orden.refresh_from_db()
        self.assertEqual(self.orden.factura_archivo.name, self.anterior)
        self.assertEqual(self.orden.numero_factura, '')
        self.assertEqual(list(Path(self.media.name).rglob('*.pdf')), [Path(self.media.name) / self.anterior])
        self.assertFalse(self.orden.bitacora.exists())

    def test_writers_lista_y_detalle_coordinan_mismo_soporte(self):
        def enviar(lista):
            close_old_connections()
            try:
                cliente = Client()
                cliente.force_login(self.admin)
                datos = {'action': 'update_factura', 'numero_factura': 'LISTA' if lista else 'DETALLE',
                         'factura_archivo': self.archivo()}
                if lista:
                    datos['orden_id'] = self.orden.pk
                return cliente.post(reverse('activos:ordenes') if lista else self.url, datos).status_code
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as executor:
            self.assertEqual(list(executor.map(enviar, [True, False])), [302, 302])
        self.orden.refresh_from_db()
        self.assertEqual(list(Path(self.media.name).rglob('*.pdf')), [Path(self.media.name) / self.orden.factura_archivo.name])
        self.assertEqual(self.orden.bitacora.count(), 2)

    def test_dos_reemplazos_concurrentes_conservan_solo_soporte_vigente(self):
        def enviar(numero):
            close_old_connections()
            try:
                return mutar_soporte(orden_id=self.orden.pk, usuario=self.admin, accion='update_factura', datos={'numero_factura': numero}, archivo=self.archivo())
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as executor:
            resultados = list(executor.map(enviar, ['F2', 'F3']))
        self.assertEqual(resultados, [[], []])
        self.orden.refresh_from_db()
        self.assertEqual(list(Path(self.media.name).rglob('*.pdf')), [Path(self.media.name) / self.orden.factura_archivo.name])
        self.assertEqual(self.orden.bitacora.count(), 2)
