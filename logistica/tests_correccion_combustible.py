from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase

from core.models import AuditLog, Notificacion, Sucursal
from logistica.models import BitacoraSalidaLlegada, CargaCombustibleUnidad, Repartidor, Unidad

User = get_user_model()


class CorreccionCombustibleTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.dg = User.objects.create_user('dg.correccion')
        cls.dg.groups.add(Group.objects.get_or_create(name='DG')[0])
        cls.admin = User.objects.create_user('admin.correccion')
        cls.admin.groups.add(Group.objects.get_or_create(name='ADMIN')[0])
        cls.chofer = User.objects.create_user('chofer.correccion')
        cls.otro = User.objects.create_user('otro.correccion', is_staff=True)
        sucursal = Sucursal.objects.create(codigo='COR', nombre='Corrección')
        cls.unidad = Unidad.objects.create(codigo='COR-1', descripcion='Prueba', sucursal=sucursal)
        cls.repartidor = Repartidor.objects.create(user=cls.chofer, sucursal=sucursal)
        cls.bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=cls.repartidor, unidad=cls.unidad, km_salida=100,
            nivel_gas_salida='1/2', foto_tablero_salida='test/tablero.jpg', cerrada=True,
        )
        cls.carga = CargaCombustibleUnidad.objects.create(
            bitacora=cls.bitacora, repartidor=cls.repartidor, unidad=cls.unidad,
            litros='52.04', importe_total='1200', foto_ticket='test/ticket.jpg',
            auditoria_estado='revision', auditoria_motivos=['importe_no_coincide'],
            auditoria_detalle={'ticket_leido': {'status': 'ok', 'importe_total': '1400'}},
        )

    def setUp(self):
        self.client.force_login(self.dg)
        self.url = f'/logistica/combustible/{self.carga.pk}/editar/'
        self.avisos = '/api/logistica/avisos-combustible/'

    def datos(self, **changes):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        data = {'litros': '52.04', 'importe_total': '1400.00', 'motivo': 'Importe confirmado en la foto',
                'revision': response.context['form']['revision'].value()}
        return dict(data, **changes)

    def corregir(self, **changes):
        data = self.datos(**changes)
        with patch('logistica.tasks.auditar_ticket_combustible.delay') as enqueue:
            with self.captureOnCommitCallbacks(execute=True):
                response = self.client.post(self.url, data)
        return response, enqueue

    def test_corrige_cerrada_y_conserva_evidencia_con_historial_y_aviso(self):
        response, enqueue = self.corregir()
        self.assertEqual(response.status_code, 302)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1400'))
        self.assertEqual(self.carga.foto_ticket.name, 'test/ticket.jpg')
        self.assertEqual(self.carga.repartidor_id, self.repartidor.pk)
        self.assertEqual(self.carga.auditoria_estado, 'pendiente')
        self.assertNotIn('ticket_verificado', self.carga.auditoria_motivos)
        log = AuditLog.objects.get(action='UPDATE', model='logistica.CargaCombustibleUnidad')
        self.assertEqual(log.payload['antes']['importe_total'], '1200.00')
        self.assertEqual(log.payload['despues']['importe_total'], '1400.00')
        self.assertEqual(log.payload['motivo'], 'Importe confirmado en la foto')
        self.assertEqual(log.user_id, self.dg.pk)
        aviso = Notificacion.objects.get(objeto_tipo='logistica.correccion_combustible')
        self.assertEqual(aviso.usuario_id, self.chofer.pk)
        self.assertEqual(aviso.actor_id, self.dg.pk)
        self.assertIn('$1,200.00', aviso.mensaje)
        self.assertIn('$1,400.00', aviso.mensaje)
        self.assertFalse(aviso.leida)
        enqueue.assert_called_once_with(self.carga.pk)

    def test_admin_puede_editar(self):
        self.client.force_login(self.admin)
        self.assertEqual(self.client.get(self.url).status_code, 200)

    def test_cola_caida_conserva_correccion_y_aviso_sin_error_http(self):
        data = self.datos()
        with patch('logistica.tasks.auditar_ticket_combustible.delay', side_effect=ConnectionError('cola caída')):
            with self.assertLogs('django.test', level='ERROR'):
                with self.captureOnCommitCallbacks(execute=True):
                    response = self.client.post(self.url, data)
        self.assertEqual(response.status_code, 302)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1400'))
        self.assertEqual(self.carga.auditoria_estado, 'pendiente')
        self.assertEqual(Notificacion.objects.count(), 1)

    def test_chofer_y_staff_sin_rol_no_pueden_editar(self):
        data = self.datos()
        for user in (self.chofer, self.otro):
            self.client.force_login(user)
            self.assertIn(self.client.get(self.url).status_code, (302, 403))
            self.assertIn(self.client.post(self.url, data).status_code, (302, 403))
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1200'))
        self.assertFalse(Notificacion.objects.exists())

    def test_campos_invalidos_no_generan_cambio(self):
        for changes in ({'importe_total': '0'}, {'importe_total': 'NaN'}, {'litros': '-1'}, {'motivo': '   '}, {'revision': 'manipulado'}):
            response = self.client.post(self.url, self.datos(**changes))
            self.assertIn(response.status_code, (200, 409))
            self.carga.refresh_from_db()
            self.assertEqual(self.carga.importe_total, Decimal('1200'))
            self.assertFalse(Notificacion.objects.exists())

    def test_sin_cambio_no_duplica_aviso(self):
        response, enqueue = self.corregir(importe_total='1200.00')
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Notificacion.objects.exists())
        self.assertFalse(AuditLog.objects.filter(action='UPDATE').exists())
        enqueue.assert_not_called()

    def test_formulario_obsoleto_no_pisa_correccion_y_doble_envio_no_duplica(self):
        data = self.datos()
        with self.captureOnCommitCallbacks(execute=False):
            self.assertEqual(self.client.post(self.url, data).status_code, 302)
        data['importe_total'] = '1600'
        self.assertEqual(self.client.post(self.url, data).status_code, 409)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1400'))
        self.assertEqual(Notificacion.objects.count(), 1)

    def test_fallo_notificacion_revierte_captura_e_historial(self):
        data = self.datos()
        with patch('logistica.views_combustible.crear_notificacion', side_effect=RuntimeError('fallo')):
            with self.assertRaises(RuntimeError):
                self.client.post(self.url, data)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1200'))
        self.assertFalse(AuditLog.objects.filter(action='UPDATE').exists())

    def test_aviso_solo_destinatario_y_una_vez_tras_entendido(self):
        self.corregir()
        aviso = Notificacion.objects.get(objeto_tipo='logistica.correccion_combustible')
        confirmar = f'{self.avisos}{aviso.pk}/confirmar/'
        self.assertEqual(self.client.get(self.avisos).json(), [])
        self.assertEqual(self.client.post(confirmar).status_code, 404)
        self.client.force_login(self.chofer)
        self.assertEqual(len(self.client.get(self.avisos).json()), 1)
        aviso.refresh_from_db()
        self.assertFalse(aviso.leida)  # Consultar no equivale a haberlo leído.
        self.assertEqual(self.client.get(confirmar).status_code, 405)
        self.assertEqual(self.client.post(confirmar).status_code, 200)
        aviso.refresh_from_db()
        fecha = aviso.leido_en
        self.assertIsNotNone(fecha)
        self.assertEqual(self.client.post(confirmar).status_code, 200)
        aviso.refresh_from_db()
        self.assertEqual(aviso.leido_en, fecha)
        self.client.logout()
        self.client.force_login(self.chofer)
        self.assertEqual(self.client.get(self.avisos).json(), [])

    def test_preview_no_consume_avisos_y_no_incluye_los_del_chofer(self):
        self.corregir()
        aviso = Notificacion.objects.get(objeto_tipo='logistica.correccion_combustible')
        self.dg.is_superuser = True
        self.dg.save(update_fields=['is_superuser'])
        self.assertEqual(self.client.get(self.avisos, {'preview_repartidor': self.repartidor.pk}).json(), [])
        response = self.client.post(f'{self.avisos}{aviso.pk}/confirmar/?preview_repartidor={self.repartidor.pk}')
        self.assertEqual(response.status_code, 403)
        aviso.refresh_from_db()
        self.assertFalse(aviso.leida)

    def test_confirmar_no_marca_otro_tipo_de_notificacion(self):
        aviso = Notificacion.objects.create(usuario=self.dg, titulo='Otra notificación', objeto_tipo='otra')
        self.assertEqual(self.client.post(f'{self.avisos}{aviso.pk}/confirmar/').status_code, 404)

    def test_segunda_correccion_genera_un_aviso_nuevo(self):
        self.corregir()
        aviso = Notificacion.objects.get(objeto_tipo='logistica.correccion_combustible')
        self.client.force_login(self.chofer)
        self.client.post(f'{self.avisos}{aviso.pk}/confirmar/')
        self.client.force_login(self.admin)
        self.corregir(importe_total='1450.00')
        self.client.force_login(self.chofer)
        pendientes = self.client.get(self.avisos).json()
        self.assertEqual(len(pendientes), 1)
        self.assertNotEqual(pendientes[0]['id'], aviso.pk)
        self.assertIn('$1,450.00', pendientes[0]['mensaje'])

    def test_capturista_inactivo_no_pierde_aviso(self):
        User.objects.filter(pk=self.chofer.pk).update(is_active=False)
        response, enqueue = self.corregir()
        self.assertContains(response, 'El capturista está inactivo')
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1200'))
        enqueue.assert_not_called()

    def test_csrf_impide_edicion_y_confirmacion_por_sesion(self):
        from django.test import Client
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.dg)
        response = client.post(self.url, self.datos())
        self.assertRedirects(response, '/login/', fetch_redirect_response=False)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.importe_total, Decimal('1200'))
        self.assertFalse(Notificacion.objects.exists())
        self.corregir()
        aviso = Notificacion.objects.get(objeto_tipo='logistica.correccion_combustible')
        client.force_login(self.chofer)
        self.assertEqual(client.post(f'{self.avisos}{aviso.pk}/confirmar/').status_code, 403)
        aviso.refresh_from_db()
        self.assertFalse(aviso.leida)

    def test_lectura_antigua_no_sobrescribe_correccion_concurrente(self):
        from logistica.services_combustible_auditoria import auditar_carga_combustible
        def lectura(_):
            CargaCombustibleUnidad.objects.filter(pk=self.carga.pk).update(importe_total=1400, auditoria_estado='pendiente')
            return {'es_ticket': True, 'legible': True, 'folio': '', 'litros': Decimal('52.04'),
                    'importe_total': Decimal('1200'), 'precio_por_litro': Decimal('23.06')}
        with patch('logistica.services_combustible_auditoria.leer_ticket', side_effect=lectura), \
             patch('logistica.services_combustible_auditoria._sha256_field_file', return_value=''), \
             patch('logistica.services_combustible_auditoria._analizar_imagen', return_value={'score': 0, 'motivos': [], 'detalle': {}}):
            auditar_carga_combustible(self.carga.pk)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.auditoria_estado, 'pendiente')

    def test_reauditoria_conserva_reconocimiento_agregado_durante_lectura(self):
        from logistica.services_combustible_auditoria import auditar_carga_combustible
        reconocimiento = dict(carga_id=self.carga.pk, periodo='2026-10', importe='1200.00',
            fuente='CONFIRMACION_DG', criterio='SIN_TICKET')
        def lectura(_):
            actual = CargaCombustibleUnidad.objects.get(pk=self.carga.pk)
            actual.auditoria_detalle['reconocimiento_costo'] = reconocimiento
            actual.save(update_fields=['auditoria_detalle'])
            return {'es_ticket': False, 'legible': True, 'folio': '', 'litros': None,
                    'importe_total': None, 'precio_por_litro': None}
        with patch('logistica.services_combustible_auditoria.leer_ticket', side_effect=lectura), \
             patch('logistica.services_combustible_auditoria._sha256_field_file', return_value=''), \
             patch('logistica.services_combustible_auditoria._analizar_imagen', return_value={'score':0,'motivos':[],'detalle':{}}):
            auditar_carga_combustible(self.carga.pk)
        self.carga.refresh_from_db()
        self.assertEqual(self.carga.auditoria_detalle['reconocimiento_costo'], reconocimiento)
        self.assertFalse(self.carga.auditoria_detalle['ticket_leido']['es_ticket'])
        self.assertNotEqual(self.carga.auditoria_estado, 'ok')
