from datetime import date
from decimal import Decimal

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog, Notificacion
from core.notificaciones import notificar_hora_extra_solicitada
from core.notificaciones_bandeja import agrupar_notificaciones, contar_grupos_pendientes
from rrhh.models import Empleado, HoraExtra


class HorasExtraBandejaTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser('jefe.extra', '', 'test-only')
        self.empleado = Empleado.objects.create(nombre='Persona de prueba', salario_diario=Decimal('400'))
        self.client.force_login(self.user)
        self.url = reverse('rrhh:rrhh_he_list')
        self.pendiente = self.extra(date(2026, 10, 9))
        self.autorizada = self.extra(date(2026, 10, 12), 'autorizado')

    def extra(self, fecha, estado='pendiente'):
        return HoraExtra.objects.create(empleado=self.empleado, jefe_directo=self.user,
            fecha=fecha, horas=Decimal('1'), estado=estado,
            autorizado_por=self.user if estado == 'autorizado' else None,
            fecha_autorizacion_jefe=timezone.now() if estado == 'autorizado' else None)

    def test_bandeja_separa_estados_y_muestra_autorizacion(self):
        response = self.client.get(self.url)
        self.assertContains(response, f'id="hora-extra-{self.pendiente.pk}"')
        self.assertNotContains(response, f'id="hora-extra-{self.autorizada.pk}"')
        response = self.client.get(self.url, {'estado': 'autorizado'})
        self.assertContains(response, f'id="hora-extra-{self.autorizada.pk}"')
        self.assertNotContains(response, f'id="hora-extra-{self.pendiente.pk}"')
        self.assertContains(response, 'Autorizó: jefe.extra')
        self.assertNotContains(response, '>Autorizar</button>')

    def test_resolver_inline_no_redirige_y_audita_sin_reautorizar(self):
        response = self.client.post(self.url, {'hora_extra_id': self.pendiente.pk, 'action': 'autorizar'},
            HTTP_ACCEPT='application/json', HTTP_X_HORAS_EXTRA_INLINE='1')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotIn('redirect', data)
        self.assertEqual(data['estado'], 'autorizado')
        self.assertNotIn('>Autorizar</button>', data['html'])
        self.assertEqual(data['conteos']['pendiente'], 0)
        self.pendiente.refresh_from_db()
        stamp = self.pendiente.fecha_autorizacion_jefe
        self.assertTrue(AuditLog.objects.filter(model='rrhh.HoraExtra', object_id=str(self.pendiente.pk), user=self.user).exists())
        response = self.client.post(self.url, {'hora_extra_id': self.pendiente.pk, 'action': 'autorizar'},
            HTTP_ACCEPT='application/json', HTTP_X_HORAS_EXTRA_INLINE='1')
        self.assertEqual(response.status_code, 400)
        self.pendiente.refresh_from_db()
        self.assertEqual(self.pendiente.fecha_autorizacion_jefe, stamp)

    def test_notificaciones_resueltas_no_piden_autorizar_ni_cuentan(self):
        notificar_hora_extra_solicitada(self.pendiente)
        notificar_hora_extra_solicitada(self.autorizada)
        before = list(Notificacion.objects.order_by('pk').values())
        self.assertEqual(contar_grupos_pendientes(self.user), 1)
        rows = agrupar_notificaciones(self.user)
        row = next(r for r in rows if r.objeto_id == str(self.autorizada.pk))
        self.assertTrue(row.grupo_resuelta)
        self.assertNotIn('pendiente', row.titulo.lower())
        self.assertNotIn('requiere autorización de jefe', row.mensaje)
        self.assertIn('estado=autorizado', row.url)
        self.assertEqual(list(Notificacion.objects.order_by('pk').values()), before)

    def test_filtro_invalido_y_usuario_ajeno(self):
        response = self.client.get(self.url, {'estado': 'desconocido'})
        self.assertNotContains(response, f'id="hora-extra-{self.autorizada.pk}"')
        otro = User.objects.create_user('otro.extra')
        self.client.force_login(otro)
        response = self.client.post(self.url, {'hora_extra_id': self.pendiente.pk, 'action': 'autorizar'},
            HTTP_ACCEPT='application/json', HTTP_X_HORAS_EXTRA_INLINE='1')
        self.assertEqual(response.status_code, 403)
        self.pendiente.refresh_from_db()
        self.assertEqual(self.pendiente.estado, 'pendiente')

    def test_resolver_retira_el_aviso_en_bandeja_y_conserva_su_historia(self):
        notificar_hora_extra_solicitada(self.pendiente)
        original = Notificacion.objects.values().get()
        response = self.client.post(self.url, {'hora_extra_id': self.pendiente.pk, 'action': 'rechazar'},
            HTTP_ACCEPT='application/json', HTTP_X_HORAS_EXTRA_INLINE='1')
        self.assertEqual(response.json()['notificaciones_pendientes'], 0)
        self.assertEqual(Notificacion.objects.values().get(), original)
        self.assertNotContains(self.client.get(reverse('notificaciones')), 'Hora extra pendiente:')
        self.assertContains(self.client.get(reverse('notificaciones'), {'estado': 'leidas'}), 'Rechazado')

    def test_notificaciones_de_todos_los_estados_y_otros_usuarios(self):
        for day, state in enumerate(('autorizado', 'rechazado', 'cancelado', 'pagado'), start=16):
            notificar_hora_extra_solicitada(self.extra(date(2026, 10, day), state))
        otro = User.objects.create_user('otro.avisos')
        Notificacion.objects.create(usuario=otro, titulo='Aviso privado')
        Notificacion.objects.create(usuario=self.user, titulo='Otra tarea pendiente')
        Notificacion.objects.create(usuario=self.user, titulo='Hora eliminada',
            objeto_tipo='rrhh.HoraExtra', objeto_id='999999999')
        self.assertEqual(contar_grupos_pendientes(self.user), 1)
        response = self.client.get(reverse('notificaciones'), {'estado': 'todas'})
        self.assertNotContains(response, 'Aviso privado')
        self.assertContains(response, 'solicitud no disponible')
        self.assertContains(response, 'Otra tarea pendiente')
