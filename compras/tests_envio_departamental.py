from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable
from .models import ItemCompraDepartamental, SolicitudCompraDepartamental


class EnvioDepartamentalTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('solicitante')
        self.area = AreaPresupuesto.objects.create(nombre='Producción', codigo='produccion')
        self.responsable = AreaPresupuestoResponsable.objects.create(area=self.area, usuario=self.user)
        self.client.force_login(self.user)

    def borrador(self):
        solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area, solicitante=self.user, periodo=date(2026, 9, 1),
        )
        ItemCompraDepartamental.objects.create(solicitud=solicitud, descripcion='Espiguero', cantidad=1)
        return solicitud

    @patch('compras.views_departamentales.timezone.localdate', return_value=date(2026, 9, 8))
    def test_mensual_se_envia_fuera_de_ventana_y_para_mes_elegido(self, _today):
        for periodo in ('2026-09', '2026-11'):
            with self.subTest(periodo=periodo):
                response = self.client.post(reverse('compras:departamental_nueva'), {
                    'area': self.area.pk, 'tipo': 'MENSUAL', 'periodo': periodo,
                    'accion': 'enviar', 'descripcion': ['Espiguero'], 'cantidad': ['1'],
                }, HTTP_ACCEPT='application/json')
                self.assertEqual(response.status_code, 200)
                solicitud = SolicitudCompraDepartamental.objects.get(periodo=periodo + '-01')
                self.assertEqual(solicitud.estado, 'ENVIADA')
                self.assertIsNotNone(solicitud.enviada_en)

    def test_envio_borrador_conserva_articulos_y_es_idempotente(self):
        solicitud = self.borrador()
        item_id = solicitud.items.get().pk
        url = reverse('compras:departamental_enviar', args=[solicitud.pk])
        detalle = self.client.get(reverse('compras:departamental_detalle', args=[solicitud.pk]))
        self.assertContains(detalle, url)
        self.assertContains(detalle, 'Pendiente de envío del área')
        self.assertEqual(self.client.get(url).status_code, 405)
        for _ in range(2):
            response = self.client.post(url, HTTP_ACCEPT='application/json')
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()['ok'])
            self.assertTrue(response.json()['reload'])
            solicitud.refresh_from_db()
            if _ == 0:
                enviada_en = solicitud.enviada_en
        self.assertEqual(solicitud.estado, 'ENVIADA')
        self.assertEqual(solicitud.enviada_en, enviada_en)
        self.assertEqual(solicitud.items.get().pk, item_id)
        self.assertEqual(solicitud.eventos.filter(tipo='ENVIO_SOLICITUD', actor=self.user).count(), 1)
        detalle = self.client.get(reverse('compras:departamental_detalle', args=[solicitud.pk]))
        self.assertNotContains(detalle, url)

    def test_ajeno_o_responsable_revocado_no_envia(self):
        solicitud = self.borrador()
        url = reverse('compras:departamental_enviar', args=[solicitud.pk])
        self.client.force_login(get_user_model().objects.create_user('ajeno'))
        self.assertEqual(self.client.post(url).status_code, 403)
        self.client.force_login(self.user)
        self.responsable.puede_capturar = False
        self.responsable.save()
        self.assertEqual(self.client.post(url).status_code, 403)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estado, 'BORRADOR')

    def test_sin_articulos_o_cancelada_no_envia(self):
        solicitud = self.borrador()
        solicitud.items.all().delete()
        url = reverse('compras:departamental_enviar', args=[solicitud.pk])
        self.assertEqual(self.client.post(url, HTTP_ACCEPT='application/json').status_code, 400)
        solicitud.estado = 'CANCELADA'
        solicitud.save()
        self.assertEqual(self.client.post(url, HTTP_ACCEPT='application/json').status_code, 409)
        solicitud.refresh_from_db()
        self.assertIsNone(solicitud.enviada_en)

    def test_envio_tradicional_regresa_al_folio_con_ancla(self):
        solicitud = self.borrador()
        response = self.client.post(reverse('compras:departamental_enviar', args=[solicitud.pk]))
        self.assertRedirects(response, reverse('compras:departamental_detalle', args=[solicitud.pk]) + '#solicitud-envio', fetch_redirect_response=False)

    def test_guardar_borrador_sigue_disponible(self):
        response = self.client.post(reverse('compras:departamental_nueva'), {
            'area': self.area.pk, 'tipo': 'MENSUAL', 'periodo': '2026-09',
            'accion': 'borrador', 'descripcion': ['Espiguero'], 'cantidad': ['1'],
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(SolicitudCompraDepartamental.objects.get().estado, 'BORRADOR')
