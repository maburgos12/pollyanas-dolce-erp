from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from threading import Barrier
from time import sleep
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connections
from django.test import Client, TestCase, TransactionTestCase, override_settings
from django.urls import reverse

from core.models import Sucursal
from mermas.models import MermaEvidencia, MermaRegistro
from recetas.models import Receta


class CapturaSetup:
    def setUp(self):
        super().setUp()
        media = TemporaryDirectory()
        self.addCleanup(media.cleanup)
        setting = override_settings(MEDIA_ROOT=media.name)
        setting.enable()
        self.addCleanup(setting.disable)
        self.user = get_user_model().objects.create_superuser('capture-safe', '', 'test')
        self.branch = Sucursal.objects.create(codigo='SAFE', nombre='Sucursal captura segura')
        self.recipe = Receta.objects.create(nombre='Producto seguro', codigo_point='SAFE-P')
        self.client.force_login(self.user)
        self.request_id = str(uuid4())

    def body(self, *, quantity='2', photo=b'producto'):
        return {'sucursal': self.branch.pk, 'receta_id[]': [str(self.recipe.pk)],
                'producto_texto[]': [self.recipe.nombre], 'cantidad[]': [quantity],
                'ticket_point': 'AUDIT-POINT', 'request_id': self.request_id,
                'ticket_fotos': [SimpleUploadedFile('ticket.jpg', b'ticket', content_type='image/jpeg')],
                'producto_fotos': [SimpleUploadedFile('producto.jpg', photo, content_type='image/jpeg')]}

    def post(self, **kwargs):
        return self.client.post(reverse('mermas:crear'), self.body(**kwargs),
                                HTTP_X_REQUESTED_WITH='XMLHttpRequest')


class CapturaIdempotenteTests(CapturaSetup, TestCase):
    def test_lost_response_retry_returns_one_receipt_and_two_photos_only(self):
        first = self.post()
        retry = self.post()
        self.assertEqual(first.status_code, 201)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(first.json(), retry.json())
        self.assertEqual(first.json()['request_id'], self.request_id)
        self.assertEqual(first.json()['redirect_url'], reverse('mermas:detalle', args=[first.json()['id']]))
        self.assertEqual(MermaRegistro.objects.count(), 1)
        self.assertEqual(MermaEvidencia.objects.count(), 2)

    def test_changed_quantity_or_photo_cannot_reuse_a_confirmed_intent(self):
        self.post()
        for changes in ({'quantity': '3'}, {'photo': b'altra-foto'}):
            self.assertEqual(self.post(**changes).status_code, 409)
        self.assertEqual(MermaRegistro.objects.count(), 1)

    def test_identical_new_intent_is_a_legitimate_new_merma(self):
        self.post()
        self.request_id = str(uuid4())
        self.assertEqual(self.post().status_code, 201)
        self.assertEqual(MermaRegistro.objects.count(), 2)

    def test_invalid_request_key_or_nonfinite_quantity_creates_nothing(self):
        self.request_id = 'invalid'
        self.assertEqual(self.post().status_code, 400)
        self.request_id = str(uuid4())
        self.assertEqual(self.post(quantity='NaN').status_code, 400)
        self.assertFalse(MermaRegistro.objects.exists())

    def test_other_actor_cannot_replay_an_intent(self):
        self.post()
        other = get_user_model().objects.create_superuser('other-safe', '', 'test')
        self.client.force_login(other)
        self.assertEqual(self.post().status_code, 409)
        self.assertEqual(MermaRegistro.objects.count(), 1)

    def test_expired_csrf_does_not_confirm_or_create_on_either_route(self):
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.user)
        for name in ('mermas:app', 'mermas:crear'):
            url = reverse(name)
            response = client.post(url, self.body(), HTTP_REFERER='http://testserver' + url,
                                   HTTP_X_REQUESTED_WITH='XMLHttpRequest', follow=True)
            self.assertEqual(response.status_code, 200)
            self.assertNotEqual(response.headers['Content-Type'], 'application/json')
        self.assertFalse(MermaRegistro.objects.exists())


class CapturaConcurrenteTests(CapturaSetup, TransactionTestCase):
    def test_first_folios_from_two_branches_are_unique_under_overlap(self):
        second = Sucursal.objects.create(codigo='SAFE2', nombre='Otra sucursal')
        barrier = Barrier(2)
        original = MermaRegistro._generate_folio

        def slower_generation(instance):
            folio = original(instance)
            sleep(.15)  # Hace visible la carrera de MAX+1 sin bloquear a un segundo escritor.
            return folio

        def write(branch):
            try:
                barrier.wait(timeout=10)
                return MermaRegistro.objects.create(sucursal_id=branch,
                                                    registrado_por_id=self.user.pk).folio
            finally:
                connections.close_all()

        with patch.object(MermaRegistro, '_generate_folio', slower_generation):
            with ThreadPoolExecutor(max_workers=2) as pool:
                folios = list(pool.map(write, [self.branch.pk, second.pk]))
        self.assertEqual(len(set(folios)), 2)
        self.assertEqual(MermaRegistro.objects.count(), 2)

    def test_inflight_duplicate_cannot_create_another_record(self):
        second_client = Client()
        second_client.force_login(self.user)
        barrier = Barrier(2)
        original = MermaRegistro._generate_folio

        def slow(instance):
            folio = original(instance)
            sleep(.2)
            return folio

        def post(client):
            try:
                barrier.wait(timeout=10)
                return client.post(reverse('mermas:app'), self.body(),
                                   HTTP_X_REQUESTED_WITH='XMLHttpRequest').status_code
            finally:
                connections.close_all()

        with patch.object(MermaRegistro, '_generate_folio', slow):
            with ThreadPoolExecutor(max_workers=2) as pool:
                statuses = list(pool.map(post, [self.client, second_client]))
        self.assertEqual(statuses.count(201), 1)
        self.assertIn(next(status for status in statuses if status != 201), (200, 409))
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(MermaRegistro.objects.count(), 1)
