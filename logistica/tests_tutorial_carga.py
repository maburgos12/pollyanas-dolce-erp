from datetime import date, datetime
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from logistica.models import Repartidor, RutaEntrega


User = get_user_model()
MAZATLAN = ZoneInfo("America/Mazatlan")


class TutorialCargaBaseTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="TUT", nombre="Tutorial", activa=True)
        self.user = User.objects.create_user(username="tutorial.chofer", password="secret")
        User.objects.filter(pk=self.user.pk).update(
            date_joined=datetime(2026, 7, 15, 12, 0, tzinfo=MAZATLAN)
        )
        self.user.refresh_from_db()
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal)


class TutorialCargaElegibilidadTests(TutorialCargaBaseTests):
    def test_existente_sin_confirmar_y_sin_ruta_activa_debe_verlo(self):
        from logistica.services_tutorial_carga import debe_mostrar_tutorial_carga

        self.assertTrue(debe_mostrar_tutorial_carga(self.repartidor))

    def test_ruta_en_curso_no_se_interrumpe(self):
        from logistica.services_tutorial_carga import debe_mostrar_tutorial_carga

        RutaEntrega.objects.create(
            nombre="Ruta activa",
            fecha_ruta=date(2026, 7, 16),
            repartidor=self.repartidor,
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )

        self.assertFalse(debe_mostrar_tutorial_carga(self.repartidor))

    def test_confirmado_no_lo_ve(self):
        from logistica.services_tutorial_carga import debe_mostrar_tutorial_carga

        self.repartidor.tutorial_carga_sucursal_visto_en = timezone.now()
        self.repartidor.save(update_fields=["tutorial_carga_sucursal_visto_en"])

        self.assertFalse(debe_mostrar_tutorial_carga(self.repartidor))

    def test_creado_despues_del_lanzamiento_no_lo_ve(self):
        from logistica.services_tutorial_carga import debe_mostrar_tutorial_carga

        User.objects.filter(pk=self.user.pk).update(
            date_joined=datetime(2026, 7, 17, 12, 0, tzinfo=MAZATLAN)
        )
        self.user.refresh_from_db()

        self.assertFalse(debe_mostrar_tutorial_carga(self.repartidor))


class TutorialCargaApiTests(TutorialCargaBaseTests):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.user)

    def test_perfil_expone_bandera(self):
        response = self.client.get(reverse("api_logistica_mi_perfil"))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["mostrar_tutorial_carga_sucursal"])

    def test_ruta_activa_oculta_bandera_sin_marcar_visto(self):
        RutaEntrega.objects.create(
            nombre="Ruta activa",
            fecha_ruta=date(2026, 7, 16),
            repartidor=self.repartidor,
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )

        response = self.client.get(reverse("api_logistica_mi_perfil"))

        self.assertFalse(response.json()["mostrar_tutorial_carga_sucursal"])
        self.repartidor.refresh_from_db()
        self.assertIsNone(self.repartidor.tutorial_carga_sucursal_visto_en)

    def test_confirmar_es_idempotente(self):
        url = reverse("api_logistica_tutorial_carga_confirmar")

        primera_respuesta = self.client.post(url)
        self.repartidor.refresh_from_db()
        primera_fecha = self.repartidor.tutorial_carga_sucursal_visto_en
        segunda_respuesta = self.client.post(url)
        self.repartidor.refresh_from_db()

        self.assertEqual(primera_respuesta.status_code, 200)
        self.assertEqual(segunda_respuesta.status_code, 200)
        self.assertIsNotNone(primera_fecha)
        self.assertEqual(self.repartidor.tutorial_carga_sucursal_visto_en, primera_fecha)

    def test_usuario_sin_repartidor_no_puede_confirmar(self):
        otro = User.objects.create_user(username="tutorial.sin.perfil")
        self.client.force_login(otro)

        response = self.client.post(reverse("api_logistica_tutorial_carga_confirmar"))

        self.assertEqual(response.status_code, 403)


class TutorialCargaPwaTests(TutorialCargaBaseTests):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.user)

    def test_pwa_declara_tutorial_accesible_y_confirmacion_unica(self):
        html = self.client.get(reverse("logistica:pwa_app")).content.decode()

        self.assertIn('aria-labelledby="tutorial-carga-title"', html)
        self.assertIn("Entendido, comenzar", html)
        self.assertIn("mostrar_tutorial_carga_sucursal", html)
        self.assertIn("prefers-reduced-motion", html)
        self.assertIn("confirmarTutorialCarga", html)
        self.assertIn("v72-copy-inicio-gps", html)
