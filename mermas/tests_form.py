from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from mermas.models import MermaRegistro


class MermaProductoFormularioTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="merma.producto.ui",
            email="",
            password="test",
        )
        self.sucursal = Sucursal.objects.create(
            codigo="PROD-UI",
            nombre="Sucursal producto UI",
            activa=True,
        )
        self.client.force_login(self.user)

    def test_ajax_muestra_error_especifico_y_no_crea_merma(self):
        response = self.client.post(
            reverse("mermas:app"),
            {
                "sucursal": self.sucursal.pk,
                "producto_texto[]": ["Pastel de prueba"],
                "receta_id[]": [""],
                "cantidad[]": ["1"],
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "Toma o sube la foto del ticket Point.")
        self.assertFalse(MermaRegistro.objects.exists())

    def test_formulario_envia_cabecera_ajax_y_muestra_error_del_servidor(self):
        response = self.client.get(reverse("mermas:app"))

        self.assertContains(response, '"X-Requested-With": "XMLHttpRequest"')
        self.assertContains(response, "payload.error")
