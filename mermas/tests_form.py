from tempfile import TemporaryDirectory

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse

from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess, UserProfile
from mermas.models import MermaProducto, MermaRegistro
from recetas.models import Receta


class MermaProductoFormularioTests(TestCase):
    def setUp(self):
        self.media_dir = TemporaryDirectory()
        self.addCleanup(self.media_dir.cleanup)
        self.media_override = override_settings(MEDIA_ROOT=self.media_dir.name)
        self.media_override.enable()
        self.addCleanup(self.media_override.disable)
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

    def test_formulario_usa_selector_nativo_de_producto_en_moviles(self):
        receta = Receta.objects.create(
            nombre="Pastel de Fresas con Crema Chico",
            codigo_point="0101",
        )

        response = self.client.get(reverse("mermas:app"))

        self.assertContains(response, 'select name="receta_id[]" data-native-select="true"')
        self.assertContains(response, f'value="{receta.pk}"')
        self.assertNotContains(response, 'list="productos-list"')

    def test_post_con_selector_conserva_receta_y_nombre_como_snapshot(self):
        receta = Receta.objects.create(
            nombre="Pastel de Fresas con Crema Chico",
            codigo_point="0101",
        )

        response = self.client.post(
            reverse("mermas:app"),
            {
                "sucursal": self.sucursal.pk,
                "ticket_point": "POINT-0101",
                "producto_texto[]": [""],
                "receta_id[]": [str(receta.pk)],
                "cantidad[]": ["1"],
                "ticket_fotos": [
                    SimpleUploadedFile("ticket.jpg", b"ticket", content_type="image/jpeg"),
                ],
                "producto_fotos": [
                    SimpleUploadedFile("producto.jpg", b"producto", content_type="image/jpeg"),
                ],
            },
        )

        producto = MermaProducto.objects.get()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(producto.receta, receta)
        self.assertEqual(producto.producto_texto, receta.nombre)

    def _branch_user(self, username, *, audits=False):
        user = get_user_model().objects.create_user(username=username, password="test")
        UserProfile.objects.create(user=user, sucursal=self.sucursal)
        UserModuleAccess.objects.create(
            user=user,
            module="mermas.captura",
            access=ACCESS_MANAGE,
        )
        if audits:
            UserModuleAccess.objects.create(
                user=user,
                module="ventas.visitas_sucursal",
                access=ACCESS_MANAGE,
            )
        return user

    def test_captura_manage_with_audits_can_register_product_waste_for_another_branch(self):
        otra_sucursal = Sucursal.objects.create(
            codigo="OTRA-UI",
            nombre="Otra sucursal",
            activa=True,
        )
        user = self._branch_user("luis.peraza", audits=True)
        self.client.force_login(user)

        form = self.client.get(reverse("mermas:app"), {"modo": "captura"})
        response = self.client.post(
            reverse("mermas:app") + "?modo=captura",
            {
                "sucursal": otra_sucursal.pk,
                "ticket_point": "TICKET-123",
                "producto_texto[]": ["Pastel de prueba"],
                "receta_id[]": [""],
                "cantidad[]": ["1"],
                "ticket_fotos": [
                    SimpleUploadedFile("ticket.jpg", b"ticket", content_type="image/jpeg"),
                ],
                "producto_fotos": [
                    SimpleUploadedFile("producto.jpg", b"producto", content_type="image/jpeg"),
                ],
            },
        )

        self.assertEqual(
            set(form.context["sucursales"].values_list("pk", flat=True)),
            {self.sucursal.pk, otra_sucursal.pk},
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(MermaRegistro.objects.get().sucursal, otra_sucursal)

    def test_captura_manage_without_audits_remains_restricted_to_own_branch(self):
        otra_sucursal = Sucursal.objects.create(
            codigo="RESTRINGIDA-UI",
            nombre="Sucursal restringida",
            activa=True,
        )
        user = self._branch_user("captura.sucursal")
        self.client.force_login(user)

        form = self.client.get(reverse("mermas:app"), {"modo": "captura"})
        response = self.client.post(
            reverse("mermas:app") + "?modo=captura",
            {
                "sucursal": otra_sucursal.pk,
                "ticket_point": "TICKET-456",
                "producto_texto[]": ["Pastel de prueba"],
                "receta_id[]": [""],
                "cantidad[]": ["1"],
                "ticket_fotos": [
                    SimpleUploadedFile("ticket.jpg", b"ticket", content_type="image/jpeg"),
                ],
                "producto_fotos": [
                    SimpleUploadedFile("producto.jpg", b"producto", content_type="image/jpeg"),
                ],
            },
        )

        self.assertEqual(
            list(form.context["sucursales"].values_list("pk", flat=True)),
            [self.sucursal.pk],
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(MermaRegistro.objects.exists())
