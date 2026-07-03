from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from reportes.models import RecetaAreaProduccion


class ManoObraAreaViewsRBACTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.superuser = self.user_model.objects.create_superuser(
            username="moa_super", email="moa_super@example.com", password="pass12345",
        )
        self.usuario_normal = self.user_model.objects.create_user(username="moa_normal", password="pass12345")

    def test_usuario_sin_permiso_recibe_403_en_clasificacion(self):
        self.client.force_login(self.usuario_normal)
        response = self.client.get(reverse("reportes:mano_obra_area_clasificacion"))
        self.assertEqual(response.status_code, 403)

    def test_usuario_sin_permiso_recibe_403_en_reporte(self):
        self.client.force_login(self.usuario_normal)
        response = self.client.get(reverse("reportes:mano_obra_area_reporte"))
        self.assertEqual(response.status_code, 403)

    def test_superuser_ve_clasificacion(self):
        self.client.force_login(self.superuser)
        response = self.client.get(reverse("reportes:mano_obra_area_clasificacion"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Familias de receta")

    def test_superuser_ve_reporte(self):
        self.client.force_login(self.superuser)
        response = self.client.get(reverse("reportes:mano_obra_area_reporte"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Hornos")


class ClasificacionAreaProduccionTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.superuser = self.user_model.objects.create_superuser(
            username="moa_super2", email="moa_super2@example.com", password="pass12345",
        )
        self.client.force_login(self.superuser)

    def test_toggle_familia_crea_y_quita(self):
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {"accion": "toggle_familia", "familia": "Pastel", "area": "HORNOS"})
        self.assertTrue(RecetaAreaProduccion.objects.filter(familia="Pastel", area="HORNOS").exists())

        self.client.post(url, {"accion": "toggle_familia", "familia": "Pastel", "area": "HORNOS"})
        self.assertFalse(RecetaAreaProduccion.objects.filter(familia="Pastel", area="HORNOS").exists())

    def test_agregar_y_quitar_excepcion(self):
        from uuid import uuid4

        from recetas.models import Receta

        receta = Receta.objects.create(
            nombre="Pay Especial",
            codigo_point=f"COD-{uuid4().hex[:6]}",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia="Pay",
            hash_contenido=f"h-{uuid4()}",
        )
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {"accion": "agregar_excepcion", "receta_id": receta.id, "area": "EMBETUNADO"})
        fila = RecetaAreaProduccion.objects.get(receta=receta, area="EMBETUNADO")

        self.client.post(url, {"accion": "quitar_excepcion", "fila_id": fila.id})
        self.assertFalse(RecetaAreaProduccion.objects.filter(id=fila.id).exists())
