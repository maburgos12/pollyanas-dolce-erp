from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal, UserModuleAccess, UserProfile

from .models import CategoriaFalla, ReporteFalla


class MisReportesActionsTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username="reporta", password="pass123")
        self.other_user = User.objects.create_user(username="otro", password="pass123")
        for user in (self.user, self.other_user):
            UserModuleAccess.objects.create(user=user, module="fallas.mis_reportes", access=UserModuleAccess.ACCESS_VIEW)
            UserModuleAccess.objects.create(user=user, module="fallas.reportar", access=UserModuleAccess.ACCESS_VIEW)

        self.sucursal = Sucursal.objects.create(codigo="LG", nombre="Las Glorias", activa=True)
        UserProfile.objects.create(user=self.user, sucursal=self.sucursal)
        UserProfile.objects.create(user=self.other_user, sucursal=self.sucursal)
        self.categoria = CategoriaFalla.objects.create(nombre="Mobiliario", tipo=CategoriaFalla.TIPO_MOBILIARIO)
        self.reporte = self._crear_reporte(self.user, "Letrero sin luz")

    def _foto(self, name="evidencia.jpg"):
        return SimpleUploadedFile(name, b"fake-image", content_type="image/jpeg")

    def _crear_reporte(self, user, titulo, estatus=ReporteFalla.ESTATUS_ABIERTO):
        return ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            titulo=titulo,
            descripcion="El letrero no funciona.",
            prioridad=ReporteFalla.PRIORIDAD_MEDIA,
            foto_evidencia=self._foto(f"{titulo}.jpg"),
            reportado_por=user,
            estatus=estatus,
        )

    def test_mis_reportes_muestra_acciones_para_reporte_propio_abierto(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("fallas:pwa-mis-reportes"))

        self.assertContains(response, "Acciones")
        self.assertContains(response, reverse("fallas:pwa-editar-reporte", args=[self.reporte.id]))
        self.assertContains(response, reverse("fallas:pwa-eliminar-reporte", args=[self.reporte.id]))

    def test_editar_reporte_propio_abierto_actualiza_campos(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("fallas:pwa-editar-reporte", args=[self.reporte.id]),
            {
                "sucursal": self.sucursal.id,
                "categoria": self.categoria.id,
                "area": ReporteFalla.AREA_VENTAS,
                "prioridad": ReporteFalla.PRIORIDAD_ALTA,
                "titulo": "Letrero sin corriente",
                "descripcion": "No prende y no tiene clavija.",
                "latitud": "",
                "longitud": "",
            },
        )

        self.assertRedirects(response, reverse("fallas:pwa-mis-reportes"))
        self.reporte.refresh_from_db()
        self.assertEqual(self.reporte.titulo, "Letrero sin corriente")
        self.assertEqual(self.reporte.prioridad, ReporteFalla.PRIORIDAD_ALTA)
        self.assertEqual(self.reporte.area, ReporteFalla.AREA_VENTAS)

    def test_eliminar_reporte_propio_abierto_lo_borra(self):
        self.client.force_login(self.user)

        response = self.client.post(reverse("fallas:pwa-eliminar-reporte", args=[self.reporte.id]))

        self.assertRedirects(response, reverse("fallas:pwa-mis-reportes"))
        self.assertFalse(ReporteFalla.objects.filter(pk=self.reporte.id).exists())

    def test_no_permite_modificar_reporte_en_revision(self):
        reporte = self._crear_reporte(self.user, "Letrero en revisión", ReporteFalla.ESTATUS_REVISION)
        self.client.force_login(self.user)

        edit_response = self.client.get(reverse("fallas:pwa-editar-reporte", args=[reporte.id]))
        delete_response = self.client.post(reverse("fallas:pwa-eliminar-reporte", args=[reporte.id]))

        self.assertEqual(edit_response.status_code, 403)
        self.assertEqual(delete_response.status_code, 403)
        self.assertTrue(ReporteFalla.objects.filter(pk=reporte.id).exists())

    def test_no_permite_modificar_reporte_de_otro_usuario(self):
        self.client.force_login(self.other_user)

        edit_response = self.client.get(reverse("fallas:pwa-editar-reporte", args=[self.reporte.id]))
        delete_response = self.client.post(reverse("fallas:pwa-eliminar-reporte", args=[self.reporte.id]))

        self.assertEqual(edit_response.status_code, 403)
        self.assertEqual(delete_response.status_code, 403)
        self.assertTrue(ReporteFalla.objects.filter(pk=self.reporte.id).exists())
