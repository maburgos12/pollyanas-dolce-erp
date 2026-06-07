import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal, UserModuleAccess, UserProfile
from core.navigation import build_nav_groups

from .models import CategoriaFalla, ReporteFalla
from .tasks import _emails_de_grupo


class MisReportesActionsTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username="reporta", password="pass123")
        self.other_user = User.objects.create_user(username="otro", password="pass123")
        self.produccion_group, _ = Group.objects.get_or_create(name="produccion")
        for user in (self.user, self.other_user):
            UserModuleAccess.objects.create(user=user, module="fallas.mis_reportes", access=UserModuleAccess.ACCESS_VIEW)
            UserModuleAccess.objects.create(user=user, module="fallas.reportar", access=UserModuleAccess.ACCESS_VIEW)

        self.sucursal, _ = Sucursal.objects.get_or_create(
            codigo="LG",
            defaults={"nombre": "Las Glorias", "activa": True},
        )
        self.cedis, _ = Sucursal.objects.get_or_create(
            codigo="CEDIS",
            defaults={"nombre": "CEDIS", "activa": True},
        )
        Sucursal.objects.filter(pk__in=[self.sucursal.pk, self.cedis.pk]).update(activa=True, fecha_apertura=None)
        self.sucursal.refresh_from_db()
        self.cedis.refresh_from_db()
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

    def test_reportes_muestra_reportes_de_otros_colaboradores(self):
        reporte_ajeno = self._crear_reporte(self.other_user, "Falla de otro usuario")
        self.client.force_login(self.user)

        response = self.client.get(reverse("fallas:pwa-mis-reportes"))

        self.assertContains(response, "Letrero sin luz")
        self.assertContains(response, reporte_ajeno.titulo)

    def test_api_mine_muestra_reporte_propio_aunque_area_no_coincida(self):
        self.user.groups.add(self.produccion_group)
        self.reporte.area = ReporteFalla.AREA_VENTAS
        self.reporte.save(update_fields=["area"])
        self._crear_reporte(self.other_user, "Falla producción ajena")
        self.client.force_login(self.user)

        response = self.client.get(reverse("fallas_api:reportes-list"), {"mine": "1"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        rows = payload["results"] if isinstance(payload, dict) else payload
        ids = [item["id"] for item in rows]
        self.assertIn(self.reporte.id, ids)
        self.assertEqual(ids, [self.reporte.id])
        self.assertTrue(rows[0]["puede_editar"])

    def test_produccion_puede_editar_reporte_propio_de_cedis(self):
        self.user.groups.add(self.produccion_group)
        self.reporte.sucursal = self.cedis
        self.reporte.area = ReporteFalla.AREA_PRODUCCION
        self.reporte.save(update_fields=["sucursal", "area"])
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("fallas:pwa-editar-reporte", args=[self.reporte.id]),
            {
                "sucursal": self.cedis.id,
                "categoria": self.categoria.id,
                "area": ReporteFalla.AREA_PRODUCCION,
                "prioridad": ReporteFalla.PRIORIDAD_ALTA,
                "titulo": "Falla CEDIS corregida",
                "descripcion": "Se actualiza desde Producción.",
                "latitud": "",
                "longitud": "",
            },
        )

        self.assertRedirects(response, reverse("fallas:pwa-mis-reportes"))
        self.reporte.refresh_from_db()
        self.assertEqual(self.reporte.sucursal, self.cedis)
        self.assertEqual(self.reporte.titulo, "Falla CEDIS corregida")

    def test_api_permite_al_creador_actualizar_reporte_abierto(self):
        self.client.force_login(self.user)

        response = self.client.patch(
            reverse("fallas_api:reporte-detail", args=[self.reporte.id]),
            data=json.dumps(
                {
                    "sucursal": self.sucursal.id,
                    "categoria": self.categoria.id,
                    "area": ReporteFalla.AREA_GENERAL,
                    "prioridad": ReporteFalla.PRIORIDAD_ALTA,
                    "titulo": "Editado por API",
                    "descripcion": "Cambio desde la app móvil.",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.reporte.refresh_from_db()
        self.assertEqual(self.reporte.titulo, "Editado por API")
        self.assertEqual(self.reporte.prioridad, ReporteFalla.PRIORIDAD_ALTA)

    def test_api_lista_reportes_de_todas_las_areas_para_usuario_fallas(self):
        self.user.groups.add(self.produccion_group)
        reporte_ajeno = self._crear_reporte(self.other_user, "Falla visible para todos")
        reporte_ajeno.area = ReporteFalla.AREA_VENTAS
        reporte_ajeno.save(update_fields=["area"])
        self.client.force_login(self.user)

        response = self.client.get(reverse("fallas_api:reportes-list"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        rows = payload["results"] if isinstance(payload, dict) else payload
        ids = {item["id"] for item in rows}
        self.assertIn(self.reporte.id, ids)
        self.assertIn(reporte_ajeno.id, ids)

    def test_navegacion_muestra_fallas_una_sola_vez_para_produccion_staff(self):
        self.user.is_staff = True
        self.user.save(update_fields=["is_staff"])
        self.user.groups.add(self.produccion_group)

        nav = build_nav_groups(self.user, "/fallas/")
        fallas_groups = [
            group
            for group in nav
            if any(item["module"] == "fallas" for item in group["items"])
        ]

        self.assertEqual([group["key"] for group in fallas_groups], ["fallas"])
        self.assertEqual(
            [item["label"] for item in fallas_groups[0]["items"] if item["module"] == "fallas"],
            ["Reportes de fallas", "Reportar falla", "Reportes"],
        )

    def test_navegacion_muestra_fallas_una_sola_vez_para_usuario_ventas_autorizado(self):
        ventas_group, _ = Group.objects.get_or_create(name="VENTAS")
        self.other_user.groups.add(ventas_group)
        UserModuleAccess.objects.create(user=self.other_user, module="fallas", access=UserModuleAccess.ACCESS_MANAGE)

        nav = build_nav_groups(self.other_user, "/fallas/reportar/")
        fallas_groups = [
            group
            for group in nav
            if any(item["module"] == "fallas" for item in group["items"])
        ]

        self.assertEqual([group["key"] for group in fallas_groups], ["fallas"])


class FallasGroupAliasCompatibilityTests(TestCase):
    def test_emails_de_grupo_legacy_dg_uses_canonical_group(self):
        user = get_user_model().objects.create_user(
            username="dg.fallas.alias",
            email="dg.fallas@example.com",
        )
        user.groups.add(Group.objects.get_or_create(name="DG")[0])

        self.assertEqual(_emails_de_grupo("dg"), ["dg.fallas@example.com"])
