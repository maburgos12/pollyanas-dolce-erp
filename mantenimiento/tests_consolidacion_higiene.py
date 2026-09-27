from datetime import date, datetime

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE
from core.models import AuditLog, Sucursal, UserModuleAccess
from fallas.models import CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene


class ConsolidacionHigieneViewTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.dg = users.objects.create_superuser(username="dg.preview", password="test")
        self.staff = users.objects.create_user(
            username="admin.preview", password="test", is_staff=True
        )
        self.mantenimiento = users.objects.create_user(
            username="mant.preview", password="test"
        )
        self.operadora = users.objects.create_user(
            username="operadora.preview", password="test"
        )
        UserModuleAccess.objects.create(
            user=self.mantenimiento,
            module="mantenimiento",
            access=ACCESS_MANAGE,
        )
        self.sucursal = Sucursal.objects.create(
            codigo="PREVIEW-HIG",
            nombre="Sucursal preview",
            activa=True,
        )
        self.categoria = CategoriaFalla.objects.create(
            nombre="Plomería preview",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )
        self.principal = self._crear_reporte(fecha=date(2026, 9, 25), indice=1)
        self.repetido = self._crear_reporte(fecha=date(2026, 9, 26), indice=2)
        self.ambiguo = self._crear_reporte(
            fecha=date(2026, 9, 27), indice=3, observacion="La tapa está rota"
        )
        self.url = reverse("mantenimiento:consolidacion-higiene")

    def _crear_reporte(self, *, fecha, indice, observacion="No descarga agua"):
        reporte = ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            titulo="Limpieza de baños · Sanitario limpio y funcional",
            descripcion=f"Hallazgo de higiene: {observacion}",
            justificacion_sin_foto="Prueba automatizada.",
            reportado_por=self.operadora,
            fecha_reporte=timezone.make_aware(
                datetime.combine(fecha, datetime.min.time())
            ),
        )
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.sucursal,
            fecha=fecha,
            clave_instancia=f"clientes-ronda-{indice}",
            plantilla_version="2026.1",
            creado_por=self.operadora,
        )
        RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="banos_sanitario",
            seccion="Limpieza de baños",
            punto_revision="Sanitario limpio y funcional",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            observacion=observacion,
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
        )
        return reporte

    def test_solo_admin_o_dg_puede_ver_y_aplicar(self):
        self.client.force_login(self.mantenimiento)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(
            self.client.post(
                self.url,
                {"pares": [f"{self.principal.id}:{self.repetido.id}"]},
            ).status_code,
            403,
        )
        for usuario in (self.dg, self.staff):
            self.client.force_login(usuario)
            self.assertEqual(self.client.get(self.url).status_code, 200)

    def test_tabla_solo_hace_seleccionables_las_coincidencias_exactas(self):
        self.client.force_login(self.dg)

        response = self.client.get(self.url)

        self.assertContains(
            response,
            f'value="{self.principal.id}:{self.repetido.id}"',
            html=False,
        )
        self.assertNotContains(
            response,
            f'value="{self.principal.id}:{self.ambiguo.id}"',
            html=False,
        )
        self.assertContains(response, "Revisión manual; no se aplicarán")
        self.assertContains(response, "data-async-action", html=False)

    def test_post_async_devuelve_toast_y_redirect_estable(self):
        self.client.force_login(self.dg)

        response = self.client.post(
            self.url,
            {"pares": [f"{self.principal.id}:{self.repetido.id}"]},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["toast"]["type"], "success")
        self.assertEqual(response.json()["redirect"], f"{self.url}#exactas-title")
        self.repetido.refresh_from_db()
        self.assertEqual(self.repetido.duplicado_de_id, self.principal.id)

    def test_post_tradicional_aplica_y_redirige_a_la_misma_revision(self):
        self.client.force_login(self.dg)

        response = self.client.post(
            self.url,
            {"pares": [f"{self.principal.id}:{self.repetido.id}"]},
        )

        self.assertRedirects(response, f"{self.url}#exactas-title")

    def test_error_html_conserva_solo_los_pares_validos_sin_reflejar_entrada_insegura(self):
        self.client.force_login(self.dg)
        par_valido = f"{self.principal.id}:{self.repetido.id}"
        entrada_insegura = '<img src=x onerror="alert(1)">'

        response = self.client.post(
            self.url,
            {"pares": [par_valido, entrada_insegura]},
        )

        self.assertEqual(response.status_code, 400)
        contenido = response.content.decode()
        self.assertRegex(contenido, rf'value="{par_valido}"[^>]*checked')
        self.assertNotIn("onerror", contenido)
        self.assertNotIn("<img src=x", contenido)
        self.repetido.refresh_from_db()
        self.assertIsNone(self.repetido.duplicado_de_id)

    def test_post_manipulado_no_aplica_una_coincidencia_ambigua(self):
        self.client.force_login(self.dg)

        response = self.client.post(
            self.url,
            {"pares": [f"{self.principal.id}:{self.ambiguo.id}"]},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["toast"]["type"], "warning")
        self.ambiguo.refresh_from_db()
        self.assertIsNone(self.ambiguo.duplicado_de_id)
        self.assertFalse(
            AuditLog.objects.filter(
                action="CONSOLIDATE", object_id=str(self.ambiguo.id)
            ).exists()
        )

    def test_post_rechaza_pares_malformados_sin_aplicar_ninguno(self):
        self.client.force_login(self.dg)

        response = self.client.post(
            self.url,
            {
                "pares": [
                    f"{self.principal.id}:{self.repetido.id}",
                    "1:2:3",
                    "abc:2",
                    "-1:2",
                ]
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["toast"]["type"], "error")
        self.repetido.refresh_from_db()
        self.assertIsNone(self.repetido.duplicado_de_id)
        self.assertFalse(AuditLog.objects.filter(action="CONSOLIDATE").exists())

    def test_post_sin_seleccion_devuelve_error_reintentable(self):
        self.client.force_login(self.dg)

        response = self.client.post(
            self.url,
            {},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertTrue(payload["toast"]["persistent"])
