from django.contrib.auth.models import Group, User
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Sucursal
from logistica.models import LavadoUnidad, Repartidor, ReporteUnidad, Unidad


@override_settings(CELERY_TASK_ALWAYS_EAGER=True, EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
class LogisticaReportesApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.grupo_repartidor = Group.objects.create(name="repartidor")
        self.grupo_compras = Group.objects.create(name="compras_logistica")

        self.user_repartidor = User.objects.create_user(
            username="repartidor.api",
            password="pass123",
            email="repartidor@example.com",
        )
        self.user_repartidor.groups.add(self.grupo_repartidor)

        self.user_compras = User.objects.create_user(
            username="compras.logistica",
            password="pass123",
            email="compras@example.com",
        )
        self.user_compras.groups.add(self.grupo_compras)

        self.user_sin_grupo = User.objects.create_user(username="sin.grupo", password="pass123")

        self.sucursal = Sucursal.objects.create(codigo="LOG-TST", nombre="Sucursal Logística Test", activa=True)
        self.unidad = Unidad.objects.create(
            codigo="UNI-TST-01",
            descripcion="Unidad de reparto test",
            sucursal=self.sucursal,
            placa="TST-001",
        )
        self.repartidor = Repartidor.objects.create(
            user=self.user_repartidor,
            unidad_asignada=self.unidad,
            telefono="6871000000",
            sucursal=self.sucursal,
        )

    def _jwt_for(self, username: str, password: str = "pass123") -> str:
        response = self.client.post(
            reverse("api_logistica_auth_token"),
            {"username": username, "password": password},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return response.data["access"]

    def _auth(self, token: str):
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")

    def test_repartidor_crea_reporte_via_api(self):
        self._auth(self._jwt_for("repartidor.api"))
        response = self.client.post(
            reverse("api_logistica_reportes"),
            {
                "tipo": ReporteUnidad.TIPO_FALLA,
                "severidad": ReporteUnidad.SEVERIDAD_URGENTE,
                "descripcion": "La unidad presenta ruido en frenos.",
                "kilometraje": 42100,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(ReporteUnidad.objects.count(), 1)
        self.assertEqual(response.data["unidad_codigo"], self.unidad.codigo)

    def test_crear_reporte_sin_autenticacion_devuelve_401(self):
        response = self.client.post(
            reverse("api_logistica_reportes"),
            {
                "tipo": ReporteUnidad.TIPO_FALLA,
                "severidad": ReporteUnidad.SEVERIDAD_URGENTE,
                "descripcion": "Reporte sin sesión.",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_compras_logistica_puede_actualizar_reporte(self):
        reporte = ReporteUnidad.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            tipo=ReporteUnidad.TIPO_MANTENIMIENTO,
            severidad=ReporteUnidad.SEVERIDAD_URGENTE,
            descripcion="Servicio preventivo pendiente.",
        )

        self._auth(self._jwt_for("compras.logistica"))
        response = self.client.patch(
            reverse("api_logistica_reporte_detail", kwargs={"reporte_id": reporte.id}),
            {
                "estatus": ReporteUnidad.ESTATUS_EN_PROCESO,
                "proveedor_servicio": "Taller autorizado",
                "notas_compras": "Cotización solicitada.",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["estatus"], ReporteUnidad.ESTATUS_EN_PROCESO)

    def test_repartidor_no_puede_actualizar_estatus_de_reporte(self):
        reporte = ReporteUnidad.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            tipo=ReporteUnidad.TIPO_MANTENIMIENTO,
            severidad=ReporteUnidad.SEVERIDAD_URGENTE,
            descripcion="Servicio preventivo pendiente.",
        )

        self._auth(self._jwt_for("repartidor.api"))
        response = self.client.patch(
            reverse("api_logistica_reporte_detail", kwargs={"reporte_id": reporte.id}),
            {"estatus": ReporteUnidad.ESTATUS_EN_PROCESO},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_repartidor_consulta_estado_lavado_unidad(self):
        LavadoUnidad.objects.create(
            unidad=self.unidad,
            fecha="2026-05-10",
            costo="120.00",
            registrado_por=self.user_repartidor,
            notas="Lavado completo previo.",
        )

        self._auth(self._jwt_for("repartidor.api"))
        response = self.client.get(reverse("api_logistica_lavados_estado"), {"unidad_id": self.unidad.id})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["unidad_codigo"], self.unidad.codigo)
        self.assertEqual(response.data["ultimo_lavado"]["costo"], "120.00")
        self.assertIn("lavado_hoy", response.data)
        self.assertIn("dias_sin_lavar", response.data)

    def test_repartidor_registra_lavado_con_tipo_caja_refrigerada_importe_y_foto(self):
        self._auth(self._jwt_for("repartidor.api"))
        foto = SimpleUploadedFile("lavado.jpg", b"fake-image", content_type="image/jpeg")

        response = self.client.post(
            reverse("api_logistica_lavados"),
            {
                "unidad": self.unidad.id,
                "tipo_lavado": "caja_refrigerada",
                "costo": "180.50",
                "foto_evidencia": foto,
                "notas": "Lavado de caja refrigerada.",
                "latitud": "25.567890",
                "longitud": "-108.456789",
            },
            format="multipart",
            REMOTE_ADDR="10.10.10.10",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        lavado = LavadoUnidad.objects.get(unidad=self.unidad)
        self.assertEqual(lavado.tipo_lavado, "caja_refrigerada")
        self.assertEqual(str(lavado.costo), "180.50")
        self.assertEqual(lavado.ip_registro, "10.10.10.10")
        self.assertTrue(lavado.foto_evidencia.name)
