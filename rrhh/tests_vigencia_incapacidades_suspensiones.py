from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from rrhh.models import Empleado, IncapacidadEmpleado, SuspensionEmpleado


HOY = date(2026, 7, 29)


class VigenciaSuspensionesViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="rrhh-vigencia-suspensiones",
            email="rrhh-suspensiones@example.com",
            password="testpass",
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado Vigencia Suspensión",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        self.finalizada = SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 7, 20),
            fecha_fin=date(2026, 7, 25),
            motivo="Suspensión histórica finalizada",
            aplicada_por=self.user,
        )
        self.vigente = SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=HOY,
            fecha_fin=HOY,
            motivo="Suspensión vigente en límite",
            aplicada_por=self.user,
        )
        self.programada = SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 2),
            motivo="Suspensión programada",
            aplicada_por=self.user,
        )
        self.cancelada = SuspensionEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 7, 20),
            fecha_fin=date(2026, 7, 25),
            motivo="Suspensión cancelada",
            estado=SuspensionEmpleado.ESTADO_CANCELADA,
            aplicada_por=self.user,
        )
        self.client.force_login(self.user)

    @patch("django.utils.timezone.localdate", return_value=HOY)
    def test_muestra_vigencia_sin_modificar_estado_administrativo(self, _localdate):
        response = self.client.get(reverse("rrhh:rrhh_suspensiones"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Estado administrativo")
        self.assertContains(response, "Vigencia")
        self.assertContains(response, "Finalizada")
        self.assertContains(response, "Vigente")
        self.assertContains(response, "Programada")
        self.assertContains(response, "Cancelada")
        self.assertContains(response, self.cancelada.motivo)

        self.finalizada.refresh_from_db()
        self.vigente.refresh_from_db()
        self.programada.refresh_from_db()
        self.cancelada.refresh_from_db()
        self.assertEqual(self.finalizada.estado, SuspensionEmpleado.ESTADO_ACTIVA)
        self.assertEqual(self.vigente.estado, SuspensionEmpleado.ESTADO_ACTIVA)
        self.assertEqual(self.programada.estado, SuspensionEmpleado.ESTADO_ACTIVA)
        self.assertEqual(self.cancelada.estado, SuspensionEmpleado.ESTADO_CANCELADA)

    @patch("django.utils.timezone.localdate", return_value=HOY)
    def test_filtra_suspensiones_por_vigencia_calculada(self, _localdate):
        response = self.client.get(
            reverse("rrhh:rrhh_suspensiones"),
            {"vigencia": "finalizada"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.finalizada.motivo)
        self.assertNotContains(response, self.vigente.motivo)
        self.assertNotContains(response, self.programada.motivo)
        self.assertNotContains(response, self.cancelada.motivo)


class VigenciaIncapacidadesViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="rrhh-vigencia-incapacidades",
            email="rrhh-incapacidades@example.com",
            password="testpass",
        )
        self.empleado = Empleado.objects.create(
            nombre="Empleado Vigencia Incapacidad",
            fecha_ingreso=date(2025, 1, 1),
            activo=True,
        )
        self.finalizada = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 7, 20),
            fecha_fin=date(2026, 7, 25),
            tipo=IncapacidadEmpleado.TIPO_ENFERMEDAD_GENERAL,
            folio="INCAP-FINALIZADA",
            estado=IncapacidadEmpleado.ESTADO_ACTIVA,
            registrada_por=self.user,
        )
        self.vigente = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=HOY,
            fecha_fin=HOY,
            tipo=IncapacidadEmpleado.TIPO_RIESGO_TRABAJO,
            folio="INCAP-VIGENTE",
            estado=IncapacidadEmpleado.ESTADO_CERRADA,
            registrada_por=self.user,
        )
        self.programada = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 2),
            tipo=IncapacidadEmpleado.TIPO_MATERNIDAD,
            folio="INCAP-PROGRAMADA",
            registrada_por=self.user,
        )
        self.cancelada = IncapacidadEmpleado.objects.create(
            empleado=self.empleado,
            fecha_inicio=date(2026, 7, 20),
            fecha_fin=date(2026, 7, 25),
            tipo=IncapacidadEmpleado.TIPO_OTRO,
            folio="INCAP-CANCELADA",
            estado=IncapacidadEmpleado.ESTADO_CANCELADA,
            registrada_por=self.user,
        )
        self.client.force_login(self.user)

    @patch("django.utils.timezone.localdate", return_value=HOY)
    def test_muestra_vigencia_sin_modificar_estado_administrativo(self, _localdate):
        response = self.client.get(reverse("rrhh:rrhh_incapacidades"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Estado administrativo")
        self.assertContains(response, "Vigencia")
        self.assertContains(response, "Finalizada")
        self.assertContains(response, "Vigente")
        self.assertContains(response, "Programada")
        self.assertContains(response, "Cancelada")
        self.assertContains(response, self.cancelada.folio)

        self.finalizada.refresh_from_db()
        self.vigente.refresh_from_db()
        self.programada.refresh_from_db()
        self.cancelada.refresh_from_db()
        self.assertEqual(self.finalizada.estado, IncapacidadEmpleado.ESTADO_ACTIVA)
        self.assertEqual(self.vigente.estado, IncapacidadEmpleado.ESTADO_CERRADA)
        self.assertEqual(self.programada.estado, IncapacidadEmpleado.ESTADO_ACTIVA)
        self.assertEqual(self.cancelada.estado, IncapacidadEmpleado.ESTADO_CANCELADA)

    @patch("django.utils.timezone.localdate", return_value=HOY)
    def test_filtra_incapacidades_por_vigencia_calculada(self, _localdate):
        response = self.client.get(
            reverse("rrhh:rrhh_incapacidades"),
            {"vigencia": "finalizada"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.finalizada.folio)
        self.assertNotContains(response, self.vigente.folio)
        self.assertNotContains(response, self.programada.folio)
        self.assertNotContains(response, self.cancelada.folio)
