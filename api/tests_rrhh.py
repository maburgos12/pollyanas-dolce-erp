from django.contrib.auth.models import Group, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from rrhh.models import Empleado, NominaPeriodo


class RRHHApiTests(APITestCase):
    def setUp(self):
        self.user_rrhh = User.objects.create_user(username="rrhh_api", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user_rrhh.groups.add(rrhh_group)

        self.user_lectura = User.objects.create_user(username="lectura_rrhh", password="pass123")
        lectura_group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user_lectura.groups.add(lectura_group)

    def test_empleados_create_and_list(self):
        self.client.force_authenticate(self.user_rrhh)
        url = reverse("api_rrhh_empleados")

        resp_create = self.client.post(
            url,
            {
                "nombre": "Empleado API",
                "area": "Compras",
                "puesto": "Analista",
                "salario_diario": "420.00",
            },
            format="json",
        )
        self.assertEqual(resp_create.status_code, status.HTTP_201_CREATED)

        resp_list = self.client.get(url)
        self.assertEqual(resp_list.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(resp_list.data["count"], 1)

    def test_nomina_create_line_and_dashboard(self):
        self.client.force_authenticate(self.user_rrhh)
        empleado = Empleado.objects.create(nombre="Empleado RRHH", salario_diario=400)

        nomina_url = reverse("api_rrhh_nominas")
        resp_nomina = self.client.post(
            nomina_url,
            {
                "tipo_periodo": "QUINCENAL",
                "fecha_inicio": "2026-02-01",
                "fecha_fin": "2026-02-15",
                "estatus": "BORRADOR",
            },
            format="json",
        )
        self.assertEqual(resp_nomina.status_code, status.HTTP_201_CREATED)
        nomina_id = resp_nomina.data["id"]

        lineas_url = reverse("api_rrhh_nomina_lineas", kwargs={"nomina_id": nomina_id})
        resp_linea = self.client.post(
            lineas_url,
            {
                "empleado_id": empleado.id,
                "dias_trabajados": "15.00",
                "bonos": "200.00",
                "descuentos": "50.00",
            },
            format="json",
        )
        self.assertEqual(resp_linea.status_code, status.HTTP_200_OK)

        periodo = NominaPeriodo.objects.get(pk=nomina_id)
        self.assertGreater(periodo.total_neto, 0)

        dashboard_url = reverse("api_rrhh_dashboard")
        resp_dash = self.client.get(dashboard_url)
        self.assertEqual(resp_dash.status_code, status.HTTP_200_OK)
        self.assertIn("empleados", resp_dash.data)
        self.assertIn("nomina", resp_dash.data)

    def test_lectura_can_view_but_not_create(self):
        self.client.force_authenticate(self.user_lectura)

        empleados_url = reverse("api_rrhh_empleados")
        resp_list = self.client.get(empleados_url)
        self.assertEqual(resp_list.status_code, status.HTTP_200_OK)

        resp_create = self.client.post(empleados_url, {"nombre": "No permitido"}, format="json")
        self.assertEqual(resp_create.status_code, status.HTTP_403_FORBIDDEN)
