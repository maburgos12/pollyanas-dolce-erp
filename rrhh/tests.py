from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse


class RRHHViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="rrhh", password="pass123")
        rrhh_group, _ = Group.objects.get_or_create(name="RRHH")
        self.user.groups.add(rrhh_group)
        self.client.login(username="rrhh", password="pass123")

    def test_empleados_view_and_create(self):
        resp = self.client.get(reverse("rrhh:empleados"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "RRHH · Empleados")

        resp_post = self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Demo",
                "area": "Producción",
                "puesto": "Pastelero",
                "salario_diario": "450.00",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Empleado Demo")

    def test_nomina_create_and_line(self):
        self.client.post(
            reverse("rrhh:empleados"),
            {
                "nombre": "Empleado Nómina",
                "area": "Ventas",
                "puesto": "Vendedor",
                "salario_diario": "350.00",
            },
            follow=True,
        )

        resp_nomina = self.client.post(
            reverse("rrhh:nomina"),
            {
                "tipo_periodo": "QUINCENAL",
                "fecha_inicio": "2026-02-01",
                "fecha_fin": "2026-02-15",
                "estatus": "BORRADOR",
            },
            follow=True,
        )
        self.assertEqual(resp_nomina.status_code, 200)
        self.assertContains(resp_nomina, "Capturar línea de nómina")

        from rrhh.models import Empleado, NominaPeriodo

        empleado = Empleado.objects.get(nombre="Empleado Nómina")
        periodo = NominaPeriodo.objects.first()

        resp_line = self.client.post(
            reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            {
                "action": "add_line",
                "empleado_id": empleado.id,
                "dias_trabajados": "15",
                "bonos": "500",
                "descuentos": "120",
            },
            follow=True,
        )
        self.assertEqual(resp_line.status_code, 200)
        periodo.refresh_from_db()
        self.assertGreater(periodo.total_neto, 0)

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("rrhh:empleados"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)
