from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_ADMIN, ROLE_ALMACEN, ROLE_VENTAS

from .models import Activo, OrdenMantenimiento, PlanMantenimiento


class ActivosFlowsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_user("admin_activos", "admin_activos@example.com", "test12345")
        self.almacen = user_model.objects.create_user("almacen_activos", "almacen_activos@example.com", "test12345")
        self.ventas = user_model.objects.create_user("ventas_activos", "ventas_activos@example.com", "test12345")

        Group.objects.get_or_create(name=ROLE_ADMIN)[0].user_set.add(self.admin)
        Group.objects.get_or_create(name=ROLE_ALMACEN)[0].user_set.add(self.almacen)
        Group.objects.get_or_create(name=ROLE_VENTAS)[0].user_set.add(self.ventas)

    def test_admin_can_create_activo_from_ui(self):
        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("activos:activos"),
            {
                "action": "create_activo",
                "nombre": "Refrigerador Cámara 01",
                "categoria": "Refrigeración",
                "estado": "OPERATIVO",
                "criticidad": "ALTA",
                "activo": "1",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(Activo.objects.filter(nombre="Refrigerador Cámara 01").exists())

    def test_almacen_can_raise_service_report(self):
        activo = Activo.objects.create(nombre="AA Oficina", categoria="Aire")
        self.client.force_login(self.almacen)
        response = self.client.post(
            reverse("activos:reportes"),
            {
                "activo_id": str(activo.id),
                "prioridad": "MEDIA",
                "descripcion": "No enfría correctamente",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            OrdenMantenimiento.objects.filter(activo_ref=activo, tipo=OrdenMantenimiento.TIPO_CORRECTIVO).exists()
        )

    def test_ventas_cannot_access_activos_module(self):
        self.client.force_login(self.ventas)
        response = self.client.get(reverse("activos:activos"))
        self.assertEqual(response.status_code, 403)

    def test_admin_can_create_plan(self):
        activo = Activo.objects.create(nombre="Horno 02", categoria="Hornos")
        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("activos:planes"),
            {
                "action": "create_plan",
                "activo_id": str(activo.id),
                "nombre": "Mantenimiento mensual horno",
                "tipo": PlanMantenimiento.TIPO_PREVENTIVO,
                "frecuencia_dias": "30",
                "estatus": PlanMantenimiento.ESTATUS_ACTIVO,
                "activo": "1",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(PlanMantenimiento.objects.filter(activo_ref=activo).exists())
