from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_LECTURA
from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida


class ControlViewsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="lectura_control_view",
            email="lectura_control_view@example.com",
            password="test12345",
        )
        group, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user.groups.add(group)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        insumo = Insumo.objects.create(nombre="Insumo control view", unidad_base=unidad, activo=True)
        ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("5.000"))

    def test_discrepancias_view_loads(self):
        self.client.force_login(self.user)
        resp = self.client.get(reverse("control:discrepancias"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Control · Discrepancias")
        self.assertContains(resp, "Semáforo global")
