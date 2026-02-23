from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_ALMACEN, ROLE_LECTURA
from core.models import Sucursal
from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta

from .models import MermaPOS, VentaPOS


class ControlViewsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user_lectura = user_model.objects.create_user(
            username="lectura_control_view",
            email="lectura_control_view@example.com",
            password="test12345",
        )
        group_lectura, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user_lectura.groups.add(group_lectura)

        self.user_almacen = user_model.objects.create_user(
            username="almacen_control_view",
            email="almacen_control_view@example.com",
            password="test12345",
        )
        group_almacen, _ = Group.objects.get_or_create(name=ROLE_ALMACEN)
        self.user_almacen.groups.add(group_almacen)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        insumo = Insumo.objects.create(nombre="Insumo control view", unidad_base=unidad, activo=True)
        ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("5.000"))
        self.sucursal = Sucursal.objects.create(codigo="MTRZ", nombre="Matriz", activa=True)
        self.receta = Receta.objects.create(
            nombre="Pastel prueba control",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="control-tests-hash-1",
        )

    def test_discrepancias_view_loads(self):
        self.client.force_login(self.user_lectura)
        resp = self.client.get(reverse("control:discrepancias"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Control · Discrepancias")
        self.assertContains(resp, "Semáforo global")

    def test_captura_movil_forbidden_for_lectura(self):
        self.client.force_login(self.user_lectura)
        resp = self.client.get(reverse("control:captura_movil"))
        self.assertEqual(resp.status_code, 403)

    def test_captura_movil_creates_venta(self):
        self.client.force_login(self.user_almacen)
        resp = self.client.post(
            reverse("control:captura_movil"),
            {
                "capture_type": "venta",
                "fecha": "2026-02-21",
                "sucursal_id": str(self.sucursal.id),
                "receta_id": str(self.receta.id),
                "cantidad": "5",
                "tickets": "2",
                "monto_total": "550.40",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(VentaPOS.objects.count(), 1)
        venta = VentaPOS.objects.first()
        self.assertEqual(venta.receta_id, self.receta.id)
        self.assertEqual(venta.sucursal_id, self.sucursal.id)
        self.assertEqual(venta.fuente, "CAPTURA_MOVIL")

    def test_captura_movil_creates_merma(self):
        self.client.force_login(self.user_almacen)
        resp = self.client.post(
            reverse("control:captura_movil"),
            {
                "capture_type": "merma",
                "fecha": "2026-02-21",
                "producto_texto": "Cheesecake Lotus individual",
                "cantidad": "1.5",
                "motivo": "Producto dañado en traslado",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(MermaPOS.objects.count(), 1)
        merma = MermaPOS.objects.first()
        self.assertEqual(merma.producto_texto, "Cheesecake Lotus individual")
        self.assertEqual(merma.fuente, "CAPTURA_MOVIL")
