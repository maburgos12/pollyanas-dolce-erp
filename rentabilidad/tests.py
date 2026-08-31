from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_LECTURA, ROLE_VENTAS
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.models import ProductoReventaCostoHistoricoMensual
from rentabilidad.models_rentabilidad import EstadoRentabilidad, SucursalRentabilidad
from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual


class RentabilidadPermissionTests(TestCase):
    def setUp(self):
        lectura_group, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        ventas_group, _ = Group.objects.get_or_create(name=ROLE_VENTAS)
        self.lectura_user = User.objects.create_user(username="lectura_rent", password="pass123")
        self.lectura_user.groups.add(lectura_group)
        self.ventas_user = User.objects.create_user(username="ventas_rent", password="pass123")
        self.ventas_user.groups.add(ventas_group)
        self.sucursal = Sucursal.objects.create(codigo="MAT", nombre="Matriz", activa=True)
        self.rentabilidad = SucursalRentabilidad.objects.create(
            sucursal=self.sucursal,
            periodo=date(2026, 3, 1),
            ventas_brutas=Decimal("1000.00"),
            costo_materia_prima=Decimal("300.00"),
            renta=Decimal("100.00"),
        )

    def test_lectura_can_view_dashboard_and_detail(self):
        self.client.login(username="lectura_rent", password="pass123")

        dashboard = self.client.get(reverse("rentabilidad_dashboard"))
        detail = self.client.get(reverse("rentabilidad_detalle", kwargs={"pk": self.rentabilidad.pk}))

        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(detail.status_code, 200)

    def test_ventas_cannot_view_rentabilidad(self):
        self.client.login(username="ventas_rent", password="pass123")

        response = self.client.get(reverse("rentabilidad_dashboard"))

        self.assertEqual(response.status_code, 403)

    def test_lectura_cannot_trigger_ai_analysis(self):
        self.client.login(username="lectura_rent", password="pass123")

        response = self.client.post(reverse("rentabilidad_analizar", kwargs={"pk": self.rentabilidad.pk}))

        self.assertEqual(response.status_code, 403)


class RentabilidadCostoReventaTests(TestCase):
    def test_non_recipe_resale_cost_reduces_margin_as_variable_cost(self):
        sucursal = Sucursal.objects.create(codigo="MAT", nombre="Matriz")
        point_branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=sucursal)
        product = PointProduct.objects.create(
            external_id="COCA450",
            sku="COCA450",
            name="COCA-COLA 450 ML",
            category="Bebidas",
        )
        PointDailySale.objects.create(
            branch=point_branch,
            product=product,
            receta=None,
            sale_date=date(2026, 3, 15),
            quantity=Decimal("3"),
            gross_amount=Decimal("90.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("90.00"),
            net_amount=Decimal("90.00"),
        )
        ProductoReventaCostoHistoricoMensual.objects.create(
            periodo=date(2026, 3, 1),
            producto_point=product,
            costo_promedio=Decimal("12.50"),
            metodo=ProductoReventaCostoHistoricoMensual.METODO_POINT_ALMACEN,
            source_date=date(2026, 3, 10),
            sample_count=1,
            weighted_quantity=Decimal("10"),
        )

        recalcular_rentabilidad_mensual(year=2026, month=3)

        rentabilidad = SucursalRentabilidad.objects.get(sucursal=sucursal, periodo=date(2026, 3, 1))
        self.assertEqual(rentabilidad.costo_materia_prima, Decimal("0.00"))
        self.assertEqual(rentabilidad.costo_reventa, Decimal("37.50"))
        self.assertEqual(rentabilidad.costo_variable_total, Decimal("37.50"))
        self.assertEqual(rentabilidad.margen_bruto, Decimal("52.50"))


class RentabilidadGastosFuenteTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MAT", nombre="Matriz", activa=True)
        self.point_branch = PointBranch.objects.create(
            external_id="MAT",
            name="Matriz",
            erp_branch=self.sucursal,
        )
        self.product = PointProduct.objects.create(
            external_id="PASTEL",
            sku="PASTEL",
            name="Pastel",
            category="Pasteles",
        )
        PointDailySale.objects.create(
            branch=self.point_branch,
            product=self.product,
            receta=None,
            sale_date=date(2026, 7, 15),
            quantity=Decimal("1"),
            gross_amount=Decimal("1000.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("1000.00"),
            net_amount=Decimal("1000.00"),
        )

    def test_recalculo_sin_fuente_no_sobrescribe_gastos_fijos_existentes(self):
        snapshot = SucursalRentabilidad.objects.create(
            sucursal=self.sucursal,
            periodo=date(2026, 7, 1),
            ventas_brutas=Decimal("900.00"),
            renta=Decimal("250.00"),
            nomina_directa=Decimal("300.00"),
        )

        recalcular_rentabilidad_mensual(year=2026, month=7)

        snapshot.refresh_from_db()
        self.assertEqual(snapshot.ventas_brutas, Decimal("1000.00"))
        self.assertEqual(snapshot.renta, Decimal("250.00"))
        self.assertEqual(snapshot.nomina_directa, Decimal("300.00"))
        self.assertEqual(snapshot.gasto_fijo_total, Decimal("550.00"))

    def test_snapshot_nuevo_con_ventas_sin_gastos_queda_sin_datos(self):
        recalcular_rentabilidad_mensual(year=2026, month=7)

        snapshot = SucursalRentabilidad.objects.get(
            sucursal=self.sucursal,
            periodo=date(2026, 7, 1),
        )
        self.assertEqual(snapshot.gasto_fijo_total, Decimal("0.00"))
        self.assertEqual(snapshot.estado, EstadoRentabilidad.SIN_DATOS)
        self.assertEqual(snapshot.punto_equilibrio_mensual, Decimal("0"))

    def test_dashboard_identifica_gastos_no_cargados(self):
        user = User.objects.create_superuser(
            username="admin_rent",
            email="admin@example.com",
            password="pass123",
        )
        snapshot = SucursalRentabilidad.objects.create(
            sucursal=self.sucursal,
            periodo=date(2026, 7, 1),
            ventas_brutas=Decimal("1000.00"),
        )
        SucursalRentabilidad.objects.filter(pk=snapshot.pk).update(
            estado=EstadoRentabilidad.RENTABLE,
            alerta_nivel=0,
        )
        self.client.login(username=user.username, password="pass123")

        response = self.client.get(reverse("rentabilidad_dashboard"), {"periodo": "2026-07"})

        self.assertContains(response, "gastos no cargados")
        self.assertContains(response, "No calculable")
        self.assertContains(response, "Sin datos suficientes")
        self.assertNotContains(response, "sin gasto fijo")
