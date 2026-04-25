from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from reportes.models import ProductoReventaCostoHistoricoMensual
from rentabilidad.models_rentabilidad import SucursalRentabilidad
from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual


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
