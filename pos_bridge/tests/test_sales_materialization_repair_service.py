from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from pos_bridge.services.sales_materialization_repair_service import BridgeSalesMaterializationRepairService
from recetas.models import Receta, RecetaCodigoPointAlias, VentaHistorica


class BridgeSalesMaterializationRepairServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="1",
            name="Matriz",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.sucursal,
        )
        self.receta = Receta.objects.create(
            nombre="Pastel Fresas Con Crema - Mediano",
            codigo_point="0100",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        self.product_ok = PointProduct.objects.create(
            external_id="100",
            sku="0100",
            name="Pastel de Fresas Con Crema Mediano",
            category="Pastel Mediano",
        )
        self.product_wrong = PointProduct.objects.create(
            external_id="939",
            sku="0147",
            name="Vaso Fresas con Crema Mediano",
            category="Individual",
        )
        RecetaCodigoPointAlias.objects.create(
            receta=self.receta,
            codigo_point="0147",
            nombre_point="Vaso Fresas con Crema Mediano",
            activo=False,
        )

    def test_repair_clears_stale_recipe_links_and_rebuilds_bridge_history(self):
        sale_date = date(2026, 3, 15)
        good_sale = PointDailySale.objects.create(
            branch=self.branch,
            product=self.product_ok,
            receta=self.receta,
            sale_date=sale_date,
            quantity=Decimal("5"),
            tickets=1,
            gross_amount=Decimal("500"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("500"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("500"),
        )
        wrong_sale = PointDailySale.objects.create(
            branch=self.branch,
            product=self.product_wrong,
            receta=self.receta,
            sale_date=sale_date,
            quantity=Decimal("2"),
            tickets=1,
            gross_amount=Decimal("210"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("210"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("210"),
        )
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=sale_date,
            cantidad=Decimal("7"),
            tickets=2,
            monto_total=Decimal("710"),
            fuente="POINT_BRIDGE_SALES",
        )

        result = BridgeSalesMaterializationRepairService().repair(start_date=sale_date, end_date=sale_date)

        good_sale.refresh_from_db()
        wrong_sale.refresh_from_db()
        history = VentaHistorica.objects.get(fuente="POINT_BRIDGE_SALES", fecha=sale_date, receta=self.receta, sucursal=self.sucursal)

        self.assertEqual(result.recipe_rows_cleared, 1)
        self.assertEqual(result.bridge_history_deleted, 1)
        self.assertEqual(result.bridge_history_created, 1)
        self.assertEqual(good_sale.receta_id, self.receta.id)
        self.assertIsNone(wrong_sale.receta_id)
        self.assertEqual(history.cantidad, Decimal("5"))
        self.assertEqual(history.monto_total, Decimal("500"))
