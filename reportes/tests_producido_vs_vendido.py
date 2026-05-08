from decimal import Decimal

from django.test import TestCase

from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSalesDailyProductFact
from recetas.models import Receta, RecetaEquivalencia
from reportes.views_produccion import ProducidoVsVendidoMermaView, _parse_period


class ProducidoVsVendidoConversionTests(TestCase):
    def test_sales_map_prefiere_point_daily_sale_sobre_fact_parcial(self):
        receta = Receta.objects.create(
            nombre="Producto Test",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Mediano",
            hash_contenido="test-sales-map-point-daily-sale",
        )
        branch = PointBranch.objects.create(external_id="test-branch", name="Sucursal Test")
        product = PointProduct.objects.create(external_id="test-product", name="Producto Test")
        PointSalesDailyProductFact.objects.create(
            branch=branch,
            sale_date="2026-04-01",
            sucursal_nombre="Sucursal Test",
            categoria="Pastel",
            producto_nombre_historico="Producto Test",
            point_product=product,
            receta=receta,
            total_cantidad=Decimal("5"),
        )
        PointDailySale.objects.create(
            branch=branch,
            product=product,
            receta=receta,
            sale_date="2026-04-01",
            quantity=Decimal("12"),
        )

        sales_map, source = ProducidoVsVendidoMermaView()._sales_map(_parse_period("2026-04"), None)

        self.assertEqual(source, "PointDailySale")
        self.assertEqual(sales_map[receta.id], Decimal("12"))

    def test_conversion_rebanada_incluye_venta_y_merma_reportada(self):
        padre = Receta.objects.create(
            nombre="Pastel Test Mediano",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Mediano",
            hash_contenido="test-conv-padre-mediano",
        )
        rebanada = Receta.objects.create(
            nombre="Pastel Test R",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="test-conv-rebanada",
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=rebanada,
            receta_padre=padre,
            factor_conversion=Decimal("10"),
            activo=True,
        )

        conversion_map = ProducidoVsVendidoMermaView()._conversion_map(
            {rebanada.id: Decimal("4")},
            {rebanada.id: Decimal("6")},
        )

        self.assertEqual(conversion_map[rebanada.id]["conversion_entrada"], Decimal("10"))
        self.assertEqual(conversion_map[rebanada.id]["enteros_equivalentes"], Decimal("1.00"))
        self.assertEqual(conversion_map[padre.id]["conversion_salida"], Decimal("1.00"))

    def test_detecta_rebanadas_vendidas_sin_equivalencia(self):
        rebanada_sin_equivalencia = Receta.objects.create(
            nombre="Pastel Test R",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="test-rebanada-sin-equivalencia",
        )
        padre = Receta.objects.create(
            nombre="Pastel Test Mediano",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Mediano",
            hash_contenido="test-padre-mediano",
        )
        rebanada_con_equivalencia = Receta.objects.create(
            nombre="Pastel Test Rebanada",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="test-rebanada-con-equivalencia",
        )
        RecetaEquivalencia.objects.create(
            receta_porcion=rebanada_con_equivalencia,
            receta_padre=padre,
            factor_conversion=Decimal("10"),
            activo=True,
        )

        missing = ProducidoVsVendidoMermaView()._missing_slice_equivalences(
            {
                rebanada_sin_equivalencia.id: Decimal("3"),
                rebanada_con_equivalencia.id: Decimal("5"),
            }
        )

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["receta_id"], rebanada_sin_equivalencia.id)
        self.assertEqual(missing[0]["receta"], "Pastel Test R")
        self.assertEqual(missing[0]["vendido"], Decimal("3"))

    def test_detecta_rebanada_con_merma_sin_equivalencia_aunque_no_tenga_venta(self):
        rebanada_sin_equivalencia = Receta.objects.create(
            nombre="Pastel Merma R",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Rebanada",
            hash_contenido="test-rebanada-merma-sin-equivalencia",
        )

        missing = ProducidoVsVendidoMermaView()._missing_slice_equivalences(
            {},
            {rebanada_sin_equivalencia.id: Decimal("2")},
        )

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["receta_id"], rebanada_sin_equivalencia.id)
        self.assertEqual(missing[0]["unidades_rebanada"], Decimal("2"))
