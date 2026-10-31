from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from control.models import MermaMensualSucursal
from core.models import Sucursal
from pos_bridge.models import PointProduct, PointBranch, PointDailySale, PointRecipeNode, PointRecipeExtractionRun
from recetas.models import Receta, RecetaAgrupacionAddon
from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual, RecetaCostoHistoricoMensual
from reportes.services_historical_costing import MonthlyHistoricalCostingService
from .agente_rentabilidad import analizar_sucursal
from .models_rentabilidad import SucursalRentabilidad
from .services_costos import _costos_reventa_para_periodo
from .services_resultado import leer_resultado_mensual


class CostosFuenteTests(TestCase):
    def test_topping_aprobado_cuesta_complemento_sin_duplicar_base_ni_confundir_sku(self):
        periodo = date(2026,9,1)
        suc = Sucursal.objects.create(codigo="TOP", nombre="Topping")
        branch = PointBranch.objects.create(external_id="TOP", name="Topping", erp_branch=suc)
        base = Receta.objects.create(nombre="Pastel", nombre_normalizado="pastel", hash_contenido="top-base")
        addon = Receta.objects.create(nombre="Topping Zanahoria", nombre_normalizado="topping zanahoria", hash_contenido="top-addon")
        RecetaAgrupacionAddon.objects.create(base_receta=base, addon_receta=addon,
            addon_codigo_point="1254", addon_nombre_point="Topping Zanahoria",
            status=RecetaAgrupacionAddon.STATUS_APPROVED, activo=True)
        for receta, costo in [(base, 100), (addon, 10)]:
            RecetaCostoHistoricoMensual.objects.create(periodo=periodo, receta=receta, costo_total=costo, coverage_pct=100)
        for ext, sku, name, receta, monto in [('base','BASE','Pastel',base,200),('addon','1254','TOPPING ZANAHORIA',base,0)]:
            product = PointProduct.objects.create(external_id=ext, sku=sku, name=name)
            PointDailySale.objects.create(branch=branch, product=product, receta=receta, sale_date=periodo, quantity=1, gross_amount=monto)
        row = leer_resultado_mensual(periodo)["sucursales"][suc.pk]
        self.assertEqual(row["costo_materia_prima"], Decimal("110"))
        self.assertEqual(row["pendientes"], [])
        product = PointProduct.objects.create(external_id="collision", sku="1254", name="TOPPING CRUNCH MINI")
        PointDailySale.objects.create(branch=branch, product=product, sale_date=periodo, quantity=1)
        row = leer_resultado_mensual(periodo)["sucursales"][suc.pk]
        self.assertEqual(row["costo_materia_prima"], Decimal("110"))
        self.assertEqual(len(row["pendientes"]), 1)

    def test_cost_above_sale_remains_visible_without_becoming_missing(self):
        periodo = date(2026, 9, 1)
        suc = Sucursal.objects.create(codigo="NEG", nombre="Negativo")
        branch = PointBranch.objects.create(external_id="NEG", name="Negativo", erp_branch=suc)
        product = PointProduct.objects.create(external_id="NEG", name="Empaque")
        ProductoReventaCosto.objects.create(producto_point=product, fecha_vigencia=periodo,
            costo_unitario=20, source_hash="negative-margin", metadata={"point_name": product.name})
        PointDailySale.objects.create(branch=branch, product=product, sale_date=periodo, quantity=1, gross_amount=10)
        row = leer_resultado_mensual(periodo)["sucursales"][suc.pk]
        self.assertEqual(row["pendientes"], [])
        self.assertEqual(len(row["advertencias"]), 1)
        self.assertEqual(row["costo_reventa"], 20)

    def test_receta_point_existente_y_relacion_con_nombre_equivocado(self):
        periodo = date(2026, 9, 1)
        suc = Sucursal.objects.create(codigo="ENV", nombre="Empaque")
        branch = PointBranch.objects.create(external_id="ENV", name="Empaque", erp_branch=suc)
        receta = Receta.objects.create(nombre="Empaque Pay", nombre_normalizado="empaque pay", hash_contenido="env")
        RecetaCostoHistoricoMensual.objects.create(periodo=periodo, receta=receta, costo_total=3, coverage_pct=100)
        run = PointRecipeExtractionRun.objects.create()
        PointRecipeNode.objects.create(run=run, identity_key="PRODUCT:1225", point_code="1225", point_name="Empaque Pay", erp_recipe=receta)
        product = PointProduct.objects.create(external_id="ENV", sku="1225", name="Empaque Pay")
        PointDailySale.objects.create(branch=branch, product=product, sale_date=periodo, quantity=2, gross_amount=10)
        row = leer_resultado_mensual(periodo)["sucursales"][suc.pk]
        self.assertEqual(row["costo_materia_prima"], 6)
        self.assertEqual(row["pendientes"], [])
        RecetaAgrupacionAddon.objects.create(base_receta=receta, addon_receta=receta, addon_codigo_point="otro", addon_nombre_point="Otro topping", status="APPROVED", activo=True)
        product = PointProduct.objects.create(external_id="OTRO", sku="otro", name="Otro topping")
        PointDailySale.objects.create(branch=branch, product=product, sale_date=periodo, quantity=1)
        row = leer_resultado_mensual(periodo)["sucursales"][suc.pk]
        self.assertEqual(row["costo_materia_prima"], 6)
        self.assertEqual(len(row["pendientes"]), 1)

    def test_merma_sin_valor_usa_receta_identificada_del_mes_sin_escribir(self):
        periodo = date(2026, 9, 1)
        suc = Sucursal.objects.create(codigo="CF", nombre="Costo fuente")
        receta = Receta.objects.create(nombre="Pastel", nombre_normalizado="pastel", hash_contenido="cf")
        hist = RecetaCostoHistoricoMensual.objects.create(periodo=periodo, receta=receta,
            costo_total=400, costo_por_unidad_rendimiento=200, coverage_pct=100)
        merma = MermaMensualSucursal.objects.create(periodo=periodo, sucursal=suc,
            receta=receta, nombre_producto="Pastel", unidades_merma=Decimal("0.25"))
        self.assertEqual(leer_resultado_mensual(periodo)["sucursales"][suc.pk]["merma"], Decimal("50"))
        merma.refresh_from_db()
        self.assertEqual(merma.costo_merma, 0)
        hist.coverage_pct = 90
        hist.save()
        row = leer_resultado_mensual(periodo)["sucursales"][suc.pk]
        self.assertEqual(row["merma"], 0)
        self.assertEqual(len(row["pendientes"]), 1)

    def test_historico_contaminado_no_usa_costo_de_otro_articulo(self):
        periodo, fin = date(2026, 9, 1), date(2026, 9, 30)
        p = PointProduct.objects.create(external_id="267", sku="0227", name="Extra 10")
        wrong = ProductoReventaCosto.objects.create(producto_point=p, fecha_vigencia=periodo,
            costo_unitario=45, source_hash="equivocado", metadata={"point_name": "Toalla Sanitas", "point_code": "FPF03800"})
        ProductoReventaCostoHistoricoMensual.objects.create(producto_point=p, periodo=periodo,
            costo_promedio=45, metadata={"source_rows": [wrong.pk]})
        self.assertNotIn(p.pk, _costos_reventa_para_periodo({p.pk}, periodo, fin))
        self.assertEqual(MonthlyHistoricalCostingService()._resale_product_costs(producto_point_id=p.pk, period_end=fin), [])
        correct = ProductoReventaCosto.objects.create(producto_point=p, fecha_vigencia=date(2026,8,1),
            costo_unitario=3, source_hash="correcto", metadata={"point_name": "Extra 10", "point_code": "0227"})
        self.assertEqual(_costos_reventa_para_periodo({p.pk}, periodo, fin)[p.pk], correct.costo_unitario)
        wrong.refresh_from_db()
        self.assertEqual(wrong.costo_unitario, 45)

    @patch("rentabilidad.agente_rentabilidad._construir_contexto", return_value="Contexto de prueba")
    @patch("openai.OpenAI")
    def test_error_proveedor_conserva_diagnostico_previo(self, client, context):
        suc = Sucursal.objects.create(codigo="IAF", nombre="IA fuente")
        rent = SucursalRentabilidad.objects.create(sucursal=suc, periodo=date(2026,9,1),
            diagnostico_ia="Diagnóstico anterior", calculado_por_agente=False)
        client.return_value.chat.completions.create.side_effect = RuntimeError("credit_balance_exhausted")
        with self.assertRaisesRegex(RuntimeError, "credit_balance_exhausted"):
            analizar_sucursal(rent)
        rent.refresh_from_db()
        self.assertEqual(rent.diagnostico_ia, "Diagnóstico anterior")
        self.assertFalse(rent.calculado_por_agente)
        client.return_value.chat.completions.create.side_effect = None
        client.return_value.chat.completions.create.return_value = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{}'))])
        with self.assertRaises(ValueError):
            analizar_sucursal(rent)
        rent.refresh_from_db()
        self.assertEqual(rent.diagnostico_ia, "Diagnóstico anterior")
