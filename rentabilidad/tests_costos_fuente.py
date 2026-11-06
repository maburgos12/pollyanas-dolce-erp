from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone
from control.models import MermaMensualSucursal
from core.models import Sucursal
from pos_bridge.models import PointProduct, PointBranch, PointDailySale, PointRecipeNode, PointRecipeExtractionRun
from recetas.models import Receta, RecetaAgrupacionAddon, RecetaCodigoPointAlias, RecetaCostoVersion
from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual, RecetaCostoHistoricoMensual
from reportes.services_historical_costing import MonthlyHistoricalCostingService
from .agente_rentabilidad import analizar_sucursal
from .models_rentabilidad import SucursalRentabilidad
from .services_costos import _costos_reventa_para_periodo, asignar_costos_filas
from .services_resultado import leer_resultado_mensual


class IdentidadDocumentalCosto2025Tests(TestCase):
    def setUp(self):
        self.periodo = date(2025, 1, 1)
        self.sucursal = Sucursal.objects.create(codigo="HD25", nombre="Histórico documental")
        branch = PointBranch.objects.create(external_id="HD25", name="Histórico", erp_branch=self.sucursal)
        self.producto = PointProduct.objects.create(external_id="409", sku="1015", name="Flan Individual")
        self.receta = Receta.objects.create(nombre=self.producto.name, codigo_point=self.producto.sku,
            hash_contenido="historico-flan", modo_costeo=Receta.MODO_COSTEO_FABRICADO)
        self.meta = {"source": "POINT_PRODUCTION_UNIT_VALUATION_2025",
            "unit_of_analysis": "ONE_NATIVE_FINISHED_PRODUCT_UNIT",
            "historical_sku": "1015", "historical_name": "Flan Individual",
            "point_product_external_id": "409", "source_rows": [{"movimiento": 1117098}]}
        self.historico = RecetaCostoHistoricoMensual.objects.create(periodo=self.periodo,
            receta=self.receta, costo_total=20, costo_por_unidad_rendimiento=20,
            coverage_pct=100, metadata=self.meta)
        self.venta = PointDailySale.objects.create(branch=branch, product=self.producto,
            sale_date=self.periodo, quantity=2, gross_amount=90)

    def fila(self, periodo=None, **cambios):
        fila = dict(receta_id=None, product_id=self.producto.pk,
            product__sku=self.producto.sku, product__name=self.producto.name)
        fila.update(cambios)
        asignar_costos_filas([fila], periodo or self.periodo, periodo or self.periodo)
        return fila

    def test_lector_recupera_costo_material_sin_escribir_venta_ni_receta(self):
        resultado = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(resultado["costo_materia_prima"], 40)
        self.assertEqual(resultado["costo_reventa"], 0)
        self.assertEqual(resultado["pendientes"], [])
        self.venta.refresh_from_db()
        self.historico.refresh_from_db()
        self.assertIsNone(self.venta.receta_id)
        self.assertEqual(self.historico.metadata, self.meta)

    def test_identidad_incompleta_o_distinta_no_transfiere_costo(self):
        for key, value in [("source", "OTRO"), ("unit_of_analysis", "BATCH"),
                           ("historical_sku", "OTRO"), ("historical_name", "Flan Grande"),
                           ("point_product_external_id", "410"), ("source_rows", [])]:
            with self.subTest(key=key):
                self.historico.metadata = {**self.meta, key: value}
                self.historico.save()
                self.assertIsNone(self.fila()["cost_evidence"])

    def test_no_usa_costo_incompleto_ni_de_otro_mes_o_ejercicio(self):
        self.historico.coverage_pct = 99
        self.historico.save()
        self.assertIsNone(self.fila()["receta_id"])
        self.historico.coverage_pct = 100
        self.historico.save()
        for periodo in [date(2024, 12, 1), date(2025, 2, 1), date(2026, 1, 1)]:
            with self.subTest(periodo=periodo):
                self.assertIsNone(self.fila(periodo)["cost_evidence"])

    def test_no_confunde_codigo_reutilizado_ni_producto_oficial_con_id_nativo(self):
        self.assertIsNone(self.fila(product__name="Otro flan")["cost_evidence"])
        self.producto.external_id = "official:1015"
        self.producto.save()
        self.assertIsNone(self.fila()["cost_evidence"])

    def test_no_elije_entre_dos_recetas_con_identidad_historica_igual(self):
        otra = Receta.objects.create(nombre=self.receta.nombre, codigo_point=self.receta.codigo_point,
            hash_contenido="historico-flan-ambiguo")
        RecetaCostoHistoricoMensual.objects.create(periodo=self.periodo, receta=otra,
            costo_total=30, coverage_pct=100, metadata=self.meta)
        self.assertIsNone(self.fila()["cost_evidence"])

    def test_conserva_receta_registrada_y_precedencia_de_adquisicion(self):
        otra = Receta.objects.create(nombre="Otra receta", hash_contenido="historico-registrado")
        RecetaCostoHistoricoMensual.objects.create(periodo=self.periodo, receta=otra,
            costo_total=10, coverage_pct=100)
        self.assertEqual(self.fila(receta_id=otra.pk)["cost_evidence"]["cost"], 10)
        ProductoReventaCosto.objects.create(producto_point=self.producto,
            fecha_vigencia=self.periodo, costo_unitario=3, source_hash="historico-adquisicion",
            fuente=ProductoReventaCosto.FUENTE_POINT_HISTORIAL)
        fila = self.fila()
        self.assertEqual(fila["cost_evidence"]["cost"], 3)
        self.assertTrue(fila["cost_is_resale"])


class AliasCanonicoCosto2026Tests(TestCase):
    def setUp(self):
        self.periodo = date(2026, 6, 1)
        self.sucursal = Sucursal.objects.create(codigo="AC26", nombre="Alias canónico")
        branch = PointBranch.objects.create(external_id="AC26", name="Alias canónico", erp_branch=self.sucursal)
        product = PointProduct.objects.create(external_id="239", sku="0239", name="CAJA G PARA VENTA")
        self.receta = Receta.objects.create(nombre=product.name, codigo_point=product.sku,
            hash_contenido="alias-caja", modo_costeo=Receta.MODO_COSTEO_FABRICADO)
        self.alias = RecetaCodigoPointAlias.objects.create(receta=self.receta,
            codigo_point=product.sku, nombre_point=product.name)
        self.costo = RecetaCostoVersion.objects.create(receta=self.receta, version_num=1,
            hash_snapshot="alias-costo", costo_total=Decimal("19.87"), rendimiento_cantidad=1,
            creado_en=timezone.make_aware(datetime(2026, 5, 21)))
        self.venta = PointDailySale.objects.create(branch=branch, product=product,
            sale_date=self.periodo, quantity=2, gross_amount=50)

    def test_alias_guardado_recupera_costo_vigente_sin_modificar_la_venta(self):
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], Decimal("39.74"))
        self.assertEqual(row["pendientes"], [])
        self.venta.refresh_from_db()
        self.assertIsNone(self.venta.receta_id)

    def test_alias_inactivo_no_acredita_identidad(self):
        self.alias.activo = False
        self.alias.save()
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], 0)
        self.assertEqual(len(row["pendientes"]), 1)

    def test_codigo_reutilizado_con_nombre_distinto_no_transfiere_costo(self):
        self.alias.nombre_point = "Manga para decorar"
        self.alias.save()
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], 0)
        self.assertEqual(len(row["pendientes"]), 1)

    def test_alias_y_nodo_con_recetas_distintas_no_eligen_el_primer_costo(self):
        otra = Receta.objects.create(nombre=self.receta.nombre, hash_contenido="alias-conflicto")
        RecetaCostoHistoricoMensual.objects.create(periodo=self.periodo, receta=otra,
            costo_total=100, coverage_pct=100)
        run = PointRecipeExtractionRun.objects.create()
        PointRecipeNode.objects.create(run=run,
            identity_key="PRODUCT:239", point_code="0239", point_name=self.receta.nombre, erp_recipe=self.receta)
        PointRecipeNode.objects.create(run=run,
            identity_key="PRODUCT:otro", point_code="0239", point_name=self.receta.nombre, erp_recipe=otra)
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], 0)
        self.assertEqual(len(row["pendientes"]), 1)

    def test_alias_no_sustituye_un_costo_de_compra_ya_disponible(self):
        ProductoReventaCosto.objects.create(producto_point=self.venta.product,
            fecha_vigencia=self.periodo, costo_unitario=3, source_hash="alias-compra-vigente",
            fuente=ProductoReventaCosto.FUENTE_POINT_HISTORIAL)
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_reventa"], 6)
        self.assertEqual(row["costo_materia_prima"], 0)
        self.assertEqual(row["pendientes"], [])

    def test_identidad_existente_no_anticipa_un_precio_futuro(self):
        self.costo.creado_en = timezone.make_aware(datetime(2026, 7, 1))
        self.costo.save()
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], 0)
        self.assertIn(f"Costo sin confirmar: producto {self.venta.product_id}, receta {self.receta.pk}", row["pendientes"])

    def test_alias_no_sustituye_una_receta_ya_registrada_en_ventas(self):
        otra = Receta.objects.create(nombre="Otra receta", hash_contenido="alias-registrada")
        RecetaCostoHistoricoMensual.objects.create(periodo=self.periodo, receta=otra,
            costo_total=10, coverage_pct=100)
        self.venta.receta = otra
        self.venta.save()
        row = leer_resultado_mensual(self.periodo)["sucursales"][self.sucursal.pk]
        self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], 20)
        self.venta.refresh_from_db()
        self.assertEqual(self.venta.receta_id, otra.pk)

    def test_no_extiende_la_regla_a_2025_ni_a_octubre2026(self):
        for periodo in [date(2025, 6, 1), date(2026, 10, 1)]:
            with self.subTest(periodo=periodo):
                self.venta.sale_date = periodo
                self.venta.save()
                RecetaCostoHistoricoMensual.objects.create(periodo=periodo, receta=self.receta,
                    costo_total=20, coverage_pct=100)
                row = leer_resultado_mensual(periodo)["sucursales"][self.sucursal.pk]
                self.assertEqual(row["costo_materia_prima"] + row["costo_reventa"], 0)
                self.assertEqual(len(row["pendientes"]), 1)


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
