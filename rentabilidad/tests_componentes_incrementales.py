from copy import deepcopy
from datetime import date
from decimal import Decimal
import csv
import io
import json

from django.core.management import call_command
from django.core.management.base import CommandError
from django.contrib.auth.models import User
from django.db import connection
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from core.models import Sucursal
from control.models import MermaMensualSucursal
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointDailySale, PointProduct
from recetas.models import LineaReceta, Receta, RecetaAgrupacionAddon
from reportes.models import FactVentaDiaria, RecetaCostoHistoricoMensual
from rentabilidad.services_costos import _costos_recetas_para_periodo, asignar_costos_filas
from rentabilidad.services_resultado import leer_resultado_mensual


PERIOD = date(2026, 10, 1)
END = date(2026, 10, 31)


def authorized_treatment():
    return {
        "contract": "DG_TOPPING_LABELS_OCT2026_V1",
        "scope": "RENTABILIDAD_SOLD_MATERIAL",
        "authorization": "Mauricio autorizó dos etiquetas, crumble en base 55, $167.69 octubre",
        "period": "2026-10-01", "rule_id": 26,
        "product_id": 186, "external_id": "961", "sku": "1254",
        "name": "TOPPING ZANAHORIA C", "recipe_id": 516, "base_recipe_id": 55,
        "unit": "PZA", "unit_cost": "1.004099",
        "components": [
            {"insumo_id": 98, "quantity": "1", "unit": "PZA", "monthly_unit_cost": "0.435795",
             "source_rows": [11095, 12253, 12616, 12575]},
            {"insumo_id": 99, "quantity": "1", "unit": "PZA", "monthly_unit_cost": "0.568304",
             "source_rows": [11235, 11572, 11890, 12256, 12617, 12578, 12840, 12900]},
        ],
        "absorbed_components": [{"insumo_id": 384, "quantity": "20", "unit": "GR", "base_recipe_id": 55}],
        "sold_quantity": "167", "sale_rows": 123,
        "rounding": "PER_BRANCH_2DP",
    }


class IncrementalLabelsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.base = Receta.objects.create(pk=55, nombre="Pastel de Zanahoria Chico", codigo_point="0066", hash_contenido="base-incremental")
        cls.addon = Receta.objects.create(pk=516, nombre="TOPPING ZANAHORIA C", codigo_point="1254",
                                         modo_costeo="SERVICIO_ACCESORIO", hash_contenido="addon-incremental")
        cls.product = PointProduct.objects.create(pk=186, external_id="961", sku="1254", name=cls.addon.nombre)
        cls.base_product = PointProduct.objects.create(external_id="267", sku="0066", name=cls.base.nombre)
        cls.rule = RecetaAgrupacionAddon.objects.create(pk=26, base_receta=cls.base, addon_receta=cls.addon,
            addon_codigo_point="1254", addon_nombre_point=cls.addon.nombre, status="APPROVED")
        cls.rule.metadata = {"monthly_sold_material_treatments": {"2026-10-01": authorized_treatment()}}
        cls.rule.save()
        RecetaCostoHistoricoMensual.objects.create(periodo=PERIOD, receta=cls.base, costo_total="139.957253",
            coverage_pct=100, lineas_totales=4, lineas_costeadas=4)
        cls.branch_quantities = {1: 55, 2: 17, 3: 10, 5: 8, 6: 6, 7: 17, 8: 18, 9: 11, 17: 14, 18: 11}
        daily_counts = [24, 14, 10, 8, 6, 14, 14, 10, 13, 10]
        sales = []
        for (branch_id, quantity), daily_count in zip(cls.branch_quantities.items(), daily_counts):
            erp, _ = Sucursal.objects.get_or_create(pk=branch_id,
                defaults={"codigo": f"INC{branch_id}", "nombre": f"Sucursal {branch_id}"})
            branch = PointBranch.objects.create(external_id=f"inc-{branch_id}", name=erp.nombre, erp_branch=erp)
            for day in range(1, daily_count + 1):
                units = quantity - daily_count + 1 if day == 1 else 1
                sales.append(PointDailySale(branch=branch, product=cls.product, receta=cls.base,
                    sale_date=date(2026, 10, day), quantity=units))
                sales.append(PointDailySale(branch=branch, product=cls.base_product, receta=cls.base,
                    sale_date=date(2026, 10, day), quantity=units, gross_amount=units * 500))
        PointDailySale.objects.bulk_create(sales)

    def test_monthly_result_costs_only_labels_and_removes_exact_addon_pending(self):
        before_sales = list(PointDailySale.objects.order_by("id").values())
        result = leer_resultado_mensual(PERIOD)
        delta = Decimal("0")
        for branch_id, quantity in self.branch_quantities.items():
            row = result["sucursales"][branch_id]
            full_base = (Decimal("139.957253") * quantity).quantize(Decimal(".01"))
            self.assertEqual(row["costo_materia_prima"] - full_base,
                             (Decimal("1.004099") * quantity).quantize(Decimal(".01")))
            self.assertEqual(row["costo_reventa"], 0)
            self.assertEqual(row["merma"], 0)
            self.assertNotIn("Costo sin confirmar: producto 186, receta 516", row["pendientes"])
            delta += (Decimal("1.004099") * quantity).quantize(Decimal(".01"))
        self.assertEqual(delta, Decimal("167.69"))
        self.assertEqual(PointDailySale.objects.filter(product=self.product).count(), 123)
        self.assertEqual(list(PointDailySale.objects.order_by("id").values()), before_sales)
        self.assertFalse(RecetaCostoHistoricoMensual.objects.filter(receta=self.addon).exists())

    def test_materialized_result_and_product_panel_match_exact_branch_rounded_delta(self):
        from types import SimpleNamespace
        from rentabilidad.services_resultado import aplicar_resultado
        from rentabilidad.views_rentabilidad import _build_productos_panel
        from rentabilidad.services_reporte_mensual import _rentabilidad_por_mes

        self.rule.refresh_from_db()
        MermaMensualSucursal.objects.create(periodo=PERIOD, sucursal_id=1, receta=self.base,
            nombre_producto=self.base.nombre, unidades_merma=1, costo_merma=0)
        MermaMensualSucursal.objects.create(periodo=PERIOD, sucursal_id=1, receta=self.addon,
            nombre_producto=self.addon.nombre, unidades_merma=1, costo_merma=20)
        waste_before = list(MermaMensualSucursal.objects.order_by("pk").values())
        authorized = deepcopy(self.rule.metadata)
        self.rule.metadata = {}
        self.rule.save()
        before = leer_resultado_mensual(PERIOD)
        export_before = _rentabilidad_por_mes(PERIOD, PERIOD, None)[PERIOD]
        before_csv = io.StringIO()
        call_command("reporte_mensual_consolidado", hasta="2026-10", meses=1, formato="csv", detalle=True, stdout=before_csv)
        self.rule.metadata = authorized
        self.rule.save()
        after = leer_resultado_mensual(PERIOD)
        export_after = _rentabilidad_por_mes(PERIOD, PERIOD, None)[PERIOD]
        after_csv = io.StringIO()
        call_command("reporte_mensual_consolidado", hasta="2026-10", meses=1, formato="csv", detalle=True, stdout=after_csv)
        delta = Decimal("0")
        for branch_id in self.branch_quantities:
            original = SimpleNamespace(sucursal_id=branch_id)
            updated = SimpleNamespace(sucursal_id=branch_id)
            aplicar_resultado(original, before)
            aplicar_resultado(updated, after)
            delta += updated.costo_materia_prima - original.costo_materia_prima
            self.assertEqual(updated.merma_registrada, original.merma_registrada)
            self.assertEqual(updated.ventas_brutas, original.ventas_brutas)
            self.assertEqual(len(original.costos_pendientes) - len(updated.costos_pendientes), 1)
        self.assertEqual(delta, Decimal("167.69"))
        self.assertEqual(sum(export_before[pk]["utilidad"] - export_after[pk]["utilidad"]
                             for pk in self.branch_quantities), delta)
        panel = _build_productos_panel(PERIOD, PERIOD, END)
        addon = next(row for row in panel["top_utilidad"] if row["nombre"] == self.addon.nombre)
        self.assertEqual(addon["tipo"], "Complemento incremental")
        self.assertEqual(addon["costo_total"], delta)
        total_before = next(row for row in csv.DictReader(io.StringIO(before_csv.getvalue())) if row["sucursal"] == "TOTAL")
        total_after = next(row for row in csv.DictReader(io.StringIO(after_csv.getvalue())) if row["sucursal"] == "TOTAL")
        self.assertEqual(Decimal(total_before["utilidad_neta"]) - Decimal(total_after["utilidad_neta"]), delta)
        self.assertEqual(list(MermaMensualSucursal.objects.order_by("pk").values()), waste_before)

    def test_other_periods_products_and_malformed_metadata_do_not_receive_override(self):
        from rentabilidad.services_componentes_incrementales import valid_treatment

        for changes in ({"period": "2026-09-01"}, {"unit_cost": "NaN"}, {"unit_cost": "Infinity"},
                        {"unit_cost": -1}, {"sold_quantity": "-167"}, {"recipe_id": 472},
                        {"components": []}, {"authorization": None}, {"scope": "ALL_COSTS"}):
            with self.subTest(changes=changes):
                self.assertFalse(valid_treatment({**authorized_treatment(), **changes}))
        for period, product_id, sku, name in ((date(2026, 9, 1), 186, "1254", self.addon.nombre),
                (date(2026, 11, 1), 186, "1254", self.addon.nombre), (PERIOD, 219, "1254", "TOPPING CRUNCH MINI")):
            row = {"product_id": product_id, "product__sku": sku, "product__name": name, "receta_id": None}
            asignar_costos_filas([row], period, period.replace(day=30), aplicar_componentes_incrementales=True)
            self.assertFalse(row.get("cost_evidence") and row["cost_evidence"].get("treatment") == "INCREMENTAL_COMPONENTS")

    def test_unopted_consumers_and_recipe_or_waste_cost_remain_unmodified(self):
        row = {"product_id": 186, "product__sku": "1254", "product__name": self.addon.nombre, "receta_id": None}
        asignar_costos_filas([row], PERIOD, END)
        self.assertIsNone(row["cost_evidence"])
        self.assertNotIn(516, _costos_recetas_para_periodo({516}, PERIOD, END))

    def test_existing_material_half_cent_tie_preserves_exact_incremental_delta(self):
        from types import SimpleNamespace
        from rentabilidad.services_resultado import aplicar_resultado

        RecetaCostoHistoricoMensual.objects.filter(receta=self.base, periodo=PERIOD).update(costo_total="0.001")
        self.rule.refresh_from_db()
        authorized = deepcopy(self.rule.metadata)
        self.rule.metadata = {}
        self.rule.save()
        before = leer_resultado_mensual(PERIOD)
        self.rule.metadata = authorized
        self.rule.save()
        after = leer_resultado_mensual(PERIOD)
        delta = Decimal("0")
        for branch_id, quantity in self.branch_quantities.items():
            original = SimpleNamespace(sucursal_id=branch_id)
            updated = SimpleNamespace(sucursal_id=branch_id)
            aplicar_resultado(original, before)
            aplicar_resultado(updated, after)
            branch_delta = updated.costo_materia_prima - original.costo_materia_prima
            self.assertEqual(branch_delta, (Decimal("1.004099") * quantity).quantize(Decimal(".01")))
            delta += branch_delta
        self.assertEqual(delta, Decimal("167.69"))

    def test_legitimate_names_can_change_without_reassigning_product_identity(self):
        self.product.name = "Nuevo nombre de catálogo"
        self.product.save()
        self.addon.nombre = "Nuevo nombre de receta"
        self.addon.save()
        row = {"product_id": 186, "product__sku": "1254", "product__name": self.product.name, "receta_id": 55}
        asignar_costos_filas([row], PERIOD, END, aplicar_componentes_incrementales=True)
        self.assertEqual(row["cost_evidence"]["cost"], Decimal("1.004099"))
        self.assertEqual(row["receta_id"], 516)
        self.assertFalse(row["cost_is_resale"])

    def test_mixed_legacy_recipe_groups_count_the_incremental_product_once(self):
        from rentabilidad.views_rentabilidad import _build_productos_panel

        legacy = Receta.objects.create(pk=472, nombre="Topping previo", codigo_point="1254", hash_contenido="legacy-topping")
        ids = list(PointDailySale.objects.filter(product=self.product).order_by("pk").values_list("pk", flat=True))
        PointDailySale.objects.filter(pk__in=ids[:40]).update(receta=legacy)
        PointDailySale.objects.filter(pk__in=ids[40:80]).update(receta=self.addon)
        panel = _build_productos_panel(PERIOD, PERIOD, END)
        products = [row for row in panel["top_utilidad"] if row["nombre"] == self.addon.nombre]
        self.assertEqual(len(products), 1)
        self.assertEqual(products[0]["cantidad"], Decimal("167"))
        self.assertEqual(products[0]["costo_total"], Decimal("167.69"))
        self.assertEqual(products[0]["sucursales"], 10)

    def test_dashboard_financial_coverage_uses_incremental_scope_and_generic_confidence_does_not(self):
        from reportes.sales_confidence import build_sales_confidence

        Sucursal.objects.filter(pk__in=self.branch_quantities).update(fecha_apertura=None)
        facts = []
        for index, sale in enumerate(PointDailySale.objects.filter(product=self.product).select_related("branch")):
            facts.append(FactVentaDiaria(fecha=sale.sale_date, sucursal_id=sale.branch.erp_branch_id,
                point_product=self.product, producto_clave="961", producto_nombre=self.product.name,
                cantidad=sale.quantity, source_kind="AUTHORITATIVE",
                receta_id=55 if index < 40 else 516 if index < 80 else None))
        FactVentaDiaria.objects.bulk_create(facts)
        self.assertEqual(build_sales_confidence(start_date=PERIOD, end_date=END)["missing_cost_rows"], 123)
        user = User.objects.create_user(username="incremental-review", is_superuser=True, is_staff=True)
        self.client.force_login(user)
        response = self.client.get(reverse("rentabilidad_dashboard"), {"periodo": "2026-10"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["sales_confidence"]["missing_cost_rows"], 0)
        self.assertEqual(response.context["sales_confidence"]["margin"], Decimal("-167.69"))
        self.assertEqual(build_sales_confidence(start_date=PERIOD, end_date=END)["missing_cost_rows"], 123)

    def test_missing_authorization_keeps_the_economic_pending(self):
        self.rule.refresh_from_db()
        metadata = deepcopy(self.rule.metadata)
        metadata["monthly_sold_material_treatments"]["2026-10-01"]["authorization"] = ""
        self.rule.metadata = metadata
        self.rule.save()
        result = leer_resultado_mensual(PERIOD)
        for branch_id in self.branch_quantities:
            self.assertIn("Costo sin confirmar: producto 186, receta 516", result["sucursales"][branch_id]["pendientes"])


class IncrementalLabelsCommandTests(TransactionTestCase):
    def setUp(self):
        IncrementalLabelsTests.setUpTestData()
        self.rule = RecetaAgrupacionAddon.objects.get(pk=26)
        self.rule.metadata = {"preserved": "previous context"}
        self.rule.save()
        piece, _ = UnidadMedida.objects.get_or_create(codigo="pza", defaults={"nombre": "Pieza", "tipo": "UNIT"})
        gram, _ = UnidadMedida.objects.get_or_create(codigo="g", defaults={"nombre": "Gramo", "tipo": "MASS"})
        for pk, name in ((98, "Etiqueta G"), (99, "Etiqueta rectangular"), (384, "Crumble Zanahoria")):
            ingredient = Insumo.objects.create(pk=pk, nombre=name, unidad_base=piece if pk != 384 else gram)
            if pk in (98, 99):
                LineaReceta.objects.create(receta_id=516, insumo=ingredient, insumo_texto=name,
                    cantidad=1, unidad_texto="PZA", unidad=piece)
            if pk == 384:
                for recipe_id in (55, 516):
                    LineaReceta.objects.create(receta_id=recipe_id, insumo=ingredient, insumo_texto=name,
                        cantidad=20, unidad_texto="Gr", unidad=gram)
        sources = (
            (98, 11095, 3, ".43", 10500), (98, 12253, 21, ".43", 8000),
            (98, 12616, 24, ".44", 10000), (98, 12575, 26, ".44", 15500),
            (99, 11235, 6, ".56", 5370), (99, 11572, 10, ".56", 3370),
            (99, 11890, 15, ".56", 2370), (99, 12256, 21, ".56", 1370),
            (99, 12617, 24, ".57", 15000), (99, 12578, 26, ".57", 16370),
            (99, 12840, 30, ".57", 15370), (99, 12900, 31, ".57", 14370),
        )
        for ingredient, pk, day, cost, weight in sources:
            CostoInsumo.objects.create(pk=pk, insumo_id=ingredient, fecha=date(2026, 10, day),
                costo_unitario=cost, source_hash=f"incremental-label-{pk}",
                raw={"source": "POINT_EXISTENCIA_ALMACEN", "unit": "pza", "quantity": weight})

    def command(self, apply=False):
        output = io.StringIO()
        call_command("aplicar_etiquetas_topping_octubre", apply=apply, stdout=output)
        return json.loads(output.getvalue())

    def test_read_only_preview_apply_only_rule26_and_idempotent_replay(self):
        sales_before = list(PointDailySale.objects.order_by("pk").values())
        with CaptureQueriesContext(connection) as preview_sql:
            preview = self.command()
        self.assertEqual(preview["total_incremental"], "167.69")
        self.assertFalse(any(query["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for query in preview_sql))
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.metadata, {"preserved": "previous context"})
        with CaptureQueriesContext(connection) as apply_sql:
            self.command(apply=True)
        writes = [query["sql"] for query in apply_sql if query["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))]
        self.assertEqual(len(writes), 1)
        self.assertTrue(writes[0].startswith('UPDATE "recetas_recetaagrupacionaddon"'))
        self.rule.refresh_from_db()
        applied = deepcopy(self.rule.metadata)
        revision = self.rule.actualizado_en
        self.assertEqual(applied["preserved"], "previous context")
        self.assertTrue(self.command(apply=True)["already_present"])
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.metadata, applied)
        self.assertEqual(self.rule.actualizado_en, revision)
        self.assertEqual(list(PointDailySale.objects.order_by("pk").values()), sales_before)

    def test_changed_prices_or_sales_fail_before_writing(self):
        price = CostoInsumo.objects.get(pk=12616)
        price.costo_unitario = "0.50"
        price.save()
        with self.assertRaises(CommandError):
            self.command(apply=True)
        price.costo_unitario = "0.44"
        price.save()
        sale = PointDailySale.objects.filter(product_id=186).first()
        sale.quantity += 1
        sale.save()
        with self.assertRaises(CommandError):
            self.command(apply=True)
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.metadata, {"preserved": "previous context"})

    def test_labels_already_in_base_or_missing_in_addon_are_rejected(self):
        ingredient = Insumo.objects.get(pk=98)
        extra = LineaReceta.objects.create(receta_id=55, insumo=ingredient, insumo_texto=ingredient.nombre,
            cantidad=1, unidad_texto="PZA", unidad=ingredient.unidad_base)
        with self.assertRaises(CommandError):
            self.command(apply=True)
        extra.delete()
        LineaReceta.objects.filter(receta_id=516, insumo_id=98).delete()
        with self.assertRaises(CommandError):
            self.command(apply=True)
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.metadata, {"preserved": "previous context"})
