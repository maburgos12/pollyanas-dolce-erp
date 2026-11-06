from calendar import monthrange
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from unittest.mock import patch

from django.test import TestCase
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointProduct, PointProductionLine, PointDailySale
from reportes.models import ProductoReventaCosto, RecetaCostoHistoricoMensual
from recetas.models import Receta
from .services_costos import asignar_costos_filas
from .services_produccion_documental import SOURCE, _manifest, payload_hash, fuentes_produccion_documental
from .services_resultado import leer_resultado_mensual


class ProduccionDocumentalTests(TestCase):
    def setUp(self):
        self.period = date(2025, 1, 1)
        self.product = PointProduct.objects.create(external_id="875", sku="01GO01", name="Galleta Oreo")
        self.branch = PointBranch.objects.create(external_id="10", name="Produccion Crucero")
        self.group = dict(period="2025-01-01", sku=self.product.sku, name=self.product.name,
                          external_id="875", branch_external_id="10", quantity="5.123456",
                          cost_total=str(Decimal("2.123456") * Decimal("10.123456") + Decimal(60)), lines=[])
        self.rows = []
        for index, (qty, cost) in enumerate([("2.123456", "10.123456"), ("3", "20")]):
            raw = {"production": {"FK_Produccion": 100 + index, "Fecha": "2025-01-15T00:00:00", "Activo": True, "Status": 3},
                   "detail": {"PK_Produccion_detalle": 200 + index, "PK_Producto": 875,
                              "Codigo": self.product.sku, "Nombre": self.product.name, "Unidad": "U",
                              "Cantidad_producida": qty, "Cantidad_solicitada": qty, "Precio_default": cost,
                              "IsInsumo": False, "Status": 3},
                   "historical_cost_review": {"source": SOURCE, "branch_external_id": "10",
                                              "production_source_sha256": "a" * 64, "movement_source_sha256": "b" * 64}}
            h = payload_hash(raw)
            self.group["lines"].append(dict(order=str(100+index), detail=str(200+index), source_hash=h))
            self.rows.append(PointProductionLine.objects.create(branch=self.branch,
                production_external_id=str(100+index), detail_external_id=str(200+index), source_hash=h,
                production_date=date(2025, 1, 15), item_name=self.product.name, item_code=self.product.sku,
                unit="U", unit_cost=cost, produced_quantity=Decimal(qty).quantize(Decimal(".001"), rounding=ROUND_HALF_UP),
                requested_quantity=Decimal(qty).quantize(Decimal(".001"), rounding=ROUND_HALF_UP), raw_payload=raw))
        self.patch = patch("rentabilidad.services_produccion_documental._manifest", return_value={"groups": [self.group]})
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def evidence(self, period=None):
        period = period or self.period
        end = period.replace(day=monthrange(period.year, period.month)[1])
        return fuentes_produccion_documental({self.product.pk}, period, end).get(self.product.pk)

    def row(self, **kwargs):
        row = dict(product_id=self.product.pk, product__sku=self.product.sku,
                   product__name=self.product.name, receta_id=None)
        row.update(kwargs)
        asignar_costos_filas([row], self.period, date(2025, 1, 31))
        return row

    def test_weighted_native_precision_and_material_consumer(self):
        expected = (Decimal(self.group["cost_total"]) / Decimal(self.group["quantity"])).quantize(Decimal(".000001"))
        self.assertEqual(self.evidence()["cost"], expected)
        self.assertFalse(self.row()["cost_is_resale"])
        sucursal = Sucursal.objects.create(codigo="NAT25", nombre="Native test")
        branch = PointBranch.objects.create(external_id="sales", name="Sales", erp_branch=sucursal)
        sale = PointDailySale.objects.create(branch=branch, product=self.product, sale_date=self.period,
                                           quantity=2, gross_amount=100)
        result = leer_resultado_mensual(self.period)["sucursales"][sucursal.pk]
        self.assertEqual(result["costo_materia_prima"], expected * 2)
        self.assertEqual(result["costo_reventa"], 0)
        self.assertEqual(result["pendientes"], [])
        self.assertFalse(self.evidence()["financial_close"])
        sale.refresh_from_db()
        self.assertIsNone(sale.receta_id)
        self.assertFalse(Receta.objects.exists())
        from rentabilidad.views_rentabilidad import _build_productos_panel
        panel = _build_productos_panel(self.period, self.period, date(2025, 1, 31))
        self.assertEqual(panel["top_utilidad"][0]["tipo"], "Fabricado")
        self.assertEqual(panel["top_utilidad"][0]["costo_unitario"], expected)
        from reportes.models import FactVentaDiaria
        from reportes.sales_confidence import build_sales_confidence
        FactVentaDiaria.objects.create(fecha=self.period, sucursal=sucursal,
            producto_clave=self.product.external_id, producto_nombre=self.product.name,
            source_kind="AUTHORITATIVE", venta_neta=100, venta_total=100, cantidad=2, costo_estimado=80,
            metadata={"costing": {"source": "producto_costo_operativo_mensual", "period": "2024-12-01", "unit_cost": "40"}})
        confidence = build_sales_confidence(start_date=self.period, end_date=date(2025, 1, 31))
        self.assertEqual(confidence["cost_coverage_pct"], 100)
        self.assertEqual(confidence["margin"], (100 - expected * 2).quantize(Decimal(".01")))
        self.assertEqual(confidence["status"], "Estimado con costo del mes")
        partial = build_sales_confidence(start_date=self.period, end_date=date(2025, 1, 20))
        self.assertEqual(partial["margin"], 20)  # Existing backed cost survives; no future production.
        self.assertEqual(partial["prior_cost_sales"], 100)

    def test_incomplete_duplicate_or_changed_pair_rejects_whole_group(self):
        first = self.rows[0]
        for field, value in [("source_hash", "f" * 64), ("unit", "PZA"), ("is_insumo", True),
                             ("produced_quantity", 99), ("requested_quantity", 99), ("unit_cost", 99), ("production_date", date(2026, 1, 15)),
                             ("item_code", "wrong"), ("item_name", "wrong")]:
            with self.subTest(field=field):
                original = getattr(first, field)
                PointProductionLine.objects.filter(pk=first.pk).update(**{field: value})
                self.assertIsNone(self.evidence())
                PointProductionLine.objects.filter(pk=first.pk).update(**{field: original})
        first.pk = None
        first.source_hash = "e" * 64
        first.save()
        self.assertIsNone(self.evidence())
        first.delete()
        self.rows[1].delete()
        self.assertIsNone(self.evidence())

    def test_unreviewed_raw_and_altered_original_rejected(self):
        first = self.rows[0]
        for payload in [{}, {**first.raw_payload, "historical_cost_review": {}},
                        {**first.raw_payload, "detail": {**first.raw_payload["detail"], "PK_Producto": self.product.pk}}]:
            PointProductionLine.objects.filter(pk=first.pk).update(raw_payload=payload)
            self.assertIsNone(self.evidence())

    def test_wrong_identity_month_and_2026_never_costed(self):
        for period in [date(2025, 2, 1), date(2024, 1, 1), date(2026, 1, 1)]:
            self.assertIsNone(self.evidence(period))
        for changes in [dict(product__name="Other"), dict(product__sku="Other")]:
            self.assertIsNone(self.row(**changes)["cost_evidence"])
        self.product.external_id = "918"
        self.product.save()
        self.assertIsNone(self.evidence())

    def test_group_totals_and_model_precision_are_checked(self):
        self.group["quantity"] = "5.123"
        self.assertIsNone(self.evidence())
        self.group["quantity"] = "5.123456"
        self.group["cost_total"] = "80"
        self.assertIsNone(self.evidence())

    def test_explicit_native_unit_is_not_a_conversion(self):
        self.group["unit"] = "PZA"
        self.assertIsNone(self.evidence())
        for row, expected in zip(self.rows, self.group["lines"]):
            row.raw_payload["detail"]["Unidad"] = "PZA"
            row.source_hash = payload_hash(row.raw_payload)
            row.unit = "PZA"
            row.save()
            expected["source_hash"] = row.source_hash
        self.assertIsNotNone(self.evidence())
        self.group["unit"] = "KG"
        self.assertIsNone(self.evidence())

    def test_native_name_difference_requires_exact_reviewed_literal(self):
        for row, expected in zip(self.rows, self.group["lines"]):
            row.item_name += " "
            row.raw_payload["detail"]["Nombre"] = row.item_name
            row.source_hash = payload_hash(row.raw_payload)
            row.save()
            expected["source_hash"] = row.source_hash
        self.assertIsNone(self.evidence())
        self.group["native_name"] = self.product.name + " "
        self.assertIsNotNone(self.evidence())
        self.group["name"] = "Different product"
        self.product.name = self.group["name"]
        self.product.save()
        self.assertIsNone(self.evidence())

    def test_multiple_native_branches_require_per_line_evidence(self):
        branch = PointBranch.objects.create(external_id="8", name="CEDIS")
        row = self.rows[1]
        row.branch = branch
        row.raw_payload["historical_cost_review"]["branch_external_id"] = "8"
        row.source_hash = payload_hash(row.raw_payload)
        row.save()
        self.group["lines"][1]["source_hash"] = row.source_hash
        self.assertIsNone(self.evidence())
        self.group["lines"][1]["branch_external_id"] = "8"
        self.assertIsNotNone(self.evidence())
        PointProductionLine.objects.filter(pk=row.pk).update(branch=self.branch)
        self.assertIsNone(self.evidence())

    def test_partial_month_never_uses_monthly_native_average(self):
        row = dict(product_id=self.product.pk, product__sku=self.product.sku,
                   product__name=self.product.name, receta_id=None)
        asignar_costos_filas([row], self.period, date(2025, 1, 20))
        self.assertIsNone(row["cost_evidence"])
        asignar_costos_filas([row], self.period, date(2025, 1, 31))
        self.assertEqual(row["cost_evidence"]["through"], date(2025, 1, 31))

    def test_positive_acquisition_keeps_precedence(self):
        ProductoReventaCosto.objects.create(producto_point=self.product, fecha_vigencia=self.period,
            costo_unitario=30, fuente=ProductoReventaCosto.FUENTE_MANUAL,
            metadata={"point_name": self.product.name})
        self.assertTrue(self.row()["cost_is_resale"])
        self.assertEqual(self.row()["cost_evidence"]["cost"], 30)

    def test_zero_native_quantity_or_cost_rejected_even_when_hash_is_reviewed(self):
        first = self.rows[0]
        for key in ["Cantidad_producida", "Precio_default"]:
            raw = {**first.raw_payload, "detail": {**first.raw_payload["detail"], key: "0"}}
            h = payload_hash(raw)
            self.group["lines"][0]["source_hash"] = h
            PointProductionLine.objects.filter(pk=first.pk).update(source_hash=h, raw_payload=raw)
            self.assertIsNone(self.evidence())

    def test_existing_recipe_and_authorized_zero_keep_precedence(self):
        recipe = Receta.objects.create(nombre="Existing", hash_contenido="native-existing")
        RecetaCostoHistoricoMensual.objects.create(receta=recipe, periodo=self.period,
                                                  costo_total=25, coverage_pct=100)
        self.assertEqual(self.row(receta_id=recipe.pk)["cost_evidence"]["cost"], 25)
        ProductoReventaCosto.objects.create(producto_point=self.product, fecha_vigencia=self.period,
            costo_unitario=0, fuente=ProductoReventaCosto.FUENTE_MANUAL,
            metadata={"cost_treatment": "NO_DIRECT_COST", "authorization": "DG", "point_external_id": "875",
                      "point_name": self.product.name})
        self.assertTrue(self.row()["cost_is_resale"])
        self.assertEqual(self.row()["cost_evidence"]["cost"], 0)


class ManifestDocumentalTests(TestCase):
    def test_reviewed_manifest_has_exact_scope_and_unique_pairs(self):
        manifest = _manifest()
        self.assertEqual(manifest["source"], SOURCE)
        self.assertEqual(len(manifest["groups"]), 76)
        lines = [line for group in manifest["groups"] for line in group["lines"]]
        self.assertEqual(len(lines), 453)
        self.assertEqual(len({(line["order"], line["detail"]) for line in lines}), 453)
        cookies = [g for g in manifest["groups"] if g["external_id"] in {"875", "876"}]
        self.assertEqual(len(cookies), 20)
        self.assertEqual(sum(len(g["lines"]) for g in cookies), 216)
        self.assertEqual({g.get("unit", "U") for g in manifest["groups"]}, {"U", "PZA"})
        self.assertTrue(all(g["period"].startswith("2025-") for g in manifest["groups"]))
