from datetime import date, datetime
from decimal import Decimal as D

from django.template.loader import render_to_string
from django.contrib.auth.models import AnonymousUser
from django.test import RequestFactory, TestCase
from django.test.utils import CaptureQueriesContext
from django.db import connection
from django.utils import timezone

from control.models import MermaPOS
from core.models import Sucursal
from inventario.models import MovimientoInventario
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointProductionLine, PointTransferLine, PointWasteLine
from recetas.models import Receta
from reportes.daily_operational_closure_service import _branch_rows, _waste_domain
from reportes.models import FactProduccionDiaria, FactVentaDiaria


class CierreWasteUnitsTests(TestCase):
    def setUp(self):
        self.day = date(2026, 11, 18)
        self.branch = Sucursal.objects.create(codigo="WASTE", nombre="Sucursal de prueba")
        self.point_branch = PointBranch.objects.create(external_id="waste-unit", name="Sucursal de prueba", erp_branch=self.branch)
        self.kg = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo="MASA", factor_to_base=1000)
        self.recipe = Receta.objects.create(nombre="Pastel", tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="unit-recipe")
        self.item = Insumo.objects.create(nombre="Harina dimensional", unidad_base=self.kg)

    def waste(self, key="unit-source", quantity="2.125", unit="pza", *, visible_quantity=None, stamp=None, insumo=False):
        raw = PointWasteLine.objects.create(
            branch=self.point_branch, erp_branch=self.branch, receta=None if insumo else self.recipe,
            insumo=self.item if insumo else None, movement_external_id=key, source_hash=key,
            movement_at=stamp or timezone.make_aware(datetime(2026, 11, 18, 12)),
            item_name="Merma de prueba", quantity=D(quantity), unit=unit, total_cost=D("12.34"),
        )
        visible = MermaPOS.objects.create(
            fecha=self.day, sucursal=self.branch, receta=raw.receta, source_hash=key,
            cantidad=D(quantity if visible_quantity is None else visible_quantity), producto_texto="Merma de prueba",
        )
        return raw, visible

    def movement(self, quantity="3.250"):
        return MovimientoInventario.objects.create(
            fecha=timezone.make_aware(datetime(2026, 11, 18, 13)), tipo="CONSUMO",
            insumo=self.item, cantidad=D(quantity), almacen="CUARTO_FRIO", referencia="MERMA|2026-11-18|r5",
        )

    def test_does_not_add_visible_product_count_to_unproven_ingredient_quantity(self):
        self.waste()
        self.movement()
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["visible_units"])
        self.assertEqual(domain["raw_cost"], D("12.34"))
        self.assertEqual(domain["visible_rows"], 2)

    def test_point_mixed_units_have_no_unqualified_physical_total(self):
        self.waste("piece", "2", "pza")
        self.waste("mass", "0.375", "kg", insumo=True)
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["raw_units"])
        self.assertEqual(domain["raw_cost"], D("24.68"))
        self.assertEqual({(g["unit"], g["quantity"]) for g in domain.get("point_quantity_groups", [])}, {("pza", D("2")), ("kg", D("0.375"))})

    def test_exact_point_link_recovers_unit_and_preserves_real_zero(self):
        self.waste(quantity="0", unit="pza")
        domain = _waste_domain(self.day)
        self.assertEqual(domain.get("branch_quantity_groups"), [{"family": "producto", "unit": "pza", "quantity": D("0"), "rows": 1}])
        self.assertEqual(domain["branch_visible_units"], D("0"))

    def test_quantity_mismatch_keeps_quantity_but_not_an_inferred_unit(self):
        self.waste(visible_quantity="8")
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["branch_visible_units"])
        self.assertEqual(domain.get("branch_unknown_rows", [])[0]["quantity"], D("8"))

    def test_missing_source_hash_does_not_match_by_name_or_recipe(self):
        self.waste()
        MermaPOS.objects.update(source_hash=None)
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["branch_visible_units"])
        self.assertEqual(len(domain.get("branch_unknown_rows", [])), 1)

    def test_different_operational_day_does_not_borrow_point_unit(self):
        stamp = timezone.make_aware(datetime(2026, 11, 17, 12))
        self.waste(stamp=stamp)
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["branch_visible_units"])
        self.assertEqual(len(domain.get("branch_unknown_rows", [])), 1)

    def test_local_date_is_used_for_source_link(self):
        stamp = timezone.make_aware(datetime(2026, 11, 18, 23, 30))
        self.waste(stamp=stamp)
        self.assertEqual(_waste_domain(self.day).get("branch_quantity_groups", [])[0]["unit"], "pza")

    def test_missing_point_unit_is_explicitly_unknown_even_for_zero(self):
        self.waste(quantity="0", unit=" ")
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["raw_units"])
        self.assertIsNone(domain["visible_units"])
        self.assertEqual(domain.get("branch_unknown_rows", [])[0]["quantity"], D("0"))

    def test_unclassified_point_rows_are_not_summed_as_one_product_family(self):
        self.waste("unknown-one", "2", "pza")
        self.waste("unknown-two", "3", "pza")
        PointWasteLine.objects.update(receta=None)
        MermaPOS.objects.update(receta=None)
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["raw_units"])
        self.assertEqual(domain["point_quantity_groups"], [])
        self.assertEqual([row["quantity"] for row in domain["point_unknown_rows"]], [D(2), D(3)])

    def test_imported_ingredient_does_not_get_master_unit_as_unproven_conversion(self):
        self.movement()
        domain = _waste_domain(self.day)
        self.assertIsNone(domain["cedis_visible_units"])
        unknown = domain.get("cedis_unknown_rows", [])
        self.assertEqual((unknown[0]["quantity"], unknown[0]["unit"]), (D("3.250"), ""))

    def test_branch_net_is_not_computed_with_ingredient_or_unknown_units(self):
        self.waste(quantity="0.375", unit="kg", insumo=True)
        FactVentaDiaria.objects.create(fecha=self.day, sucursal=self.branch, producto_clave="one", cantidad=2)
        FactProduccionDiaria.objects.create(fecha=self.day, sucursal=self.branch, producido=10, vendido=2)
        row = _branch_rows(self.day)[0]
        self.assertIsNone(row["waste_units_visible"])
        self.assertIsNone(row["net_units"])
        self.assertEqual(row["produced_units"], D("10"))

    def piece_activity(self):
        self.waste(quantity="2", unit="pza")
        FactProduccionDiaria.objects.create(fecha=self.day, sucursal=self.branch, receta=self.recipe, producido=10, vendido=0)
        PointProductionLine.objects.create(
            branch=self.point_branch, erp_branch=self.branch, receta=self.recipe, production_date=self.day,
            production_external_id="unit-production", detail_external_id="one", source_hash="unit-production",
            item_name="Pastel", produced_quantity=10, unit="pza", is_insumo=False,
        )
        FactVentaDiaria.objects.create(fecha=self.day, sucursal=self.branch, receta=self.recipe, producto_clave="piece", cantidad=0)


        stamp = timezone.make_aware(datetime(2026, 11, 18, 15))
        PointTransferLine.objects.create(
            origin_branch=self.point_branch, destination_branch=self.point_branch,
            erp_destination_branch=self.branch, receta=self.recipe, registered_at=stamp, received_at=stamp,
            transfer_external_id="zero-transfer", detail_external_id="one", source_hash="zero-transfer",
            item_name="Pastel", received_quantity=0, unit="pza", is_insumo=False,
        )

    def test_product_piece_branch_net_remains_comparable(self):
        self.piece_activity()
        self.assertEqual(_branch_rows(self.day)[0]["net_units"], D("8"))

    def test_product_recipe_does_not_supply_a_unit_to_nonzero_sales(self):
        self.piece_activity()
        for quantity in (D("0.5"), D(2)):
            FactVentaDiaria.objects.update(cantidad=quantity)
            FactProduccionDiaria.objects.update(vendido=quantity)
            self.assertIsNone(_branch_rows(self.day)[0]["net_units"])

    def test_production_unit_or_quantity_mismatch_does_not_certify_net(self):
        self.piece_activity()
        for unit, quantity in (("kg", 10), ("", 10), ("pza", 9)):
            with self.subTest(unit=unit, quantity=quantity):
                PointProductionLine.objects.update(unit=unit, produced_quantity=quantity)
                self.assertIsNone(_branch_rows(self.day)[0]["net_units"])

    def test_source_recipe_and_sales_count_must_match_materialized_fact(self):
        self.piece_activity()
        FactVentaDiaria.objects.update(cantidad=3)
        self.assertIsNone(_branch_rows(self.day)[0]["net_units"])
        FactVentaDiaria.objects.update(cantidad=0)
        self.assertEqual(_branch_rows(self.day)[0]["net_units"], D("8"))
        other = Receta.objects.create(nombre="Otra", tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="other-unit-recipe")
        PointProductionLine.objects.update(receta=other)
        self.assertIsNone(_branch_rows(self.day)[0]["net_units"])

    def test_transfer_requires_exact_receipt_and_explicit_piece_unit(self):
        self.piece_activity()
        FactProduccionDiaria.objects.update(transferido=3)
        stamp = timezone.make_aware(datetime(2026, 11, 18, 15))
        transfer = PointTransferLine.objects.create(
            origin_branch=self.point_branch, destination_branch=self.point_branch,
            erp_destination_branch=self.branch, receta=self.recipe, registered_at=stamp, received_at=stamp,
            transfer_external_id="units-transfer", detail_external_id="one", source_hash="units-transfer",
            item_name="Pastel", received_quantity=3, unit="pza", is_insumo=False,
        )
        self.assertEqual(_branch_rows(self.day)[0]["net_units"], D("11"))
        for unit, quantity in (("kg", 3), ("", 3), ("pza", 2)):
            transfer.unit, transfer.received_quantity = unit, quantity
            transfer.save(update_fields=["unit", "received_quantity"])
            self.assertIsNone(_branch_rows(self.day)[0]["net_units"])

    def test_proven_zero_operands_and_waste_remain_numeric_zero(self):
        self.piece_activity()
        PointProductionLine.objects.update(produced_quantity=0)
        FactProduccionDiaria.objects.update(producido=0, vendido=0)
        FactVentaDiaria.objects.update(cantidad=0)
        PointWasteLine.objects.update(quantity=0)
        MermaPOS.objects.update(cantidad=0)
        row = _branch_rows(self.day)[0]
        self.assertEqual((row["produced_units"], row["waste_units_visible"], row["net_units"]), (D(0), D(0), D(0)))

    def test_missing_zero_source_is_not_an_explicit_neutral_operand(self):
        self.piece_activity()
        FactVentaDiaria.objects.all().delete()
        self.assertIsNone(_branch_rows(self.day)[0]["net_units"])
        FactVentaDiaria.objects.create(fecha=self.day, sucursal=self.branch, receta=self.recipe, producto_clave="zero", cantidad=0)
        PointTransferLine.objects.all().delete()
        self.assertIsNone(_branch_rows(self.day)[0]["net_units"])

    def test_offsetting_nonzero_sales_are_not_an_accredited_zero(self):
        self.piece_activity()
        for key, quantity in (("positive", 2), ("negative", -2)):
            FactVentaDiaria.objects.create(fecha=self.day, sucursal=self.branch, receta=self.recipe, producto_clave=key, cantidad=quantity)
        self.assertIsNone(_branch_rows(self.day)[0]["net_units"])

    def test_branch_bulk_queries_do_not_grow_with_source_rows(self):
        self.piece_activity()
        with CaptureQueriesContext(connection) as small:
            _branch_rows(self.day)
        for i in range(12):
            self.waste(f"branch-units-{i}")
        with CaptureQueriesContext(connection) as many:
            _branch_rows(self.day)
        self.assertEqual(len(small), len(many))
        self.assertTrue(all(q["sql"].lstrip().upper().startswith("SELECT") for q in many))

    def test_html_preserves_fraction_zero_and_explains_nd_without_requesting_capture(self):
        self.waste("zero", "0", "pza")
        self.movement()
        domain = _waste_domain(self.day)
        request = RequestFactory().get("/reportes/cierre-operativo/")
        request.user = AnonymousUser()
        html = render_to_string("reportes/cierre_operativo_diario.html", {
            "closure": {"waste": domain, "domains": [domain], "branch_rows": _branch_rows(self.day)},
            "target_date": self.day,
        }, request=request)
        self.assertIn("0 pza", html)
        self.assertRegex(html, r"3[.,]25")
        self.assertIn("no que falte la captura", html)
        self.assertIn("Merma de insumos importada", html)
        self.assertIn("N/D", html)

    def test_product_piece_waste_does_not_certify_a_net_with_unknown_fact_units(self):
        self.waste(quantity="2", unit="pza")
        FactProduccionDiaria.objects.create(fecha=self.day, sucursal=self.branch, producido=10, vendido=2)
        row = _branch_rows(self.day)[0]
        self.assertEqual(row["waste_units_visible"], D("2"))
        self.assertIsNone(row["net_units"])

    def test_html_does_not_round_reported_sales_or_production_to_zero_or_call_them_pieces(self):
        FactVentaDiaria.objects.create(fecha=self.day, sucursal=self.branch, producto_clave="fraction", cantidad=D("0.5"))
        FactProduccionDiaria.objects.create(fecha=self.day, sucursal=self.branch, producido=D("0.75"))
        request = RequestFactory().get("/reportes/cierre-operativo/")
        request.user = AnonymousUser()
        html = render_to_string("reportes/cierre_operativo_diario.html", {
            "closure": {"sales": {"fact_units": D("0.5"), "fact_tickets": 7},
                        "production": {"fact_produced": D("0.75")}, "branch_rows": _branch_rows(self.day)},
        }, request=request)
        self.assertIn("Cantidad reportada", html)
        self.assertNotIn("piezas", html)
        self.assertRegex(html, r'<td class="is-right">0[.,]50{0,2}</td>')
        self.assertRegex(html, r'<td class="is-right">0[.,]750?</td>')
        self.assertIn("unidad por acreditar", html)

    def test_html_uses_operational_labels_and_preserves_fractional_plan(self):
        request = RequestFactory().get("/reportes/cierre-operativo/")
        request.user = AnonymousUser()
        html = render_to_string("reportes/cierre_operativo_diario.html", {
            "closure": {"production": {"plan_units": D("0.125")},
                        "domains": [{"key": "produccion", "plan_units": D("0.125")}]}}, request=request)
        self.assertIn("Registros Point", html)
        self.assertIn("Registros del resumen ERP", html)
        self.assertNotIn("Venta fact", html)
        self.assertRegex(html, r"0[.,]125 · unidad por acreditar")
        self.assertNotIn("0 u", html)

    def test_reader_does_not_change_sources_and_has_no_per_row_queries(self):
        self.waste()
        self.movement()
        before = list(MermaPOS.objects.values()), list(MovimientoInventario.objects.values()), list(PointWasteLine.objects.values())
        with CaptureQueriesContext(connection) as small:
            _waste_domain(self.day)
        for i in range(12):
            self.waste(f"unit-{i}")
        with CaptureQueriesContext(connection) as many:
            _waste_domain(self.day)
        self.assertEqual(len(small), len(many))
        self.assertTrue(all(not q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for q in many))
        self.assertEqual(list(MovimientoInventario.objects.values()), before[1])
