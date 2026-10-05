from copy import deepcopy
from datetime import date, datetime, timezone as dt_timezone
from decimal import Decimal

from django.test import TestCase
from django.db import connection
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointConversionLine, PointRecipeExtractionRun, PointRecipeNode, PointSyncJob, PointWasteLine
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from pos_bridge.services.product_month_closure_service import ProductMonthClosureService
from reportes.models import ProductBusinessRule


class DocumentaryCommercialClassificationTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="DOC", nombre="Matriz")
        self.branch = PointBranch.objects.create(external_id="DOC", name="Matriz", erp_branch=self.sucursal)
        self.rule, _ = ProductBusinessRule.objects.update_or_create(product_name="COCA-COLA 450 ML", defaults={"classification": "REVENTA", "is_fixed": True})
        self.run = PointRecipeExtractionRun.objects.create()
        self.coca_node = self._node("COCA450", "COCA-COLA 450 ML", 850, "Bebidas", "Coca-cola")
        self.vela_node = self._node("875", "VELA INDIVIDUAL ", 1001, "Velas", "Alegría")
        self.waste_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_WASTE, status="SUCCESS", parameters={"start_date": "2026-09-01", "end_date": "2026-09-30"}, result_summary={"waste_lines_seen": 1})
        self.conversion_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status="SUCCESS", parameters={"source": "point_conversion_lines", "date_from": "2026-09-01", "date_to": "2026-09-30"}, result_summary={"created": 1, "skipped": 0, "skipped_unmatched_branch": 0, "total_rows": 1, "report_pk": "report-doc"})
        self.waste = PointWasteLine.objects.create(branch=self.branch, erp_branch=self.sucursal, sync_job=self.waste_job, movement_external_id="1666364", source_hash="waste-documentary", movement_at=datetime(2026, 9, 8, 21, 6, 45, 827000, tzinfo=dt_timezone.utc), item_name="COCA-COLA 450 ML", quantity=1, unit="PZA", unit_cost="15.63", total_cost="15.63", raw_payload={"movement": {"PK_Movimiento": 1666364, "Fecha": "2026-09-08T21:06:45.827", "Sucursal": "Matriz", "Costo": 15.63}, "details": [{"Articulo": "COCA-COLA 450 ML", "Cantidad": 1, "Unidad": "PZA", "Costo_unitario": 15.63, "Costo_total": 15.63}]})
        self.conversion = PointConversionLine.objects.create(branch=self.branch, erp_branch=self.sucursal, sync_job=self.conversion_job, movement_external_id="AGG-vela", source_hash="conversion-documentary", movement_at=datetime(2026, 9, 1, 7, tzinfo=dt_timezone.utc), item_code="875", item_name="VELA INDIVIDUAL ", quantity=11, unit="PZA", source_endpoint="/Report/crea_Reporte_Largo", raw_payload={"CÓDIGO": "875", "PRODUCTO": "VELA INDIVIDUAL ", "CATEGORÍA": "Alegría", "UNIDAD": "PZA", "SUCURSAL": "Matriz", "CANTIDAD": 11, "COSTO": 0})

    def _node(self, code, name, pk, family, category):
        return PointRecipeNode.objects.create(run=self.run, identity_key=f"PRODUCT:{code}", source_type="PRODUCT", node_kind="FINAL_PRODUCT", point_pk=str(pk), point_code=code, point_name=name, family=family, category=category, raw_detail={"PK_Producto": pk, "Codigo": code, "Nombre": name, "Produccion": False, "Rastreable": False, "Activo": True, "FK_Unidad": 5})

    def _read(self, family):
        service = MonthlyPointProductBalanceService()
        kwargs = {"month_start": date(2026, 9, 1), "month_end": date(2026, 9, 30)}
        if family == "waste":
            values, meta, unresolved = service._load_waste(**kwargs)
            return values, meta, unresolved
        values, unresolved, movements, counts, meta = service._load_conversions(**kwargs)
        return values, meta, unresolved + movements

    def test_exact_coca_rule_classifies_without_product_fk_or_source_mutation(self):
        original = deepcopy(self.waste.raw_payload)
        values, meta, unresolved = self._read("waste")
        self.assertEqual(unresolved, [])
        self.assertEqual(values, {})
        self.assertTrue(meta["authoritative"])
        self.assertEqual(meta["rows_read"], 1)
        decision = meta["excluded_documentary_rows"][0]
        self.assertEqual(decision["classification"], "REVENTA")
        self.assertEqual(decision["quantity"], "1.000")
        self.assertEqual(decision["rule"]["id"], self.rule.pk)
        self.assertFalse(decision["transactional_product_identity_verified"])
        self.waste.refresh_from_db()
        self.assertEqual(self.waste.raw_payload, original)
        self.assertIsNone(self.waste.receta_id)
        self.assertIsNone(self.waste.insumo_id)

    def test_exact_vela_keeps_aggregate_not_execution(self):
        values, meta, unresolved = self._read("conversions")
        self.assertEqual(unresolved, [])
        self.assertEqual(values, {})
        decision = meta["excluded_documentary_rows"][0]
        self.assertEqual(decision["classification"], "ACCESORIO")
        self.assertEqual(decision["quantity"], "11.000")
        self.assertFalse(decision["execution_origin_verified"])
        self.assertFalse(decision["transactional_product_identity_verified"])
        self.assertEqual(meta, self._read("conversions")[1])

    def test_classification_does_not_make_incomplete_source_authoritative(self):
        self.waste_job.result_summary = {"waste_lines_seen": 2}
        self.waste_job.save()
        _, meta, unresolved = self._read("waste")
        self.assertEqual(unresolved, [])
        self.assertFalse(meta["authoritative"])
        self.assertEqual(len(meta["excluded_documentary_rows"]), 1)

    def test_rule_or_catalog_contradiction_preserves_unresolved(self):
        self.rule.classification = "FABRICADO"
        self.rule.save()
        self.assertEqual(len(self._read("waste")[2]), 1)
        self.vela_node.source_type = "INSUMO"
        self.vela_node.save()
        self.assertTrue(self._read("conversions")[2])

    def test_raw_quantity_or_branch_mutation_preserves_unresolved(self):
        self.waste.raw_payload["details"][0]["Cantidad"] = 2
        self.waste.save()
        self.assertEqual(len(self._read("waste")[2]), 1)
        self.conversion.raw_payload["SUCURSAL"] = "Otra sucursal"
        self.conversion.save()
        self.assertTrue(self._read("conversions")[2])

    def test_incoherent_node_identity_key_is_not_corroboration(self):
        self.vela_node.identity_key = "INSUMO:875"
        self.vela_node.save()
        self.assertTrue(self._read("conversions")[2])

    def test_candidate_with_second_product_pk_is_ambiguous(self):
        self.run = PointRecipeExtractionRun.objects.create()
        self._node("875", "VELA INDIVIDUAL ", 1002, "Velas", "Alegría")
        self.assertTrue(self._read("conversions")[2])

    def test_consistent_repeated_run_is_documented_without_transactional_fk(self):
        self.run = PointRecipeExtractionRun.objects.create()
        self._node("875", "VELA INDIVIDUAL ", 1001, "Velas", "Alegría")
        _, meta, unresolved = self._read("conversions")
        self.assertEqual(unresolved, [])
        self.assertEqual(len(meta["excluded_documentary_rows"][0]["nodes"]), 2)

    def test_used_raw_changes_metadata_but_unrelated_node_does_not(self):
        original = self._read("waste")[1]
        self._node("OTHER", "Producto ajeno", 9999, "Velas", "Alegría")
        self.assertEqual(original, self._read("waste")[1])
        self.waste.raw_payload["justifications"] = [{"Justificacion": "Documento original adicional"}]
        self.waste.save()
        self.assertNotEqual(original, self._read("waste")[1])

    def test_malformed_raw_and_node_values_fail_closed(self):
        original = deepcopy(self.waste.raw_payload)
        for value in (True, None, "NaN", "Infinity", "bad", 2):
            with self.subTest(value=value):
                self.waste.raw_payload = deepcopy(original)
                self.waste.raw_payload["details"][0]["Cantidad"] = value
                self.waste.save()
                self.assertTrue(self._read("waste")[2])
        self.waste.raw_payload = deepcopy(original)
        self.waste.raw_payload["details"][0]["Articulo"] = 123
        self.waste.save()
        self.assertTrue(self._read("waste")[2])
        for value in (True, 1001.0, "1001", None):
            with self.subTest(pk=value):
                self.vela_node.raw_detail["PK_Producto"] = value
                self.vela_node.save()
                self.assertTrue(self._read("conversions")[2])

    def test_extra_and_manufactured_aggregates_remain_unresolved(self):
        for code, name in (("0227", "Extra10"), ("0058", "Snickers Rebanada")):
            with self.subTest(code=code):
                self.conversion.item_code = code
                self.conversion.item_name = name
                self.conversion.save()
                self.assertTrue(self._read("conversions")[2])

    def test_bulk_classification_queries_do_not_grow_with_rows(self):
        with CaptureQueriesContext(connection) as one:
            self._read("waste")
        for index in range(1, 11):
            raw = deepcopy(self.waste.raw_payload)
            raw["movement"]["PK_Movimiento"] += index
            PointWasteLine.objects.create(branch=self.branch, erp_branch=self.sucursal,
                sync_job=self.waste_job, movement_external_id=str(1666364 + index),
                source_hash=f"waste-bulk-{index}", movement_at=self.waste.movement_at,
                item_name=self.waste.item_name, quantity=1, unit="PZA", unit_cost="15.63",
                total_cost="15.63", raw_payload=raw)
        self.waste_job.result_summary = {"waste_lines_seen": 11}
        self.waste_job.save()
        with CaptureQueriesContext(connection) as many:
            _, meta, unresolved = self._read("waste")
        self.assertEqual(unresolved, [])
        self.assertEqual(len(meta["excluded_documentary_rows"]), 11)
        self.assertEqual(len(one), len(many))
        catalog_queries = [query for query in many if
                           "FROM \"pos_bridge_recipe_nodes\"" in query["sql"] or
                           "FROM \"reportes_productbusinessrule\"" in query["sql"]]
        self.assertEqual(len(catalog_queries), 2)

    def test_used_rule_node_and_raw_change_canonical_metadata_fingerprint(self):
        digest = ProductMonthClosureService._canonical_source_metadata_digest
        initial = digest({"waste_meta": self._read("waste")[1]})
        self.rule.is_fixed = False
        self.rule.save()
        self.assertNotEqual(initial, digest({"waste_meta": self._read("waste")[1]}))
        self.rule.is_fixed = True
        self.rule.save()
        before_node = digest({"waste_meta": self._read("waste")[1]})
        self.coca_node.raw_detail["Descripcion"] = "Nueva evidencia comercial"
        self.coca_node.save()
        self.assertNotEqual(before_node, digest({"waste_meta": self._read("waste")[1]}))
        before_raw = digest({"waste_meta": self._read("waste")[1]})
        self.waste.raw_payload["justifications"] = [{"Justificacion": "Evidencia ampliada"}]
        self.waste.save()
        self.assertNotEqual(before_raw, digest({"waste_meta": self._read("waste")[1]}))
