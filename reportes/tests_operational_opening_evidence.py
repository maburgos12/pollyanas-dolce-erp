"""An observed opening is evidence, never a monthly balance replacement."""
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import RequestFactory, TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointExtractionLog, PointInventorySnapshot, PointSyncJob
from reportes.tests_inventory_audit_agent import InventoryAuditAgentFixtures
from reportes.views_produccion import ProducidoVsVendidoMermaView
from reportes.models import ProductInventoryAuditRun
from recetas.models import Receta
from reportes.services_operational_opening_evidence import add_operational_opening_evidence


class OperationalOpeningEvidenceTests(InventoryAuditAgentFixtures, TestCase):
    def setUp(self):
        erp, _ = Sucursal.objects.get_or_create(pk=18, defaults={"codigo": "TEST-LEYVA", "nombre": "Leyva"})
        self.branch = PointBranch.objects.create(pk=28, external_id="14", name="Leyva", erp_branch=erp)
        self.audit_run.month = self.month = date(2026, 10, 1)
        self.audit_run.save(update_fields=["month"])
        self.case = self.make_case(opening_point=0)
        self.job = PointSyncJob.objects.create(pk=81086, job_type="inventory", status="SUCCESS")
        self.log = PointExtractionLog.objects.create(pk=302317, sync_job=self.job,
            message="Sucursal procesada 14.", context={"branch_id": 28, "branch_external_id": "14"})
        self.raw = {"row": [self.product.external_id, self.product.sku, self.product.name,
            "Pastel", "3.25", "PZA", "0", "0", "", "false"],
            "headers": ["Código", "Producto", "Cantidad", "Unidad", "Costo Unitario", "Costo Total", "Último Movimiento", "Opciones"]}
        self.snapshot = PointInventorySnapshot.objects.create(branch=self.branch, product=self.product,
            sync_job=self.job, stock=Decimal("3.25"), raw_payload=self.raw,
            captured_at=datetime(2026, 10, 3, 17, 48, 28, 494613, tzinfo=timezone.utc))
        self.visible = {"id": self.case.pk, "opening": None, "closing": Decimal("9")}
        self.groups = [{"rows": [{"cases": [self.visible]}]}]

    def add(self, cutoff=date(2026, 10, 31)):
        add_operational_opening_evidence(cutoff, self.groups)

    def test_exact_capture_preserves_positive_quantity_and_monthly_pending_with_only_three_reads(self):
        before = deepcopy(self.visible)
        with CaptureQueriesContext(connection) as queries:
            self.add()
        evidence = self.visible.pop("operational_opening_evidence")
        self.assertEqual(self.visible, before)
        self.assertEqual(evidence["quantity"], Decimal("3.25"))
        self.assertEqual(evidence["snapshot_id"], self.snapshot.pk)
        self.assertEqual(evidence["captured_at"].hour, 10)
        self.assertEqual(evidence["unit"], "PZA")
        self.assertEqual(len(queries), 3)
        self.assertTrue(all(query["sql"].lstrip().startswith("SELECT") for query in queries))
        self.case.refresh_from_db()
        self.assertEqual(self.case.opening_point, Decimal("0"))

    def test_other_month_and_already_known_opening_are_not_enriched(self):
        self.add(date(2026, 9, 30))
        self.assertNotIn("operational_opening_evidence", self.visible)
        self.visible["opening"] = Decimal("0")
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)

    def test_failed_job_or_wrong_branch_provenance_is_not_shown(self):
        self.job.status = "FAILED"
        self.job.save()
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)
        self.job.status = "SUCCESS"
        self.job.save()
        self.log.context["branch_external_id"] = "13"
        self.log.save()
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)

    def test_corrupt_raw_identity_quantity_domain_and_capture_are_rejected(self):
        for index, value in ((0, "OTHER"), (4, "NaN"), (4, "4"), (4, True), (4, "-1"), (5, ""), (9, "true")):
            with self.subTest(index=index, value=value):
                raw = deepcopy(self.raw)
                raw["row"][index] = value
                self.snapshot.raw_payload = raw
                self.snapshot.save()
                self.add()
                self.assertNotIn("operational_opening_evidence", self.visible)
        self.snapshot.raw_payload = self.raw
        self.snapshot.captured_at = datetime(2026, 10, 4, tzinfo=timezone.utc)
        self.snapshot.save()
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)

    def test_html_shows_observation_but_exports_and_data_do_not_enrich_or_change(self):
        self.case.movement_status = "SOURCE_INCOMPLETE"
        self.case.save(update_fields=["movement_status"])
        Receta.objects.create(nombre=self.product.name, codigo_point=self.product.sku,
            tipo=Receta.TIPO_PRODUCTO_FINAL, categoria="Pastel Chico", pasa_modulo_produccion=True,
            hash_contenido="operational-test")
        actor = get_user_model().objects.create_superuser(username="operational-admin")
        factory = RequestFactory()
        def response(**params):
            request = factory.get("/reportes/produccion/", {"periodo": "2026-10", "branch": "18", **params})
            request.user = actor
            if params.get("metadata") == "data":
                request.resolver_match = SimpleNamespace(url_name="producido_vs_vendido_data")
            view = ProducidoVsVendidoMermaView()
            view.setup(request)
            result = view.get(request)
            if hasattr(result, "render"):
                result.render()
            return result
        html = response()
        self.assertContains(html, "Inicio operativo observado")
        self.assertContains(html, "03/10/2026 10:48:28")
        self.assertContains(html, "3.250000 PZA")
        self.assertContains(html, "Saldo al 30/09 pendiente")
        self.assertContains(html, f"Captura Point #{self.snapshot.pk}")
        with patch("reportes.views_produccion.add_operational_opening_evidence") as enrich:
            before = [response(export="csv").content, response(metadata="data").content]
            enrich.assert_not_called()
        self.snapshot.delete()
        self.assertEqual(before, [response(export="csv").content, response(metadata="data").content])
        self.assertNotContains(response(), "Inicio operativo observado")

    def test_unselected_case_and_ambiguous_duplicate_are_not_enriched(self):
        self.visible["id"] = 2147483647
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)
        self.visible["id"] = self.case.pk
        PointInventorySnapshot.objects.create(branch=self.branch, product=self.product,
            sync_job=self.job, stock=Decimal("3.25"), raw_payload=self.raw,
            captured_at=self.snapshot.captured_at)
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)

    def test_case_month_and_existing_erp_branch_mapping_are_required(self):
        previous_run = ProductInventoryAuditRun.objects.create(month=date(2026, 9, 1),
            status="READY", calculation_fingerprint="c" * 64)
        previous_case = self.make_case(run=previous_run, month=date(2026, 9, 1))
        self.visible["id"] = previous_case.pk
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)
        self.visible["id"] = self.case.pk
        self.branch.erp_branch = None
        self.branch.save(update_fields=["erp_branch"])
        self.add()
        self.assertNotIn("operational_opening_evidence", self.visible)
