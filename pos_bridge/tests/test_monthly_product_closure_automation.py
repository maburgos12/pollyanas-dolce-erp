from datetime import date
from decimal import Decimal
from uuid import uuid4
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Sucursal
from inventario.models_conteos import ConteoSucursal, LecturaConteoSucursal, LineaConteoSucursal
from pos_bridge.models import PointProduct
from pos_bridge.tasks.run_monthly_product_closure import (
    _apply_monthly_automation_gates,
    _audit_materialization_gate,
    _physical_count_gate,
)
from recetas.models import ProductoMonthClosure
from reportes.models import ProductInventoryAuditRun


class MonthlyProductClosurePhysicalGateTests(TestCase):
    month_end = date(2026, 10, 31)

    def setUp(self):
        self.user = get_user_model().objects.create_user("count-gate", password="test-local")
        self.matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Sucursal Matriz")
        self.leyva = Sucursal.objects.create(codigo="LEYVA", nombre="Sucursal Leyva")
        self.product = PointProduct.objects.create(external_id="gate-1", sku="GATE-1", name="Producto cierre")

    def count(self, branch, *, state="ACEPTADO", difference="0"):
        count = ConteoSucursal.objects.create(
            sucursal=branch,
            responsable=self.user,
            creado_por=self.user,
            fecha=self.month_end,
            titulo=f"Cierre {branch.codigo}",
            estado=state,
            request_id=uuid4(),
            payload_hash="a" * 64,
        )
        line = LineaConteoSucursal.objects.create(
            conteo=count,
            producto=self.product,
            codigo=self.product.sku,
            nombre=self.product.name,
            unidad="PZA",
            fuente_unidad="Point",
        )
        LecturaConteoSucursal.objects.create(
            linea=line,
            ronda=1,
            cantidad=Decimal("1"),
            actor=self.user,
        )
        count.conciliacion = {
            "estado": "CALCULADA",
            "lineas": {str(line.pk): {"estado": "CALCULADA", "diferencia": difference}},
        }
        count.save(update_fields=["conciliacion"])
        return count

    def gate(self):
        with patch(
            "pos_bridge.tasks.run_monthly_product_closure.canonical_point_network_branch_qs",
            return_value=Sucursal.objects.filter(pk__in=[self.matriz.pk, self.leyva.pk]),
        ):
            return _physical_count_gate(self.month_end)

    def test_all_operational_branches_with_zero_difference_are_ready(self):
        self.count(self.matriz)
        self.count(self.leyva)

        result = self.gate()

        self.assertTrue(result["ready"])
        self.assertEqual(result["ready_branch_count"], 2)
        self.assertEqual(result["issues"], [])

    def test_missing_branch_blocks_monthly_lock(self):
        self.count(self.matriz)

        result = self.gate()

        self.assertFalse(result["ready"])
        self.assertIn("LEYVA: falta conteo físico", result["issues"][0])

    def test_difference_or_recount_blocks_monthly_lock(self):
        self.count(self.matriz, difference="1")
        self.count(self.leyva, state="RECONTEO")

        result = self.gate()

        self.assertFalse(result["ready"])
        self.assertTrue(any("requiere reconteo" in issue for issue in result["issues"]))
        self.assertTrue(any("está RECONTEO" in issue for issue in result["issues"]))

    def test_gate_evidence_is_persisted_without_copying_reservations_as_sales(self):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2026, 10, 1),
            month_end=self.month_end,
            status=ProductoMonthClosure.STATUS_BUILT,
            metadata={"validation": {"blocking_issues": [], "lock_ready": True}},
        )
        physical = {"ready": False, "issues": ["LEYVA: falta conteo."], "branches": []}
        audit = {"ready": True, "status": "updated", "run_status": "READY"}

        with (
            patch(
                "pos_bridge.tasks.run_monthly_product_closure._physical_count_gate",
                return_value=physical,
            ),
            patch(
                "pos_bridge.tasks.run_monthly_product_closure._audit_materialization_gate",
                return_value=audit,
            ),
        ):
            closure, evidence = _apply_monthly_automation_gates(closure, month_end=self.month_end)

        self.assertFalse(closure.metadata["validation"]["lock_ready"])
        self.assertIn("LEYVA: falta conteo", closure.metadata["validation"]["blocking_issues"][0])
        self.assertEqual(evidence["audit_refresh"]["run_status"], "READY")
        self.assertEqual(
            closure.metadata["reservation_policy"]["treatment"],
            "availability_only_not_sale",
        )

    def test_visible_audit_must_be_ready_before_lock(self):
        run = ProductInventoryAuditRun.objects.create(
            month=date(2026, 10, 1),
            status=ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
            source_issues=[{"code": "SOURCE_INCOMPLETE", "message": "Falta cierre Point."}],
            summary={
                "created": 0,
                "updated": 0,
                "unchanged": 0,
                "reopened": 0,
                "balanced": 0,
                "exceptions": 0,
                "source_incomplete": 1,
            },
            calculation_fingerprint="b" * 64,
        )
        with patch(
            "pos_bridge.tasks.run_monthly_product_closure.refresh_inventory_audit_month",
            return_value={"status": "incomplete"},
        ):
            blocked = _audit_materialization_gate(run.month)
        self.assertFalse(blocked["ready"])

        run.status = ProductInventoryAuditRun.Status.READY
        run.source_issues = []
        run.save(update_fields=["status", "source_issues"])
        with patch(
            "pos_bridge.tasks.run_monthly_product_closure.refresh_inventory_audit_month",
            return_value={"status": "updated"},
        ):
            ready = _audit_materialization_gate(run.month)
        self.assertTrue(ready["ready"])
        self.assertEqual(ready["run_status"], ProductInventoryAuditRun.Status.READY)
