from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointInsumoInventorySnapshot, PointSyncJob
from pos_bridge.services.canonical_insumo_inventory_capture_service import (
    CanonicalInsumoInventoryCaptureService,
)
from pos_bridge.tasks.celery_tasks import task_canonical_insumo_inventory_sync


class CanonicalInsumoInventoryCaptureServiceTests(TestCase):
    @patch("pos_bridge.tasks.celery_tasks.CanonicalInsumoInventoryCaptureService")
    def test_central_task_marks_cycle_success_only_after_complete_capture(self, service_class):
        service_class.return_value.capture.return_value = {
            "complete": True,
            "locations": {"ALMACEN": {"snapshots": 1}, "CEDIS": {"snapshots": 1}},
            "blockers": [],
        }

        result = task_canonical_insumo_inventory_sync()

        job = PointSyncJob.objects.get(pk=result["job_id"])
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertTrue(job.parameters["canonical_insumo_inventory"])
        self.assertEqual(job.parameters["locations"], ["ALMACEN", "CEDIS"])

    def test_capture_persists_official_supply_rows_for_both_locations(self):
        gram = UnidadMedida.objects.create(
            codigo="g", nombre="Gramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1
        )
        UnidadMedida.objects.create(
            codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA, factor_to_base=1000
        )
        insumo = Insumo.objects.create(
            codigo_point="017", nombre="Fresa Fresca", categoria="FRUTAS", unidad_base=gram
        )
        PointBranch.objects.create(external_id="9", name="Almacen")
        PointBranch.objects.create(external_id="8", name="CEDIS")
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_RUNNING,
            parameters={"canonical_insumo_inventory": True, "locations": ["ALMACEN", "CEDIS"]},
        )
        rows = {
            "ALMACEN": [self._row("53.470999", "KG")],
            "CEDIS": [self._row("-14.140", "KG")],
        }
        captured_branches = []

        def capture_all_rows(*, branch_hint, branch_external_id):
            captured_branches.append((branch_hint, branch_external_id))
            return rows[branch_hint]

        capture = SimpleNamespace(capture_all_rows=capture_all_rows)
        identity = SimpleNamespace(
            resolve_insumo=lambda **kwargs: SimpleNamespace(
                insumo=insumo,
                method="POINT_CODE",
                score=100,
            )
        )

        result = CanonicalInsumoInventoryCaptureService(
            capture_service=capture,
            identity_service=identity,
        ).capture(sync_job=job)

        self.assertTrue(result["complete"])
        self.assertEqual(PointInsumoInventorySnapshot.objects.count(), 2)
        self.assertEqual(captured_branches, [("ALMACEN", "9"), ("CEDIS", "8")])
        almacen = PointInsumoInventorySnapshot.objects.get(branch__normalized_name="almacen")
        cedis = PointInsumoInventorySnapshot.objects.get(branch__normalized_name="cedis")
        self.assertEqual(almacen.quantity_base, Decimal("53470.999000"))
        self.assertEqual(cedis.quantity_base, Decimal("-14140.000000"))

    def test_unmatched_row_does_not_invalidate_confirmed_inventory(self):
        unit = UnidadMedida.objects.create(
            codigo="pza", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA, factor_to_base=1
        )
        insumo = Insumo.objects.create(
            codigo_point="OK-01", nombre="Insumo confirmado", unidad_base=unit
        )
        PointBranch.objects.create(external_id="9", name="Almacen")
        PointBranch.objects.create(external_id="8", name="CEDIS")
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_RUNNING,
            parameters={"canonical_insumo_inventory": True},
        )
        valid = SimpleNamespace(**{**vars(self._row("3", "pza")), "point_code": "OK-01"})
        unmatched = SimpleNamespace(**{**vars(self._row("4", "pza")), "point_code": "UNKNOWN-01"})
        capture = SimpleNamespace(
            capture_all_rows=lambda **kwargs: [valid, unmatched]
        )

        def resolve_insumo(*, point_code, point_name):
            if point_code == "OK-01":
                return SimpleNamespace(insumo=insumo, method="POINT_CODE", score=100)
            return SimpleNamespace(insumo=None, method="NO_MATCH", score=0)

        result = CanonicalInsumoInventoryCaptureService(
            capture_service=capture,
            identity_service=SimpleNamespace(resolve_insumo=resolve_insumo),
        ).capture(sync_job=job)

        self.assertTrue(result["complete"])
        self.assertEqual(PointInsumoInventorySnapshot.objects.count(), 2)
        self.assertEqual(
            [item["reason"] for item in result["blockers"]],
            ["MATCH_NO_CONFIABLE", "MATCH_NO_CONFIABLE"],
        )

    def test_conflicting_duplicate_removes_that_insumo_from_cycle(self):
        unit = UnidadMedida.objects.create(
            codigo="pza-conflict", nombre="Pieza conflicto", tipo=UnidadMedida.TIPO_PIEZA, factor_to_base=1
        )
        insumo = Insumo.objects.create(
            codigo_point="DUP-01", nombre="Insumo duplicado", unidad_base=unit
        )
        PointBranch.objects.create(external_id="9", name="Almacen")
        PointBranch.objects.create(external_id="8", name="CEDIS")
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_RUNNING,
            parameters={"canonical_insumo_inventory": True},
        )
        first = SimpleNamespace(**{**vars(self._row("3", "pza")), "point_code": "DUP-01"})
        second = SimpleNamespace(**{**vars(self._row("4", "pza")), "point_code": "DUP-01"})
        capture = SimpleNamespace(capture_all_rows=lambda **kwargs: [first, second])
        identity = SimpleNamespace(
            resolve_insumo=lambda **kwargs: SimpleNamespace(insumo=insumo, method="POINT_CODE", score=100)
        )

        result = CanonicalInsumoInventoryCaptureService(
            capture_service=capture,
            identity_service=identity,
        ).capture(sync_job=job)

        self.assertFalse(result["complete"])
        self.assertFalse(PointInsumoInventorySnapshot.objects.exists())
        self.assertEqual(
            [item["reason"] for item in result["blockers"]],
            [
                "SALDO_DUPLICADO_CONFLICTIVO",
                "SIN_INSUMOS_CONFIRMADOS",
                "SALDO_DUPLICADO_CONFLICTIVO",
                "SIN_INSUMOS_CONFIRMADOS",
            ],
        )

    @staticmethod
    def _row(quantity, unit):
        return SimpleNamespace(
            kind="supply",
            point_code="017",
            point_name="Fresa Fresca",
            point_category="FRUTAS",
            quantity=Decimal(quantity),
            unit=unit,
            branch_name="",
            raw_row=["17", "017", "Fresa Fresca", "FRUTAS", quantity, unit, "0", "0"],
        )
