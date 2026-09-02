from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointConversionLine, PointSyncJob
from pos_bridge.services.conversion_sync_service import (
    _created_report_pk,
    _normalize_inventory_report_rows,
    _poll_report,
    sync_conversion_lines,
)
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from recetas.models import Receta


class PointConversionRerunAuthorityTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="CONV-RERUN", nombre="Conversión rerun", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="CONV-RERUN",
            name="Conversión rerun",
            erp_branch=self.sucursal,
        )
        self.recipe = Receta.objects.create(
            nombre="Pastel conversión rerun",
            codigo_point="CONV-RERUN-001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-conversion-rerun",
        )
        self.report_rows = [
            {
                "PK_Movimiento": "MOV-RERUN-001",
                "Fecha": "2026-07-15 10:00:00",
                "Producto": self.recipe.nombre,
                "Codigo": self.recipe.codigo_point,
                "Cantidad": "4",
                "ProductoOrigen": self.recipe.nombre,
                "CodigoOrigen": self.recipe.codigo_point,
            }
        ]

    def _sync(self, *, branch_filter=None, resolve_branch=None):
        client_context = MagicMock()
        client_context.return_value.__enter__.return_value = MagicMock()
        with (
            patch("pos_bridge.services.conversion_sync_service.PointHttpSessionClient", client_context),
            patch(
                "pos_bridge.services.conversion_sync_service._create_report",
                return_value=SimpleNamespace(text="ok"),
            ),
            patch(
                "pos_bridge.services.conversion_sync_service._poll_report",
                return_value={"PK_Reporte": "REPORT-RERUN"},
            ),
            patch("pos_bridge.services.conversion_sync_service._download_report", return_value=b"report"),
            patch("pos_bridge.services.conversion_sync_service._read_report_rows", return_value=self.report_rows),
            patch("pos_bridge.services.conversion_sync_service._build_branch_map", return_value={}),
            patch("pos_bridge.services.conversion_sync_service._build_recipe_map", return_value={}),
            patch(
                "pos_bridge.services.conversion_sync_service._resolve_branch",
                side_effect=resolve_branch or (lambda row, branch_map: self.branch),
            ),
            patch("pos_bridge.services.conversion_sync_service._resolve_recipe", return_value=self.recipe),
            patch(
                "pos_bridge.services.conversion_sync_service._make_hash",
                side_effect=lambda row: (
                    "conversion-rerun-hash"
                    if row["PK_Movimiento"] == "MOV-RERUN-001"
                    else f"conversion-rerun-hash-{row['PK_Movimiento']}"
                ),
            ),
        ):
            return sync_conversion_lines(
                date_from=date(2026, 7, 1),
                date_to=date(2026, 7, 31),
                branch_filter=branch_filter,
            )

    def _conversion_authority(self):
        service = MonthlyPointProductBalanceService()
        service._build_conversion_cache = {}
        _rows, _unresolved, _movements, _counts, metadata = service._load_conversions(month_start=date(2026, 7, 1))
        return metadata

    def test_complete_duplicate_rerun_relinks_observed_rows_and_remains_authoritative(self):
        first = self._sync()
        first_job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_INVENTORY)

        self.assertEqual(first["created"], 1)
        self.assertEqual(first["skipped"], 0)
        self.assertEqual(first["relinked"], 0)
        first_authority = self._conversion_authority()
        self.assertTrue(first_authority["authoritative"])
        self.assertEqual(first_authority["selected_sync_job_ids"], (first_job.id,))

        second = self._sync()
        second_job = PointSyncJob.objects.order_by("-id").first()
        line = PointConversionLine.objects.get(source_hash="conversion-rerun-hash")

        self.assertEqual(second["created"], 0)
        self.assertEqual(second["skipped"], 1)
        self.assertEqual(second["relinked"], 1)
        self.assertEqual(line.sync_job_id, second_job.id)
        second_authority = self._conversion_authority()
        self.assertTrue(second_authority["authoritative"])
        self.assertEqual(second_authority["selected_sync_job_ids"], (second_job.id,))
        self.assertNotIn("CONVERSION_SYNC_JOB_MIXED", second_authority["authority_issues"])

    def test_unreconciled_duplicate_skip_does_not_become_false_authority(self):
        self._sync()
        latest = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            parameters={
                "source": "point_conversion_lines",
                "date_from": "2026-07-01",
                "date_to": "2026-07-31",
                "branch_filter": "",
            },
            result_summary={
                "created": 0,
                "skipped": 1,
                "skipped_unmatched_branch": 0,
                "relinked": 0,
                "total_rows": 1,
                "report_pk": "REPORT-UNRECONCILED",
            },
        )

        metadata = self._conversion_authority()

        self.assertEqual(metadata["selected_sync_job_ids"], (latest.id,))
        self.assertFalse(metadata["authoritative"])
        self.assertIn("CONVERSION_SYNC_COUNT_MISMATCH", metadata["authority_issues"])

    def test_filtered_retry_does_not_rebind_full_month_rows_or_replace_its_authority(self):
        self._sync()
        full_job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_INVENTORY)

        filtered = self._sync(branch_filter=self.branch.external_id)
        filtered_job = PointSyncJob.objects.order_by("-id").first()
        line = PointConversionLine.objects.get(source_hash="conversion-rerun-hash")
        metadata = self._conversion_authority()

        self.assertEqual(filtered["created"], 0)
        self.assertEqual(filtered["relinked"], 0)
        self.assertEqual(line.sync_job_id, full_job.id)
        self.assertNotEqual(filtered_job.id, full_job.id)
        self.assertTrue(metadata["authoritative"])
        self.assertEqual(metadata["selected_sync_job_ids"], (full_job.id,))
        self.assertEqual(metadata["coverage_scope"], "all_branches")

    def test_filtered_rows_newer_than_full_month_are_preserved_and_block_authority(self):
        self._sync()
        full_job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_INVENTORY)
        self.report_rows = [
            {
                **self.report_rows[0],
                "PK_Movimiento": "MOV-RERUN-FILTERED-ONLY",
                "Cantidad": "9",
            }
        ]

        self._sync(branch_filter=self.branch.external_id)
        filtered_job = PointSyncJob.objects.order_by("-id").first()
        service = MonthlyPointProductBalanceService()
        service._build_conversion_cache = {}
        rows, _unresolved, _movements, _counts, metadata = service._load_conversions(
            month_start=date(2026, 7, 1)
        )

        self.assertFalse(metadata["authoritative"])
        self.assertEqual(metadata["selected_sync_job_ids"], (full_job.id,))
        self.assertIn(filtered_job.id, metadata["restricted_row_sync_job_ids"])
        self.assertIn("CONVERSION_FILTERED_NEW_ROWS", metadata["authority_issues"])
        self.assertEqual(rows[self.recipe.id].conversion_in, 13)

    def test_invalid_quantity_and_date_are_omitted_and_make_job_partial(self):
        self.report_rows = [
            {**self.report_rows[0], "PK_Movimiento": "BAD-QTY", "Cantidad": "no-es-numero"},
            {**self.report_rows[0], "PK_Movimiento": "BAD-DATE", "Fecha": "fecha-imposible"},
        ]

        result = self._sync()
        job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_INVENTORY)

        self.assertEqual(PointConversionLine.objects.count(), 0)
        self.assertEqual(result["invalid_rows"], 2)
        self.assertEqual(result["invalid_quantity_rows"], 1)
        self.assertEqual(result["invalid_date_rows"], 1)
        self.assertEqual(
            result["issues"],
            ["CONVERSION_INVALID_QUANTITY", "CONVERSION_INVALID_DATE"],
        )
        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)
        self.assertFalse(self._conversion_authority()["authoritative"])

    def test_report_normalization_preserves_blank_quantity_for_validation(self):
        records = [
            {"A": "SUCURSAL", "B": "PRODUCTO", "C": "CANTIDAD", "D": "FECHA"},
            {"A": self.branch.name, "B": self.recipe.nombre, "C": None, "D": "2026-07-15"},
        ]

        rows = _normalize_inventory_report_rows(records)

        self.assertEqual(len(rows), 1)
        self.assertIn("CANTIDAD", rows[0])
        self.assertIsNone(rows[0]["CANTIDAD"])

    def test_report_normalization_preserves_rows_missing_branch_or_product_for_accounting(self):
        records = [
            {"A": "SUCURSAL", "B": "PRODUCTO", "C": "CANTIDAD", "D": "FECHA", "E": "PK_MOVIMIENTO"},
            {"A": None, "B": self.recipe.nombre, "C": "1", "D": "2026-07-15", "E": "NO-BRANCH"},
            {"A": self.branch.name, "B": None, "C": "2", "D": "2026-07-16", "E": "NO-PRODUCT"},
            {"A": None, "B": None, "C": "3", "D": "2026-07-17", "E": None},
        ]

        rows = _normalize_inventory_report_rows(records)

        self.assertEqual(len(rows), 3)

    def test_missing_branch_product_or_movement_id_rows_are_counted_and_make_job_partial(self):
        self.report_rows = [
            {**self.report_rows[0], "PK_Movimiento": "NO-BRANCH", "Sucursal": ""},
            {
                **self.report_rows[0],
                "PK_Movimiento": "NO-PRODUCT",
                "Sucursal": self.branch.name,
                "Producto": "",
                "Codigo": "",
            },
            {
                **self.report_rows[0],
                "PK_Movimiento": "NO-BRANCH-PRODUCT",
                "Sucursal": "",
                "Producto": "",
                "Codigo": "",
            },
            {
                **self.report_rows[0],
                "PK_Movimiento": "",
                "Sucursal": self.branch.name,
            },
        ]

        result = self._sync(
            resolve_branch=lambda row, branch_map: (
                None if not row.get("Sucursal") else self.branch
            )
        )
        job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_INVENTORY)

        self.assertEqual(result["total_rows"], 4)
        self.assertEqual(result["skipped_unmatched_branch"], 2)
        self.assertEqual(result["invalid_identity_rows"], 2)
        self.assertEqual(result["invalid_rows"], 2)
        self.assertEqual(PointConversionLine.objects.count(), 0)
        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)
        self.assertFalse(self._conversion_authority()["authoritative"])

    def test_poll_report_never_falls_back_to_historical_generic_report(self):
        created_after = timezone.make_aware(datetime(2026, 7, 1, 12, 0, 0))
        response = MagicMock()
        response.json.return_value = [
            {
                "PK_Reporte": "STALE",
                "Nombre_reporte": "MOVIMIENTOS DE INVENTARIOS",
                "Modulo": "movimientos",
                "Status": 1,
                "Fecha_creacion": "2026-06-30 10:00:00",
            }
        ]
        client = MagicMock()
        client._request.return_value = response

        with (
            patch("pos_bridge.services.conversion_sync_service.POLL_MAX_ATTEMPTS", 1),
            patch("pos_bridge.services.conversion_sync_service.POLL_INTERVAL_SECONDS", 0),
            self.assertRaises(TimeoutError),
        ):
            _poll_report(client, created_after=created_after)

    def test_poll_report_accepts_the_exact_report_id_returned_by_creation(self):
        created_after = timezone.make_aware(datetime(2026, 7, 1, 12, 0, 0))
        requested = {
            "PK_Reporte": "REQUESTED",
            "Nombre_reporte": "MOVIMIENTOS DE INVENTARIOS",
            "Modulo": "movimientos",
            "Status": 1,
            "Fecha_creacion": "2026-06-30 10:00:00",
        }
        response = MagicMock()
        response.json.return_value = [
            {**requested, "PK_Reporte": "OTHER"},
            requested,
        ]
        client = MagicMock()
        client._request.return_value = response

        with (
            patch("pos_bridge.services.conversion_sync_service.POLL_MAX_ATTEMPTS", 1),
            patch("pos_bridge.services.conversion_sync_service.POLL_INTERVAL_SECONDS", 0),
        ):
            selected = _poll_report(
                client,
                created_after=created_after,
                expected_report_pk="REQUESTED",
            )

        self.assertEqual(selected["PK_Reporte"], "REQUESTED")

    def test_create_report_pk_accepts_scalar_json_response(self):
        response = MagicMock()
        response.json.return_value = "REQUESTED"

        self.assertEqual(_created_report_pk(response), "REQUESTED")

    def test_create_report_pk_rejects_boolean_identifiers(self):
        scalar = MagicMock()
        scalar.json.return_value = True
        mapping = MagicMock()
        mapping.json.return_value = {"PK_Reporte": True}

        self.assertEqual(_created_report_pk(scalar), "")
        self.assertEqual(_created_report_pk(mapping), "")

    def test_failed_success_transition_rolls_back_relinks_and_new_rows(self):
        first_result = self._sync()
        first_job = PointSyncJob.objects.get(job_type=PointSyncJob.JOB_TYPE_INVENTORY)
        self.report_rows.append(
            {
                **self.report_rows[0],
                "PK_Movimiento": "MOV-RERUN-NEW",
                "Codigo": "CONV-RERUN-NEW",
            }
        )
        original_save = PointSyncJob.save

        def fail_success_save(instance, *args, **kwargs):
            if instance.status == PointSyncJob.STATUS_SUCCESS:
                raise RuntimeError("fallo inyectado al confirmar SUCCESS")
            return original_save(instance, *args, **kwargs)

        with patch.object(PointSyncJob, "save", new=fail_success_save):
            with self.assertRaisesRegex(RuntimeError, "fallo inyectado"):
                self._sync()

        first_job.refresh_from_db()
        original_line = PointConversionLine.objects.get(source_hash="conversion-rerun-hash")
        failed_job = PointSyncJob.objects.exclude(pk=first_job.pk).get()

        self.assertEqual(first_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(first_job.result_summary, first_result)
        self.assertEqual(original_line.sync_job_id, first_job.id)
        self.assertFalse(
            PointConversionLine.objects.filter(source_hash="conversion-rerun-hash-MOV-RERUN-NEW").exists()
        )
        self.assertEqual(PointConversionLine.objects.count(), 1)
        self.assertEqual(failed_job.status, PointSyncJob.STATUS_FAILED)
        self.assertEqual(failed_job.result_summary, {})
        self.assertFalse(PointConversionLine.objects.filter(sync_job=failed_job).exists())

        metadata = self._conversion_authority()
        self.assertEqual(metadata["selected_sync_job_ids"], (first_job.id,))
        self.assertTrue(metadata["authoritative"])
        self.assertNotIn("CONVERSION_SYNC_JOB_FAILED", metadata["authority_issues"])
        self.assertNotIn("CONVERSION_SYNC_JOB_MIXED", metadata["authority_issues"])
