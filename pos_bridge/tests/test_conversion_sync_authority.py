from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointConversionLine, PointSyncJob
from pos_bridge.services.conversion_sync_service import sync_conversion_lines
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

    def _sync(self):
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
            patch("pos_bridge.services.conversion_sync_service._resolve_branch", return_value=self.branch),
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
            return sync_conversion_lines(date_from=date(2026, 7, 1), date_to=date(2026, 7, 31))

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
        self.assertEqual(metadata["selected_sync_job_ids"], (failed_job.id,))
        self.assertIn("CONVERSION_SYNC_JOB_FAILED", metadata["authority_issues"])
        self.assertIn("CONVERSION_SYNC_JOB_MIXED", metadata["authority_issues"])
