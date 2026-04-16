from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import SimpleNamespace

from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import (
    PointBranch,
    PointSalesDailyCategoryFact,
    PointSalesDailyProductFact,
    PointSalesExtractionTask,
    PointSalesNormalized,
    PointSalesQualityAlert,
    PointSalesRawStaging,
    PointSyncJob,
)
from pos_bridge.services.sales_pipeline import PointSalesRebuildService, PointSalesTaskQueueService
from recetas.models import Receta
from ventas.models import VentaAutoritativaPoint


class PointSalesTaskQueueServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MAT", nombre="MATRIZ")
        self.branch = PointBranch.objects.create(external_id="1", name="MATRIZ", erp_branch=self.sucursal)
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
            parameters={"pipeline_code": PointSalesTaskQueueService.PIPELINE_CODE},
        )
        self.service = PointSalesTaskQueueService()

    def test_plan_and_claim_tasks(self):
        planned = self.service.plan_tasks(
            sync_job=self.sync_job,
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 2),
            credito_scope="null",
        )

        self.assertEqual(planned, 2)
        tasks = self.service.claim_tasks(sync_job=self.sync_job, worker_name="worker-a", limit=1)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].status, PointSalesExtractionTask.STATUS_RUNNING)
        self.assertEqual(tasks[0].worker_name, "worker-a")


class PointSalesRebuildServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MAT", nombre="MATRIZ")
        self.branch = PointBranch.objects.create(external_id="1", name="MATRIZ", erp_branch=self.sucursal)
        self.receta = Receta.objects.create(
            nombre="Pastel de Prueba",
            codigo_point="0108",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia="Pasteles",
            categoria="Pasteles",
            temporalidad=Receta.TEMPORALIDAD_PERMANENTE,
            hash_contenido="hash-test-sales-pipeline-0108",
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
            parameters={"pipeline_code": PointSalesTaskQueueService.PIPELINE_CODE},
        )
        self.task = PointSalesExtractionTask.objects.create(
            sync_job=self.sync_job,
            branch=self.branch,
            sale_date=date(2026, 4, 4),
            credito_scope="null",
            status=PointSalesExtractionTask.STATUS_PENDING,
        )

    def _build_service(self):
        temp = NamedTemporaryFile(delete=False, suffix=".xls")
        Path(temp.name).write_bytes(b"point report bytes")
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: SimpleNamespace(session=SimpleNamespace(close=lambda: None))),
            fetch_report_with_session=lambda **kwargs: SimpleNamespace(report_path=temp.name, request_url="https://example.test/report"),
            parse_report=lambda **kwargs: SimpleNamespace(
                rows=[
                    {
                        "Categoria": "Pasteles",
                        "Codigo": "0108",
                        "Nombre": "Pastel de Prueba",
                        "Cantidad": "2",
                        "Bruto": "120",
                        "Descuento": "20",
                        "Venta": "100",
                        "IVA": "8",
                        "Venta_neta": "92",
                    }
                ],
                summary={"venta_neta": "92"},
            ),
        )
        return PointSalesRebuildService(report_service=report_service), temp.name

    def test_process_task_builds_staging_facts_and_authoritative_rows(self):
        service, temp_path = self._build_service()
        try:
            self.task.mark_running(worker_name="worker-a")
            self.task.attempts = 1
            self.task.save(update_fields=["status", "worker_name", "claimed_at", "started_at", "attempts"])

            summary = service.process_task(task=self.task, session_cache={}, promote_authoritative=True)

            self.assertEqual(summary["raw_rows"], 1)
            self.assertEqual(PointSalesRawStaging.objects.filter(task=self.task).count(), 1)
            self.assertEqual(PointSalesNormalized.objects.filter(task=self.task).count(), 1)
            self.assertEqual(PointSalesDailyCategoryFact.objects.filter(branch=self.branch, sale_date=self.task.sale_date).count(), 1)
            self.assertEqual(PointSalesDailyProductFact.objects.filter(branch=self.branch, sale_date=self.task.sale_date).count(), 1)
            self.assertEqual(VentaAutoritativaPoint.objects.filter(branch=self.sucursal, sale_date=self.task.sale_date).count(), 1)
            normalized = PointSalesNormalized.objects.get(task=self.task)
            self.assertEqual(normalized.receta_id, self.receta.id)
            self.assertEqual(normalized.match_catalogo_status, PointSalesNormalized.MATCH_EXACT_CODE)
            self.task.refresh_from_db()
            self.assertEqual(self.task.status, PointSalesExtractionTask.STATUS_SUCCESS)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_finalize_job_creates_quality_alert_when_unmatched_rows_remain(self):
        temp = NamedTemporaryFile(delete=False, suffix=".xls")
        Path(temp.name).write_bytes(b"point report bytes")
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: SimpleNamespace(session=SimpleNamespace(close=lambda: None))),
            fetch_report_with_session=lambda **kwargs: SimpleNamespace(report_path=temp.name, request_url="https://example.test/report"),
            parse_report=lambda **kwargs: SimpleNamespace(
                rows=[
                    {
                        "Categoria": "Café",
                        "Codigo": "0-9",
                        "Nombre": "Americano",
                        "Cantidad": "1",
                        "Bruto": "25",
                        "Descuento": "0",
                        "Venta": "25",
                        "IVA": "3.45",
                        "Venta_neta": "21.55",
                    }
                ],
                summary={"venta_neta": "21.55"},
            ),
        )
        service = PointSalesRebuildService(report_service=report_service)
        try:
            self.task.mark_running(worker_name="worker-a")
            self.task.attempts = 1
            self.task.save(update_fields=["status", "worker_name", "claimed_at", "started_at", "attempts"])

            service.process_task(task=self.task, session_cache={}, promote_authoritative=True)
            finalized = service.finalize_job_if_complete(sync_job=self.sync_job)

            self.assertEqual(finalized.status, PointSyncJob.STATUS_SUCCESS)
            alert = PointSalesQualityAlert.objects.get(sync_job=self.sync_job, alert_type="UNMATCHED_CATALOG_ROWS")
            self.assertEqual(alert.severity, PointSalesQualityAlert.SEVERITY_CRITICAL)
            self.assertIn("Americano", str(alert.payload_json))
        finally:
            Path(temp.name).unlink(missing_ok=True)
