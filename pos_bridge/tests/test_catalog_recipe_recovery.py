from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from django.test import TestCase, RequestFactory
from django.contrib.auth import get_user_model
from django.utils import timezone
from maestros.models import Insumo, UnidadMedida, CostoInsumo
from recetas.models import Receta, LineaReceta
from pos_bridge.models import PointSyncJob, PointRecipeExtractionRun, PointRecipeNode
from pos_bridge.tasks.celery_tasks import task_catalog_recipe_sync
from recetas.views.recetas import _queue_catalog_recipe_sync


class CatalogRecoveryTests(TestCase):
    def setUp(self):
        self.unit = UnidadMedida.objects.get_or_create(
            codigo="g",
            defaults={"nombre": "Gramo", "tipo": "MASS", "factor_to_base": 1},
        )[0]
        self.insumo = Insumo.objects.create(
            nombre="GRAGEA DOT CAKE", unidad_base=self.unit
        )
        self.recipe = Receta.objects.create(nombre="Dot Cake", codigo_point="4358")
        LineaReceta.objects.create(
            receta=self.recipe, insumo=self.insumo, cantidad=8, unidad=self.unit
        )
        self.user = get_user_model().objects.create_user(username="costs")

    def fake_sync(self, **kwargs):
        run = PointRecipeExtractionRun.objects.create(sync_job=kwargs["sync_job"])
        PointRecipeNode.objects.create(
            run=run,
            identity_key="4358",
            point_name="Dot Cake",
            point_code="4358",
            erp_recipe=self.recipe,
        )
        return SimpleNamespace(
            summary={
                "products_selected": 1,
                "recipes_completed_successfully": 1,
                "run_id": run.id,
            },
            raw_export_path="",
        )

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_missing_cost_must_not_report_success(self, svc):
        svc.return_value.sync.side_effect = self.fake_sync
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            parameters={"action": "SYNC_ALL_RECIPES"},
        )
        # Do not contact Point in this regression; cost retrieval gets no purchases.
        with (
            patch("pos_bridge.services.point_http_client.PointHttpSessionClient.login"),
            patch(
                "pos_bridge.services.point_http_client.PointHttpSessionClient._catalog_rows",
                return_value=[],
            ),
        ):
            task_catalog_recipe_sync(job_id=job.id)
        job.refresh_from_db()
        self.assertEqual(job.status, "PARTIAL")
        self.assertEqual(job.result_summary["cost_validation"]["missing_count"], 1)

    @patch("recetas.views.recetas.task_catalog_recipe_sync.apply_async")
    def test_abandoned_pending_job_does_not_block_new_click(self, delay):
        old = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS"},
        )
        PointSyncJob.objects.filter(id=old.id).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )
        req = RequestFactory().post("/")
        req.user = self.user
        req.session = {}
        new = _queue_catalog_recipe_sync(req, action_label="SYNC_ONLY_NEW_PRODUCTS")
        self.assertNotEqual(old.id, new.id)
        old.refresh_from_db()
        self.assertEqual(old.status, "FAILED")

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_cancelled_queued_delivery_cannot_restart(self, svc):
        svc.return_value.discover_new_product_codes.return_value = {"new_codes": []}
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status="FAILED",
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS"},
        )
        task_catalog_recipe_sync(job_id=job.id)
        svc.assert_not_called()

    @patch(
        "pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot",
        return_value={"total_items": 1},
    )
    @patch("pos_bridge.services.point_http_client.PointHttpSessionClient.login")
    @patch("pos_bridge.services.point_http_client.PointHttpSessionClient._catalog_rows")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_purchase_cost_conversion_scoped_snapshot_and_idempotency(
        self, svc, rows, login, snapshot
    ):
        UnidadMedida.objects.get_or_create(
            codigo="kg",
            defaults={"nombre": "Kilogramo", "tipo": "MASS", "factor_to_base": 1000},
        )
        svc.return_value.sync.side_effect = self.fake_sync
        today = timezone.localdate().isoformat()
        rows.side_effect = [
            [
                {
                    "FK_Movimiento": 123,
                    "Fecha_compra": today,
                    "Folio": "G-1",
                    "Proveedor": "Proveedor",
                    "Sucursal": "Almacen",
                }
            ],
            [
                {
                    "Articulo": self.insumo.nombre,
                    "Cantidad": 1.0,
                    "Unidad": "KG",
                    "Costo_unitario": 44.0,
                    "Costo_total": 44.0,
                }
            ],
        ]
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            parameters={"action": "SYNC_ALL_RECIPES"},
        )
        task_catalog_recipe_sync(job_id=job.id)
        job.refresh_from_db()
        self.assertEqual(job.status, "SUCCESS")
        from recetas.utils.costeo_snapshot import resolve_line_snapshot_cost

        line = LineaReceta.objects.get(receta=self.recipe)
        cost, source = resolve_line_snapshot_cost(line)
        self.assertEqual(cost * line.cantidad, Decimal("0.352"))
        snapshot.assert_called_once_with(
            receta_ids=[self.recipe.id], include_addons=False, triggered_by=None
        )
        task_catalog_recipe_sync(job_id=job.id)
        self.assertEqual(CostoInsumo.objects.filter(insumo=self.insumo).count(), 1)

    @patch("recetas.views.recetas.task_catalog_recipe_sync.apply_async")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_retry_includes_already_created_recipe(self, svc, delay):
        PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status="PARTIAL",
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS", "resume_codes": ["4358"]},
        )
        req = RequestFactory().post("/")
        req.user = self.user
        req.session = {}
        job = _queue_catalog_recipe_sync(req, action_label="SYNC_ONLY_NEW_PRODUCTS")
        svc.return_value.discover_new_product_codes.return_value = {"new_codes": []}
        svc.return_value.sync.side_effect = self.fake_sync
        with (
            patch("pos_bridge.services.point_http_client.PointHttpSessionClient.login"),
            patch(
                "pos_bridge.services.point_http_client.PointHttpSessionClient._catalog_rows",
                return_value=[],
            ),
        ):
            task_catalog_recipe_sync(job_id=job.id)
        self.assertEqual(
            svc.return_value.sync.call_args.kwargs["product_codes"], ["4358"]
        )
        job.refresh_from_db()
        self.assertEqual(job.status, "PARTIAL")

    def test_partial_preparation_does_not_hide_missing_child_cost(self):
        from pos_bridge.services.catalog_recipe_costs import validate_recipe_costs

        prep = Receta.objects.create(
            nombre="Preparacion",
            codigo_point="PREP",
            hash_contenido="prep",
            tipo=Receta.TIPO_PREPARACION,
        )
        prep_input = Insumo.objects.create(
            nombre="Preparacion", codigo_point="PREP", unidad_base=self.unit
        )
        LineaReceta.objects.create(
            receta=prep, insumo=self.insumo, cantidad=1, unidad=self.unit
        )
        LineaReceta.objects.create(
            receta=self.recipe, insumo=prep_input, cantidad=1, unidad=self.unit
        )
        check = validate_recipe_costs([self.recipe.id])
        self.assertIn(prep.id, check["recipe_ids"])
        self.assertGreater(check["missing_count"], 0)

    def test_expired_deadline_does_not_start_http_request(self):
        import time
        from pos_bridge.services.catalog_recipe_execution import DEADLINE
        from pos_bridge.services.point_http_client import PointHttpSessionClient

        client = PointHttpSessionClient(
            SimpleNamespace(timeout_ms=30000, retry_attempts=2)
        )
        token = DEADLINE.set(time.monotonic() - 1)
        try:
            with patch.object(client.session, "request") as request:
                with self.assertRaises(TimeoutError):
                    client._request("GET", "/test")
                request.assert_not_called()
        finally:
            DEADLINE.reset(token)
            client.close()

    @patch("pos_bridge.services.point_http_client.PointHttpSessionClient.login")
    @patch("pos_bridge.services.point_http_client.PointHttpSessionClient._catalog_rows")
    def test_unknown_purchase_unit_is_not_guessed(self, rows, login):
        from pos_bridge.services.catalog_recipe_costs import complete_recipe_costs

        rows.side_effect = [
            [{"FK_Movimiento": 1, "Fecha_compra": timezone.localdate().isoformat()}],
            [
                {
                    "Articulo": self.insumo.nombre,
                    "Cantidad": 1,
                    "Unidad": "BULTO DESCONOCIDO",
                    "Costo_unitario": 44,
                }
            ],
        ]
        job = PointSyncJob.objects.create()
        result = complete_recipe_costs(recipe_ids=[self.recipe.id], job=job)
        self.assertEqual(result["missing_count"], 1)
        self.assertFalse(CostoInsumo.objects.filter(insumo=self.insumo).exists())

    @patch(
        "pos_bridge.services.point_http_client.PointHttpSessionClient.login",
        side_effect=RuntimeError("Point no disponible"),
    )
    def test_point_failure_leaves_explicit_pending_cost(self, login):
        from pos_bridge.services.catalog_recipe_costs import complete_recipe_costs

        result = complete_recipe_costs(
            recipe_ids=[self.recipe.id], job=PointSyncJob.objects.create()
        )
        self.assertEqual(result["missing_count"], 1)
        self.assertIn("Point no disponible", result["search_error"])


from django.test import TransactionTestCase
from django.db import connections
import threading


class CatalogExecutorOwnershipTests(TransactionTestCase):
    def test_live_executor_is_not_reclaimed(self):
        from pos_bridge.services.catalog_recipe_execution import (
            execution_lock,
            recover_abandoned_catalog_jobs,
        )

        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status="RUNNING",
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS"},
        )
        PointSyncJob.objects.filter(id=job.id).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )
        ready = threading.Event()
        release = threading.Event()
        errors = []

        def worker():
            try:
                with execution_lock(job.id) as acquired:
                    if not acquired:
                        raise AssertionError("lock not acquired")
                    ready.set()
                    release.wait(10)
            except Exception as exc:
                errors.append(exc)
                ready.set()
            finally:
                connections.close_all()

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            self.assertTrue(ready.wait(5))
            self.assertFalse(errors)
            recover_abandoned_catalog_jobs()
            job.refresh_from_db()
            self.assertEqual(job.status, "RUNNING")
        finally:
            release.set()
            thread.join(10)
        recover_abandoned_catalog_jobs()
        job.refresh_from_db()
        self.assertEqual(job.status, "FAILED")

    def test_recovery_does_not_overwrite_a_job_that_just_finished(self):
        from contextlib import contextmanager
        from pos_bridge.services.catalog_recipe_execution import (
            recover_abandoned_catalog_jobs,
        )

        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status="RUNNING",
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS"},
        )
        PointSyncJob.objects.filter(id=job.id).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        @contextmanager
        def completed_before_lock(job_id):
            PointSyncJob.objects.filter(id=job_id).update(
                status="SUCCESS", updated_at=timezone.now()
            )
            yield True

        with patch(
            "pos_bridge.services.catalog_recipe_execution.execution_lock",
            completed_before_lock,
        ):
            recover_abandoned_catalog_jobs()
        job.refresh_from_db()
        self.assertEqual(job.status, "SUCCESS")
