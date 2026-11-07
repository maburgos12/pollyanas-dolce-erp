from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from pos_bridge.models import PointSyncJob
from pos_bridge.tasks.celery_tasks import task_catalog_recipe_sync


class CatalogRecipeSyncTaskTests(TestCase):
    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_incomplete_recipes_are_partial_not_success(self, service_cls, snapshot):
        job = PointSyncJob.objects.create(parameters={"action": "SYNC_ALL_RECIPES"})
        service_cls.return_value.sync.return_value = SimpleNamespace(
            summary={"products_selected": 2, "recipes_completed_successfully": 1, "recipes_with_unresolved_inputs": 1},
            raw_export_path="/tmp/recipes.json",
        )
        snapshot.return_value = {}
        task_catalog_recipe_sync(job_id=job.id)
        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)

    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="catalog_recipe_sync",
            password="test12345",
        )

    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_full_sync_updates_precreated_job_and_snapshot(self, service_cls, weekly_snapshot):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ALL_RECIPES", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        from recetas.models import Receta, LineaReceta
        from maestros.models import Insumo, UnidadMedida, CostoInsumo
        from pos_bridge.models import PointRecipeExtractionRun, PointRecipeNode
        unit = UnidadMedida.objects.get_or_create(codigo="pza", defaults={"nombre":"Pieza"})[0]
        recipe = Receta.objects.create(nombre="Costed recipe", hash_contenido="costed-test")
        insumo = Insumo.objects.create(nombre="Costed input", unidad_base=unit)
        CostoInsumo.objects.create(insumo=insumo, fecha="2026-09-05", costo_unitario=1)
        LineaReceta.objects.create(receta=recipe, insumo=insumo, cantidad=1, unidad=unit)
        def sync_with_progress(**kwargs):
            run = PointRecipeExtractionRun.objects.create(sync_job=job)
            PointRecipeNode.objects.create(run=run, identity_key="test", point_name="Costed recipe", erp_recipe=recipe)
            job.refresh_from_db()
            self.assertEqual(job.parameters["progress"]["stage"], "IMPORTING")
            self.assertIsNotNone(job.started_at)
            return SimpleNamespace(
                summary={"products_selected": 12, "recipes_completed_successfully": 11},
                raw_export_path="/tmp/point-recipes.json",
            )

        def snapshot_with_progress(**kwargs):
            job.refresh_from_db()
            self.assertEqual(job.parameters["progress"]["stage"], "COSTING")
            return {"week_start": "2026-07-27", "total_items": 12}

        service_cls.return_value.sync.side_effect = sync_with_progress
        weekly_snapshot.side_effect = snapshot_with_progress

        payload = task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(job.result_summary["products_selected"], 12)
        self.assertEqual(job.result_summary["weekly_cost_snapshot"]["total_items"], 12)
        self.assertEqual(job.artifacts["raw_export_path"], "/tmp/point-recipes.json")
        self.assertEqual(job.parameters["progress"]["stage"], "COMPLETED")
        service_cls.return_value.sync.assert_called_once_with(
            branch_hint="MATRIZ",
            product_codes=None,
            include_without_recipe=False,
            sync_job=job,
        )
        self.assertEqual(payload["job_id"], job.id)
        self.assertEqual(payload["status"], PointSyncJob.STATUS_SUCCESS)

    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_new_sync_finishes_cleanly_when_point_has_no_new_recipes(self, service_cls, weekly_snapshot):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        def discovery_with_progress(**kwargs):
            job.refresh_from_db()
            self.assertEqual(job.parameters["progress"]["stage"], "DISCOVERING")
            return {"new_codes": [], "blocked_candidates_count": 0}

        service_cls.return_value.discover_new_product_codes.side_effect = discovery_with_progress

        payload = task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(job.result_summary["products_selected"], 0)
        self.assertEqual(job.result_summary["new_products_imported"], 0)
        self.assertEqual(job.result_summary["discovery"]["new_codes"], [])
        self.assertEqual(job.parameters["progress"]["stage"], "COMPLETED")
        service_cls.return_value.sync.assert_not_called()
        weekly_snapshot.assert_not_called()
        self.assertEqual(payload["status"], PointSyncJob.STATUS_SUCCESS)

    @patch("pos_bridge.tasks.celery_tasks.run_weekly_cost_snapshot")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_new_sync_preserves_blocked_candidates_for_catalog_status(self, service_cls, weekly_snapshot):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        service_cls.return_value.discover_new_product_codes.return_value = {
            "new_codes": [],
            "blocked_candidates_count": 1,
            "blocked_candidates": [
                {
                    "codigo_point": "EXTRA-FRESA-C",
                    "nombre": "Extra Fresa Chico",
                    "detection_reason": "POINT_NO_BOM",
                }
            ],
        }

        task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_PARTIAL)
        self.assertEqual(job.result_summary["discovery"]["blocked_candidates_count"], 1)
        self.assertEqual(
            job.result_summary["discovery"]["blocked_candidates"][0]["codigo_point"],
            "EXTRA-FRESA-C",
        )
        service_cls.return_value.sync.assert_not_called()
        weekly_snapshot.assert_not_called()

    @patch("pos_bridge.tasks.celery_tasks.complete_recipe_costs")
    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_new_sync_imports_confirmed_recipe_and_reports_unconfirmed_products(self, service_cls, costs):
        from maestros.models import Insumo, UnidadMedida, seed_unidades_basicas
        from pos_bridge.models import PointProduct
        from pos_bridge.services.product_recipe_sync_service import PointProductRecipeSyncService
        from pos_bridge.services.product_count_units import product_count_unit
        from pos_bridge.tests.test_product_recipe_sync_service import FakePointHttpClient
        from pos_bridge.utils.exceptions import ExtractionError
        from recetas.models import Receta

        products = [
            {"PK_Producto": str(pk), "Codigo": code, "Nombre": name, "hasReceta": True}
            for pk, code, name in [(1, "NEW-OK", "Pastel confirmado"), (2, "NEW-FAIL", "Pay sin respuesta"),
                                   (3, "NEW-EMPTY", "Pastel sin composición")]
        ]
        products[0]["Unidad"] = "PZA"
        seed_unidades_basicas()
        Insumo.objects.create(
            nombre="Insumo nuevo", codigo_point="INP-1",
            unidad_base=UnidadMedida.objects.get(codigo="pza"),
        )
        costs.return_value = {"recipe_ids": [], "missing_count": 0, "search_error": ""}
        client = FakePointHttpClient({
            "products": products,
            "details": {"1": products[0], "3": products[2]},
            "boms": {"1": [{"PK_Articulo": 500, "Codigo_Articulo": "INP-1",
                            "Articulo": "Insumo nuevo", "Cantidad": 1, "Unidad_corto": "PZA"}], "3": []},
        })
        real_detail = client.get_product_detail

        def get_detail(pk):
            if pk == "2":
                raise ExtractionError("Point no respondió")
            return real_detail(pk)

        service_cls.return_value = PointProductRecipeSyncService(http_client_factory=lambda: client)
        job = PointSyncJob.objects.create(job_type="recipes", parameters={"action": "SYNC_ONLY_NEW_PRODUCTS"})
        with (
            patch.object(client, "get_product_detail", side_effect=get_detail),
            patch.object(client, "get_all_products", return_value=products),
            patch.object(client, "get_products", return_value=[]),
        ):
            task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, "PARTIAL")
        self.assertEqual(job.result_summary["products_selected"], 1)
        review = job.result_summary["discovery"]["blocked_candidates"]
        self.assertEqual(
            {item["codigo_point"]: item["detection_reason"] for item in review},
            {"NEW-FAIL": "POINT_RECIPE_UNAVAILABLE", "NEW-EMPTY": "POINT_NO_BOM"},
        )
        self.assertEqual(job.parameters["new_product_codes"], ["NEW-EMPTY", "NEW-FAIL"])
        self.assertEqual(product_count_unit(PointProduct.objects.get(sku="NEW-OK"))[0], "PZA")
        self.assertTrue(Receta.objects.filter(codigo_point="NEW-OK").exists())
        self.assertFalse(Receta.objects.filter(codigo_point__in=["NEW-FAIL", "NEW-EMPTY"]).exists())

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_already_successful_job_is_not_executed_twice(self, service_cls):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_SUCCESS,
            parameters={"action": "SYNC_ALL_RECIPES", "branch_hint": "MATRIZ"},
            result_summary={"products_selected": 12},
            triggered_by=self.user,
        )

        payload = task_catalog_recipe_sync(job_id=job.id)

        self.assertEqual(payload["job_id"], job.id)
        self.assertEqual(payload["status"], PointSyncJob.STATUS_SUCCESS)
        service_cls.assert_not_called()

    @patch("pos_bridge.tasks.celery_tasks.PointProductRecipeSyncService")
    def test_sync_failure_is_persisted_on_job(self, service_cls):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_PENDING,
            parameters={"action": "SYNC_ALL_RECIPES", "branch_hint": "MATRIZ"},
            triggered_by=self.user,
        )
        service_cls.return_value.sync.side_effect = RuntimeError("Point timeout")

        with self.assertRaisesRegex(RuntimeError, "Point timeout"):
            task_catalog_recipe_sync(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        self.assertIn("Point timeout", job.error_message)
        self.assertEqual(job.parameters["progress"]["stage"], "FAILED")
