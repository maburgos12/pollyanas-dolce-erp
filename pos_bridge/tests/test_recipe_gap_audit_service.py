from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointSyncJob
from pos_bridge.services.recipe_gap_audit_service import PointRecipeGapAuditResult, PointRecipeGapAuditService
from pos_bridge.services.sync_service import PointSyncService
from recetas.models import Receta


class FakePointAuditHttpClient:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def login(self, *, branch_hint=None):
        del branch_hint
        return {"branch_name": "Matriz", "branch_id": 1, "account_id": "acc"}

    def get_products(self):
        return list(self.payload["products"])

    def get_product_detail(self, product_id):
        return dict(self.payload["details"][product_id])

    def get_product_bom(self, product_id):
        return list(self.payload["product_boms"][product_id])

    def get_articulos(self, *, search="", category=None):
        del category
        return list(self.payload["articulo_searches"].get(search, []))

    def get_articulo_detail(self, articulo_id):
        return dict(self.payload["articulo_details"][articulo_id])


class PointRecipeGapAuditServiceTests(TestCase):
    def setUp(self):
        self.unit_pza = UnidadMedida.objects.create(codigo="pza", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA)
        self.unit_g = UnidadMedida.objects.create(codigo="g", nombre="Gramo", tipo=UnidadMedida.TIPO_MASA)
        self.internal_insumo = Insumo.objects.create(
            nombre="Insumo Bolitas Nuez",
            codigo_point="01INSBOLIT",
            unidad_base=self.unit_pza,
            tipo_item=Insumo.TIPO_INTERNO,
            activo=True,
        )
        Receta.objects.create(
            nombre="Insumo Bolitas Nuez",
            codigo_point="01INSBOLIT",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="prep-bolitas",
        )

    def test_audit_classifies_missing_product_as_corroborated_when_internal_bom_exists(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 441,
                    "Codigo": "00445",
                    "Nombre": "Bolitas de Nuez 10 PZ",
                    "Familia": "Galletas",
                    "Categoria": "Galletas",
                    "hasReceta": False,
                }
            ],
            "details": {
                441: {
                    "PK_Producto": 441,
                    "Codigo": "00445",
                    "Nombre": "Bolitas de Nuez 10 PZ",
                }
            },
            "product_boms": {441: []},
            "articulo_searches": {
                "00445": [],
                "Bolitas de Nuez 10 PZ": [
                    {
                        "PK_Articulo": 429,
                        "Codigo_Articulo": "01INSBOLIT",
                        "Nombre_Articulo": "Insumo Bolitas Nuez",
                        "Categoria": "GALLETAS",
                        "HasReceta": True,
                    }
                ],
                "bolitas nuez": [
                    {
                        "PK_Articulo": 429,
                        "Codigo_Articulo": "01INSBOLIT",
                        "Nombre_Articulo": "Insumo Bolitas Nuez",
                        "Categoria": "GALLETAS",
                        "HasReceta": True,
                    }
                ],
            },
            "articulo_details": {
                429: {
                    "PKInsumo": 429,
                    "CodigoInsumo": "01INSBOLIT",
                    "Nombre": "Insumo Bolitas Nuez",
                    "BOM": [
                        {
                            "PKInsumo": 411,
                            "CodigoInsumo": "01BNPKG",
                            "Nombre": "Masa Bolitas de Nuez por KG",
                            "Cantidad": 8.0,
                            "UnidadVenta": {"Abreviacion": "Gr"},
                        }
                    ],
                }
            },
        }
        service = PointRecipeGapAuditService(http_client_factory=lambda: FakePointAuditHttpClient(payload))

        result = service.audit(product_codes=["00445"])

        self.assertEqual(result.summary["products_audited"], 1)
        self.assertEqual(result.summary["corroborated_from_insumos"], 1)
        self.assertTrue(result.report_path.endswith(".csv"))
        self.assertTrue(result.raw_export_path.endswith(".json"))

    def test_audit_marks_missing_when_no_internal_candidate_exists(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 442,
                    "Codigo": "00446",
                    "Nombre": "Vaso Choco Oreo Mini",
                    "Familia": "Vasos Preparados",
                    "Categoria": "Vaso Preparado Mini",
                    "hasReceta": False,
                }
            ],
            "details": {
                442: {
                    "PK_Producto": 442,
                    "Codigo": "00446",
                    "Nombre": "Vaso Choco Oreo Mini",
                }
            },
            "product_boms": {442: []},
            "articulo_searches": {
                "00446": [],
                "Vaso Choco Oreo Mini": [],
                "choco oreo": [],
            },
            "articulo_details": {},
        }
        service = PointRecipeGapAuditService(http_client_factory=lambda: FakePointAuditHttpClient(payload))

        result = service.audit(product_codes=["00446"])

        self.assertEqual(result.summary["products_audited"], 1)
        self.assertEqual(result.summary["missing_without_candidates"], 1)

    def test_audit_classifies_slice_product_as_derived_presentation(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 3,
                    "Codigo": "0003",
                    "Nombre": "Pay de Queso Rebanada",
                    "Familia": "Pay",
                    "Categoria": "Rebanada",
                    "hasReceta": False,
                }
            ],
            "details": {
                3: {
                    "PK_Producto": 3,
                    "Codigo": "0003",
                    "Nombre": "Pay de Queso Rebanada",
                }
            },
            "product_boms": {3: []},
            "articulo_searches": {
                "0003": [],
                "Pay de Queso Rebanada": [],
                "pay queso": [
                    {
                        "PK_Articulo": 436,
                        "Codigo_Articulo": "01801",
                        "Nombre_Articulo": "Pay de Queso Horneado Mediano",
                        "Categoria": "PAN",
                        "HasReceta": True,
                    }
                ],
            },
            "articulo_details": {
                436: {
                    "PKInsumo": 436,
                    "CodigoInsumo": "01801",
                    "Nombre": "Pay de Queso Horneado Mediano",
                    "BOM": [
                        {
                            "PKInsumo": 377,
                            "CodigoInsumo": "01BPQ19",
                            "Nombre": "Batida pay de queso",
                            "Cantidad": 800.0,
                            "UnidadVenta": {"Abreviacion": "ML"},
                        }
                    ],
                }
            },
        }
        service = PointRecipeGapAuditService(http_client_factory=lambda: FakePointAuditHttpClient(payload))

        result = service.audit(product_codes=["0003"])

        self.assertEqual(result.summary["products_audited"], 1)
        self.assertEqual(result.summary["products_derived_presentations"], 1)


class FakeRecipeGapAuditService:
    def audit(self, *, branch_hint=None, product_codes=None, limit=None):
        del branch_hint, product_codes, limit
        return PointRecipeGapAuditResult(
            summary={
                "workspace": "Matriz",
                "products_seen": 10,
                "products_with_product_bom": 8,
                "products_missing_recipe_in_point": 2,
                "products_audited": 2,
                "products_derived_presentations": 1,
                "corroborated_from_insumos": 1,
                "possible_matches_requiring_review": 0,
                "internal_candidates_without_bom": 0,
                "missing_without_candidates": 0,
            },
            report_path="/tmp/point_recipe_gap_audit.csv",
            raw_export_path="/tmp/point_recipe_gap_audit.json",
        )


class PointSyncServiceRecipeGapAuditTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_recipe_audit",
            email="admin_recipe_audit@example.com",
            password="test12345",
        )

    def test_run_recipe_gap_audit_creates_successful_job(self):
        service = PointSyncService(recipe_gap_audit_service=FakeRecipeGapAuditService())

        sync_job = service.run_recipe_gap_audit(
            triggered_by=self.user,
            branch_hint="MATRIZ",
            product_codes=["00445", "00446"],
        )

        self.assertEqual(sync_job.job_type, PointSyncJob.JOB_TYPE_RECIPES)
        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(sync_job.result_summary["products_audited"], 2)
        self.assertEqual(sync_job.result_summary["products_derived_presentations"], 1)
        self.assertEqual(sync_job.result_summary["report_path"], "/tmp/point_recipe_gap_audit.csv")
