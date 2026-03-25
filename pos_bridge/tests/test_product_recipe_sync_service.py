from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointProduct, PointRecipeExtractionRun, PointRecipeNode, PointSyncJob
from pos_bridge.services.product_recipe_sync_service import PointProductRecipeSyncService, PointRecipeSyncResult
from pos_bridge.services.sync_service import PointSyncService
from recetas.models import LineaReceta, Receta


class FakePointHttpClient:
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
        return list(self.payload["boms"][product_id])

    def get_articulos(self, *, search="", category=None):
        del category
        search = (search or "").strip().lower()
        rows = list(self.payload.get("articulos", []))
        if not search:
            return rows
        return [
            row
            for row in rows
            if search in str(row.get("Codigo_Articulo") or "").lower()
            or search in str(row.get("Nombre_Articulo") or "").lower()
        ]

    def get_articulo_detail(self, articulo_id):
        return dict(self.payload["articulo_details"][articulo_id])


class PointProductRecipeSyncServiceTests(TestCase):
    def setUp(self):
        self.unit_pza = UnidadMedida.objects.create(codigo="pza", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA)
        self.insumo = Insumo.objects.create(
            nombre="Insumo Bolitas Nuez",
            codigo_point="01INSBOLIT",
            unidad_base=self.unit_pza,
            activo=True,
        )

    def test_sync_can_hydrate_selected_codes_not_returned_by_catalog(self):
        PointProduct.objects.create(
            external_id="824",
            sku="SFRESAG",
            name="Sabor Fresa Grande Pay",
            category="Pay Grande",
        )
        payload = {
            "products": [],
            "details": {
                "824": {
                    "PK_Producto": 824,
                    "Codigo": "SFRESAG",
                    "Nombre": "Sabor Fresa Grande Pay",
                }
            },
            "boms": {
                "824": [
                    {
                        "PK_Articulo": 429,
                        "Codigo_Articulo": "01INSBOLIT",
                        "Articulo": "Insumo Bolitas Nuez",
                        "Cantidad": 1,
                        "Unidad_corto": "PZA",
                    }
                ]
            },
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.sync(product_codes=["SFRESAG"])

        self.assertEqual(result.summary["products_selected"], 1)
        self.assertTrue(Receta.objects.filter(codigo_point="SFRESAG").exists())

    def test_discover_new_product_codes_excludes_existing_recipes(self):
        Receta.objects.create(
            nombre="Sabor Fresa Grande Pay",
            codigo_point="SFRESAG",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-existing",
        )
        payload = {
            "products": [
                {
                    "PK_Producto": 824,
                    "Codigo": "SFRESAG",
                    "Nombre": "Sabor Fresa Grande Pay",
                    "Familia": "Pay",
                    "Categoria": "Pay Grande",
                    "hasReceta": True,
                },
                {
                    "PK_Producto": 900,
                    "Codigo": "SNUEVO",
                    "Nombre": "Sabor Nuevo",
                    "Familia": "Pay",
                    "Categoria": "Pay Grande",
                    "hasReceta": True,
                },
            ],
            "details": {},
            "boms": {},
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], ["SNUEVO"])

    def test_sync_creates_recipe_and_line_from_point_bom(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 259,
                    "Codigo": "01BLN01",
                    "Nombre": "Bolitas de Nuez 10 PZ",
                    "Familia": "Galletas",
                    "Categoria": "Galletas",
                    "hasReceta": True,
                }
            ],
            "details": {
                259: {
                    "PK_Producto": 259,
                    "Codigo": "01BLN01",
                    "Nombre": "Bolitas de Nuez 10 PZ",
                }
            },
            "boms": {
                259: [
                    {
                        "PK_Articulo": 429,
                        "Codigo_Articulo": "01INSBOLIT",
                        "Articulo": "Insumo Bolitas Nuez",
                        "Cantidad": 10,
                        "Unidad_corto": "PZA",
                    }
                ]
            },
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(
            http_client_factory=lambda: FakePointHttpClient(payload),
        )

        result = service.sync(product_codes=["01BLN01"])

        self.assertEqual(result.summary["products_selected"], 1)
        self.assertEqual(result.summary["recipes_created"], 1)
        self.assertEqual(result.summary["lineas_created"], 1)
        self.assertEqual(result.summary["lineas_auto"], 1)

        receta = Receta.objects.get(codigo_point="01BLN01")
        self.assertEqual(receta.sheet_name, PointProductRecipeSyncService.SHEET_NAME)
        self.assertEqual(receta.tipo, Receta.TIPO_PRODUCTO_FINAL)

        linea = LineaReceta.objects.get(receta=receta)
        self.assertEqual(linea.insumo_id, self.insumo.id)
        self.assertEqual(linea.match_status, LineaReceta.STATUS_AUTO)
        self.assertEqual(linea.match_method, "POINT_CODE")
        self.assertEqual(linea.unidad_id, self.unit_pza.id)
        self.assertEqual(PointRecipeExtractionRun.objects.count(), 1)
        self.assertEqual(PointRecipeNode.objects.count(), 1)

    def test_sync_skips_creating_recipe_when_point_bom_is_empty(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 441,
                    "Codigo": "00445",
                    "Nombre": "Vaso Galleta Chispas Chocolate Mini",
                    "Familia": "Vasos Preparados",
                    "Categoria": "Vaso Preparado Mini",
                    "hasReceta": False,
                }
            ],
            "details": {
                441: {
                    "PK_Producto": 441,
                    "Codigo": "00445",
                    "Nombre": "Vaso Galleta Chispas Chocolate Mini",
                }
            },
            "boms": {441: []},
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(
            http_client_factory=lambda: FakePointHttpClient(payload),
        )

        result = service.sync(product_codes=["00445"], include_without_recipe=True)

        self.assertEqual(result.summary["products_without_recipe_in_point"], 1)
        self.assertEqual(result.summary["recipes_created"], 0)
        self.assertFalse(Receta.objects.filter(codigo_point="00445").exists())

    def test_sync_extracts_prepared_input_recipe_and_yield(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 101,
                    "Codigo": "0101",
                    "Nombre": "Pastel de Fresas Con Crema Chico",
                    "Familia": "Pastel",
                    "Categoria": "Pastel Chico",
                    "hasReceta": True,
                }
            ],
            "details": {
                101: {
                    "PK_Producto": 101,
                    "Codigo": "0101",
                    "Nombre": "Pastel de Fresas Con Crema Chico",
                }
            },
            "boms": {
                101: [
                    {
                        "PK_Articulo": 350,
                        "Codigo_Articulo": "01DW01",
                        "Articulo": "Betún Dream Whip Pastel",
                        "Cantidad": "760.0",
                        "Unidad_corto": "Gr",
                    },
                    {
                        "PK_Articulo": 998,
                        "Codigo_Articulo": "C85-45",
                        "Articulo": "Domo Chico C85",
                        "Cantidad": 1,
                        "Unidad_corto": "PZA",
                    },
                ]
            },
            "articulos": [
                {
                    "PK_Articulo": 350,
                    "Codigo_Articulo": "01DW01",
                    "Nombre_Articulo": "Betún Dream Whip Pastel",
                    "Categoria": "Betún, Cremas, Rellenos",
                    "HasReceta": True,
                }
            ],
            "articulo_details": {
                350: {
                    "PKInsumo": 350,
                    "CodigoInsumo": "01DW01",
                    "Nombre": "Betún Dream Whip Pastel",
                    "UnidadBase": "KG",
                    "UnidadVenta": "Gr",
                    "ConvUnidadVenta": 1000,
                    "Categoria": "Betún, Cremas, Rellenos",
                    "BOM": [
                        {"CodigoInsumo": "005", "Nombre": "QUESO CREMA", "Cantidad": "171.76", "UnidadVenta": "Gr"},
                        {"CodigoInsumo": "006", "Nombre": "MEDIA CREMA", "Cantidad": "447.47", "UnidadVenta": "ML"},
                    ],
                }
            },
        }
        packaging = Insumo.objects.create(
            nombre="Domo Chico C85",
            codigo_point="C85-45",
            unidad_base=self.unit_pza,
            tipo_item=Insumo.TIPO_EMPAQUE,
            activo=True,
        )
        service = PointProductRecipeSyncService(
            http_client_factory=lambda: FakePointHttpClient(payload),
        )

        result = service.sync(product_codes=["0101"])

        self.assertEqual(result.summary["recipes_created"], 1)
        self.assertEqual(result.summary["preparations_created"], 1)
        self.assertEqual(result.summary["internal_insumos_created"], 1)

        receta_final = Receta.objects.get(codigo_point="0101")
        receta_betun = Receta.objects.get(codigo_point="01DW01")
        self.assertEqual(receta_final.tipo, Receta.TIPO_PRODUCTO_FINAL)
        self.assertEqual(receta_betun.tipo, Receta.TIPO_PREPARACION)
        self.assertEqual(str(receta_betun.rendimiento_cantidad), "1.000000")
        self.assertEqual(receta_betun.rendimiento_unidad.codigo, "kg")

        linea_padre = LineaReceta.objects.get(receta=receta_final, insumo_texto="Betún Dream Whip Pastel")
        self.assertEqual(linea_padre.insumo.codigo_point, "01DW01")
        self.assertEqual(linea_padre.match_status, LineaReceta.STATUS_AUTO)

        node_betun = PointRecipeNode.objects.get(point_code="01DW01")
        self.assertEqual(node_betun.node_kind, PointRecipeNode.KIND_PREPARED_INPUT)
        self.assertEqual(node_betun.yield_unit.codigo, "kg")
        self.assertEqual(node_betun.lines.count(), 2)
        self.assertTrue(node_betun.lines.filter(classification="UNRESOLVED").exists())
        self.assertEqual(packaging.codigo_point, "C85-45")


class FakeRecipeSyncService:
    def sync(self, *, branch_hint=None, product_codes=None, limit=None, include_without_recipe=False, sync_job=None):
        del branch_hint, product_codes, limit, include_without_recipe, sync_job
        return PointRecipeSyncResult(
            summary={
                "workspace": "Matriz",
                "products_seen": 1,
                "products_selected": 1,
                "products_without_recipe_in_point": 0,
                "recipes_created": 1,
                "recipes_updated": 0,
                "recipes_unchanged": 0,
                "preparations_created": 0,
                "preparations_updated": 0,
                "preparations_unchanged": 0,
                "lineas_created": 2,
                "lineas_auto": 2,
                "lineas_needs_review": 0,
                "lineas_rejected": 0,
                "graph_nodes": 1,
                "graph_lines": 2,
                "aliases_synced": 0,
                "internal_insumos_created": 0,
                "run_id": 77,
            },
            raw_export_path="/tmp/point_product_recipes.json",
        )


class PointSyncServiceRecipeJobTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_recipe_bridge",
            email="admin_recipe_bridge@example.com",
            password="test12345",
        )

    def test_run_product_recipe_sync_creates_recipe_job(self):
        service = PointSyncService(recipe_sync_service=FakeRecipeSyncService())

        sync_job = service.run_product_recipe_sync(
            triggered_by=self.user,
            branch_hint="MATRIZ",
            product_codes=["01BLN01"],
        )

        self.assertEqual(sync_job.job_type, PointSyncJob.JOB_TYPE_RECIPES)
        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(sync_job.result_summary["recipes_created"], 1)
        self.assertEqual(sync_job.result_summary["raw_exports"], ["/tmp/point_product_recipes.json"])
