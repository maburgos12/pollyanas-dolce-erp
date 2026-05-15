from __future__ import annotations

from io import StringIO
from datetime import timedelta
from unittest.mock import patch

from django.core.management import call_command
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointExtractionLog, PointProduct, PointRecipeExtractionRun, PointRecipeNode, PointRecipeNodeLine, PointSyncJob
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

    def test_sync_can_hydrate_selected_codes_with_hyphenated_sku(self):
        PointProduct.objects.create(
            external_id="1012",
            sku="0-1",
            name="Capuchino",
            category="Café",
        )
        payload = {
            "products": [],
            "details": {
                "1012": {
                    "PK_Producto": 1012,
                    "Codigo": "0-1",
                    "Nombre": "Capuchino",
                }
            },
            "boms": {
                "1012": [
                    {
                        "PK_Articulo": 13,
                        "Codigo_Articulo": "013",
                        "Articulo": "Agua Purificada",
                        "Cantidad": 455,
                        "Unidad_corto": "ML",
                    }
                ]
            },
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.sync(product_codes=["0-1"])

        self.assertEqual(result.summary["products_selected"], 1)
        self.assertTrue(Receta.objects.filter(codigo_point="0-1").exists())

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

    def test_discover_new_product_codes_promotes_recipe_like_candidate_when_bom_exists(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 910,
                    "Codigo": "TOP-FRESA-C",
                    "Nombre": "Topping Fresa Chico",
                    "Familia": "Otros postres",
                    "Categoria": "Pastel Chico",
                    "hasReceta": False,
                },
            ],
            "details": {},
            "boms": {
                910: [
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

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], ["TOP-FRESA-C"])
        self.assertEqual(result["blocked_codes"], [])
        self.assertEqual(result["new_candidates"][0]["detection_reason"], "BOM_PROBE_POSITIVE")
        self.assertEqual(result["new_candidates"][0]["bom_lines"], 1)

    def test_discover_new_product_codes_reports_blocked_recipe_like_candidate_without_bom(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 911,
                    "Codigo": "EXTRA-FRESA-C",
                    "Nombre": "Extra Fresa Chico",
                    "Familia": "Otros postres",
                    "Categoria": "Pastel Chico",
                    "hasReceta": False,
                },
            ],
            "details": {},
            "boms": {
                911: []
            },
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], [])
        self.assertEqual(result["blocked_codes"], ["EXTRA-FRESA-C"])
        self.assertEqual(result["blocked_candidates_count"], 1)
        self.assertEqual(result["blocked_candidates"][0]["detection_reason"], "POINT_NO_BOM")

    def test_discover_new_product_codes_blocks_duplicate_point_codes(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 912,
                    "Codigo": "0227",
                    "Nombre": "Extra 10",
                    "Familia": "Otros postres",
                    "Categoria": "Otros postres",
                    "hasReceta": False,
                },
                {
                    "PK_Producto": 913,
                    "Codigo": "0227",
                    "Nombre": "Extra Fresa Chico",
                    "Familia": "Otros postres",
                    "Categoria": "Pastel Chico",
                    "hasReceta": False,
                },
            ],
            "details": {},
            "boms": {
                912: [],
                913: [],
            },
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], [])
        self.assertEqual(result["blocked_candidates_count"], 2)
        self.assertEqual(result["blocked_candidates"][0]["detection_reason"], "DUPLICATE_POINT_CODE")
        self.assertEqual(result["blocked_candidates"][1]["detection_reason"], "DUPLICATE_POINT_CODE")

    def test_discover_new_product_codes_hydrates_recent_pointproduct_missing_from_catalog(self):
        point_product = PointProduct.objects.create(
            external_id="1027",
            sku="01GM01",
            name="Galleta M&MS",
            category="Galletas",
        )
        payload = {
            "products": [],
            "details": {
                "1027": {
                    "PK_Producto": "1027",
                    "Codigo": "01GM01",
                    "Nombre": "Galleta M&MS",
                    "Familia": "Galletas",
                    "Categoria": "Galletas",
                }
            },
            "boms": {
                "1027": [
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

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], ["01GM01"])
        self.assertEqual(result["new_candidates"][0]["point_external_id"], "1027")
        point_product.refresh_from_db()
        self.assertTrue(point_product.created_at >= timezone.now() - timedelta(days=1))

    def test_discover_new_product_codes_ignores_historical_pointproduct_backlog(self):
        point_product = PointProduct.objects.create(
            external_id="268",
            sku="0228",
            name="Extra 15",
            category="Otros postres",
        )
        PointProduct.objects.filter(pk=point_product.pk).update(
            created_at=timezone.now() - timedelta(days=20),
        )
        payload = {
            "products": [
                {
                    "PK_Producto": 268,
                    "Codigo": "0228",
                    "Nombre": "Extra 15",
                    "Familia": "Otros postres",
                    "Categoria": "Otros postres",
                    "hasReceta": False,
                }
            ],
            "details": {},
            "boms": {
                268: []
            },
            "articulos": [],
            "articulo_details": {},
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], [])
        self.assertEqual(result["blocked_codes"], [])
        self.assertEqual(result["ignored_candidates_count"], 1)

    def test_discover_new_product_codes_uses_last_successful_sync_as_baseline(self):
        point_product = PointProduct.objects.create(
            external_id="1027",
            sku="01GM01",
            name="Galleta M&MS",
            category="Galletas",
        )
        PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_SUCCESS,
            parameters={"action": "SYNC_ONLY_NEW_PRODUCTS", "product_codes": ["OLD-001"]},
            finished_at=timezone.now() - timedelta(days=3),
        )
        PointProduct.objects.filter(pk=point_product.pk).update(
            created_at=timezone.now() - timedelta(days=2),
        )
        payload = {
            "products": [],
            "details": {
                "1027": {
                    "PK_Producto": "1027",
                    "Codigo": "01GM01",
                    "Nombre": "Galleta M&MS",
                    "Familia": "Galletas",
                    "Categoria": "Galletas",
                }
            },
            "boms": {
                "1027": [
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

        result = service.discover_new_product_codes()

        self.assertEqual(result["new_codes"], ["01GM01"])

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
        self.assertEqual(result.summary["recipes_completed_successfully"], 0)
        self.assertEqual(result.summary["recipes_with_unresolved_inputs"], 1)
        self.assertEqual(result.summary["new_products_imported"], 0)
        self.assertEqual(result.summary["new_preparations_imported"], 0)
        self.assertEqual(result.summary["recursive_nodes_created"], 0)
        self.assertEqual(result.summary["imported_products_status"][0]["status"], "BLOCKED_UNRESOLVED")
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
        self.assertEqual(result.summary["new_products_imported"], 1)
        self.assertEqual(result.summary["new_preparations_imported"], 1)
        self.assertEqual(result.summary["recursive_nodes_created"], 1)
        self.assertEqual(result.summary["recipes_completed_successfully"], 1)
        self.assertEqual(result.summary["recipes_with_unresolved_inputs"], 0)
        self.assertEqual(result.summary["unresolved_inputs_count"], 0)
        self.assertEqual(result.summary["imported_products_status"][0]["status"], "SUCCESS_COMPLETE")
        self.assertEqual(len(result.summary["imported_products_status"][0]["created_preparations"]), 1)

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
        self.assertFalse(node_betun.lines.filter(classification="UNRESOLVED").exists())
        self.assertEqual(packaging.codigo_point, "C85-45")

    def test_sync_creates_direct_catalog_inputs_for_unmapped_bom_rows(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 1012,
                    "Codigo": "0-1",
                    "Nombre": "Capuchino",
                    "Familia": "Café",
                    "Categoria": "Café",
                    "hasReceta": True,
                }
            ],
            "details": {
                1012: {
                    "PK_Producto": 1012,
                    "Codigo": "0-1",
                    "Nombre": "Capuchino",
                }
            },
            "boms": {
                1012: [
                    {
                        "PK_Articulo": 13,
                        "Codigo_Articulo": "013",
                        "Articulo": "AGUA",
                        "Cantidad": 455,
                        "Unidad_corto": "ML",
                    },
                    {
                        "PK_Articulo": 539,
                        "Codigo_Articulo": "52152102",
                        "Articulo": "VASO 16OZ NESCAFE",
                        "Cantidad": 1,
                        "Unidad_corto": "PZA",
                    },
                ]
            },
            "articulos": [],
            "articulo_details": {
                13: {
                    "PKInsumo": 13,
                    "CodigoInsumo": "013",
                    "Nombre": "AGUA",
                    "UnidadBase": "ML",
                    "UnidadVenta": "ML",
                    "Categoria": {"Categoria": "BEBIDAS"},
                    "BOM": [],
                },
                539: {
                    "PKInsumo": 539,
                    "CodigoInsumo": "52152102",
                    "Nombre": "VASO 16OZ NESCAFE",
                    "UnidadBase": "PZA",
                    "UnidadVenta": "PZA",
                    "Categoria": {"Categoria": "DESECHABLES"},
                    "BOM": [],
                },
            },
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.sync(product_codes=["0-1"])

        self.assertEqual(result.summary["catalog_insumos_created"], 2)
        agua = Insumo.objects.get(codigo_point="013")
        vaso = Insumo.objects.get(codigo_point="52152102")
        self.assertEqual(agua.tipo_item, Insumo.TIPO_MATERIA_PRIMA)
        self.assertEqual(vaso.tipo_item, Insumo.TIPO_EMPAQUE)
        receta = Receta.objects.get(codigo_point="0-1")
        self.assertEqual(receta.lineas.count(), 2)

    def test_sync_marks_root_recipe_with_warnings_when_recursive_child_has_unresolved_input(self):
        payload = {
            "products": [
                {
                    "PK_Producto": 101,
                    "Codigo": "0101",
                    "Nombre": "Pastel con hijo incompleto",
                    "Familia": "Pastel",
                    "Categoria": "Pastel Chico",
                    "hasReceta": True,
                }
            ],
            "details": {
                101: {
                    "PK_Producto": 101,
                    "Codigo": "0101",
                    "Nombre": "Pastel con hijo incompleto",
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
                    }
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
                        {"CodigoInsumo": "", "Nombre": "", "Cantidad": "1", "UnidadVenta": "Gr"},
                    ],
                }
            },
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        result = service.sync(product_codes=["0101"])

        self.assertEqual(result.summary["recipes_completed_successfully"], 0)
        self.assertEqual(result.summary["recipes_with_unresolved_inputs"], 1)
        self.assertEqual(result.summary["unresolved_inputs_count"], 1)
        self.assertEqual(result.summary["imported_products_status"][0]["status"], "SUCCESS_WITH_WARNINGS")
        self.assertEqual(len(result.summary["imported_products_status"][0]["unresolved_inputs"]), 1)

    def test_sync_reassigns_prepared_input_point_code_when_stale_internal_matches_other_base(self):
        stale = Insumo.objects.create(
            nombre="Flan 3 Pecados",
            codigo="DERIVADO:RECETA:30:PREPARACION",
            codigo_point="01FLANMINI",
            nombre_point="Flan 3 Pecados Mini",
            unidad_base=self.unit_pza,
            tipo_item=Insumo.TIPO_INTERNO,
            activo=True,
        )
        correct = Insumo.objects.create(
            nombre="Flan 3 Pecados Mini",
            codigo="DERIVADO:RECETA:30:PRESENTACION:38",
            unidad_base=self.unit_pza,
            tipo_item=Insumo.TIPO_INTERNO,
            activo=True,
        )
        payload = {
            "products": [
                {
                    "PK_Producto": 857,
                    "Codigo": "P3PMINI",
                    "Nombre": "Pastel 3 Pecados Mini",
                    "Familia": "Pastel",
                    "Categoria": "Pastel Mini",
                    "hasReceta": True,
                }
            ],
            "details": {
                857: {
                    "PK_Producto": 857,
                    "Codigo": "P3PMINI",
                    "Nombre": "Pastel 3 Pecados Mini",
                }
            },
            "boms": {
                857: [
                    {
                        "PK_Articulo": 437,
                        "Codigo_Articulo": "01FLANMINI",
                        "Articulo": "Flan 3 Pecados Mini",
                        "Cantidad": 1,
                        "Unidad_corto": "U",
                    }
                ]
            },
            "articulos": [
                {
                    "PK_Articulo": 437,
                    "Codigo_Articulo": "01FLANMINI",
                    "Nombre_Articulo": "Flan 3 Pecados Mini",
                    "Categoria": "Flan",
                    "HasReceta": True,
                }
            ],
            "articulo_details": {
                437: {
                    "PKInsumo": 437,
                    "CodigoInsumo": "01FLANMINI",
                    "Nombre": "Flan 3 Pecados Mini",
                    "UnidadBase": "U",
                    "UnidadVenta": "U",
                    "ConvUnidadVenta": 1,
                    "Categoria": "Flan",
                    "BOM": [
                        {"CodigoInsumo": "005", "Nombre": "QUESO CREMA", "Cantidad": "24", "UnidadVenta": "Gr"},
                    ],
                }
            },
        }
        service = PointProductRecipeSyncService(http_client_factory=lambda: FakePointHttpClient(payload))

        service.sync(product_codes=["P3PMINI"])

        receta_final = Receta.objects.get(codigo_point="P3PMINI")
        linea_padre = LineaReceta.objects.get(receta=receta_final, insumo_texto="Flan 3 Pecados Mini")
        correct.refresh_from_db()
        stale.refresh_from_db()
        self.assertEqual(linea_padre.insumo_id, correct.id)
        self.assertEqual(correct.codigo_point, "01FLANMINI")
        self.assertEqual(correct.nombre_point, "Flan 3 Pecados Mini")
        self.assertEqual(stale.codigo_point, "")
        self.assertEqual(stale.nombre_point, "")


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
                "catalog_insumos_created": 0,
                "new_products_imported": 1,
                "new_preparations_imported": 0,
                "recursive_nodes_created": 0,
                "recipes_completed_successfully": 1,
                "recipes_with_unresolved_inputs": 0,
                "unresolved_inputs_count": 0,
                "imported_products_status": [
                    {
                        "codigo_point": "01BLN01",
                        "nombre": "Bolitas de Nuez 10 PZ",
                        "status": "SUCCESS_COMPLETE",
                        "unresolved_inputs": [],
                        "created_preparations": [],
                        "message": "Se importó Bolitas de Nuez 10 PZ correctamente con toda su receta.",
                    }
                ],
                "run_id": 77,
            },
            raw_export_path="/tmp/point_product_recipes.json",
        )


class FakeRecipeSyncServiceWithWarnings(FakeRecipeSyncService):
    def sync(self, *, sync_job=None, **kwargs):
        result = super().sync(sync_job=sync_job, **kwargs)
        PointExtractionLog.objects.create(
            sync_job=sync_job,
            level=PointExtractionLog.LEVEL_WARNING,
            message="retry",
            context={"event": "point_http_retry", "attempt": 1},
        )
        PointExtractionLog.objects.create(
            sync_job=sync_job,
            level=PointExtractionLog.LEVEL_WARNING,
            message="retry",
            context={"event": "point_http_retry", "attempt": 2},
        )
        PointExtractionLog.objects.create(
            sync_job=sync_job,
            level=PointExtractionLog.LEVEL_WARNING,
            message="relogin",
            context={"event": "point_relogin", "attempt": 1},
        )
        return result


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
        self.assertEqual(sync_job.result_summary["point_retry_events"], 0)
        self.assertEqual(sync_job.result_summary["point_relogin_events"], 0)

    def test_run_product_recipe_sync_counts_point_health_warning_events(self):
        service = PointSyncService(recipe_sync_service=FakeRecipeSyncServiceWithWarnings())

        sync_job = service.run_product_recipe_sync(
            triggered_by=self.user,
            branch_hint="MATRIZ",
            product_codes=["01BLN01"],
        )

        self.assertEqual(sync_job.status, PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(sync_job.result_summary["point_retry_events"], 2)
        self.assertEqual(sync_job.result_summary["point_relogin_events"], 1)


class RunProductRecipeSyncCommandTests(TestCase):
    def setUp(self):
        self.unit_pza = UnidadMedida.objects.create(codigo="pza", nombre="Pieza", tipo=UnidadMedida.TIPO_PIEZA)

    def _fake_run_product_recipe_sync(self, **kwargs):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_RECIPES,
            status=PointSyncJob.STATUS_SUCCESS,
            parameters={
                "branch_hint": kwargs.get("branch_hint") or "",
                "product_codes": kwargs.get("product_codes") or [],
            },
            result_summary={
                "products_selected": 1,
                "lineas_created": 1,
                "raw_exports": ["/tmp/nonexistent_command_dry_run_recipe_sync.json"],
            },
        )
        run = PointRecipeExtractionRun.objects.create(
            sync_job=job,
            workspace="Matriz",
            root_codes=kwargs.get("product_codes") or [],
        )
        node = PointRecipeNode.objects.create(
            run=run,
            source_type=PointRecipeNode.SOURCE_PRODUCT,
            node_kind=PointRecipeNode.KIND_FINAL_PRODUCT,
            point_pk="383",
            point_code="11203",
            point_name="Bollo Mini Chocolate",
            depth=0,
        )
        PointRecipeNodeLine.objects.create(
            node=node,
            position=1,
            point_code="HARINA",
            point_name="Harina Nevada",
            quantity="0.100",
            unit=self.unit_pza,
            classification=PointRecipeNodeLine.COMPONENT_DIRECT_INPUT,
            match_method="POINT_CODE",
            match_score=100,
        )
        return job

    def test_run_product_recipe_sync_dry_run_rolls_back_and_prints_preview(self):
        out = StringIO()

        with patch(
            "pos_bridge.management.commands.run_product_recipe_sync.run_product_recipe_sync",
            side_effect=self._fake_run_product_recipe_sync,
        ):
            call_command(
                "run_product_recipe_sync",
                "--product-code",
                "11203",
                "--dry-run",
                stdout=out,
            )

        output = out.getvalue()
        self.assertIn('"dry_run": true', output)
        self.assertIn("Bollo Mini Chocolate", output)
        self.assertIn("Harina Nevada", output)
        self.assertIn("[DRY-RUN] No se persistió nada", output)
        self.assertEqual(PointSyncJob.objects.count(), 0)
        self.assertEqual(PointRecipeExtractionRun.objects.count(), 0)
        self.assertEqual(PointRecipeNode.objects.count(), 0)
