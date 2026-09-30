from __future__ import annotations

from decimal import Decimal
from unittest.mock import Mock, patch

from django.core.cache import cache
from django.test import TestCase

from core.models import Sucursal
from maestros.models import Insumo
from pos_bridge.models import PointBranch
from pos_bridge.services.live_inventory_lookup_service import (
    PointLiveInventoryLookupError,
    PointLiveInventoryLookupService,
)


class _FakePointClient:
    def __init__(self, _settings):
        self.login = Mock()
        self.get_stock_products = Mock()
        self.get_product_stock = Mock()
        self.get_insumo_categories = Mock()
        self.get_branch_insumos = Mock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class PointLiveInventoryLookupServiceTests(TestCase):
    def setUp(self):
        cache.clear()
        self.sucursal = Sucursal.objects.create(codigo="CRUCERO", nombre="Sucursal Bamoa")
        self.point_branch = PointBranch.objects.create(
            external_id="2",
            name="Bamoa",
            erp_branch=self.sucursal,
        )
        self.client = _FakePointClient(None)
        self.client_factory = Mock(return_value=self.client)
        self.env = patch.dict(
            "os.environ",
            {
                "PICKUP_LIVE_POINT_LOOKUP_ENABLED": "1",
                "PICKUP_LIVE_POINT_LOOKUP_CACHE_SECONDS": "20",
            },
            clear=False,
        )
        self.settings = patch(
            "pos_bridge.services.live_inventory_lookup_service.load_point_bridge_settings",
            return_value=Mock(),
        )
        self.env.start()
        self.settings.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.settings.stop)

    def _service(self):
        return PointLiveInventoryLookupService(client_factory=self.client_factory)

    def test_insumo_uses_official_branch_inventory_and_returns_point_quantity(self):
        Insumo.objects.create(
            codigo="MP-FRESA",
            codigo_point="017",
            nombre="Fresa Fresca",
            categoria="FRUTAS",
            activo=True,
        )
        self.client.get_stock_products.return_value = [
            {"PK": 17, "Codigo": "017", "Nombre": "Fresa Fresca", "isInsumo": True}
        ]
        self.client.get_insumo_categories.return_value = [
            {"PK_Categoria_insumo": 12, "Categoria": "FRUTAS"}
        ]
        self.client.get_branch_insumos.return_value = [
            {
                "PK_articulo": 17,
                "Codigo": "017",
                "Nombre": "Fresa Fresca",
                "Cantidad": 53.47099999999971,
                "Unidad": "KG",
            }
        ]

        result = self._service().get_stock(
            product_codes=["017"],
            sucursal=self.sucursal,
            point_branch=self.point_branch,
        )

        self.assertEqual(result.stock_qty, Decimal("53.47099999999971"))
        self.assertEqual(result.point_branch_id, "2")
        self.assertEqual(result.point_branch_name, "Bamoa")
        self.client.get_branch_insumos.assert_called_once_with(
            branch_id="2",
            category_id=12,
            timeout=5,
        )
        self.client.get_product_stock.assert_not_called()

    def test_finished_product_keeps_existing_product_stock_endpoint(self):
        self.client.get_stock_products.return_value = [
            {"PK": 99, "Codigo": "PASTEL-1", "Nombre": "Pastel", "isInsumo": False}
        ]
        self.client.get_product_stock.return_value = [
            {"PK_Sucursal": 2, "Sucursal": "Bamoa", "Cantidad": 7}
        ]

        result = self._service().get_stock(
            product_codes=["PASTEL-1"],
            sucursal=self.sucursal,
            point_branch=self.point_branch,
        )

        self.assertEqual(result.stock_qty, Decimal("7"))
        self.client.get_product_stock.assert_called_once_with(99, timeout=5)
        self.client.get_branch_insumos.assert_not_called()

    def test_insumo_without_local_category_fails_closed_instead_of_using_zero_endpoint(self):
        Insumo.objects.create(
            codigo="MP-FRESA",
            codigo_point="017",
            nombre="Fresa Fresca",
            categoria="",
            activo=True,
        )
        self.client.get_stock_products.return_value = [
            {"PK": 17, "Codigo": "017", "Nombre": "Fresa Fresca", "isInsumo": True}
        ]

        with self.assertRaisesMessage(
            PointLiveInventoryLookupError,
            "categoría",
        ):
            self._service().get_stock(
                product_codes=["017"],
                sucursal=self.sucursal,
                point_branch=self.point_branch,
            )

        self.client.get_product_stock.assert_not_called()
        self.client.get_branch_insumos.assert_not_called()

    def test_cache_key_version_does_not_reuse_previous_zero_stock_entries(self):
        key = self._service()._cache_key(
            codes=["017"],
            sucursal=self.sucursal,
            point_branch=self.point_branch,
        )

        self.assertIn("pickup_live_point:v3:", key)

    def test_exact_point_id_wins_over_earlier_historical_erp_name(self):
        self.client.get_stock_products.return_value = [
            {"PK": 101, "Codigo": "PASTEL-1", "Nombre": "Pastel", "isInsumo": False}
        ]
        self.client.get_product_stock.return_value = [
            {"PK_Sucursal": 10, "Sucursal": "Crucero", "Cantidad": 0},
            {"PK_Sucursal": 2, "Sucursal": "Bamoa", "Cantidad": 3},
        ]
        result = self._service().get_stock(
            product_codes=["PASTEL-1"], sucursal=self.sucursal, point_branch=self.point_branch,
        )
        self.assertEqual((result.point_branch_id, result.point_branch_name, result.stock_qty),
                         ("2", "Bamoa", Decimal("3")))

    def test_ambiguous_or_missing_point_identity_fails_closed(self):
        for rows in (
            [{"PK_Sucursal": 2, "Sucursal": "Bamoa"}, {"PK_Sucursal": 2, "Sucursal": "Bamoa"}],
            [{"PK_Sucursal": 10, "Sucursal": "Bamoa"}],
            [{"Sucursal": "Bamoa"}],
        ):
            with self.subTest(rows=rows), self.assertRaises(PointLiveInventoryLookupError):
                self._service()._find_branch_row(
                    stock_rows=rows, sucursal=self.sucursal, point_branch=self.point_branch,
                )

    def test_nonnumeric_point_alias_uses_only_its_point_name(self):
        self.point_branch.external_id = "Bamoa"
        rows = [{"PK_Sucursal": 10, "Sucursal": "Crucero"}, {"PK_Sucursal": 2, "Sucursal": "Bamoa"}]
        self.assertIs(self._service()._find_branch_row(
            stock_rows=rows, sucursal=self.sucursal, point_branch=self.point_branch,
        ), rows[1])
        for candidates in (rows[:1], [rows[1], {"PK_Sucursal": 3, "Sucursal": "Bamoa"}]):
            with self.subTest(rows=candidates), self.assertRaises(PointLiveInventoryLookupError):
                self._service()._find_branch_row(
                    stock_rows=candidates, sucursal=self.sucursal, point_branch=self.point_branch,
                )

    def test_legacy_erp_name_match_requires_one_candidate(self):
        rows = [{"PK_Sucursal": 2, "Sucursal": "Sucursal Bamoa"}]
        self.assertIs(self._service()._find_branch_row(
            stock_rows=rows, sucursal=self.sucursal, point_branch=None,
        ), rows[0])
        with self.assertRaises(PointLiveInventoryLookupError):
            self._service()._find_branch_row(
                stock_rows=rows + [{"PK_Sucursal": 10, "Sucursal": "Crucero"}],
                sucursal=self.sucursal, point_branch=None,
            )

    def test_blank_point_name_does_not_match_a_row_without_source_identity(self):
        self.point_branch.external_id = "Bamoa"
        self.point_branch.name = " "
        with self.assertRaises(PointLiveInventoryLookupError):
            self._service()._find_branch_row(
                stock_rows=[{"Cantidad": 3}], sucursal=self.sucursal, point_branch=self.point_branch,
            )

    @patch("pos_bridge.services.live_inventory_lookup_service.point_account_session_lock")
    def test_live_lookup_does_not_open_a_session_while_monthly_sync_owns_point(self, session_lock):
        session_lock.return_value.__enter__.return_value = False

        with self.assertRaisesMessage(PointLiveInventoryLookupError, "sincronización"):
            self._service().get_stock(
                product_codes=["PASTEL-1"],
                sucursal=self.sucursal,
                point_branch=self.point_branch,
            )

        self.client_factory.assert_not_called()
