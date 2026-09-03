from datetime import date
from decimal import Decimal

from django.db import IntegrityError
from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointInventorySnapshot,
    PointProduct,
)
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from recetas.models import Receta


class HistoricalInventoryClosingTests(TestCase):
    def setUp(self):
        self.recipe = Receta.objects.create(
            nombre="Pastel histórico exacto",
            codigo_point="HIST-1",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="pastel-historico-exacto",
        )
        self.product = PointProduct.objects.create(external_id="857", sku="HIST-1", name=self.recipe.nombre)
        self.branches = []
        for index in (1, 2):
            erp = Sucursal.objects.create(codigo=f"HIST-{index}", nombre=f"Histórica {index}")
            self.branches.append(PointBranch.objects.create(
                external_id=str(index), name=erp.nombre, erp_branch=erp,
            ))

    def closing(self, *, status=PointHistoricalInventoryClosing.STATUS_VERIFIED):
        return PointHistoricalInventoryClosing.objects.create(
            operational_date=date(2026, 7, 31),
            status=status,
            source=PointHistoricalInventoryClosing.SOURCE_STOCK_HISTORY,
            source_fingerprint="a" * 64,
            expected_branch_ids=[branch.id for branch in self.branches],
            expected_product_ids=[self.product.id],
        )

    def test_verified_closing_is_a_separate_exact_date_source_for_month_opening(self):
        closing = self.closing()
        for branch, stock in zip(self.branches, (Decimal("3"), Decimal("2"))):
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing,
                branch=branch,
                product=self.product,
                stock=stock,
                evidence={"method": "anchored_stock_history"},
            )

        values, meta, unresolved = MonthlyPointProductBalanceService()._load_opening(
            snapshot_date=date(2026, 7, 31)
        )

        self.assertEqual(PointInventorySnapshot.objects.count(), 0)
        self.assertEqual(values[self.recipe.id], (Decimal("5"), 2))
        self.assertEqual(meta["source"], "PointHistoricalInventoryClosing")
        self.assertEqual(meta["effective_date"], date(2026, 7, 31))
        self.assertTrue(meta["authoritative"])
        self.assertEqual(unresolved, [])

    def test_draft_or_incomplete_closing_is_not_used_as_authoritative_inventory(self):
        closing = self.closing(status=PointHistoricalInventoryClosing.STATUS_DRAFT)
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branches[0], product=self.product, stock=Decimal("3")
        )

        values, meta, _ = MonthlyPointProductBalanceService()._load_opening(
            snapshot_date=date(2026, 7, 31)
        )

        self.assertEqual(values, {})
        self.assertFalse(meta["source_present"])

    def test_branch_product_line_is_unique_inside_a_closing(self):
        closing = self.closing()
        PointHistoricalInventoryClosingLine.objects.create(
            closing=closing, branch=self.branches[0], product=self.product, stock=Decimal("3")
        )
        with self.assertRaises(IntegrityError):
            PointHistoricalInventoryClosingLine.objects.create(
                closing=closing, branch=self.branches[0], product=self.product, stock=Decimal("4")
            )
