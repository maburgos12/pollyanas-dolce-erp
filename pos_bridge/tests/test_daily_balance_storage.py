from datetime import datetime, timedelta
from decimal import Decimal
from io import StringIO
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test import TestCase

from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob


class PointDailyBalanceStorageTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(external_id="GM", name="Guamuchil")
        self.product = PointProduct.objects.create(external_id="P1", name="Pastel fresa chico")
        self.job = PointSyncJob.objects.create()
        self.opened = datetime(2025, 1, 15, 8, tzinfo=ZoneInfo("America/Mazatlan"))

    def snapshot(self, when, stock):
        return PointInventorySnapshot(
            branch=self.branch, product=self.product, sync_job=self.job,
            captured_at=when, stock=stock, min_stock=0, max_stock=15,
            raw_payload={"row": [str(stock)]},
        )

    def physical_state_ids(self, ids):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT state_id, stock FROM pos_bridge_inventory_cuts
                WHERE id = ANY (%s) ORDER BY id
            """, [ids])
            return cursor.fetchall()

    def test_new_writes_reuse_exact_states_but_keep_independent_cut_ids_and_times(self):
        observations = PointInventorySnapshot.objects.bulk_create([
            self.snapshot(self.opened, 5),
            self.snapshot(self.opened + timedelta(hours=6), 5),
            self.snapshot(self.opened + timedelta(hours=13), 2),
            self.snapshot(self.opened + timedelta(days=1), 5),
        ])
        ids = [row.pk for row in observations]
        points = self.physical_state_ids(ids)
        self.assertEqual(points[0][0], points[1][0])
        self.assertEqual(len({state for state, _ in points}), 3)
        self.assertTrue(all(state is not None and raw is None for state, raw in points))
        cuts = PointInventorySnapshot.objects.filter(pk__in=ids).order_by("id")
        self.assertEqual(list(cuts.values_list("id", "captured_at", "stock")), [
            (row.pk, row.captured_at, Decimal(row.stock)) for row in observations
        ])

        cuts.filter(pk=observations[1].id).update(stock=2)
        self.assertEqual(self.physical_state_ids(ids)[1], self.physical_state_ids(ids)[2])
        self.assertEqual(cuts.first().stock, 5)

    def test_backfill_preserves_old_row_then_becomes_idempotent(self):
        row = self.snapshot(self.opened, 5)
        row.save()
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE pos_bridge_inventory_cuts
                SET state_id = NULL, stock = 5, min_stock = 0, max_stock = 15
                WHERE id = %s
            """, [row.pk])
        expected = PointInventorySnapshot.objects.filter(pk=row.pk).values().get()
        output = StringIO()
        call_command("compact_point_inventory_balances", "2025-01-15", apply=True, stdout=output)
        self.assertIn("updated=1", output.getvalue())
        self.assertEqual(PointInventorySnapshot.objects.filter(pk=row.pk).values().get(), expected)
        self.assertIsNone(self.physical_state_ids([row.pk])[0][1])
        output = StringIO()
        call_command("compact_point_inventory_balances", "2025-01-15", apply=True, stdout=output)
        self.assertIn("updated=0", output.getvalue())

    def test_failed_reconstruction_leaves_the_original_balance(self):
        row = self.snapshot(self.opened, 5)
        row.save()
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE pos_bridge_inventory_cuts
                SET state_id = NULL, stock = 5, min_stock = 0, max_stock = 15
                WHERE id = %s
            """, [row.pk])
        with patch.object(PointInventorySnapshot.objects, "filter") as reader:
            reader.return_value.order_by.return_value.values_list.return_value = []
            with self.assertRaisesMessage(CommandError, "no coinciden"):
                call_command("compact_point_inventory_balances", "2025-01-15", apply=True)
        self.assertEqual(self.physical_state_ids([row.pk]), [(None, Decimal(5))])
