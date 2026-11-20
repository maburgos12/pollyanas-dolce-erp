from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.db import IntegrityError, connection, transaction
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.views import plan


# Reference query predating the indexed latest-pair lookup; kept for exact parity.
ORIGINAL_SQL = """
            WITH ranked_snapshots AS (
                SELECT
                    branch.erp_branch_id AS erp_branch_id,
                    snapshot.stock AS stock,
                    snapshot.captured_at AS captured_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY snapshot.branch_id, snapshot.product_id
                        ORDER BY snapshot.captured_at DESC, snapshot.id DESC
                    ) AS rn
                FROM pos_bridge_inventory_snapshots AS snapshot
                INNER JOIN pos_bridge_branches AS branch
                    ON snapshot.branch_id = branch.id
                INNER JOIN core_sucursal AS sucursal
                    ON branch.erp_branch_id = sucursal.id
                WHERE snapshot.captured_at < %s
                  AND branch.erp_branch_id IS NOT NULL
                  AND sucursal.activa
            )
            SELECT
                erp_branch_id,
                COALESCE(SUM(stock), 0) AS total_stock,
                MAX(captured_at) AS latest_snapshot_at
            FROM ranked_snapshots
            WHERE rn = 1
            GROUP BY erp_branch_id
            """


class DgInventoryAsOfTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.opening = datetime(2026, 10, 31, tzinfo=ZoneInfo("America/Mazatlan"))
        cls.closing = datetime(2026, 11, 1, 12, tzinfo=ZoneInfo("America/Mazatlan"))
        cls.erp = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        cls.empty_erp = Sucursal.objects.create(codigo="NO_HISTORY", nombre="Sin historial", activa=True)
        cls.future_erp = Sucursal.objects.create(codigo="FUTURE", nombre="Solo futuro", activa=True)
        inactive_erp = Sucursal.objects.create(codigo="INACTIVE", nombre="Inactiva", activa=False)
        cls.branch = PointBranch.objects.create(external_id="main", name="Main", erp_branch=cls.erp)
        alias = PointBranch.objects.create(external_id="alias", name="Alias", erp_branch=cls.erp, status=PointBranch.STATUS_INACTIVE)
        PointBranch.objects.create(external_id="empty", name="Empty", erp_branch=cls.empty_erp)
        cls.future_branch = PointBranch.objects.create(external_id="future", name="Future", erp_branch=cls.future_erp)
        inactive = PointBranch.objects.create(external_id="inactive", name="Inactive", erp_branch=inactive_erp)
        unmapped = PointBranch.objects.create(external_id="unmapped", name="Unmapped")
        cls.products = [PointProduct.objects.create(external_id=str(i), sku=str(i), name=str(i), active=i != 2) for i in range(6)]
        cls.job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)

        def snapshot(branch, product, stock, captured):
            return PointInventorySnapshot.objects.create(branch=branch, product=product, stock=Decimal(stock), captured_at=captured, sync_job=cls.job)

        snapshot(cls.branch, cls.products[0], "70", cls.opening - timedelta(days=17))
        snapshot(cls.branch, cls.products[0], "0", cls.opening - timedelta(hours=1))
        snapshot(cls.branch, cls.products[1], "-2.125", cls.opening - timedelta(hours=4))
        snapshot(cls.branch, cls.products[2], "3.250", cls.opening - timedelta(hours=3))
        snapshot(cls.branch, cls.products[3], "4.875", cls.opening - timedelta(days=500))
        snapshot(cls.branch, cls.products[4], "5", cls.opening - timedelta(hours=2))
        snapshot(cls.branch, cls.products[4], "6", cls.opening - timedelta(hours=2))
        snapshot(alias, cls.products[0], "8", cls.opening - timedelta(hours=5))
        snapshot(cls.branch, cls.products[0], "100", cls.opening)
        snapshot(cls.branch, cls.products[0], "2", cls.closing - timedelta(seconds=1))
        snapshot(cls.branch, cls.products[0], "777", cls.closing)
        snapshot(cls.future_branch, cls.products[0], "99", cls.closing)
        snapshot(inactive, cls.products[0], "1000", cls.opening - timedelta(hours=1))
        snapshot(unmapped, cls.products[0], "1000", cls.opening - timedelta(hours=1))

    @staticmethod
    def original(*, before_dt, recent_cutoff=None):
        with connection.cursor() as cursor:
            cursor.execute(ORIGINAL_SQL, [before_dt])
            rows = cursor.fetchall()
        return (
            {branch_id: stock for branch_id, stock, captured in rows},
            {branch_id for branch_id, stock, captured in rows if recent_cutoff is not None and captured >= recent_cutoff},
        )

    def test_complete_results_match_original_for_temporal_and_recency_boundaries(self):
        cuts = [self.opening - timedelta(days=600), self.opening, self.opening + timedelta(microseconds=1), self.closing, self.closing + timedelta(microseconds=1)]
        for before in cuts:
            for recent in (None, self.opening - timedelta(hours=2), self.opening, self.closing):
                with self.subTest(before=before, recent=recent):
                    self.assertEqual(plan._latest_point_inventory_branch_totals(before_dt=before, recent_cutoff=recent), self.original(before_dt=before, recent_cutoff=recent))

    def test_keeps_last_zero_negative_inactive_product_and_point_alias(self):
        self.assertEqual(plan._latest_point_inventory_branch_totals(before_dt=self.opening), ({self.erp.pk: Decimal("20.000")}, set()))
        self.assertEqual(plan._latest_point_inventory_branch_totals(before_dt=self.closing, recent_cutoff=self.opening), ({self.erp.pk: Decimal("22.000")}, {self.erp.pk}))
        self.assertEqual(plan._latest_point_inventory_branch_totals(before_dt=self.closing + timedelta(microseconds=1))[0], {self.erp.pk: Decimal("797.000"), self.future_erp.pk: Decimal("99.000")})

    def test_catalog_pairs_without_history_do_not_create_zero_branches(self):
        totals, recent = plan._latest_point_inventory_branch_totals(before_dt=self.opening, recent_cutoff=self.opening - timedelta(hours=2))
        self.assertNotIn(self.empty_erp.pk, totals)
        self.assertNotIn(self.future_erp.pk, totals)
        self.assertEqual(recent, {self.erp.pk})
        self.assertTrue(all(isinstance(stock, Decimal) for stock in totals.values()))

    def test_product_identity_is_nonnull_and_enforced_by_postgresql(self):
        with self.assertRaises(IntegrityError), transaction.atomic():
            PointInventorySnapshot.objects.create(branch=self.branch, product_id=None, stock=0, captured_at=self.opening, sync_job=self.job)
        with self.assertRaises(IntegrityError), transaction.atomic():
            PointInventorySnapshot.objects.create(branch=self.branch, product_id=2**62, stock=0, captured_at=self.opening, sync_job=self.job)
            connection.check_constraints(table_names=[PointInventorySnapshot._meta.db_table])

    def test_lookup_stays_one_query_and_avoids_global_history_ranking(self):
        with CaptureQueriesContext(connection) as queries:
            plan._latest_point_inventory_branch_totals(before_dt=self.opening)
        self.assertEqual(len(queries), 1)
        with connection.cursor() as cursor:
            cursor.execute("EXPLAIN (FORMAT JSON) " + queries[0]["sql"])
            execution_plan = cursor.fetchone()[0][0]["Plan"]
        def node_types(node):
            return {node["Node Type"]}.union(*(node_types(child) for child in node.get("Plans", [])))
        self.assertNotIn("WindowAgg", node_types(execution_plan))
        self.assertIn("Limit", node_types(execution_plan))

    def test_closure_dto_matches_original_for_both_inventory_cutoffs(self):
        with patch.object(plan, "_latest_point_inventory_branch_totals", self.original):
            original = plan._build_point_closure_summary(self.opening.date())
        actual = plan._build_point_closure_summary(self.opening.date())
        self.assertTrue(actual["available"])
        self.assertEqual(actual, original)
