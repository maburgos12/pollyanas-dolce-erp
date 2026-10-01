from datetime import timedelta
from decimal import Decimal

from django.db import connection
from django.db.models import Max
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import Sucursal
from crm.services.pickup import PickupAvailabilityService
from crm.services.sucursal_resolution import SucursalResolverService, resolve_sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob


class SnapshotBranchResolutionTests(TestCase):
    def setUp(self):
        self.now = timezone.now()
        self.sucursal = Sucursal.objects.create(codigo="BAMOA", nombre="Bamoa")
        self.first = PointBranch.objects.create(external_id="2", name="Bamoa", erp_branch=self.sucursal)
        self.alias = PointBranch.objects.create(external_id="BAMOA", name="Bamoa alias", erp_branch=self.sucursal)
        self.product = PointProduct.objects.create(external_id="101", sku="0101", name="Pastel Fresas Chico")
        self.other_product = PointProduct.objects.create(external_id="145", sku="0145", name="Vaso Fresas Chico")
        self.job = PointSyncJob.objects.create(job_type="inventory", status="SUCCESS")
        self.resolver = SucursalResolverService()
        self.pickup = PickupAvailabilityService()

    def snapshot(self, branch, captured_at, product=None):
        return PointInventorySnapshot.objects.create(
            branch=branch, product=product or self.product, sync_job=self.job,
            captured_at=captured_at, stock=Decimal("7"))

    def resolver_choice(self):
        return self.resolver._pick_best_point_branch(list(PointBranch.objects.filter(erp_branch=self.sucursal)))

    def expected_choice(self, active_first):
        branches = list(PointBranch.objects.filter(erp_branch=self.sucursal))
        latest = {row["branch_id"]: row["latest"] for row in PointInventorySnapshot.objects.filter(
            branch_id__in=[branch.id for branch in branches]).values("branch_id").annotate(latest=Max("captured_at"))}
        def key(branch):
            rank = (latest.get(branch.id) is not None, latest.get(branch.id) or self.resolver.MIN_DATETIME,
                    branch.last_seen_at or self.resolver.MIN_DATETIME,
                    branch.updated_at or self.resolver.MIN_DATETIME, branch.id)
            return (branch.status == "ACTIVE", *rank) if active_first else rank
        return max(branches, key=key)

    def test_both_choices_use_bounded_index_friendly_latest_reads(self):
        self.snapshot(self.first, self.now)
        for choose in (self.resolver_choice, lambda: self.pickup._resolve_point_branch(self.sucursal)):
            with self.subTest(choose=choose):
                with CaptureQueriesContext(connection) as queries:
                    self.assertEqual(choose().id, self.first.id)
                snapshots = [query["sql"] for query in queries if "pos_bridge_inventory_snapshots" in query["sql"]]
                self.assertEqual(len(snapshots), 2)
                for sql in snapshots:
                    self.assertNotIn("MAX(", sql.upper())
                    self.assertNotIn("GROUP BY", sql.upper())
                    self.assertIn("LIMIT 1", sql.upper())
                    self.assertIn("ORDER BY", sql.upper())
                    self.assertIn("DESC", sql.upper())
                    self.assertIn('"captured_at" IS NOT NULL', sql)
                    self.assertNotIn('"id"', sql)

    def test_active_priority_remains_distinct_from_pickup_snapshot_priority(self):
        self.snapshot(self.first, self.now - timedelta(days=1))
        self.snapshot(self.alias, self.now)
        PointBranch.objects.filter(pk=self.alias.id).update(status="INACTIVE")
        self.assertEqual(self.resolver_choice().id, self.first.id)
        self.assertEqual(self.pickup._resolve_point_branch(self.sucursal).id, self.alias.id)
        self.assertEqual(self.resolver_choice().id, self.expected_choice(True).id)
        self.assertEqual(self.pickup._resolve_point_branch(self.sucursal).id, self.expected_choice(False).id)

    def test_latest_capture_uses_all_products_not_only_requested_product(self):
        self.snapshot(self.first, self.now - timedelta(days=2))
        self.snapshot(self.alias, self.now - timedelta(days=1))
        self.snapshot(self.first, self.now, product=self.other_product)
        self.assertEqual(self.resolver_choice().id, self.first.id)
        self.assertEqual(self.pickup._resolve_point_branch(self.sucursal).id, self.first.id)

    def test_ties_empty_snapshots_and_null_last_seen_preserve_original_key(self):
        PointBranch.objects.filter(erp_branch=self.sucursal).update(last_seen_at=None, updated_at=self.now)
        for choice in (self.resolver_choice(), self.pickup._resolve_point_branch(self.sucursal)):
            self.assertEqual(choice.id, self.alias.id)
        self.snapshot(self.first, self.now)
        self.snapshot(self.first, self.now, product=self.other_product)
        self.snapshot(self.alias, self.now)
        for choose, active_first in ((self.resolver_choice, True), (lambda: self.pickup._resolve_point_branch(self.sucursal), False)):
            self.assertEqual(choose().id, self.expected_choice(active_first).id)
        PointBranch.objects.filter(pk=self.first.id).update(last_seen_at=self.now + timedelta(minutes=1))
        self.assertEqual(self.resolver_choice().id, self.first.id)
        self.assertEqual(self.pickup._resolve_point_branch(self.sucursal).id, self.first.id)

    def test_historical_crucero_capture_never_participates_in_bamoa(self):
        historical = Sucursal.objects.create(codigo="CRUCERO", nombre="Crucero histórico")
        historical_point = PointBranch.objects.create(external_id="4", name="Crucero", erp_branch=historical)
        self.snapshot(historical_point, self.now + timedelta(days=1))
        self.snapshot(self.first, self.now)
        self.assertEqual(resolve_sucursal("BAMOA").point_branch.id, self.first.id)
        self.assertEqual(self.pickup._resolve_point_branch(self.sucursal).id, self.first.id)

    def test_sucursal_without_point_candidates_remains_none(self):
        empty = Sucursal.objects.create(codigo="EMPTY", nombre="Sin Point")
        self.assertIsNone(self.resolver._resolve_point_branch_for_sucursal(empty))
        self.assertIsNone(self.pickup._resolve_point_branch(empty))
