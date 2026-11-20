from datetime import datetime, timedelta, timezone
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import connection
from django.test.utils import CaptureQueriesContext
from rest_framework.test import APITestCase

from core.models import Sucursal
from pos_bridge.api.views.inventory import InventoryViewSet
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob


class InventoryCurrentScopeTests(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(username="inventory_scope")
        sede = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        cls.branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=sede)
        cls.other = PointBranch.objects.create(external_id="2", name="Otra sede")
        cls.old_job = PointSyncJob.objects.create(job_type="inventory", status="SUCCESS")
        cls.new_job = PointSyncJob.objects.create(job_type="inventory", status="SUCCESS")
        cls.stamp = datetime(2026, 11, 20, 1, 43, 37, 728213, tzinfo=timezone.utc)
        cls.product = PointProduct.objects.create(
            external_id="818", sku="0160", name="Bollo Lotus", category="Bollo",
        )
        cls.historical = PointProduct.objects.create(
            external_id="758", sku="CND000979", name="Vela colores", category="Alegria", active=False,
        )
        cls.snapshots = []
        cls._snapshot(cls.product, cls.branch, 0, cls.stamp - timedelta(days=7), cls.old_job)
        cls._snapshot(cls.product, cls.branch, 1, cls.stamp, cls.new_job)
        cls.winner = cls._snapshot(cls.product, cls.branch, 2, cls.stamp, cls.new_job)
        cls._snapshot(cls.product, cls.other, 5, cls.stamp, cls.new_job)
        cls._snapshot(cls.historical, cls.branch, 0, cls.stamp - timedelta(days=6), cls.old_job)
        for index in range(70):
            product = PointProduct.objects.create(
                external_id=f"history-{index}", sku=f"H{index:03}",
                name=f"Historico {index:03}", category="Historico", active=index % 2 == 0,
            )
            cls._snapshot(product, cls.branch, index % 3, cls.stamp - timedelta(days=8), cls.old_job)

    @classmethod
    def _snapshot(cls, product, branch, quantity, stamp, job):
        row = PointInventorySnapshot.objects.create(
            product=product, branch=branch, stock=quantity, min_stock=1, max_stock=8,
            captured_at=stamp, sync_job=job,
        )
        cls.snapshots.append(row)
        return row

    def setUp(self):
        self.client.force_authenticate(self.user)

    def request_current(self, params=None):
        response = self.client.get(
            "/api/pos-bridge/inventory/current/", params or {}, HTTP_ACCEPT="application/json",
        )
        self.assertEqual(response.status_code, 200)
        return response.json()

    def expected_products(self, params):
        """Business oracle: winner per pair, then the existing identity filters."""
        winners = {}
        for snap in self.snapshots:
            pair = (snap.branch_id, snap.product_id)
            previous = winners.get(pair)
            if previous is None or (snap.captured_at, snap.pk) > (previous.captured_at, previous.pk):
                winners[pair] = snap
        rows = []
        for snap in winners.values():
            product, branch = snap.product, snap.branch
            value = params.get("product", "").strip().lower()
            if value and not (
                value in product.name.lower() or value == product.sku.lower()
                or value == product.external_id.lower()
            ):
                continue
            value = params.get("branch", "").strip().lower()
            erp_code = branch.erp_branch.codigo if branch.erp_branch_id else ""
            if value and not (
                value in branch.name.lower() or value == branch.external_id.lower()
                or value == erp_code.lower()
            ):
                continue
            value = params.get("category", "").strip().lower()
            if value and value not in product.category.lower():
                continue
            rows.append(snap)
        grouped = {}
        for snap in sorted(rows, key=lambda row: (row.product.name, row.branch.name, row.pk)):
            entry = grouped.setdefault(snap.product_id, {
                "product_sku": snap.product.sku, "product_name": snap.product.name,
                "product_category": snap.product.category, "branches": [], "total_stock": Decimal(0),
            })
            entry["branches"].append({
                "branch": snap.branch.name, "branch_external_id": snap.branch.external_id,
                "erp_branch_code": snap.branch.erp_branch.codigo if snap.branch.erp_branch_id else "",
                "stock": float(snap.stock), "min_stock": float(snap.min_stock),
                "max_stock": float(snap.max_stock), "captured_at": snap.captured_at.isoformat(),
            })
            entry["total_stock"] += Decimal(snap.stock)
        result = list(grouped.values())
        for entry in result:
            entry["total_stock"] = f"{entry['total_stock']:.3f}"
        return result

    def test_filtered_latest_does_not_resolve_unrelated_pairs(self):
        with CaptureQueriesContext(connection) as captured:
            result = self.request_current({"product": "0160", "branch": "1", "category": "bol"})
        self.assertEqual(result["results"], self.expected_products({"product": "0160", "branch": "1"}))
        sql = [row["sql"] for row in captured if 'FROM "pos_bridge_inventory_snapshots"' in row["sql"]]
        self.assertEqual(len(sql), 1)
        with connection.cursor() as cursor:
            cursor.execute("EXPLAIN (ANALYZE, FORMAT JSON, TIMING FALSE) " + sql[0])
            plan = cursor.fetchone()[0][0]["Plan"]

        def nodes(node):
            yield node
            for child in node.get("Plans", []):
                yield from nodes(child)

        # Measure rows resolved by latest-per-pair, not noisy elapsed milliseconds.
        unique = [node for node in nodes(plan) if node["Node Type"] == "Unique"]
        self.assertTrue(unique)
        self.assertTrue(all(node["Actual Rows"] <= 1 for node in unique), unique)

    def test_identity_filters_keep_the_full_json_contract(self):
        for params in (
            {}, {"product": "0160"}, {"product": "818"}, {"product": "LoTuS"},
            {"branch": "1"}, {"branch": "mAtRiZ"}, {"branch": "sede"},
            {"category": "bOL"}, {"product": "0160", "branch": "1", "category": "bol"},
            {"product": " CND000979 ", "branch": " MATRIZ "},
            {"product": "unknown"}, {"branch": "unknown"}, {"category": "unknown"},
        ):
            with self.subTest(params=params):
                result = self.request_current(params)
                expected = self.expected_products(params)
                self.assertEqual(result, {"count": len(expected), "next": None, "previous": None, "results": expected})

    def test_timestamp_tie_uses_largest_id_and_preserves_old_zero(self):
        result = self.request_current({"product": "0160", "branch": "1"})["results"][0]
        self.assertEqual(result["branches"][0]["stock"], 2)
        self.assertEqual(result["branches"][0]["captured_at"], self.winner.captured_at.isoformat())
        old = self.request_current({"product": "CND000979", "branch": "1"})["results"][0]
        self.assertEqual(old["branches"][0]["stock"], 0)
        self.assertEqual(old["branches"][0]["captured_at"], (self.stamp - timedelta(days=6)).isoformat())

    def test_seventy_historical_pairs_and_inactive_products_remain(self):
        result = self.request_current({"category": "Historico"})
        self.assertEqual(result["count"], 70)
        self.assertTrue(all(row["branches"][0]["captured_at"] == (self.stamp - timedelta(days=8)).isoformat() for row in result["results"]))

    def test_product_or_filter_is_not_replaced_by_sku_only(self):
        extra = PointProduct.objects.create(external_id="0160", sku="OTHER", name="Otra coincidencia", category="Bollo")
        snap = PointInventorySnapshot.objects.create(product=extra, branch=self.other, stock=3, captured_at=self.stamp, sync_job=self.new_job)
        self.snapshots.append(snap)
        try:
            self.assertEqual(self.request_current({"product": "0160"})["results"], self.expected_products({"product": "0160"}))
        finally:
            self.snapshots.pop()

    def test_pagination_and_order_remain(self):
        result = self.request_current({"page_size": 10})
        self.assertEqual(result["count"], 72)
        self.assertEqual(result["results"], self.expected_products({})[:10])
        self.assertIsNotNone(result["next"])
        self.assertIsNone(result["previous"])
        second = self.request_current({"page_size": 10, "page": 2})
        self.assertEqual(second["results"], self.expected_products({})[10:20])

    def test_current_remains_one_query_for_many_products(self):
        with CaptureQueriesContext(connection) as captured:
            self.request_current()
        self.assertEqual(len(captured), 1)
        self.assertEqual(sum('FROM "pos_bridge_inventory_snapshots"' in row["sql"] for row in captured), 1)

    def test_scoped_queryset_stays_lazy_and_empty_is_not_a_fallback(self):
        view = InventoryViewSet()
        with self.assertNumQueries(0):
            queryset = view._latest_snapshot_qs(
                "1", scoped_queryset=view.get_queryset().filter(product=self.product),
            )
        self.assertEqual(list(queryset.values_list("pk", flat=True)), [self.winner.pk])
        self.assertFalse(view._latest_snapshot_qs(scoped_queryset=view.get_queryset().none()).exists())

    def test_list_and_retrieve_preserve_history_filters(self):
        response = self.client.get("/api/pos-bridge/inventory/", {"product": "818", "branch": "MATRIZ"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 3)
        self.assertEqual(response.json()["results"][0]["id"], self.winner.pk)
        old = self.snapshots[0]
        response = self.client.get(f"/api/pos-bridge/inventory/{old.pk}/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(datetime.fromisoformat(response.json()["captured_at"].replace("Z", "+00:00")), old.captured_at)

    def test_availability_and_low_stock_filter_latest_not_an_old_match(self):
        low = self.client.get("/api/pos-bridge/inventory/low-stock/", {"branch": "1"}).json()
        self.assertNotIn("0160", [row["product_sku"] for row in low["results"]])
        available = self.client.get("/api/pos-bridge/inventory/availability/", {"sku": "0160", "branch": "1"}).json()
        self.assertEqual(available["results"][0]["total_stock"], "2.000")
        self.assertTrue(available["results"][0]["available"])
        inactive = self.client.get("/api/pos-bridge/inventory/availability/", {"sku": "CND000979"}).json()
        self.assertEqual(inactive["count"], 0)
