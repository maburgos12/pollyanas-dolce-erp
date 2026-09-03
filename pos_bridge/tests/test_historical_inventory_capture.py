from datetime import date
from decimal import Decimal

from django.test import SimpleTestCase, TestCase

from core.models import Sucursal
from pos_bridge.management.commands.capture_point_historical_closing import (
    select_default_branches,
    select_default_products,
)
from pos_bridge.models import (
    PointBranch,
    PointHistoricalInventoryClosing,
    PointInventorySnapshot,
    PointProduct,
    PointSyncJob,
)
from pos_bridge.services.historical_inventory_capture import (
    HistoricalInventoryCaptureError,
    HistoricalPointInventoryClosingCapture,
    resolve_stock_at_close,
)
from recetas.models import Receta


class HistoricalStockResolutionTests(SimpleTestCase):
    def test_uses_latest_movement_inside_operational_close_date(self):
        history = [
            {"Fecha": "2026-08-01T01:00:00", "FK_Movimiento": 30, "Existencia_anterior": 3,
             "Existencia_nueva": 2, "Cancelado": False},
            {"Fecha": "2026-07-31T23:40:42.913", "FK_Movimiento": 20, "Existencia_anterior": 4,
             "Existencia_nueva": 3, "Cancelado": False},
            {"Fecha": "2026-07-31T08:00:00", "FK_Movimiento": 10, "Existencia_anterior": 5,
             "Existencia_nueva": 4, "Cancelado": False},
        ]

        result = resolve_stock_at_close(history, operational_date=date(2026, 7, 31))

        self.assertEqual(result.stock, Decimal("3"))
        self.assertEqual(result.evidence["method"], "latest_movement_at_or_before_close")
        self.assertEqual(result.evidence["movement_id"], 20)

    def test_uses_opening_of_first_later_movement_when_full_history_is_available(self):
        history = [
            {"Fecha": "2026-08-03T10:00:00", "FK_Movimiento": 40, "Existencia_anterior": 7,
             "Existencia_nueva": 6, "Cancelado": False},
            {"Fecha": "2026-08-01T09:00:00", "FK_Movimiento": 30, "Existencia_anterior": 8,
             "Existencia_nueva": 7, "Cancelado": False},
        ]

        result = resolve_stock_at_close(history, operational_date=date(2026, 7, 31), history_limit=500)

        self.assertEqual(result.stock, Decimal("8"))
        self.assertEqual(result.evidence["method"], "opening_before_first_later_movement")
        self.assertEqual(result.evidence["movement_id"], 30)

    def test_rejects_truncated_history_that_does_not_reach_the_close(self):
        history = [
            {"Fecha": f"2026-08-{(index % 28) + 1:02d}T09:00:00", "FK_Movimiento": index,
             "Existencia_anterior": 2, "Existencia_nueva": 1, "Cancelado": False}
            for index in range(500)
        ]

        with self.assertRaisesMessage(HistoricalInventoryCaptureError, "no alcanza el cierre"):
            resolve_stock_at_close(history, operational_date=date(2026, 7, 31), history_limit=500)

    def test_empty_history_is_zero_only_when_point_current_stock_confirms_zero(self):
        result = resolve_stock_at_close(
            [], operational_date=date(2026, 7, 31), current_stock=Decimal("0")
        )

        self.assertEqual(result.stock, Decimal("0"))
        self.assertEqual(result.evidence["method"], "no_history_current_zero")

        with self.assertRaisesMessage(HistoricalInventoryCaptureError, "sin historial"):
            resolve_stock_at_close(
                [], operational_date=date(2026, 7, 31), current_stock=Decimal("2")
            )

    def test_rejects_cancelled_boundary_movement(self):
        with self.assertRaisesMessage(HistoricalInventoryCaptureError, "cancelado"):
            resolve_stock_at_close(
                [{"Fecha": "2026-07-31T23:00:00", "FK_Movimiento": 99,
                  "Existencia_anterior": 3, "Existencia_nueva": 2, "Cancelado": True}],
                operational_date=date(2026, 7, 31),
            )


class _FakePointClient:
    def __init__(self, history_by_key, current_by_product, *, current_failures=0):
        self.history_by_key = history_by_key
        self.current_by_product = current_by_product
        self.current_failures = current_failures
        self.login_calls = 0

    def login(self):
        self.login_calls += 1

    def get_stock_history(self, product_id, branch_id, *, movements=500):
        return self.history_by_key[(str(branch_id), str(product_id))]

    def get_product_stock(self, product_id):
        if self.current_failures:
            self.current_failures -= 1
            raise HistoricalInventoryCaptureError("respuesta de sesión inesperada")
        return self.current_by_product[str(product_id)]


class HistoricalInventoryCapturePersistenceTests(TestCase):
    def setUp(self):
        erp = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        self.branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=erp)
        self.product = PointProduct.objects.create(external_id="857", sku="P-857", name="Producto")

    def test_complete_manifest_is_saved_verified_and_is_idempotent(self):
        client = _FakePointClient(
            history_by_key={("1", "857"): [
                {"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 123, "Movimiento": "VENTA",
                 "Existencia_anterior": 4, "Existencia_nueva": 3, "Cancelado": False}
            ]},
            current_by_product={"857": [{"PK_Sucursal": 1, "Cantidad": 2}]},
        )
        capture = HistoricalPointInventoryClosingCapture(client=client)

        first = capture.capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product]
        )
        second = capture.capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product]
        )

        self.assertEqual(first.closing.status, PointHistoricalInventoryClosing.STATUS_VERIFIED)
        self.assertEqual(first.closing.lines.get().stock, Decimal("3"))
        self.assertEqual(first.closing.pk, second.closing.pk)
        self.assertEqual(PointHistoricalInventoryClosing.objects.count(), 1)
        self.assertEqual(client.login_calls, 2)

    def test_unresolved_manifest_is_saved_as_draft_not_verified(self):
        client = _FakePointClient(
            history_by_key={("1", "857"): []},
            current_by_product={"857": [{"PK_Sucursal": 1, "Cantidad": 2}]},
        )

        result = HistoricalPointInventoryClosingCapture(client=client).capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product]
        )

        self.assertEqual(result.closing.status, PointHistoricalInventoryClosing.STATUS_DRAFT)
        self.assertEqual(result.closing.lines.count(), 0)
        self.assertEqual(result.unresolved_count, 1)
        self.assertEqual(result.closing.metadata["unresolved"][0]["branch_external_id"], "1")

    def test_relogs_once_when_point_session_expires_mid_capture(self):
        client = _FakePointClient(
            history_by_key={("1", "857"): [
                {"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 123,
                 "Existencia_anterior": 4, "Existencia_nueva": 3, "Cancelado": False}
            ]},
            current_by_product={"857": [{"PK_Sucursal": 1, "Cantidad": 2}]},
            current_failures=1,
        )

        result = HistoricalPointInventoryClosingCapture(client=client).capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product]
        )

        self.assertEqual(result.closing.status, PointHistoricalInventoryClosing.STATUS_VERIFIED)
        self.assertEqual(client.login_calls, 2)

    def test_persistent_product_stock_failure_creates_draft_instead_of_aborting(self):
        client = _FakePointClient(
            history_by_key={},
            current_by_product={},
            current_failures=2,
        )

        result = HistoricalPointInventoryClosingCapture(client=client).capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product]
        )

        self.assertEqual(result.closing.status, PointHistoricalInventoryClosing.STATUS_DRAFT)
        self.assertEqual(result.unresolved_count, 1)
        self.assertIn("respuesta de sesión", result.closing.metadata["unresolved"][0]["reason"])


class HistoricalInventoryCaptureManifestTests(TestCase):
    def test_defaults_use_numeric_network_branches_and_latest_mapped_snapshot_products(self):
        matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        cedis, _ = Sucursal.objects.get_or_create(codigo="CEDIS", defaults={"nombre": "CEDIS"})
        PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=matriz)
        PointBranch.objects.create(external_id="Matriz", name="Matriz alias", erp_branch=matriz)
        PointBranch.objects.create(external_id="8", name="CEDIS", erp_branch=cedis)
        mapped = PointProduct.objects.create(external_id="857", sku="P-857", name="Producto mapeado")
        unmapped = PointProduct.objects.create(external_id="999", sku="P-999", name="Sin receta")
        Receta.objects.create(
            nombre="Producto mapeado",
            codigo_point="P-857",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="producto-mapeado",
        )
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        for product in (mapped, unmapped):
            PointInventorySnapshot.objects.create(branch=PointBranch.objects.get(external_id="1"), product=product, sync_job=job)

        branches = select_default_branches(date(2026, 7, 31))
        products = select_default_products()

        self.assertEqual([branch.external_id for branch in branches], ["1", "8"])
        self.assertEqual([product.external_id for product in products], ["857"])
