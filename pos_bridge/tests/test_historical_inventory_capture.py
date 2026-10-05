from datetime import date, datetime, timezone as datetime_timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.db import connection
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
    historical_waste_effect,
)
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from recetas.models import Receta


class HistoricalStockResolutionTests(SimpleTestCase):
    def test_malformed_special_original_cost_returns_unknown_instead_of_aborting_batch(self):
        raw = {"FK_Movimiento": 1680267, "FK_Tipo_Movimiento": 5, "Movimiento": "MERMA",
               "Fecha": "2026-10-01T06:59:59Z", "Cantidad": 4, "Existencia_anterior": 4,
               "Existencia_nueva": 0, "Cancelado": True, "isCargo": True, "Costo_Unitario": "garbage"}
        row = SimpleNamespace(raw_payload=raw, movement_type="MERMA", cancelled=True)
        self.assertEqual(AuditStockHistoryService._validated_waste_effect(row, None).status, "INVALID")

    def test_historical_waste_contract_rejects_unproven_specials_and_preserves_ordinary(self):
        debit = {"FK_Movimiento": 1680267, "FK_Tipo_Movimiento": 5, "Movimiento": "MERMA", "Cantidad": 4,
                 "Existencia_anterior": 4, "Existencia_nueva": 0, "Cancelado": True, "isCargo": True}
        undo = {**debit, "FK_Tipo_Movimiento": 15, "Movimiento": "CANCELACION DE MERMA",
                "Existencia_anterior": 0, "Existencia_nueva": 4, "Cancelado": False, "isCargo": False}
        self.assertEqual(historical_waste_effect({**debit, "Cancelado": False}).status, "NO_SPECIAL")
        self.assertEqual(historical_waste_effect({**undo, "Movimiento": "MERMA"}).status, "INVALID")
        for mutation in ({"FK_Tipo_Movimiento": True}, {"FK_Tipo_Movimiento": "5"},
                         {"FK_Tipo_Movimiento": 5.0}, {"isCargo": False}, {"isCargo": 1},
                         {"Cantidad": 0}, {"Cantidad": -4}, {"Cantidad": True},
                         {"Cantidad": "NaN"}, {"Existencia_nueva": 1}, {"isInsumo": True},
                         {"isInsumo": "true"}, {"isInsumo": 0}):
            with self.subTest(mutation=mutation):
                self.assertEqual(historical_waste_effect({**debit, **mutation}).status, "INVALID")
        for mutation in ({"FK_Tipo_Movimiento": 5}, {"Movimiento": "CANCELACION MERMA"},
                         {"Cancelado": True}, {"isCargo": True}, {"Existencia_nueva": 3}):
            with self.subTest(mutation=mutation):
                self.assertEqual(historical_waste_effect({**undo, **mutation}).status, "INVALID")
        self.assertEqual(historical_waste_effect({**debit, "Cancelado": "true", "isCargo": "true"}).status, "VALID")
        self.assertEqual(historical_waste_effect({key: value for key, value in debit.items() if key != "isCargo"}).status, "VALID")

    def test_special_waste_boundary_requires_exact_original_movement_identity(self):
        debit = {"FK_Movimiento": 1680267, "FK_Tipo_Movimiento": 5, "Movimiento": "MERMA",
                 "Fecha": "2026-10-01T06:59:59Z", "Cantidad": 4,
                 "Existencia_anterior": 4, "Existencia_nueva": 0, "Cancelado": True, "isCargo": True}
        for identity in (1680267.9, True, "１６８０２６７", None, 0, -1):
            with self.subTest(identity=identity):
                raw = {**debit, "FK_Movimiento": identity}
                self.assertEqual(historical_waste_effect(raw).status, "INVALID")
                with self.assertRaises(HistoricalInventoryCaptureError):
                    resolve_stock_at_close([raw], operational_date=date(2026, 9, 30))
        self.assertEqual(historical_waste_effect({**debit, "FK_Movimiento": "1680267"}).status, "VALID")

    def test_cancelled_waste_boundary_preserves_historical_debit(self):
        debit = {"FK_Movimiento": 1680267, "FK_Tipo_Movimiento": 5, "Movimiento": "MERMA",
                 "Fecha": "2026-10-01T06:59:59Z", "Cantidad": 4,
                 "Existencia_anterior": 4, "Existencia_nueva": 0, "Cancelado": True, "isCargo": True}
        result = resolve_stock_at_close([debit], operational_date=date(2026, 9, 30))
        self.assertEqual(result.stock, Decimal("0"))
        later = {**debit, "Fecha": "2026-10-01T07:00:00Z"}
        result = resolve_stock_at_close([later], operational_date=date(2026, 9, 30))
        self.assertEqual(result.stock, Decimal("4"))

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

        # Stock's frontend reads naive Fecha with moment.utc, not local time.
        self.assertEqual(result.stock, Decimal("2"))
        self.assertEqual(result.evidence["method"], "latest_movement_at_or_before_close")
        self.assertEqual(result.evidence["movement_id"], 30)

    def test_utc_midnight_is_previous_operational_day_without_rewriting_raw(self):
        history = [{"Fecha": "2026-10-01T02:01:31.863", "FK_Movimiento": 1686312,
                    "Existencia_anterior": 2, "Existencia_nueva": 0, "Cancelado": False},
                   {"Fecha": "2026-10-01T07:00:00Z", "FK_Movimiento": 1687000,
                    "Existencia_anterior": 0, "Existencia_nueva": 1, "Cancelado": False}]
        result = resolve_stock_at_close(history, operational_date=date(2026, 9, 30))
        self.assertEqual(result.stock, Decimal("0"))
        self.assertEqual(result.evidence["movement_id"], 1686312)
        self.assertEqual(history[0]["Fecha"], "2026-10-01T02:01:31.863")

    def test_stock_instant_preserves_explicit_offset_and_rejects_invalid_raw(self):
        from pos_bridge.services.historical_inventory_capture import point_stock_history_instant
        expected = datetime(2026, 10, 1, 2, tzinfo=datetime_timezone.utc)
        self.assertEqual(point_stock_history_instant({"Fecha": "2026-10-01T02:00:00"}), expected)
        self.assertEqual(point_stock_history_instant({"Fecha": "2026-09-30T19:00:00-07:00"}), expected)
        for raw in ({}, {"Fecha": "not-a-date"}):
            with self.assertRaises(HistoricalInventoryCaptureError):
                point_stock_history_instant(raw)

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
        self.history_calls = []
        self.product_stock_calls = []

    def login(self):
        self.login_calls += 1

    def get_stock_history(self, product_id, branch_id, *, movements=500):
        self.history_calls.append((str(branch_id), str(product_id)))
        return self.history_by_key[(str(branch_id), str(product_id))]

    def get_product_stock(self, product_id):
        self.product_stock_calls.append(str(product_id))
        if self.current_failures:
            self.current_failures -= 1
            raise HistoricalInventoryCaptureError("respuesta de sesión inesperada")
        return self.current_by_product[str(product_id)]


class HistoricalInventoryCapturePersistenceTests(TestCase):
    def setUp(self):
        erp = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        self.branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=erp)
        self.product = PointProduct.objects.create(external_id="857", sku="P-857", name="Producto")

    def test_closing_reuses_and_retains_canonical_history_for_auditor(self):
        client = _FakePointClient({("1", "857"): [
            {"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 123,
             "Movimiento": "VENTA", "Cantidad": 1,
             "Existencia_anterior": 4, "Existencia_nueva": 3, "Cancelado": False},
        ]}, {"857": [{"PK_Sucursal": 1, "Cantidad": 3}]})
        audit = AuditStockHistoryService(client=client)
        audit.capture(self.branch, self.product, date(2026, 7, 1))
        result = HistoricalPointInventoryClosingCapture(client=client).capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product])
        self.assertEqual(result.closing.lines.get().stock, Decimal("3"))
        self.assertEqual(client.history_calls, [("1", "857")])
        self.assertEqual(audit.reconcile(self.branch, self.product, date(2026, 7, 1)).sales, Decimal("1"))

    def test_old_retained_rows_cannot_fill_gap_in_latest_truncated_history(self):
        client = _FakePointClient({("1", "857"): [
            {"Fecha": "2026-08-31T22:00:00", "FK_Movimiento": 123,
             "Existencia_anterior": 4, "Existencia_nueva": 3, "Cancelado": False},
        ]}, {"857": [{"PK_Sucursal": 1, "Cantidad": 2}]})
        audit = AuditStockHistoryService(client=client)
        audit.capture(self.branch, self.product, date(2026, 8, 1), force=True)
        client.history_by_key[("1", "857")] = [
            {"Fecha": "2026-10-02T01:00:00", "FK_Movimiento": 1000 + index,
             "Existencia_anterior": 3, "Existencia_nueva": 2, "Cancelado": False}
            for index in range(500)]
        audit.capture(self.branch, self.product, date(2026, 9, 1), force=True)
        self.assertEqual(audit.reconcile(self.branch, self.product, date(2026, 9, 1)).coverage_status, "INCOMPLETE")
        result = HistoricalPointInventoryClosingCapture(client=client).capture(
            operational_date=date(2026, 9, 30), branches=[self.branch], products=[self.product])
        self.assertEqual(result.closing.status, PointHistoricalInventoryClosing.STATUS_DRAFT)
        self.assertEqual(result.closing.lines.count(), 0)

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
        self.assertEqual(client.login_calls, 1)

    def test_capture_locks_operational_month_inside_persistence_transaction(self):
        client = _FakePointClient(
            history_by_key={
                ("1", "857"): [
                    {
                        "Fecha": "2026-07-31T22:00:00",
                        "FK_Movimiento": 123,
                        "Movimiento": "VENTA",
                        "Existencia_anterior": 4,
                        "Existencia_nueva": 3,
                        "Cancelado": False,
                    }
                ]
            },
            current_by_product={"857": [{"PK_Sucursal": 1, "Cantidad": 2}]},
        )
        baseline_depth = len(connection.atomic_blocks)
        acquired_months = []
        lock_depths = []
        persistence_saw_lock = []
        real_get_or_create = PointHistoricalInventoryClosing.objects.get_or_create

        def acquire(values):
            lock_depths.append(len(connection.atomic_blocks))
            result = lock_product_month_sources(values)
            acquired_months.extend(result)
            return result

        def get_or_create_after_lock(*args, **kwargs):
            persistence_saw_lock.append(bool(acquired_months))
            return real_get_or_create(*args, **kwargs)

        with (
            patch(
                "pos_bridge.services.historical_inventory_capture.lock_product_month_sources",
                side_effect=acquire,
            ),
            patch.object(
                PointHistoricalInventoryClosing.objects,
                "get_or_create",
                side_effect=get_or_create_after_lock,
            ),
        ):
            HistoricalPointInventoryClosingCapture(client=client).capture(
                operational_date=date(2026, 7, 31),
                branches=[self.branch],
                products=[self.product],
            )

        self.assertEqual(acquired_months, [date(2026, 7, 1)])
        self.assertEqual(lock_depths, [baseline_depth + 1])
        self.assertEqual(persistence_saw_lock, [True])

    def test_verified_closing_adds_only_missing_branch_and_preserves_prior_lines(self):
        client = _FakePointClient({("1", "857"): [
            {"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 123,
             "Existencia_anterior": 4, "Existencia_nueva": 3, "Cancelado": False},
        ]}, {"857": [{"PK_Sucursal": 1, "Cantidad": 3}]})
        first = HistoricalPointInventoryClosingCapture(client=client).capture(
            operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product])
        previous_line = first.closing.lines.values().get()
        bamoa = Sucursal.objects.create(codigo="BAMOA", nombre="Bamoa")
        second_branch = PointBranch.objects.create(external_id="2", name="Bamoa", erp_branch=bamoa)
        extension_client = _FakePointClient({("2", "857"): [
            {"Fecha": "2026-07-31T23:00:00", "FK_Movimiento": 456,
             "Existencia_anterior": 3, "Existencia_nueva": 2, "Cancelado": False},
        ]}, {"857": [{"PK_Sucursal": 2, "Cantidad": 2}]})
        extension = HistoricalPointInventoryClosingCapture(client=extension_client)
        result = extension.capture(operational_date=date(2026, 7, 31),
            branches=[self.branch, second_branch], products=[self.product])
        self.assertEqual(result.closing.pk, first.closing.pk)
        self.assertEqual(result.closing.status, PointHistoricalInventoryClosing.STATUS_VERIFIED)
        self.assertEqual(result.closing.lines.filter(branch=self.branch).values().get(), previous_line)
        self.assertEqual(result.closing.lines.count(), 2)
        self.assertEqual(PointHistoricalInventoryClosing.objects.count(), 1)
        self.assertEqual(extension_client.history_calls, [("2", "857")])
        extension.capture(operational_date=date(2026, 7, 31),
            branches=[self.branch, second_branch], products=[self.product])
        self.assertEqual(extension_client.history_calls, [("2", "857")])

    def test_failed_verified_extension_leaves_original_closing_untouched(self):
        original = HistoricalPointInventoryClosingCapture(client=_FakePointClient(
            {("1", "857"): [{"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 123,
              "Existencia_nueva": 3, "Cancelado": False}]},
            {"857": [{"PK_Sucursal": 1, "Cantidad": 3}]})).capture(
                operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product])
        before = PointHistoricalInventoryClosing.objects.values().get(pk=original.closing.pk)
        bamoa = Sucursal.objects.create(codigo="BAMOA", nombre="Bamoa")
        second_branch = PointBranch.objects.create(external_id="2", name="Bamoa", erp_branch=bamoa)
        with self.assertRaises(HistoricalInventoryCaptureError):
            HistoricalPointInventoryClosingCapture(client=_FakePointClient(
                {("2", "857"): []}, {"857": [{"PK_Sucursal": 2, "Cantidad": 2}]})).capture(
                    operational_date=date(2026, 7, 31), branches=[self.branch, second_branch], products=[self.product])
        self.assertEqual(PointHistoricalInventoryClosing.objects.values().get(pk=original.closing.pk), before)
        self.assertEqual(original.closing.lines.count(), 1)
        self.assertEqual(PointHistoricalInventoryClosing.objects.count(), 1)

    def test_locked_month_cannot_extend_verified_closing(self):
        from recetas.models import ProductoMonthClosure

        capture = HistoricalPointInventoryClosingCapture(client=_FakePointClient(
            {("1", "857"): [{"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 123,
              "Existencia_nueva": 3, "Cancelado": False}]},
            {"857": [{"PK_Sucursal": 1, "Cantidad": 3}]}))
        result = capture.capture(operational_date=date(2026, 7, 31), branches=[self.branch], products=[self.product])
        ProductoMonthClosure.objects.create(month_start=date(2026, 7, 1), month_end=date(2026, 7, 31), is_locked=True)
        second_branch = PointBranch.objects.create(external_id="2", name="Bamoa")
        capture.client.history_by_key[("2", "857")] = [
            {"Fecha": "2026-07-31T22:00:00", "FK_Movimiento": 456, "Existencia_nueva": 2, "Cancelado": False}]
        with self.assertRaises(HistoricalInventoryCaptureError):
            capture.capture(operational_date=date(2026, 7, 31), branches=[self.branch, second_branch], products=[self.product])
        result.closing.refresh_from_db()
        self.assertEqual(result.closing.expected_branch_ids, [self.branch.id])
        self.assertEqual(result.closing.lines.count(), 1)

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

    def test_retry_reuses_resolved_draft_lines_and_fetches_only_pending_pairs(self):
        second_erp = Sucursal.objects.create(codigo="CENTRO", nombre="Centro")
        second_branch = PointBranch.objects.create(
            external_id="2", name="Centro", erp_branch=second_erp
        )
        first_client = _FakePointClient(
            history_by_key={
                ("1", "857"): [
                    {
                        "Fecha": "2026-07-31T22:00:00",
                        "FK_Movimiento": 123,
                        "Existencia_anterior": 4,
                        "Existencia_nueva": 3,
                        "Cancelado": False,
                    }
                ],
                ("2", "857"): [],
            },
            current_by_product={
                "857": [
                    {"PK_Sucursal": 1, "Cantidad": 3},
                    {"PK_Sucursal": 2, "Cantidad": 2},
                ]
            },
        )
        first = HistoricalPointInventoryClosingCapture(client=first_client).capture(
            operational_date=date(2026, 7, 31),
            branches=[self.branch, second_branch],
            products=[self.product],
        )
        self.assertEqual(first.closing.status, PointHistoricalInventoryClosing.STATUS_DRAFT)
        self.assertEqual(first.closing.lines.count(), 1)

        retry_client = _FakePointClient(
            history_by_key={
                ("2", "857"): [
                    {
                        "Fecha": "2026-07-31T21:00:00",
                        "FK_Movimiento": 456,
                        "Existencia_anterior": 3,
                        "Existencia_nueva": 2,
                        "Cancelado": False,
                    }
                ]
            },
            current_by_product={
                "857": [
                    {"PK_Sucursal": 1, "Cantidad": 3},
                    {"PK_Sucursal": 2, "Cantidad": 2},
                ]
            },
        )
        retried = HistoricalPointInventoryClosingCapture(client=retry_client).capture(
            operational_date=date(2026, 7, 31),
            branches=[self.branch, second_branch],
            products=[self.product],
        )

        self.assertEqual(retried.closing.pk, first.closing.pk)
        self.assertEqual(retried.closing.status, PointHistoricalInventoryClosing.STATUS_VERIFIED)
        self.assertEqual(retried.closing.lines.count(), 2)
        self.assertEqual(retry_client.history_calls, [("2", "857")])

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
