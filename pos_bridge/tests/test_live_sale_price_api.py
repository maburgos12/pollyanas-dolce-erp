from contextlib import contextmanager
from decimal import Decimal
import time
from unittest.mock import patch

import requests
from django.contrib.auth import get_user_model
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from rest_framework.test import APITestCase

from pos_bridge.models import PointProduct


@contextmanager
def unavailable_lock(**kwargs):
    yield False


class LiveSalePriceApiTests(APITestCase):
    url = "/api/pos-bridge/products/sale-price/"

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="price-reader")
        self.client.force_authenticate(self.user)
        self.product = PointProduct.objects.create(
            external_id="101", sku="0101", name="Pastel Fresas con Crema Chico",
            precio=Decimal("999.00"), metadata={"Cost_U": 12},
        )
        self.detail = {
            "PK": 101, "Codigo": "0101", "Nombre": self.product.name,
            "Activo": True, "Precio_default": 340.0, "Cost_U": 12, "Cost_T": 80,
        }
        self.settings_patch = patch("pos_bridge.config.load_point_bridge_settings")
        self.settings_patch.start()
        self.addCleanup(self.settings_patch.stop)
        self.login_patch = patch("pos_bridge.services.point_http_client.PointHttpSessionClient.login")
        self.login = self.login_patch.start()
        self.addCleanup(self.login_patch.stop)
        self.detail_patch = patch(
            "pos_bridge.services.point_http_client.PointHttpSessionClient.get_product_detail",
            side_effect=lambda product_id: self.detail,
        )
        self.get_detail = self.detail_patch.start()
        self.addCleanup(self.detail_patch.stop)

    def read(self, **params):
        return self.client.get(self.url, params or {"product_code": "0101"})

    def assert_unknown(self, response, status_code):
        self.assertEqual(response.status_code, status_code)
        self.assertEqual(response.data.get("status"), "UNKNOWN")
        self.assertIs(response.data["is_fresh"], False)
        self.assertIsNone(response.data["amount"])
        self.assertIsNone(response.data["checked_at"])

    def test_verified_price_uses_live_detail_and_has_no_database_writes(self):
        before = timezone.now()
        with CaptureQueriesContext(connection) as queries:
            response = self.read()
        self.assertEqual(response.status_code, 200)
        payload = response.data
        self.assertEqual(payload["status"], "VERIFIED")
        self.assertEqual(payload["amount"], "340.0")
        self.assertEqual(payload["point_product_id"], "101")
        self.assertEqual(payload["point_product_code"], "0101")
        self.assertEqual(payload["point_product_name"], self.product.name)
        self.assertEqual(payload["product_code"], "0101")
        self.assertEqual(payload["currency"], "MXN")
        self.assertEqual(payload["currency_source"], "ERP_POLICY")
        self.assertEqual(payload["source"], "ERP_POS_BRIDGE_LIVE_POINT")
        self.assertIs(payload["is_fresh"], True)
        checked_at = timezone.datetime.fromisoformat(payload["checked_at"])
        self.assertGreaterEqual(checked_at, before)
        self.assertLessEqual(checked_at, timezone.now())
        self.assertFalse(any(q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for q in queries))
        self.product.refresh_from_db()
        self.assertEqual(self.product.precio, Decimal("999.00"))
        self.get_detail.assert_called_once_with("101")
        self.login.assert_called_once_with()

    def test_requires_authentication(self):
        self.client.force_authenticate(None)
        self.assertEqual(self.read().status_code, 401)
        self.get_detail.assert_not_called()

    def test_invalid_or_missing_code_does_not_contact_point(self):
        for params in ({"product_code": ""}, {"product_code": "x" * 121}, {"other": "0101"}):
            with self.subTest(params=params):
                self.assert_unknown(self.client.get(self.url, params), 400)
        self.get_detail.assert_not_called()

    def test_unknown_inactive_and_duplicate_mappings_fail_closed(self):
        self.assert_unknown(self.read(product_code="absent"), 404)
        self.product.active = False
        self.product.save()
        self.assert_unknown(self.read(), 404)
        self.product.active = True
        self.product.save()
        PointProduct.objects.create(external_id="145", sku="0101", name=self.product.name)
        self.assert_unknown(self.read(), 409)
        self.get_detail.assert_not_called()

    def test_mapping_requires_canonical_positive_point_id(self):
        for value in ("0", "-1", "01", "alias", "1.0", "١٠١"):
            with self.subTest(value=value):
                self.product.external_id = value
                self.product.save()
                self.assert_unknown(self.read(), 409)
        self.get_detail.assert_not_called()

    def test_detail_requires_exact_identity_and_explicit_active(self):
        for field, value in (
            ("PK", 145), ("PK", True), ("PK", "0101"), ("Codigo", "0145"),
            ("Nombre", "Vaso Fresas con Crema Chico"), ("Nombre", ""),
            ("Activo", False), ("Activo", 1), ("Activo", "true"), ("Activo", None),
        ):
            with self.subTest(field=field, value=value):
                original = self.detail[field]
                self.detail[field] = value
                self.assert_unknown(self.read(), 502)
                self.detail[field] = original

    def test_invalid_sale_price_never_falls_back_to_replica_or_cost(self):
        for value in (None, True, False, 0, -1, "NaN", "Infinity", "-Infinity", "invalid", {}, []):
            with self.subTest(value=value):
                self.detail["Precio_default"] = value
                self.assert_unknown(self.read(), 502)
        self.detail.pop("Precio_default")
        self.assert_unknown(self.read(), 502)

    def test_network_and_login_errors_are_generic(self):
        self.get_detail.side_effect = requests.Timeout("password=secret")
        response = self.read()
        self.assert_unknown(response, 503)
        self.assertNotIn("secret", str(response.data))
        self.get_detail.side_effect = None
        self.login.side_effect = TimeoutError("password=secret")
        response = self.read()
        self.assert_unknown(response, 503)
        self.assertNotIn("secret", str(response.data))

    def test_busy_point_lock_does_not_login(self):
        with patch("pos_bridge.services.point_account_session_lock.point_account_session_lock", unavailable_lock):
            self.assert_unknown(self.read(), 503)
        self.login.assert_not_called()
        self.get_detail.assert_not_called()

    def test_point_read_has_local_deadline_and_preserves_outer_deadline(self):
        from pos_bridge.services.catalog_recipe_execution import DEADLINE, remaining_seconds

        def bounded_detail(product_id):
            remaining = remaining_seconds()
            self.assertIsNotNone(remaining)
            self.assertGreater(remaining, 0)
            self.assertLessEqual(remaining, 10)
            return self.detail

        self.get_detail.side_effect = bounded_detail
        self.assertEqual(self.read().status_code, 200)
        self.assertIsNone(DEADLINE.get())
        outer = time.monotonic() + 5
        token = DEADLINE.set(outer)
        try:
            self.assertEqual(self.read().status_code, 200)
            self.assertEqual(DEADLINE.get(), outer)
        finally:
            DEADLINE.reset(token)

    def test_expired_read_deadline_is_unknown_and_restored(self):
        from pos_bridge.services.catalog_recipe_execution import DEADLINE

        token = DEADLINE.set(time.monotonic() - 1)
        try:
            self.assert_unknown(self.read(), 503)
            self.assertLess(DEADLINE.get(), time.monotonic())
        finally:
            DEADLINE.reset(token)
