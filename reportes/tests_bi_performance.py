import inspect
import json
import logging
import re
from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import PermissionDenied
from django.db import DatabaseError, connection, transaction
from django.http import HttpResponse
from django.test import RequestFactory, TestCase, override_settings
from django.test.utils import CaptureQueriesContext

from reportes import views
from reportes.tests import _clear_reportes_runtime_caches


@override_settings(
    ALLOWED_HOSTS=["testserver"],
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}},
)
class BIPerformanceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(
            username="private-user-canary", is_superuser=True, is_staff=True
        )

    def setUp(self):
        _clear_reportes_runtime_caches()
        self.factory = RequestFactory()
        self.response = HttpResponse(b"existing response", status=202, headers={"X-Existing": "kept"})

    def request(self, params=None, **headers):
        request = self.factory.get(
            "/reportes/bi/", params or {}, HTTP_COOKIE="sessionid=private-cookie-canary", **headers
        )
        request.user = self.user
        return request

    def call_home(self, request, commercial=None, render=None):
        with patch("reportes.views.can_view_reportes", return_value=True), patch(
            "reportes.commercial_analytics.commercial_panel_from_request",
            side_effect=commercial,
            return_value={},
        ), patch("reportes.views.render", side_effect=render, return_value=self.response):
            return views.bi(request)

    def metric(self, captured):
        self.assertEqual(len(captured.records), 1)
        value = json.loads(captured.records[0].getMessage())
        self.assertEqual(
            set(value),
            {"event", "view", "variant", "total_ms", "sql_ms", "query_count", "status", "failed", "cf_ray_id"},
        )
        self.assertEqual(value["event"], "bi_request_metrics")
        self.assertEqual(value["view"], "reportes.bi")
        self.assertGreaterEqual(value["total_ms"], value["sql_ms"])
        self.assertGreaterEqual(value["sql_ms"], 0)
        return value

    def test_one_private_line_counts_view_and_render_sql_without_extra_queries(self):
        def commercial(request):
            with connection.cursor() as cursor:
                cursor.execute("SELECT %s", ["private-sql-parameter-canary"])
                return {"current": cursor.fetchone()[0]}

        def render(*args):
            with connection.cursor() as cursor:
                cursor.execute("SELECT 42")
                self.assertEqual(cursor.fetchone()[0], 42)
            return self.response

        with self.assertLogs("erp.performance", level="INFO") as captured, CaptureQueriesContext(connection) as queries:
            response = self.call_home(
                self.request({"action_canary": "private-query-canary"}, HTTP_CF_RAY="230B030023AE2822-SJC"),
                commercial=commercial,
                render=render,
            )
        metric = self.metric(captured)
        self.assertIs(response, self.response)
        self.assertEqual(len(queries), 2)
        self.assertEqual(metric["query_count"], 2)
        self.assertGreater(metric["sql_ms"], 0)
        self.assertEqual((metric["status"], metric["failed"], metric["variant"]), (202, False, "portada"))
        self.assertEqual(metric["cf_ray_id"], "230b030023ae2822")
        for forbidden in ("SELECT", "private-", "SJC", "cookie", "username", "query_string"):
            self.assertNotIn(forbidden, captured.records[0].getMessage())

    def test_zero_query_request_does_not_collect_another_requests_counts(self):
        wrappers = list(connection.execute_wrappers)
        for _ in range(2):
            with self.assertLogs("erp.performance", level="INFO") as captured, self.assertNumQueries(0):
                self.assertIs(self.call_home(self.request()), self.response)
            metric = self.metric(captured)
            self.assertEqual((metric["query_count"], metric["sql_ms"], metric["cf_ray_id"]), (0, 0, None))
            self.assertEqual(connection.execute_wrappers, wrappers)

    @override_settings(DEBUG=False)
    def test_error_response_and_hex_only_ray_work_without_debug_query_logging(self):
        self.response.status_code = 503
        with self.assertLogs("erp.performance", level="INFO") as captured:
            self.assertIs(self.call_home(self.request(HTTP_CF_RAY="230b030023ae2822")), self.response)
        metric = self.metric(captured)
        self.assertEqual((metric["status"], metric["failed"], metric["cf_ray_id"]), (503, True, "230b030023ae2822"))

    def test_error_is_reraised_unchanged_and_message_is_never_logged(self):
        error = RuntimeError("private-error-canary")
        with self.assertLogs("erp.performance", level="INFO") as captured:
            with self.assertRaises(RuntimeError) as raised:
                self.call_home(self.request(), render=lambda *args: (_ for _ in ()).throw(error))
        self.assertIs(raised.exception, error)
        metric = self.metric(captured)
        self.assertTrue(metric["failed"])
        self.assertIsNone(metric["status"])
        self.assertNotIn("private-error-canary", captured.records[0].getMessage())

    def test_failed_sql_is_counted_and_wrapper_is_removed(self):
        def failed_query(request):
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1/0")

        wrappers = list(connection.execute_wrappers)
        with self.assertLogs("erp.performance", level="INFO") as captured, self.assertRaises(DatabaseError):
            with transaction.atomic():
                self.call_home(self.request(), commercial=failed_query)
        metric = self.metric(captured)
        self.assertEqual(metric["query_count"], 1)
        self.assertTrue(metric["failed"])
        self.assertEqual(connection.execute_wrappers, wrappers)

    def test_logging_failure_preserves_response_and_original_exception(self):
        with patch.object(logging.getLogger("erp.performance"), "info", side_effect=OSError("sink unavailable")):
            self.assertIs(self.call_home(self.request()), self.response)
            error = ValueError("private-error-canary")
            with self.assertRaises(ValueError) as raised:
                self.call_home(self.request(), render=lambda *args: (_ for _ in ()).throw(error))
            self.assertIs(raised.exception, error)

    def test_invalid_ray_canaries_are_null_and_not_logged(self):
        for raw in ("230b030023ae2822-SJC\nprivate-canary", "230b030023ae2822-private-canary", "private-canary", "f" * 100):
            with self.subTest(raw=raw), self.assertLogs("erp.performance", level="INFO") as captured:
                self.call_home(self.request(HTTP_CF_RAY=raw))
            self.assertIsNone(self.metric(captured)["cf_ray_id"])
            self.assertNotIn("private-canary", captured.records[0].getMessage())

    def test_permission_exception_and_anonymous_redirect_keep_existing_behavior(self):
        with patch("reportes.views.can_view_reportes", return_value=False):
            with self.assertLogs("erp.performance", level="INFO") as captured, self.assertRaises(PermissionDenied):
                views.bi(self.request())
        self.assertTrue(self.metric(captured)["failed"])
        request = self.request()
        request.user = AnonymousUser()
        with self.assertNoLogs("erp.performance", level="INFO"):
            response = views.bi(request)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/?next=", response.url)

    @override_settings(DATA_UPLOAD_MAX_NUMBER_FIELDS=1)
    def test_invalid_query_cannot_replace_permission_exception(self):
        request = self.request({"first": "private-query-canary", "second": "private-query-canary"})
        with patch("reportes.views.can_view_reportes", return_value=False):
            with self.assertLogs("erp.performance", level="INFO") as captured, self.assertRaises(PermissionDenied):
                views.bi(request)
        metric = self.metric(captured)
        self.assertEqual(metric["variant"], "unknown")
        self.assertTrue(metric["failed"])
        self.assertNotIn("private-query-canary", captured.records[0].getMessage())

    def test_portada_detail_and_implicit_detail_keep_rendered_html_and_dto(self):
        original = inspect.unwrap(views.bi)
        for params, variant in (({}, "portada"), ({"detalle": "1"}, "detalle"), ({"period_days": "90"}, "detalle")):
            params = {"sales_year": "2026", "sales_month": "10", **params}
            with self.subTest(variant=variant, params=params), patch(
                "reportes.views.timezone.localdate", return_value=date(2026, 11, 19)
            ):
                _clear_reportes_runtime_caches()
                # Existing builders materialize snapshots: compare the same DB
                # state, not first-build output against a subsequent snapshot.
                with transaction.atomic():
                    baseline = original(self.request(params))
                    transaction.set_rollback(True)
                _clear_reportes_runtime_caches()
                with self.assertLogs("erp.performance", level="INFO") as captured:
                    with transaction.atomic():
                        measured = views.bi(self.request(params))
                        transaction.set_rollback(True)
                self.assertEqual(self.metric(captured)["variant"], variant)
                normalize = lambda body: re.sub(rb'name="csrfmiddlewaretoken" value="[^"]+"', b'name="csrfmiddlewaretoken" value="CSRF"', body)
                self.assertEqual(normalize(baseline.content), normalize(measured.content))
                self.assertEqual(dict(baseline.headers), dict(measured.headers))
                self.assertIn(b'"commercial-analytics-data"', measured.content)
                if variant == "detalle":
                    self.assertIn(b'"forecast-weekly-data"', measured.content)
