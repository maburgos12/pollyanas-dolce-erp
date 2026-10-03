import json
from unittest.mock import Mock, patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.db import connection
from django.http import JsonResponse
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from rest_framework.authtoken.models import Token
from rest_framework.test import APIClient
from rest_framework_simplejwt.tokens import RefreshToken

from crm.models import PedidoCliente, PickupReservation
from integraciones.models import PublicApiAccessLog, PublicApiClient
from integraciones.sales_read_middleware import SalesReadBoundaryMiddleware
from integraciones.sales_read_policy import SALES_CAPABILITY, SALES_GROUP, allows_sales_read


class SalesReadBoundaryTests(TestCase):
    token_paths = (
        "/api/pos-bridge/products/sale-price/",
        "/api/integraciones/horarios-especiales/effective/",
    )
    pickup_path = "/api/public/v1/pickup-availability/"
    forbidden_paths = (
        "/api/pos-bridge/products/1/recipe/", "/api/pos-bridge/products/",
        "/api/rrhh/empleados/", "/rrhh/", "/api/finanzas/",
        "/api/reportes/estado-resultados/", "/admin/", "/api/public/v1/recetas/",
        "/api/public/v1/pickup-reservations/",
        "/api/public/v1/pickup-reservations/test/confirm/",
        "/api/public/v1/pickup-reservations/test/release/",
    )

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="sales-boundary")
        self.user.groups.add(Group.objects.get_or_create(name=SALES_GROUP)[0])
        self.token = Token.objects.create(user=self.user)
        self.operator = get_user_model().objects.create_user(username="operator", is_staff=True, is_superuser=True)
        self.operator_token = Token.objects.create(user=self.operator)
        self.public, self.key = PublicApiClient.create_with_generated_key(nombre="Sales only")
        self.public.capabilities = [SALES_CAPABILITY]
        self.public.save(update_fields=["capabilities"])
        self.operations, self.operations_key = PublicApiClient.create_with_generated_key(nombre="Operations")
        self.operations.capabilities = [PublicApiClient.CAPABILITY_OMNICHANNEL]
        self.operations.save(update_fields=["capabilities"])
        self.factory = RequestFactory()
        self.api = APIClient()
        self.downstream_calls = 0

    def invoke(self, path, method="GET", user=None, **headers):
        request = self.factory.generic(method, path, **headers)
        request.user = user if user is not None else AnonymousUser()
        original_user = request.user

        def downstream(request):
            self.downstream_calls += 1
            self.assertIs(request.user, original_user)
            return JsonResponse({"downstream": True})

        return SalesReadBoundaryMiddleware(downstream)(request)

    def assert_denied(self, response):
        self.assertEqual(response.status_code, 403)
        self.assertEqual(json.loads(response.content), {"detail": "Acceso no autorizado"})

    def test_sensitive_routes_block_before_downstream_for_every_restricted_kind(self):
        for headers, user in (({"HTTP_AUTHORIZATION": f"Token {self.token.key}"}, None),
                              ({"HTTP_X_API_KEY": self.key}, None), ({}, self.user)):
            for path in self.forbidden_paths:
                for method in ("GET", "POST"):
                    with self.subTest(path=path, method=method, session=user is not None):
                        self.assert_denied(self.invoke(path, method, user=user, **headers))
        self.assertEqual(self.downstream_calls, 0)

    def test_exact_allowed_gets_pass_and_other_methods_never_do(self):
        for headers, paths in (({"HTTP_AUTHORIZATION": f"Token {self.token.key}"}, self.token_paths),
                               ({"HTTP_X_API_KEY": "  " + self.key + "  "}, (self.pickup_path,))):
            for path in paths:
                self.assertEqual(self.invoke(path, **headers).status_code, 200)
                for method in ("HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE", "TRACE", "CONNECT", "CUSTOM"):
                    with self.subTest(method=method):
                        self.assert_denied(self.invoke(path, method, HTTP_X_HTTP_METHOD_OVERRIDE="GET", **headers))
        self.assertEqual(self.downstream_calls, 3)

    def test_credentials_intersect_and_privileged_principals_cannot_override(self):
        combinations = (
            ({"HTTP_AUTHORIZATION": f"Token {self.token.key}", "HTTP_X_API_KEY": self.key}, None),
            ({"HTTP_AUTHORIZATION": f"Token {self.operator_token.key}", "HTTP_X_API_KEY": self.key}, None),
            ({"HTTP_AUTHORIZATION": f"Token {self.token.key}"}, self.operator),
            ({"HTTP_AUTHORIZATION": f"Token {self.operator_token.key}"}, self.user),
        )
        for index, (headers, user) in enumerate(combinations):
            for path in (*self.token_paths, self.pickup_path, "/admin/"):
                # An unrestricted principal contributes no additional paths.
                if (index == 1 and path == self.pickup_path) or (index in (2, 3) and path in self.token_paths):
                    continue
                with self.subTest(path=path, user=user):
                    self.assert_denied(self.invoke(path, user=user, **headers))
        self.assertEqual(self.downstream_calls, 0)

    def test_restricted_inactive_staff_or_superuser_denied_even_on_allowed_route(self):
        for field in ("is_active", "is_staff", "is_superuser"):
            original = getattr(self.user, field)
            setattr(self.user, field, field != "is_active")
            self.user.save(update_fields=[field])
            self.assert_denied(self.invoke(self.token_paths[0], HTTP_AUTHORIZATION=f"Token {self.token.key}"))
            self.assert_denied(self.invoke(self.token_paths[0], user=self.user))
            setattr(self.user, field, original)
            self.user.save(update_fields=[field])
        self.assertEqual(self.downstream_calls, 0)

    def test_invalid_or_inactive_credentials_do_not_authenticate_or_replace_session(self):
        self.public.activo = False
        self.public.save(update_fields=["activo"])
        for headers in ({"HTTP_AUTHORIZATION": "Token " + "0" * 40},
                        {"HTTP_AUTHORIZATION": "Token"}, {"HTTP_AUTHORIZATION": "Token bad extra"},
                        {"HTTP_AUTHORIZATION": "Bearer " + self.token.key},
                        {"HTTP_X_API_KEY": self.key}, {"HTTP_X_API_KEY": self.key[:12] + "wrong"}):
            with self.subTest(headers=list(headers)):
                self.assertEqual(self.invoke("/admin/", user=self.operator, **headers).status_code, 200)
                self.assert_denied(self.invoke("/admin/", user=self.user, **headers))

    def test_operations_credentials_are_unchanged(self):
        for headers in ({"HTTP_AUTHORIZATION": f"Token {self.operator_token.key}"},
                        {"HTTP_X_API_KEY": self.operations_key}):
            for method in ("GET", "POST", "CUSTOM"):
                self.assertEqual(self.invoke("/admin/", method, **headers).status_code, 200)

    def test_oversized_headers_fail_closed_without_lookup_or_downstream(self):
        for name in ("HTTP_AUTHORIZATION", "HTTP_X_API_KEY"):
            with self.subTest(name=name), CaptureQueriesContext(connection) as queries:
                self.assert_denied(self.invoke(self.token_paths[0], **{name: "x" * 4097}))
            self.assertEqual(len(queries), 0)
        self.assertEqual(self.downstream_calls, 0)

    def test_header_limit_boundary_and_drf_token_case_whitespace(self):
        for name in ("HTTP_AUTHORIZATION", "HTTP_X_API_KEY"):
            self.assertEqual(self.invoke("/admin/", **{name: "x" * 4096}).status_code, 200)
        self.assert_denied(self.invoke("/admin/", HTTP_AUTHORIZATION=f"  tOkEn\t{self.token.key}  "))
        self.assertEqual(self.invoke(self.token_paths[0], HTTP_AUTHORIZATION=f"  tOkEn\t{self.token.key}  ").status_code, 200)

    def test_guard_uses_only_selects_and_never_marks_key_used(self):
        for headers, path in (({"HTTP_AUTHORIZATION": f"Token {self.token.key}"}, "/admin/"),
                              ({"HTTP_X_API_KEY": self.key}, self.pickup_path)):
            with CaptureQueriesContext(connection) as queries:
                self.invoke(path, **headers)
            self.assertFalse(any(q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for q in queries))
        self.public.refresh_from_db()
        self.assertIsNone(self.public.last_used_at)

    def test_real_denied_routes_and_writes_have_no_service_calls_or_record_changes(self):
        from crm.services import PickupAvailabilityService
        before = (PedidoCliente.objects.count(), PickupReservation.objects.count(), PublicApiAccessLog.objects.count())
        with patch.object(PickupAvailabilityService, "create_reservation") as create, \
             patch.object(PickupAvailabilityService, "confirm_reservation") as confirm, \
             patch.object(PickupAvailabilityService, "release_reservation") as release:
            for headers in ({"HTTP_AUTHORIZATION": f"Token {self.token.key}"}, {"HTTP_X_API_KEY": self.key}):
                for path in self.forbidden_paths:
                    self.assert_denied(self.api.get(path, **headers))
                    self.assert_denied(self.api.post(path, {"cliente_nombre": "Test", "descripcion": "Test"}, format="json", **headers))
            create.assert_not_called()
            confirm.assert_not_called()
            release.assert_not_called()
        self.assertEqual(before, (PedidoCliente.objects.count(), PickupReservation.objects.count(), PublicApiAccessLog.objects.count()))

    def test_real_invalid_credentials_remain_rejected_by_authentication(self):
        for header in ("Token " + "0" * 40, "Token", "Token bad extra"):
            self.assertEqual(self.api.get(self.token_paths[0], HTTP_AUTHORIZATION=header).status_code, 401)
        self.assertEqual(self.api.get(self.pickup_path, HTTP_X_API_KEY=self.key[:12] + "wrong").status_code, 401)
        self.public.activo = False
        self.public.save(update_fields=["activo"])
        self.assertEqual(self.api.get(self.pickup_path, HTTP_X_API_KEY=self.key).status_code, 401)

    def test_middleware_order_preserves_authentication_and_csrf(self):
        guard = "integraciones.sales_read_middleware.SalesReadBoundaryMiddleware"
        auth = settings.MIDDLEWARE.index("django.contrib.auth.middleware.AuthenticationMiddleware")
        self.assertEqual(settings.MIDDLEWARE[auth + 1], guard)
        self.assertIn("django.middleware.csrf.CsrfViewMiddleware", settings.MIDDLEWARE)

    def test_capability_constant_uses_existing_json_contract(self):
        self.assertEqual(PublicApiClient.CAPABILITY_SALES_READ_ONLY, SALES_CAPABILITY)

    def test_allowed_real_routes_use_normal_auth_and_return_consumable_shapes(self):
        from core.models import Sucursal
        from pos_bridge.models import PointBranch, PointProduct, PointInventorySnapshot, PointSyncJob
        from recetas.models import Receta
        from django.core.cache import cache
        cache.clear()
        branch = Sucursal.objects.create(codigo="BAMOA", nombre="Sucursal Bamoa")
        point_branch = PointBranch.objects.create(external_id="1", name="Bamoa", erp_branch=branch, status=PointBranch.STATUS_ACTIVE)
        product = PointProduct.objects.create(external_id="101", sku="0101", name="Pastel Fresas")
        Receta.objects.create(nombre="Pastel Fresas", codigo_point="0101", tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="sales-boundary-fixture")
        job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)
        PointInventorySnapshot.objects.create(branch=point_branch, product=product, stock=5, captured_at=timezone.now(), sync_job=job)
        self.assertFalse(self.user.has_usable_password())
        self.assertFalse(self.user.is_staff)
        self.assertFalse(self.user.is_superuser)
        self.assertEqual(list(self.user.groups.values_list("name", flat=True)), [SALES_GROUP])
        self.assertFalse(self.public.has_capability(PublicApiClient.CAPABILITY_OMNICHANNEL))
        auth = {"HTTP_AUTHORIZATION": f"Token {self.token.key}"}
        with patch("pos_bridge.config.load_point_bridge_settings"), \
             patch("pos_bridge.services.point_http_client.PointHttpSessionClient.login"), \
             patch("pos_bridge.services.point_http_client.PointHttpSessionClient.get_product_detail", return_value={
                 "PK_Producto": 101, "Codigo": "0101", "Nombre": product.name, "Activo": True, "Precio_default": 340.0}):
            response = self.api.get(self.token_paths[0], {"product_code": "0101"}, **auth)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "VERIFIED")
        self.assertEqual(response.json()["amount"], "340.0")
        self.assertEqual(response.json()["currency"], "MXN")
        self.assertTrue(response.json()["is_fresh"])
        network = Mock(status_code=200)
        network.json.return_value = [{"id": 11, "name": "Sucursal Bamoa", "erp_branch_code": "BAMOA", "slug": "sucursal-bamoa", "schedule": '{"lun-sab":"09:00-19:30"}'}]
        with patch("requests.get", return_value=network):
            response = self.api.get(self.token_paths[1], {"branch_name": "Bamoa", "target_date": "2026-09-30"}, **auth)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["regular"]["status"], "VERIFIED")
        self.assertEqual(response.json()["effective"]["status"], "REGULAR_ONLY")
        with patch.dict("os.environ", {"PICKUP_LIVE_POINT_LOOKUP_ENABLED": "0"}), override_settings(PICKUP_AVAILABILITY_RESPONSE_CACHE_SECONDS=0):
            response = self.api.get(self.pickup_path, {"product_code": "0101", "branch_code": "BAMOA", "quantity": "1"}, HTTP_X_API_KEY=self.key)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "AVAILABLE")
        self.assertTrue(response.json()["available"])
        self.assertIn("available_to_promise", response.json())

    def test_real_session_cannot_be_elevated_by_operator_token(self):
        self.api.force_login(self.user)
        self.assert_denied(self.api.get("/admin/", HTTP_AUTHORIZATION=f"Token {self.operator_token.key}"))
        self.assert_denied(self.api.get(self.pickup_path, HTTP_X_API_KEY=self.key))

    def test_real_operator_token_cannot_elevate_sales_public_key(self):
        self.assert_denied(self.api.get(self.token_paths[0], HTTP_AUTHORIZATION=f"Token {self.operator_token.key}", HTTP_X_API_KEY=self.key))

    def test_operational_capability_cannot_override_sales_marker(self):
        self.public.capabilities.append(PublicApiClient.CAPABILITY_OMNICHANNEL)
        self.public.save(update_fields=["capabilities"])
        self.assert_denied(self.api.post("/api/public/v1/pickup-reservations/", {}, format="json", HTTP_X_API_KEY=self.key))

    def jwt_header(self, user):
        return {"HTTP_AUTHORIZATION": f"Bearer {RefreshToken.for_user(user).access_token}"}

    def test_sales_jwt_blocks_before_downstream_and_real_logistics_views(self):
        auth = self.jwt_header(self.user)
        for path in ("/api/logistica/mi-perfil/", "/api/logistica/unidades/", *self.forbidden_paths):
            with self.subTest(path=path, layer="middleware"):
                self.assert_denied(self.invoke(path, **auth))
            with self.subTest(path=path, layer="real-view"):
                self.assert_denied(self.api.get(path, **auth))
        self.assertEqual(self.downstream_calls, 0)

    def test_privileged_sales_jwt_is_denied_even_on_allowed_read(self):
        auth = self.jwt_header(self.user)
        for field in ("is_staff", "is_superuser"):
            setattr(self.user, field, True)
            self.user.save(update_fields=[field])
            self.assert_denied(self.invoke(self.token_paths[0], **auth))
            self.assert_denied(self.api.get("/api/logistica/unidades/", **auth))
            setattr(self.user, field, False)
            self.user.save(update_fields=[field])
        self.assertEqual(self.downstream_calls, 0)

    def test_jwt_intersects_key_and_cannot_override_restricted_session(self):
        for path in (*self.token_paths, self.pickup_path, "/api/logistica/unidades/"):
            self.assert_denied(self.invoke(path, HTTP_X_API_KEY=self.key, **self.jwt_header(self.user)))
        operator_auth = self.jwt_header(self.operator)
        self.assert_denied(self.invoke(self.token_paths[0], HTTP_X_API_KEY=self.key, **operator_auth))
        self.assert_denied(self.invoke("/api/logistica/unidades/", user=self.user, **operator_auth))
        self.assert_denied(self.invoke("/api/logistica/unidades/", user=self.operator, **self.jwt_header(self.user)))
        self.assertEqual(self.invoke(self.pickup_path, HTTP_X_API_KEY=self.key, **operator_auth).status_code, 200)

    def test_invalid_jwt_neither_authenticates_nor_breaks_normal_rejection(self):
        expired = RefreshToken.for_user(self.user).access_token
        from datetime import timedelta
        expired.set_exp(lifetime=timedelta(seconds=-1))
        wrong_user = RefreshToken.for_user(self.user).access_token
        wrong_user["user_id"] = 99999999
        refresh = RefreshToken.for_user(self.user)
        access = str(refresh.access_token)
        invalid_signature = access.rsplit(".", 1)[0] + "." + "A" * 43
        for raw in ("malformed", "", "invalid.extra.parts", str(expired), str(wrong_user), str(refresh), invalid_signature):
            with self.subTest(kind="invalid-jwt"):
                auth = {"HTTP_AUTHORIZATION": f"Bearer {raw}"}
                self.assertEqual(self.invoke("/api/logistica/unidades/", **auth).status_code, 200)
                self.assertIn(self.api.get("/api/logistica/unidades/", **auth).status_code, (401, 403))
                self.assert_denied(self.invoke("/api/logistica/unidades/", user=self.user, **auth))

    def test_jwt_restriction_does_not_grant_authentication_to_allowed_routes(self):
        auth = self.jwt_header(self.user)
        for path in self.token_paths:
            self.assertEqual(self.invoke(path, **auth).status_code, 200)
            self.assertEqual(self.api.get(path, **auth).status_code, 401)

    def test_oversized_bearer_is_denied_without_orm_or_downstream(self):
        with CaptureQueriesContext(connection) as queries:
            self.assert_denied(self.invoke("/api/logistica/unidades/", HTTP_AUTHORIZATION="Bearer " + "x" * 4090))
        self.assertEqual(len(queries), 0)
        self.assertEqual(self.downstream_calls, 0)

    def test_operational_jwt_keeps_real_logistics_access(self):
        auth = self.jwt_header(self.operator)
        self.assertEqual(self.invoke("/api/logistica/unidades/", **auth).status_code, 200)
        self.assertEqual(self.api.get("/api/logistica/unidades/", **auth).status_code, 200)


class SalesReadPolicyTests(SimpleTestCase):
    token_paths = (
        "/api/pos-bridge/products/sale-price/",
        "/api/integraciones/horarios-especiales/effective/",
    )
    public_key_paths = ("/api/public/v1/pickup-availability/",)

    def test_allows_exact_get_paths_for_each_credential_kind(self):
        for kind, paths in (
            ("token", self.token_paths),
            ("public_key", self.public_key_paths),
        ):
            for path in paths:
                with self.subTest(kind=kind, path=path):
                    self.assertTrue(allows_sales_read(kind, "GET", path))

    def test_denies_every_other_http_method(self):
        for method in (
            "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE", "TRACE",
            "CONNECT", "CUSTOM", "get", "Get", " GET", "GET ", "", None,
        ):
            for kind, paths in (
                ("token", self.token_paths),
                ("public_key", self.public_key_paths),
            ):
                for path in paths:
                    with self.subTest(kind=kind, method=method, path=path):
                        self.assertFalse(allows_sales_read(kind, method, path))

    def test_denies_unknown_credential_kinds(self):
        for kind in ("", None, "unknown", "Token", "PUBLIC_KEY", "session"):
            for path in self.token_paths + self.public_key_paths:
                with self.subTest(kind=kind, path=path):
                    self.assertFalse(allows_sales_read(kind, "GET", path))

    def test_denies_paths_assigned_to_another_credential_kind(self):
        for kind, paths in (
            ("token", self.public_key_paths),
            ("public_key", self.token_paths),
        ):
            for path in paths:
                with self.subTest(kind=kind, path=path):
                    self.assertFalse(allows_sales_read(kind, "GET", path))

    def test_denies_sensitive_and_operational_paths(self):
        for kind in ("token", "public_key"):
            for path in (
                "/api/recetas/", "/api/pos-bridge/recipes/",
                "/api/rrhh/empleados/", "/api/rrhh/nomina/",
                "/api/reportes/estado-resultados/", "/api/finanzas/",
                "/api/public/v1/reservations/", "/api/public/v1/pickup-reservations/",
                "/admin/", "/api/admin/", "/",
            ):
                with self.subTest(kind=kind, path=path):
                    self.assertFalse(allows_sales_read(kind, "GET", path))

    def test_denies_prefixes_suffixes_and_traversal_without_normalization(self):
        for kind, paths in (
            ("token", self.token_paths),
            ("public_key", self.public_key_paths),
        ):
            for path in paths:
                for altered_path in (
                    path.rstrip("/"), path + "extra/", path + "../",
                    path + "../../rrhh/", path + "%2e%2e/",
                    path + "?branch=2", path + "#fragment", path + "/",
                    "/prefix" + path, path.upper(), path.replace("/api/", "/api/../api/"),
                    path.replace("/api/", "//api/"), " " + path, path + " ", "", None,
                ):
                    with self.subTest(kind=kind, path=altered_path):
                        self.assertFalse(allows_sales_read(kind, "GET", altered_path))
