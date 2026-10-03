from django.test import SimpleTestCase

from integraciones.sales_read_policy import allows_sales_read


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
