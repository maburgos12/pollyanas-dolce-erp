"""Enumeración completa del catálogo Point (tope de 150 filas por respuesta)."""

from types import SimpleNamespace

from django.test import SimpleTestCase

from unittest.mock import patch

from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.utils.exceptions import ExtractionError


def _client() -> PointHttpSessionClient:
    return PointHttpSessionClient(SimpleNamespace(base_url="https://point.test"))


def _fake_point(catalog: list[dict], limit: int):
    """Simula la búsqueda 'contiene' de Point con tope de filas."""

    def fetch(term: str) -> list[dict]:
        rows = [row for row in catalog if term in row["nombre"]]
        return rows[:limit]

    return fetch


class EnumeracionCatalogoTests(SimpleTestCase):
    def test_recupera_catalogo_completo_pese_al_tope(self):
        # 60 nombres que comparten prefijo: una sola búsqueda 'p' se satura.
        catalog = [{"PK": i, "nombre": f"pastel {i:03d}"} for i in range(60)]
        client = _client()
        rows = client._enumerate_catalog(
            _fake_point(catalog, limit=10), pk_field="PK", label="test", page_limit=10, pause_seconds=0
        )
        self.assertEqual(len(rows), 60)

    def test_deduplica_por_pk(self):
        catalog = [
            {"PK": 1, "nombre": "azucar"},
            {"PK": 2, "nombre": "avena azul"},
        ]
        client = _client()
        rows = client._enumerate_catalog(
            _fake_point(catalog, limit=150), pk_field="PK", label="test", pause_seconds=0
        )
        # "azucar" matchea con a, z, u, c, r… pero entra una sola vez.
        self.assertEqual(sorted(r["PK"] for r in rows), [1, 2])

    def test_error_reintenta_con_relogin_y_no_pierde_el_termino(self):
        catalog = [{"PK": 1, "nombre": "harina"}]
        estado = {"fallas": 0, "logins": 0}

        def fetch(term):
            if term == "h" and estado["fallas"] == 0:
                estado["fallas"] += 1
                raise ExtractionError("sesión caducada")
            return _fake_point(catalog, limit=150)(term)

        client = _client()
        client._last_branch_hint = "Matriz"
        capturado = {}

        def fake_login(**kw):
            estado["logins"] += 1
            capturado.update(kw)

        client.login = fake_login
        with patch("pos_bridge.services.point_http_client.time.sleep"):
            rows = client._enumerate_catalog(fetch, pk_field="PK", label="test", pause_seconds=0)
        self.assertEqual([r["PK"] for r in rows], [1])
        self.assertEqual(estado["logins"], 1)
        # El relogin regresa al mismo workspace, no al default.
        self.assertEqual(capturado.get("branch_hint"), "Matriz")

    def test_demasiadas_fallas_aborta(self):
        def fetch(term):
            raise ExtractionError("Point caído")

        client = _client()
        client.login = lambda **kw: None
        with patch("pos_bridge.services.point_http_client.time.sleep"), self.assertRaises(ExtractionError):
            client._enumerate_catalog(fetch, pk_field="PK", label="test", max_failures=3, pause_seconds=0)


class ConversionUnidadSyncAlmacenTests(SimpleTestCase):
    """La cantidad de Point se convierte a la unidad base del insumo ERP."""

    databases = "__all__"

    def test_ml_a_litros(self):
        from decimal import Decimal

        from django.test import TestCase  # noqa: F401  (usa BD)
        from maestros.models import Insumo, UnidadMedida
        from pos_bridge.management.commands.sync_inventario_desde_point import (
            _cantidad_en_unidad_erp,
        )

        ml = UnidadMedida.objects.create(codigo="ml", nombre="Mililitro", tipo="VOLUME", factor_to_base=1)
        lt = UnidadMedida.objects.create(codigo="lt", nombre="Litro", tipo="VOLUME", factor_to_base=1000)
        kg = UnidadMedida.objects.create(codigo="kg", nombre="Kilo", tipo="MASS", factor_to_base=1000)
        insumo = Insumo.objects.create(nombre="Desmoldante test", unidad_base=lt)

        cantidad, nota = _cantidad_en_unidad_erp(Decimal("23000"), "ml", insumo)
        self.assertEqual(cantidad, Decimal("23"))
        self.assertIn("convertido", nota)

        # misma unidad: sin cambio
        cantidad, nota = _cantidad_en_unidad_erp(Decimal("5"), "lt", insumo)
        self.assertEqual(cantidad, Decimal("5"))
        self.assertEqual(nota, "")

        # incompatible (masa vs volumen): se reporta, no se convierte
        insumo_kg = Insumo.objects.create(nombre="Harina test", unidad_base=kg)
        cantidad, nota = _cantidad_en_unidad_erp(Decimal("10"), "ml", insumo_kg)
        self.assertEqual(cantidad, Decimal("10"))
        self.assertIn("INCOMPATIBLE", nota)
