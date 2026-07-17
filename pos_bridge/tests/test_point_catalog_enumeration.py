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
            _fake_point(catalog, limit=10), pk_field="PK", label="test", page_limit=10, pause_seconds=0, relogin_every=0
        )
        self.assertEqual(len(rows), 60)

    def test_deduplica_por_pk(self):
        catalog = [
            {"PK": 1, "nombre": "azucar"},
            {"PK": 2, "nombre": "avena azul"},
        ]
        client = _client()
        rows = client._enumerate_catalog(
            _fake_point(catalog, limit=150), pk_field="PK", label="test", pause_seconds=0, relogin_every=0
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

        ml, _ = UnidadMedida.objects.get_or_create(codigo="ml", defaults={"nombre": "Mililitro", "tipo": "VOLUME", "factor_to_base": 1})
        lt, _ = UnidadMedida.objects.get_or_create(codigo="lt", defaults={"nombre": "Litro", "tipo": "VOLUME", "factor_to_base": 1000})
        kg, _ = UnidadMedida.objects.get_or_create(codigo="kg", defaults={"nombre": "Kilo", "tipo": "MASS", "factor_to_base": 1000})
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


class EnumeracionV4Tests(SimpleTestCase):
    def test_cache_por_instancia_no_reenumera(self):
        client = _client()
        contador = {"llamadas": 0}
        catalog = [{"PK_Producto": 1, "Nombre": "pastel"}]

        def fake_get_products(*, text_art="", **kw):
            contador["llamadas"] += 1
            return [r for r in catalog if text_art in r["Nombre"]]

        client.get_products = fake_get_products
        primera = client.get_all_products()
        llamadas_primera = contador["llamadas"]
        segunda = client.get_all_products()
        self.assertEqual(primera, segunda)
        self.assertEqual(contador["llamadas"], llamadas_primera)  # sin consultas nuevas

    def test_relogin_proactivo_cada_n_consultas(self):
        client = _client()
        client._last_branch_hint = "Matriz"
        logins = {"n": 0}
        client.login = lambda **kw: logins.__setitem__("n", logins["n"] + 1)
        catalog = [{"PK": i, "nombre": ch} for i, ch in enumerate("abcdefgh")]
        client._enumerate_catalog(
            _fake_point(catalog, limit=150), pk_field="PK", label="test",
            pause_seconds=0, relogin_every=3,
        )
        self.assertGreaterEqual(logins["n"], 2)


class ConversionTransfersTests(SimpleTestCase):
    """Transfers y producción convierten la unidad Point a la del ERP."""

    databases = "__all__"

    def _insumo_gramos(self):
        from maestros.models import Insumo, UnidadMedida

        kg, _ = UnidadMedida.objects.get_or_create(codigo="kg", defaults={"nombre": "Kilo", "tipo": "MASS", "factor_to_base": 1000})
        g, _ = UnidadMedida.objects.get_or_create(codigo="g", defaults={"nombre": "Gramo", "tipo": "MASS", "factor_to_base": 1})
        return Insumo.objects.create(nombre="Queso crema conv-test", unidad_base=g)

    def test_transfer_convierte_kg_a_gramos(self):
        from decimal import Decimal

        from django.utils import timezone

        from inventario.models import ExistenciaInsumo, MovimientoInventario
        from pos_bridge.models import PointBranch, PointTransferLine
        from pos_bridge.services.movement_sync_service import PointMovementSyncService

        insumo = self._insumo_gramos()
        branch, _ = PointBranch.objects.get_or_create(external_id="conv-br", defaults={"name": "Conv"})
        line = PointTransferLine.objects.create(
            transfer_external_id="T-1", source_hash="hash-conv-1", destination_branch=branch, origin_branch=branch,
            insumo=insumo, received_quantity=Decimal("32"), unit="KG",
            received_at=timezone.now(), registered_at=timezone.now(),
        )
        service = PointMovementSyncService()
        service._upsert_transfer_inventory_movement(line=line)
        mov = MovimientoInventario.objects.get(source_hash="hash-conv-1")
        self.assertEqual(mov.cantidad, Decimal("32000"))
        self.assertEqual(ExistenciaInsumo.objects.get(insumo=insumo).stock_actual, Decimal("32000"))

    def test_backfill_corrige_movimiento_viejo(self):
        from decimal import Decimal
        from io import StringIO

        from django.core.management import call_command
        from django.utils import timezone

        from inventario.models import MovimientoInventario
        from pos_bridge.models import PointBranch, PointTransferLine

        insumo = self._insumo_gramos()
        branch, _ = PointBranch.objects.get_or_create(external_id="conv-br", defaults={"name": "Conv"})
        PointTransferLine.objects.create(
            transfer_external_id="T-2", source_hash="hash-conv-2", destination_branch=branch, origin_branch=branch,
            insumo=insumo, received_quantity=Decimal("32"), unit="KG",
            received_at=timezone.now(), registered_at=timezone.now(),
        )
        MovimientoInventario.objects.create(
            source_hash="hash-conv-2", fecha=timezone.now(),
            tipo=MovimientoInventario.TIPO_ENTRADA, insumo=insumo,
            cantidad=Decimal("32"), referencia="POINT-TRANSFER:T-2",
        )
        salida = StringIO()
        call_command("corregir_unidades_movimientos_point", stdout=salida)
        mov = MovimientoInventario.objects.get(source_hash="hash-conv-2")
        self.assertEqual(mov.cantidad, Decimal("32000"))
        # idempotente
        call_command("corregir_unidades_movimientos_point", stdout=salida)
        self.assertEqual(MovimientoInventario.objects.get(source_hash="hash-conv-2").cantidad, Decimal("32000"))
