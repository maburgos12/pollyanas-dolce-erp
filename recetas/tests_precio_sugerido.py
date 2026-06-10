"""Tests de regresión para el panel de precio mínimo rentable del monitor de márgenes.

Cubren la lógica financiera: margen meta como piso (objetivo vs P&L), costo vivo
universal, combinación de sabores (addons) en la base, y precio sugerido como piso
(nunca recomienda bajar un precio sano).
"""

import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from recetas.models import Receta, RecetaAgrupacionAddon
from recetas.views.recetas import _margen_meta_desde_pnl, _ventana_meses_inicio
from reportes.models import EmpresaResultadoMensual, ProductoCostoOperativoMensual


class _FakeCost:
    def __init__(self, total):
        self.total_cost = Decimal(str(total))


class MargenMetaPisoTests(TestCase):
    """Lógica pura del margen meta (piso vs P&L)."""

    def setUp(self):
        self.inicio = date(2026, 1, 1)
        self.fin = date(2026, 6, 1)

    def _empresa(self, comercial, corporativo, venta=Decimal("1000")):
        EmpresaResultadoMensual.objects.create(
            periodo=self.fin,
            venta_total=venta,
            gasto_comercial_total=comercial,
            gasto_corporativo_total=corporativo,
        )

    def test_sin_pnl_usa_objetivo(self):
        meta, desg, fuente = _margen_meta_desde_pnl(self.inicio, self.fin, Decimal("15"), Decimal("65"))
        self.assertEqual(meta, Decimal("65.0"))
        self.assertEqual(fuente, "OBJETIVO")
        self.assertEqual(desg["razon"], "SIN_PNL")

    def test_gastos_incompletos_usa_objetivo(self):
        self._empresa(Decimal("0"), Decimal("0"))
        meta, desg, fuente = _margen_meta_desde_pnl(self.inicio, self.fin, Decimal("15"), Decimal("65"))
        self.assertEqual(meta, Decimal("65.0"))
        self.assertEqual(fuente, "OBJETIVO")
        self.assertEqual(desg["razon"], "GASTOS_INCOMPLETOS")

    def test_objetivo_es_piso_cuando_pnl_deriva_menos(self):
        # comercial 20% + corporativo 10% + utilidad 15% = 45% < 65% piso
        self._empresa(Decimal("200"), Decimal("100"))
        meta, desg, fuente = _margen_meta_desde_pnl(self.inicio, self.fin, Decimal("15"), Decimal("65"))
        self.assertEqual(meta, Decimal("65.0"))
        self.assertEqual(fuente, "PNL_PISO")
        self.assertEqual(desg["derivado_pct"], "45.0")

    def test_pnl_sube_el_meta_cuando_supera_el_objetivo(self):
        # comercial 40% + corporativo 20% + utilidad 15% = 75% > 65%
        self._empresa(Decimal("400"), Decimal("200"))
        meta, desg, fuente = _margen_meta_desde_pnl(self.inicio, self.fin, Decimal("15"), Decimal("65"))
        self.assertEqual(meta, Decimal("75.0"))
        self.assertEqual(fuente, "PNL")

    def test_objetivo_configurable(self):
        meta, _, fuente = _margen_meta_desde_pnl(self.inicio, self.fin, Decimal("15"), Decimal("70"))
        self.assertEqual(meta, Decimal("70.0"))
        self.assertEqual(fuente, "OBJETIVO")


class PrecioMinimoViewTests(TestCase):
    """Vista completa: costo vivo, addons combinados, precio como piso."""

    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_precio_min",
            email="admin_precio_min@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)
        self.sucursal = Sucursal.objects.create(
            codigo="S1", nombre="Sucursal Prueba", fecha_apertura=date(2020, 1, 1)
        )
        self.current_period = date.today().replace(day=1)

        # Productos vendibles
        self.ok = self._receta("Pastel Sano", "OK01", costo_vivo=95, asp=500)
        self.critico = self._receta("Pastel Critico", "CR01", costo_vivo=100, asp=90)
        self.ajuste = self._receta("Pastel Ajuste", "AJ01", costo_vivo=100, asp=150)

        # Base con un sabor (addon) que se combina y se excluye del listado
        self.base = self._receta("Pay de Queso Grande", "0001", costo_vivo=178, asp=500, unidades=800)
        self.addon = self._receta("Sabor Fresa Grande Pay", "SFRESAG", costo_vivo=73, asp=0, unidades=800)
        RecetaAgrupacionAddon.objects.create(
            base_receta=self.base,
            addon_receta=self.addon,
            addon_codigo_point="SFRESAG",
            addon_nombre_point="Sabor Fresa Grande Pay",
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )

        # Mapa de costo vivo por receta
        self.cost_map = {
            self.ok.id: _FakeCost(95),
            self.critico.id: _FakeCost(100),
            self.ajuste.id: _FakeCost(100),
            self.base.id: _FakeCost(178),
            self.addon.id: _FakeCost(73),
        }

    def _receta(self, nombre, codigo, costo_vivo, asp, unidades=10):
        receta = Receta.objects.create(
            nombre=nombre,
            codigo_point=codigo,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            hash_contenido=f"hash-{uuid4()}",
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=self.current_period,
            receta=receta,
            unidades_base=Decimal(str(unidades)),
            asp=Decimal(str(asp)),
            costo_fabricacion_unit=Decimal("0"),
        )
        return receta

    def _fetch(self, **params):
        params.setdefault("meses", 6)
        params.setdefault("objetivo_margen", "65")
        with patch("recetas.views.recetas.resolve_recipe_cost_map", return_value=self.cost_map), \
             patch("recetas.views.recetas._median_point_prices_bulk", return_value={}):
            response = self.client.get(
                reverse("recetas:monitor_margenes_precio_sugerido"), params
            )
        self.assertEqual(response.status_code, 200)
        return json.loads(response.content)

    def _row(self, data, receta):
        return next((r for r in data["rows"] if r["receta_id"] == receta.id), None)

    def test_addon_se_excluye_del_listado(self):
        data = self._fetch()
        self.assertIsNone(self._row(data, self.addon))

    def test_addon_se_combina_en_la_base(self):
        data = self._fetch()
        row = self._row(data, self.base)
        self.assertIsNotNone(row)
        # base 178 + sabor 73 (mismas unidades) = 251
        self.assertEqual(row["costo_completo"], "251.00")
        self.assertTrue(row["tiene_sabores"])
        self.assertEqual(row["n_sabores"], 1)
        self.assertEqual(data["combinados"], 1)

    def test_producto_sano_no_sugiere_bajar(self):
        data = self._fetch()
        row = self._row(data, self.ok)
        # margen meta 65% objetivo; sugerido = 95/0.35 = 271.4 -> 275; ASP 500 >> 275
        self.assertEqual(row["estado"], "OK")
        self.assertEqual(row["precio_sugerido"], "275.00")
        self.assertIsNone(row["falta_subir_pct"])  # nunca recomienda bajar

    def test_producto_bajo_minimo_pide_subir(self):
        data = self._fetch()
        row = self._row(data, self.ajuste)
        # cost 100 -> sugerido 100/0.35 = 285.7 -> 290; ASP 150 < 290
        self.assertEqual(row["estado"], "AJUSTE")
        self.assertEqual(row["precio_sugerido"], "290.00")
        self.assertIsNotNone(row["falta_subir_pct"])
        self.assertGreater(float(row["falta_subir_pct"]), 0)

    def test_producto_bajo_costo_es_critico(self):
        data = self._fetch()
        row = self._row(data, self.critico)
        # ASP 90 < costo 100 -> margen negativo
        self.assertEqual(row["estado"], "CRITICO")
        self.assertEqual(row["margen_actual"], "-11.1")
        self.assertGreaterEqual(data["criticos"], 1)

    def test_margen_meta_es_objetivo_sin_pnl(self):
        data = self._fetch()
        self.assertEqual(data["margen_meta"], "65.0")
        self.assertEqual(data["margen_meta_fuente"], "OBJETIVO")

    def test_export_csv(self):
        with patch("recetas.views.recetas.resolve_recipe_cost_map", return_value=self.cost_map), \
             patch("recetas.views.recetas._median_point_prices_bulk", return_value={}):
            response = self.client.get(
                reverse("recetas:monitor_margenes_precio_sugerido"),
                {"meses": 6, "export": "csv"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
