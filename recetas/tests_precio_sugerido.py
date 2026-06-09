"""Tests de regresión para el panel de precio sugerido del monitor de márgenes.

Cubren la lógica financiera: margen meta derivado del P&L, costo de fabricación
completo como base, contribución y estados (OK / AJUSTE / CRÍTICO).
"""

import json
from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from recetas.models import Receta
from recetas.views.recetas import _ventana_meses_inicio
from reportes.models import (
    EmpresaResultadoMensual,
    ProductoCostoOperativoMensual,
    ProductoSucursalContribucionMensual,
)


class PrecioSugeridoTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_precio_sugerido",
            email="admin_precio_sugerido@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.sucursal = Sucursal.objects.create(
            codigo="S1",
            nombre="Sucursal Prueba",
            fecha_apertura=date(2020, 1, 1),
        )

        self.meses = 6
        self.current_period = date.today().replace(day=1)
        self.start_period = _ventana_meses_inicio(date.today(), self.meses)

        # P&L empresa: gasto comercial 20% y corporativo 10% de la venta.
        for periodo in (self.start_period, self.current_period):
            EmpresaResultadoMensual.objects.create(
                periodo=periodo,
                venta_total=Decimal("1000"),
                gasto_comercial_total=Decimal("200"),
                gasto_corporativo_total=Decimal("100"),
            )

        # Producto sano: costo fabricación sube 80 -> 95, ASP 200, contribución +.
        self.producto_ok = Receta.objects.create(
            nombre="Pastel Sano",
            codigo_point="OK001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            hash_contenido=f"hash-{uuid4()}",
        )
        self._costo_operativo(self.producto_ok, self.start_period, Decimal("80"), Decimal("200"))
        self._costo_operativo(self.producto_ok, self.current_period, Decimal("95"), Decimal("200"))
        self._contribucion(self.producto_ok, Decimal("200"), Decimal("95"), Decimal("75"))

        # Producto crítico: costo = precio, contribución negativa.
        self.producto_critico = Receta.objects.create(
            nombre="Pastel Critico",
            codigo_point="CR001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            hash_contenido=f"hash-{uuid4()}",
        )
        self._costo_operativo(self.producto_critico, self.start_period, Decimal("100"), Decimal("100"))
        self._costo_operativo(self.producto_critico, self.current_period, Decimal("100"), Decimal("100"))
        self._contribucion(self.producto_critico, Decimal("100"), Decimal("100"), Decimal("-15"))

    def _costo_operativo(self, receta, periodo, costo_fab, asp):
        ProductoCostoOperativoMensual.objects.create(
            periodo=periodo,
            receta=receta,
            unidades_base=Decimal("10"),
            venta_total=asp * Decimal("10"),
            asp=asp,
            costo_mp_unit=costo_fab * Decimal("0.6"),
            mano_obra_prod_unit=costo_fab * Decimal("0.2"),
            indirecto_prod_unit=costo_fab * Decimal("0.1"),
            empaque_prod_unit=costo_fab * Decimal("0.1"),
            costo_fabricacion_unit=costo_fab,
        )

    def _contribucion(self, receta, asp, costo_unit, contribucion_unit):
        unidades = Decimal("10")
        ProductoSucursalContribucionMensual.objects.create(
            periodo=self.current_period,
            receta=receta,
            sucursal=self.sucursal,
            unidades_vendidas=unidades,
            venta_total=asp * unidades,
            asp=asp,
            costo_producto_unit=costo_unit,
            costo_producto_total=costo_unit * unidades,
            gasto_comercial_unit=Decimal("0"),
            gasto_comercial_total=Decimal("0"),
            contribucion_total=contribucion_unit * unidades,
            contribucion_unit=contribucion_unit,
            margen_contribucion_pct=(contribucion_unit / asp * Decimal("100")),
        )

    def _fetch(self, **params):
        params.setdefault("meses", self.meses)
        params.setdefault("utilidad_meta", "15")
        response = self.client.get(
            reverse("recetas:monitor_margenes_precio_sugerido"), params
        )
        self.assertEqual(response.status_code, 200)
        return json.loads(response.content)

    def _row(self, data, receta):
        return next(r for r in data["rows"] if r["receta_id"] == receta.id)

    def test_margen_meta_se_deriva_del_pnl(self):
        data = self._fetch()
        # comercial 20% + corporativo 10% + utilidad meta 15% = 45%
        self.assertEqual(data["margen_meta"], "45.0")
        self.assertEqual(data["margen_meta_fuente"], "PNL")
        self.assertEqual(data["margen_meta_desglose"]["comercial_pct"], "20.0")
        self.assertEqual(data["margen_meta_desglose"]["corporativo_pct"], "10.0")
        self.assertEqual(data["margen_meta_desglose"]["utilidad_meta_pct"], "15.0")

    def test_utilidad_meta_mueve_el_margen_meta(self):
        data = self._fetch(utilidad_meta="25")
        # 20 + 10 + 25 = 55
        self.assertEqual(data["margen_meta"], "55.0")

    def test_producto_sano_es_ok_y_usa_costo_fabricacion_completo(self):
        row = self._row(self._fetch(), self.producto_ok)
        self.assertEqual(row["estado"], "OK")
        self.assertEqual(row["costo_ultimo"], "95.00")
        # margen bruto = (200 - 95) / 200 = 52.5% >= 45% meta
        self.assertEqual(row["margen_actual"], "52.5")
        # precio sugerido = 95 / (1 - 0.45) = 172.7 -> redondeo techo $5 = 175
        self.assertEqual(row["precio_sugerido"], "175.00")
        # variación de costo 80 -> 95 = +18.8%
        self.assertEqual(row["variacion_costo_pct"], "18.8")
        # desglose de componentes presente (fabricación completa)
        self.assertIsNotNone(row["componentes"])
        self.assertFalse(row["costo_solo_mp"])
        self.assertEqual(row["contribucion_unit"], "75.00")

    def test_producto_con_contribucion_negativa_es_critico(self):
        data = self._fetch()
        row = self._row(data, self.producto_critico)
        self.assertEqual(row["estado"], "CRITICO")
        self.assertEqual(row["contribucion_unit"], "-15.00")
        # margen bruto 0% no alcanza; sugiere subir precio
        self.assertEqual(row["margen_actual"], "0.0")
        self.assertEqual(data["criticos"], 1)

    def test_criticos_aparecen_primero_en_el_orden(self):
        data = self._fetch()
        self.assertEqual(data["rows"][0]["receta_id"], self.producto_critico.id)

    def test_filtro_por_familia_inexistente_devuelve_vacio(self):
        data = self._fetch(familia="NoExiste")
        self.assertEqual(data["total"], 0)

    def test_sin_pnl_cae_a_objetivo_de_margen_bruto(self):
        EmpresaResultadoMensual.objects.all().delete()
        data = self._fetch()
        # default objetivo 65%, fuente OBJETIVO, razón SIN_PNL
        self.assertEqual(data["margen_meta"], "65.0")
        self.assertEqual(data["margen_meta_fuente"], "OBJETIVO")
        self.assertEqual(data["margen_meta_desglose"]["razon"], "SIN_PNL")

    def test_gastos_pnl_incompletos_caen_a_objetivo(self):
        # P&L con venta pero gastos comercial/corporativo en cero (como producción).
        EmpresaResultadoMensual.objects.all().delete()
        EmpresaResultadoMensual.objects.create(
            periodo=self.current_period,
            venta_total=Decimal("1000"),
            gasto_comercial_total=Decimal("0"),
            gasto_corporativo_total=Decimal("0"),
        )
        data = self._fetch(objetivo_margen="65")
        self.assertEqual(data["margen_meta"], "65.0")
        self.assertEqual(data["margen_meta_fuente"], "OBJETIVO")
        self.assertEqual(data["margen_meta_desglose"]["razon"], "GASTOS_INCOMPLETOS")

    def test_objetivo_margen_es_configurable(self):
        EmpresaResultadoMensual.objects.all().delete()
        data = self._fetch(objetivo_margen="70")
        self.assertEqual(data["margen_meta"], "70.0")
        self.assertEqual(data["margen_meta_fuente"], "OBJETIVO")

    def test_export_csv_responde_archivo(self):
        response = self.client.get(
            reverse("recetas:monitor_margenes_precio_sugerido"),
            {"meses": self.meses, "export": "csv"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        self.assertIn("attachment", response["Content-Disposition"])
