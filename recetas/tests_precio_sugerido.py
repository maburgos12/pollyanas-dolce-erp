"""Tests de regresión del panel de Precio mínimo rentable (Monitor de Márgenes).

Semántica honesta de costo:
- FAB_COMPLETO cuando ProductoCostoOperativoMensual trae componentes reales
  (mano de obra + indirectos + empaque > 0).
- MP_FALLBACK (solo materia prima, rotulado) cuando no hay fabricación completa.
- REVENTA_HISTORICO desde ProductoReventaCostoHistoricoMensual.
- SIN_COSTO si no hay ninguna fuente.
Catálogo/precio/familia desde Point; margen meta según fuente; precio sugerido como piso.
"""

import json
from io import BytesIO
from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from openpyxl import load_workbook

from core.models import Sucursal
from recetas.models import Receta, RecetaAgrupacionAddon, RecetaCostoVersion
from pos_bridge.models import PointProduct
from reportes.models import (
    ProductoCostoOperativoMensual,
    ProductoReventaCostoHistoricoMensual,
    RecetaCostoHistoricoMensual,
)


class PrecioSugeridoViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_ps", email="admin_ps@example.com", password="test12345",
        )
        self.client.force_login(self.user)
        self.sucursal = Sucursal.objects.create(
            codigo="S1", nombre="Sucursal Prueba", fecha_apertura=date(2020, 1, 1)
        )
        self.period = date.today().replace(day=1)

    # ---- helpers ----
    def _receta(self, nombre, codigo, modo=Receta.MODO_COSTEO_FABRICADO):
        return Receta.objects.create(
            nombre=nombre, codigo_point=codigo, tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=modo, familia="ERP", hash_contenido=f"h-{uuid4()}",
        )

    def _point(self, codigo, nombre, precio, categoria="Pastel Mediano", activo=True):
        return PointProduct.objects.create(
            external_id=f"ext-{codigo}-{uuid4().hex[:6]}", name=nombre, sku=codigo,
            category=categoria, precio=Decimal(str(precio)), precio_activo=activo,
        )

    def _operativo(self, receta, fab, mp=0, mo=0, ind=0, emp=0, unidades=10, periodo=None):
        return ProductoCostoOperativoMensual.objects.create(
            periodo=periodo or self.period, receta=receta,
            unidades_base=Decimal(str(unidades)), asp=Decimal("0"),
            costo_mp_unit=Decimal(str(mp)), mano_obra_prod_unit=Decimal(str(mo)),
            indirecto_prod_unit=Decimal(str(ind)), empaque_prod_unit=Decimal(str(emp)),
            costo_fabricacion_unit=Decimal(str(fab)),
        )

    def _mp_hist(self, receta, costo, periodo=None):
        return RecetaCostoHistoricoMensual.objects.create(
            periodo=periodo or self.period, receta=receta, costo_total=Decimal(str(costo)),
        )

    def _version(self, receta, costo):
        return RecetaCostoVersion.objects.create(
            receta=receta, version_num=1, hash_snapshot=f"s-{uuid4()}",
            costo_total=Decimal(str(costo)),
        )

    def _reventa_hist(self, point, costo, periodo=None):
        return ProductoReventaCostoHistoricoMensual.objects.create(
            periodo=periodo or self.period, producto_point=point,
            costo_promedio=Decimal(str(costo)),
        )

    def _fetch(self, **params):
        params.setdefault("meses", 6)
        resp = self.client.get(reverse("recetas:monitor_margenes_precio_sugerido"), params)
        self.assertEqual(resp.status_code, 200)
        return json.loads(resp.content)

    def _row(self, data, receta):
        return next((r for r in data["rows"] if r["receta_id"] == receta.id), None)

    def _row_by_name(self, data, name):
        return next((r for r in data["rows"] if r["nombre"] == name), None)

    # ---- fuente de costo ----
    def test_fab_completo_cuando_operativo_tiene_componentes(self):
        r = self._receta("Pastel Fab", "FAB1")
        self._point("FAB1", "Pastel Fab", precio=500)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["costo_fuente"], "FAB_COMPLETO")
        self.assertEqual(row["costo_completo"], "100.00")
        self.assertIsNotNone(row["breakdown"])
        self.assertEqual(row["breakdown"]["mo"], "20.000000")

    def test_mp_fallback_cuando_no_hay_operativo(self):
        r = self._receta("Pastel MP", "MP1")
        self._point("MP1", "Pastel MP", precio=500)
        self._mp_hist(r, 80)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["costo_fuente"], "MP_FALLBACK")
        self.assertEqual(row["costo_completo"], "80.00")
        self.assertIsNone(row["breakdown"])

    def test_operativo_sin_componentes_es_mp_fallback(self):
        # costo_fab>0 pero mo/ind/emp=0 -> NO es fabricación completa
        r = self._receta("Pastel MPop", "MP2")
        self._point("MP2", "Pastel MPop", precio=500)
        self._operativo(r, fab=90, mp=90, mo=0, ind=0, emp=0)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["costo_fuente"], "MP_FALLBACK")
        self.assertEqual(row["costo_completo"], "90.00")

    def test_version_es_ultimo_fallback(self):
        r = self._receta("Pastel Ver", "VER1")
        self._point("VER1", "Pastel Ver", precio=500)
        self._version(r, 70)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["costo_fuente"], "MP_FALLBACK")
        self.assertEqual(row["costo_completo"], "70.00")

    def test_reventa_usa_historico(self):
        r = self._receta("Agua", "RV1", modo=Receta.MODO_COSTEO_REVENTA)
        p = self._point("RV1", "Agua", precio=30, categoria="Granmark")
        self._reventa_hist(p, 12)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["costo_fuente"], "REVENTA_HISTORICO")
        self.assertEqual(row["costo_completo"], "12.00")
        self.assertTrue(row["es_reventa"])
        self.assertEqual(row["margen_meta"], "30")

    def test_sin_costo_cuando_no_hay_fuente(self):
        r = self._receta("Sin Costo", "SC1")
        self._point("SC1", "Sin Costo", precio=200)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["estado"], "SIN_COSTO")
        self.assertEqual(row["costo_fuente"], "SIN_COSTO")

    # ---- estados / margen 55% ----
    def test_margen_mayor_55_es_ok_y_no_baja(self):
        r = self._receta("Sano", "OK1")
        self._point("OK1", "Sano", precio=500)
        self._operativo(r, fab=95, mp=55, mo=20, ind=10, emp=10)
        row = self._row(self._fetch(), r)
        # margen (500-95)/500 = 81% >= 55 -> OK; sugerido 95/0.45=211->215 < 500
        self.assertEqual(row["estado"], "OK")
        self.assertEqual(row["precio_sugerido"], "215.00")
        self.assertIsNone(row["falta_subir_pct"])

    def test_margen_menor_55_requiere_ajuste(self):
        r = self._receta("Ajuste", "AJ1")
        self._point("AJ1", "Ajuste", precio=150)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        row = self._row(self._fetch(), r)
        # margen (150-100)/150 = 33% < 55 -> AJUSTE; sugerido 100/0.45=222->225
        self.assertEqual(row["estado"], "AJUSTE")
        self.assertEqual(row["precio_sugerido"], "225.00")
        self.assertIsNotNone(row["falta_subir_pct"])

    def test_precio_menor_costo_es_critico(self):
        r = self._receta("Critico", "CR1")
        self._point("CR1", "Critico", precio=90)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["estado"], "CRITICO")
        self.assertEqual(row["margen_actual"], "-11.1")

    # ---- catálogo Point ----
    def test_inactivo_se_excluye(self):
        r = self._receta("Inactivo", "IN1")
        self._point("IN1", "Inactivo", precio=200, activo=False)
        self._operativo(r, fab=50, mp=30, mo=10, ind=5, emp=5)
        self.assertIsNone(self._row(self._fetch(), r))

    def test_archivado_con_precio_vivo_se_excluye(self):
        # Producto desactivado en Point (active=False) que aún conserva precio
        # vigente NO debe aparecer en el catálogo de "productos activos de Point".
        r = self._receta("Archivado", "ARCH1")
        p = self._point("ARCH1", "Archivado", precio=200)
        p.active = False
        p.save(update_fields=["active"])
        self._operativo(r, fab=50, mp=30, mo=10, ind=5, emp=5)
        self.assertIsNone(self._row(self._fetch(), r))

    def test_familia_y_precio_vienen_de_point(self):
        r = self._receta("Conf", "CF1")
        self._point("CF1", "Conf", precio=400, categoria="Pay Grande")
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["familia_point"], "Pay Grande")
        self.assertEqual(row["precio_actual"], "400.00")

    def test_activo_sin_precio_point_no_desaparece(self):
        r = self._receta("Mini activo sin precio", "MINI1")
        p = self._point("MINI1", "Mini activo sin precio", precio=0, categoria="Pastel Mini")
        p.precio = None
        p.save(update_fields=["precio"])
        self._operativo(r, fab=50, mp=50, mo=0, ind=0, emp=0)
        row = self._row(self._fetch(), r)
        self.assertIsNotNone(row)
        self.assertEqual(row["familia_point"], "Pastel Mini")
        self.assertEqual(row["estado"], "SIN_PRECIO")
        self.assertIsNone(row["precio_actual"])

    def test_filtro_familia_point(self):
        r1 = self._receta("A", "A1"); self._point("A1", "A", 300, categoria="Pastel Mediano")
        self._operativo(r1, fab=100, mp=60, mo=20, ind=10, emp=10)
        r2 = self._receta("B", "B1"); self._point("B1", "B", 300, categoria="Pay Grande")
        self._operativo(r2, fab=100, mp=60, mo=20, ind=10, emp=10)
        data = self._fetch(familia="Pay Grande")
        self.assertIsNone(self._row(data, r1))
        self.assertIsNotNone(self._row(data, r2))

    # ---- addons ----
    def test_addon_se_lista_como_combinacion_base_mas_sabor(self):
        base = self._receta("Pay de Queso Grande", "0001")
        self._point("0001", "Pay de Queso Grande", precio=500)
        self._operativo(base, fab=178, mp=178, mo=0, ind=0, emp=0, unidades=800)
        self._mp_hist(base, 178)  # MP fallback de la base
        addon = self._receta("Sabor Fresa Grande", "SFRESAG")
        self._operativo(addon, fab=73, mp=73, mo=0, ind=0, emp=0, unidades=800)
        RecetaAgrupacionAddon.objects.create(
            base_receta=base, addon_receta=addon, addon_codigo_point="SFRESAG",
            addon_nombre_point="Sabor Fresa Grande", status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        data = self._fetch()
        self.assertIsNone(self._row(data, addon))  # addon excluido
        row = self._row_by_name(data, "Pay de Queso Grande")
        self.assertIsNotNone(row)
        self.assertEqual(row["costo_completo"], "178.00")
        self.assertFalse(row["tiene_sabores"])

        combo = self._row_by_name(data, "Pay de Queso Grande + Sabor Fresa Grande")
        self.assertIsNotNone(combo)
        self.assertEqual(combo["codigo_point"], "0001 + SFRESAG")
        self.assertEqual(combo["costo_completo"], "251.00")
        self.assertEqual(combo["addon_cost"], "73.00")
        self.assertEqual(combo["precio_actual"], "500.00")
        self.assertTrue(combo["tiene_sabores"])
        self.assertEqual(combo["precio_sugerido"], "720.00")

    # ---- export ----
    def test_export_csv(self):
        r = self._receta("CSV", "CS1")
        self._point("CS1", "CSV", precio=300)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        resp = self.client.get(
            reverse("recetas:monitor_margenes_precio_sugerido"), {"meses": 6, "export": "csv"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/csv", resp["Content-Type"])

    def test_export_xlsx_con_branding_y_agrupado_por_familia(self):
        r1 = self._receta("Pastel A", "XA1")
        self._point("XA1", "Pastel A", precio=300, categoria="Pastel Mediano")
        self._operativo(r1, fab=100, mp=60, mo=20, ind=10, emp=10)
        r2 = self._receta("Pay B", "XB1")
        self._point("XB1", "Pay B", precio=300, categoria="Pay Grande")
        self._operativo(r2, fab=100, mp=60, mo=20, ind=10, emp=10)

        resp = self.client.get(
            reverse("recetas:monitor_margenes_precio_sugerido"), {"meses": 6, "export": "xlsx"},
        )

        self.assertEqual(resp.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resp["Content-Type"],
        )
        wb = load_workbook(BytesIO(resp.content))
        ws = wb["Piso rentable"]
        self.assertEqual(ws["A5"].value, "Pollyana's Dolce - Precio minimo rentable")
        family_headers = [
            cell.value for row in ws.iter_rows(min_row=13, max_col=1) for cell in row
            if isinstance(cell.value, str) and cell.value.startswith("Familia:")
        ]
        self.assertIn("Familia: Pastel Mediano (1 productos)", family_headers)
        self.assertIn("Familia: Pay Grande (1 productos)", family_headers)
        self.assertTrue(any(drawing.anchor._from.row == 0 for drawing in ws._images))

    def test_export_xlsx_refleja_escenario_custom(self):
        r = self._receta("Pastel custom", "XPC1")
        self._point("XPC1", "Pastel custom", precio=300)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)

        resp = self.client.get(
            reverse("recetas:monitor_margenes_precio_sugerido"),
            {
                "meses": 6, "export": "xlsx",
                "costo_produccion_pct": "40",
                "margen_venta_pct": "60",
                "margen_reventa_pct": "25",
            },
        )

        self.assertEqual(resp.status_code, 200)
        ws = load_workbook(BytesIO(resp.content))["Piso rentable"]
        self.assertIn("costo producción 40%", ws["A7"].value)
        self.assertIn("margen fabricación 60%", ws["A7"].value)
        self.assertIn("margen reventa 25%", ws["A7"].value)

    # ---- margen meta por fuente ----
    def test_margen_meta_55_para_fab_completo(self):
        r = self._receta("Fab", "MF1")
        self._point("MF1", "Fab", precio=500)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        data = self._fetch()
        self.assertEqual(data["costo_produccion_pct"], "35")
        self.assertEqual(data["margen_meta_fab"], "55")
        self.assertEqual(data["margen_meta_mp"], "65")
        self.assertEqual(self._row(data, r)["margen_meta"], "55")

    def test_margen_fabricacion_custom_cambia_estado_y_piso(self):
        r = self._receta("Fab Custom", "MFC1")
        self._point("MFC1", "Fab Custom", precio=240)
        self._operativo(r, fab=100, mp=60, mo=20, ind=10, emp=10)
        row = self._row(self._fetch(margen_venta_pct="60"), r)
        self.assertEqual(row["margen_meta"], "60")
        self.assertEqual(row["estado"], "AJUSTE")
        self.assertEqual(row["precio_sugerido"], "250.00")

    def test_mp_fallback_usa_margen_meta_65(self):
        # Solo MP: meta 65%. Precio 200, costo 80 -> margen 60% (>=55 pero <65) -> AJUSTE.
        r = self._receta("SoloMP", "MM1")
        self._point("MM1", "SoloMP", precio=200)
        self._mp_hist(r, 80)
        row = self._row(self._fetch(), r)
        self.assertEqual(row["margen_meta"], "65")
        self.assertEqual(row["estado"], "AJUSTE")
        # sugerido = 80 / (1 - 0.65) = 228.57 -> redondeo techo $5 = 230
        self.assertEqual(row["precio_sugerido"], "230.00")

    def test_costo_produccion_custom_deriva_margen_mp(self):
        r = self._receta("SoloMP Custom", "MMC1")
        self._point("MMC1", "SoloMP Custom", precio=200)
        self._mp_hist(r, 80)
        data = self._fetch(costo_produccion_pct="40")
        row = self._row(data, r)
        self.assertEqual(data["costo_produccion_pct"], "40")
        self.assertEqual(data["margen_meta_mp"], "60")
        self.assertEqual(row["margen_meta"], "60")
        self.assertEqual(row["estado"], "OK")
        self.assertEqual(row["precio_sugerido"], "200.00")

    def test_combo_con_sabor_hereda_margen_mp_custom(self):
        base = self._receta("Pay Base", "CB1")
        self._point("CB1", "Pay Base", precio=500)
        self._mp_hist(base, 178)
        addon = self._receta("Sabor Nuez", "SNUEZ")
        self._operativo(addon, fab=73, mp=73, unidades=800)
        RecetaAgrupacionAddon.objects.create(
            base_receta=base, addon_receta=addon, addon_codigo_point="SNUEZ",
            addon_nombre_point="Sabor Nuez", status=RecetaAgrupacionAddon.STATUS_APPROVED,
        )
        combo = self._row_by_name(self._fetch(costo_produccion_pct="40"), "Pay Base + Sabor Nuez")
        self.assertEqual(combo["margen_meta"], "60")
        self.assertEqual(combo["precio_sugerido"], "630.00")

    def test_reventa_usa_margen_meta_30_y_no_fuerza_markup_fabricado(self):
        r = self._receta("Caja carton", "CAJA1", modo=Receta.MODO_COSTEO_REVENTA)
        p = self._point("CAJA1", "Caja carton", precio=60, categoria="Accesorios")
        self._reventa_hist(p, Decimal("19.87"))
        data = self._fetch()
        row = self._row(data, r)
        self.assertEqual(data["margen_meta_reventa"], "30")
        self.assertEqual(row["margen_meta"], "30")
        self.assertEqual(row["estado"], "OK")
        # sugerido = 19.87 / (1 - 0.30) = 28.39 -> redondeo techo $5 = 30
        self.assertEqual(row["precio_sugerido"], "30.00")
        self.assertIsNone(row["falta_subir_pct"])

    def test_margen_reventa_custom(self):
        r = self._receta("Caja custom", "CAJA2", modo=Receta.MODO_COSTEO_REVENTA)
        p = self._point("CAJA2", "Caja custom", precio=45, categoria="Accesorios")
        self._reventa_hist(p, Decimal("30"))
        data = self._fetch(margen_reventa_pct="40")
        row = self._row(data, r)
        self.assertEqual(data["margen_meta_reventa"], "40")
        self.assertEqual(row["margen_meta"], "40")
        self.assertEqual(row["precio_sugerido"], "50.00")

    def test_parametro_porcentaje_invalido_responde_400(self):
        resp = self.client.get(
            reverse("recetas:monitor_margenes_precio_sugerido"),
            {"meses": 6, "costo_produccion_pct": "5"},
        )
        self.assertEqual(resp.status_code, 400)
        payload = json.loads(resp.content)
        self.assertIn("costo_produccion_pct", payload["param_errors"])
