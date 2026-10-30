from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from core.models import Sucursal
from control.models import MermaMensualSucursal
from pos_bridge.models import PointBranch, PointProduct, PointDailySale
from reportes.models import (AreaPresupuesto, RubroPresupuesto, LineaPresupuestoMensual,
    ProductoReventaCostoHistoricoMensual)
from reportes.services_rentabilidad_mensual import leer_costos_mensuales, aplicar_costos_en_memoria
from .models_rentabilidad import SucursalRentabilidad, EstadoRentabilidad
from .services_resultado import leer_resultado_mensual, registros_del_mes
from .services_reporte_mensual import build_reporte_mensual_consolidado, reporte_a_json
from .agente_rentabilidad import _construir_contexto


class ResultadoIntegralTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.periodo = date(2026, 9, 1)
        cls.sucursal = Sucursal.objects.create(codigo="INT", nombre="Sucursal de prueba")
        cls.branch = PointBranch.objects.create(external_id="INT", name="Prueba", erp_branch=cls.sucursal)
        cls.producto = PointProduct.objects.create(external_id="INT", sku="INT", name="Producto")
        cls.venta = PointDailySale.objects.create(branch=cls.branch, product=cls.producto,
            sale_date=cls.periodo, quantity=2, gross_amount=1160, tax_amount=160,
            discount_amount=50, net_amount=950, total_amount=1110)
        ProductoReventaCostoHistoricoMensual.objects.create(periodo=cls.periodo,
            producto_point=cls.producto, costo_promedio=100, source_date=cls.periodo)
        cls.rent = SucursalRentabilidad.objects.create(sucursal=cls.sucursal, periodo=cls.periodo,
            ventas_brutas=Decimal("9999"), nomina_directa=Decimal("500"))
        MermaMensualSucursal.objects.create(periodo=cls.periodo, sucursal=cls.sucursal,
            nombre_producto="Merma", unidades_merma=1, costo_merma=20)
        area = AreaPresupuesto.objects.create(codigo="produccion", nombre="Producción")
        rubro = RubroPresupuesto.objects.create(area=area, concepto="Nómina fábrica", tipo="EGRESO")
        cls.central = LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=cls.periodo,
            monto_real=300, fuente_real="AUTO:NOMINA")
        LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=cls.periodo,
            version="REVISADO", monto_real=300, fuente_real="AUTO:NOMINA")
        rubro_mp = RubroPresupuesto.objects.create(area=area, concepto="Material", tipo="COSTO")
        LineaPresupuestoMensual.objects.create(rubro=rubro_mp, periodo=cls.periodo,
            monto_real=200, fuente_real="AUTO:CONSUMO_MP")

    def test_fuente_neta_merma_y_central_sin_duplicar_versiones_o_material(self):
        resultado = leer_resultado_mensual(self.periodo)
        self.assertEqual(resultado["central_registrado"], self.central.monto_real)
        self.assertEqual(len(resultado["centrales"]), 1)
        rent = registros_del_mes(self.periodo)[0]
        self.assertEqual(rent.ventas_netas, self.venta.net_amount)
        self.assertEqual(rent.costo_variable_total, Decimal("220"))
        self.assertEqual(rent.utilidad_operativa, Decimal("730"))
        self.assertEqual(rent.estado, EstadoRentabilidad.SIN_DATOS)
        self.assertIsNone(rent.roi_anualizado)
        self.assertIsNone(rent.payback_meses_estimados)

    def test_lectura_exportacion_e_ia_coinciden_y_no_escriben(self):
        with CaptureQueriesContext(connection) as queries:
            rent = registros_del_mes(self.periodo)[0]
            reporte = build_reporte_mensual_consolidado(hasta=self.periodo, meses=1)
            contexto = _construir_contexto(rent)
        self.assertEqual(reporte["filas"][0]["ingresos"], rent.ventas_netas)
        self.assertEqual(reporte["filas"][0]["utilidad_neta"], rent.utilidad_operativa)
        self.assertIn("$730.00", contexto)
        self.assertIn("no calculables", contexto)
        self.assertIsNone(reporte_a_json(reporte)["filas"][0]["utilidad_neta_confirmada"])
        self.assertFalse([q for q in queries if q["sql"].lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"}])
        self.rent.refresh_from_db()
        self.assertEqual(self.rent.ventas_brutas, Decimal("9999"))
        self.assertEqual(self.rent.nomina_directa, Decimal("500"))

    def test_snapshot_sin_fuente_no_infla_consolidado(self):
        fantasma = Sucursal.objects.create(codigo="GHOST", nombre="Cerrada", activa=False)
        SucursalRentabilidad.objects.create(sucursal=fantasma, periodo=self.periodo,
            ventas_brutas=Decimal("88888"), renta=Decimal("800"))
        self.assertEqual([r.sucursal_id for r in registros_del_mes(self.periodo)], [self.sucursal.pk])
        self.assertTrue(SucursalRentabilidad.objects.filter(sucursal=fantasma).exists())

    def test_costo_ausente_se_marca_no_se_inventa_treinta_por_ciento(self):
        ProductoReventaCostoHistoricoMensual.objects.all().delete()
        rent = registros_del_mes(self.periodo)[0]
        self.assertEqual(rent.costo_reventa, 0)
        self.assertTrue(rent.costos_pendientes)

    def test_dashboard_muestra_resultado_central_y_detalle_no_afirma_recuperacion(self):
        user = get_user_model().objects.create_superuser("integral", password="test")
        self.client.force_login(user)
        response = self.client.get(reverse("rentabilidad_dashboard"), {"periodo": "2026-09"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["totales"]["resultado_empresa_parcial"], Decimal("430"))
        self.assertContains(response, "Nómina fábrica")
        response = self.client.get(reverse("rentabilidad_detalle", args=[self.rent.pk]))
        self.assertContains(response, "No calculable")
        self.assertNotContains(response, "Completado")

    def test_receta_lote_se_convierte_a_unidad_sin_mo_ni_porcentaje_inventado(self):
        from recetas.models import Receta, RecetaCostoVersion
        from django.utils import timezone
        receta = Receta.objects.create(nombre="Lote", nombre_normalizado="lote", hash_contenido="integral")
        version = RecetaCostoVersion.objects.create(receta=receta, version_num=1,
            hash_snapshot="integral", costo_total=1300, costo_mp=1000,
            costo_mo=200, costo_indirecto=100, rendimiento_cantidad=10)
        RecetaCostoVersion.objects.filter(pk=version.pk).update(creado_en=timezone.make_aware(datetime(2026, 8, 1)))
        PointDailySale.objects.filter(pk=self.venta.pk).update(receta=receta, gross_amount=150, tax_amount=0, discount_amount=0)
        rent = registros_del_mes(self.periodo)[0]
        self.assertEqual(rent.costo_materia_prima, Decimal("200"))
        self.assertEqual(rent.costo_reventa, 0)
        # Incluso cuando supera la venta no se reemplaza por un 30% ficticio.
        self.assertGreater(rent.costo_materia_prima, rent.ventas_netas)

    def test_ventas_fuente_sin_snapshot_se_incluyen_sin_crear_captura(self):
        self.rent.delete()
        rows = registros_del_mes(self.periodo)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].pk)
        self.assertEqual(rows[0].ventas_netas, self.venta.net_amount)
        self.assertFalse(SucursalRentabilidad.objects.exists())

    def test_totales_financieros_comparten_merma_y_central(self):
        from reportes.services_operating_finance import OperatingFinanceSnapshotService
        totales = OperatingFinanceSnapshotService()._profitability_totals(self.periodo)
        self.assertEqual(totales["ventas_netas"], self.venta.net_amount)
        self.assertEqual(totales["merma"], Decimal("20"))
        self.assertEqual(totales["central_registrado"], self.central.monto_real)
