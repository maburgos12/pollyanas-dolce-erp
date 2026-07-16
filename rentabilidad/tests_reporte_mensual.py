"""Tests del reporte mensual consolidado (12 meses)."""

from datetime import date
from decimal import Decimal
from io import StringIO
import json

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal
from rentabilidad.models_rentabilidad import SucursalRentabilidad
from rentabilidad.services_reporte_mensual import build_reporte_mensual_consolidado
from rrhh.models import Empleado, HoraExtra


class ReporteMensualConsolidadoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.suc_a = Sucursal.objects.create(codigo="SUC-A", nombre="Centro")
        cls.suc_b = Sucursal.objects.create(codigo="SUC-B", nombre="Guamúchil")

        # P&L 2026-06 sucursal A: ingresos 98,000 · nómina 20,000 · utilidad 31,500
        SucursalRentabilidad.objects.create(
            sucursal=cls.suc_a,
            periodo=date(2026, 6, 1),
            ventas_brutas=Decimal("100000"),
            descuentos=Decimal("2000"),
            costo_materia_prima=Decimal("30000"),
            renta=Decimal("10000"),
            nomina_directa=Decimal("20000"),
            servicios_luz_agua=Decimal("3000"),
            mantenimiento=Decimal("1000"),
            gastos_admin_prorrateados=Decimal("2000"),
            otros_gastos_fijos=Decimal("500"),
        )
        # P&L 2026-06 sucursal B: ingresos 50,000 · nómina 10,000 · utilidad 15,000
        SucursalRentabilidad.objects.create(
            sucursal=cls.suc_b,
            periodo=date(2026, 6, 1),
            ventas_brutas=Decimal("50000"),
            costo_materia_prima=Decimal("20000"),
            renta=Decimal("5000"),
            nomina_directa=Decimal("10000"),
        )
        # P&L 2025-06 sucursal A (base YoY): ingresos 80,000 · nómina 15,000 · utilidad 25,000
        SucursalRentabilidad.objects.create(
            sucursal=cls.suc_a,
            periodo=date(2025, 6, 1),
            ventas_brutas=Decimal("80000"),
            costo_materia_prima=Decimal("30000"),
            renta=Decimal("10000"),
            nomina_directa=Decimal("15000"),
        )

        cls.empleada = Empleado.objects.create(nombre="Empleada A", sucursal_ref=cls.suc_a)
        cls.empleado_sin_suc = Empleado.objects.create(nombre="Empleado Sin Sucursal")

        HoraExtra.objects.create(
            empleado=cls.empleada, fecha=date(2026, 6, 5), horas=Decimal("2"),
            monto_calculado=Decimal("500"), estado=HoraExtra.ESTADO_AUTORIZADO,
        )
        HoraExtra.objects.create(
            empleado=cls.empleada, fecha=date(2026, 6, 20), horas=Decimal("1.5"),
            monto_calculado=Decimal("300"), estado=HoraExtra.ESTADO_PAGADO,
        )
        # Pendiente y rechazada NO cuentan como nómina
        HoraExtra.objects.create(
            empleado=cls.empleada, fecha=date(2026, 6, 21), horas=Decimal("1"),
            monto_calculado=Decimal("200"), estado=HoraExtra.ESTADO_PENDIENTE,
        )
        HoraExtra.objects.create(
            empleado=cls.empleada, fecha=date(2026, 6, 22), horas=Decimal("1"),
            monto_calculado=Decimal("150"), estado=HoraExtra.ESTADO_RECHAZADO,
        )
        HoraExtra.objects.create(
            empleado=cls.empleado_sin_suc, fecha=date(2026, 6, 10), horas=Decimal("1"),
            monto_calculado=Decimal("100"), estado=HoraExtra.ESTADO_PAGADO,
        )

    def _fila(self, reporte, anio, mes):
        for fila in reporte["filas"]:
            if fila["periodo"] == date(anio, mes, 1):
                return fila
        self.fail(f"No hay fila para {anio}-{mes:02d}")

    def test_consolidado_cruza_ingresos_nomina_horas_extra_y_utilidad(self):
        reporte = build_reporte_mensual_consolidado(hasta=date(2026, 6, 1), meses=12)
        self.assertEqual(len(reporte["filas"]), 12)

        fila = self._fila(reporte, 2026, 6)
        self.assertEqual(fila["ingresos"], Decimal("148000.00"))
        self.assertEqual(fila["nomina"], Decimal("30000.00"))
        # Solo autorizado + pagado: 500 + 300 + 100
        self.assertEqual(fila["horas_extra"], Decimal("900.00"))
        self.assertEqual(fila["nomina_total"], Decimal("30900.00"))
        self.assertEqual(fila["nomina_pct_ventas"], Decimal("20.88"))
        self.assertEqual(fila["utilidad_neta"], Decimal("46500.00"))

    def test_variacion_yoy_contra_mismo_mes_anio_anterior(self):
        reporte = build_reporte_mensual_consolidado(hasta=date(2026, 6, 1), meses=12)
        fila = self._fila(reporte, 2026, 6)
        self.assertEqual(fila["yoy"]["ingresos"], Decimal("85.00"))
        self.assertEqual(fila["yoy"]["nomina_total"], Decimal("106.00"))
        self.assertEqual(fila["yoy"]["utilidad_neta"], Decimal("86.00"))

        # Mes sin base del año anterior → YoY None
        fila_sin_base = self._fila(reporte, 2026, 5)
        self.assertIsNone(fila_sin_base["yoy"]["ingresos"])

    def test_mes_sin_datos_aparece_en_cero(self):
        reporte = build_reporte_mensual_consolidado(hasta=date(2026, 6, 1), meses=12)
        fila = self._fila(reporte, 2025, 12)
        self.assertEqual(fila["ingresos"], Decimal("0.00"))
        self.assertEqual(fila["utilidad_neta"], Decimal("0.00"))
        self.assertIsNone(fila["nomina_pct_ventas"])

    def test_desglose_por_sucursal_incluye_sin_sucursal(self):
        reporte = build_reporte_mensual_consolidado(hasta=date(2026, 6, 1), meses=12)
        fila = self._fila(reporte, 2026, 6)
        por_nombre = {s["sucursal"]: s for s in fila["sucursales"]}
        self.assertEqual(por_nombre["Centro"]["horas_extra"], Decimal("800.00"))
        self.assertEqual(por_nombre["Centro"]["nomina_total"], Decimal("20800.00"))
        self.assertEqual(por_nombre["Centro"]["utilidad_neta"], Decimal("31500.00"))
        self.assertEqual(por_nombre["Sin sucursal asignada"]["horas_extra"], Decimal("100.00"))

    def test_filtro_por_sucursal(self):
        reporte = build_reporte_mensual_consolidado(
            hasta=date(2026, 6, 1), meses=12, sucursal_id=self.suc_a.pk
        )
        fila = self._fila(reporte, 2026, 6)
        self.assertEqual(fila["ingresos"], Decimal("98000.00"))
        self.assertEqual(fila["nomina"], Decimal("20000.00"))
        self.assertEqual(fila["horas_extra"], Decimal("800.00"))
        self.assertEqual(fila["utilidad_neta"], Decimal("31500.00"))

    def test_comando_formato_json(self):
        salida = StringIO()
        call_command(
            "reporte_mensual_consolidado",
            "--hasta", "2026-06", "--formato", "json",
            stdout=salida,
        )
        payload = json.loads(salida.getvalue())
        self.assertEqual(payload["meses"], 12)
        junio = [f for f in payload["filas"] if f["periodo"] == "2026-06-01"][0]
        self.assertEqual(junio["ingresos"], "148000.00")
        self.assertEqual(junio["horas_extra"], "900.00")
        self.assertEqual(junio["yoy"]["ingresos"], "85.00")

    def test_comando_formato_csv_con_detalle(self):
        salida = StringIO()
        call_command(
            "reporte_mensual_consolidado",
            "--hasta", "2026-06", "--formato", "csv", "--detalle",
            "--sucursal", "Centro",
            stdout=salida,
        )
        contenido = salida.getvalue()
        self.assertIn("2026-06,TOTAL,98000.00", contenido)
        self.assertIn("Centro", contenido)


class ReporteMensualViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.suc = Sucursal.objects.create(codigo="SUC-V", nombre="Vista")
        SucursalRentabilidad.objects.create(
            sucursal=cls.suc,
            periodo=date(2026, 6, 1),
            ventas_brutas=Decimal("10000"),
            nomina_directa=Decimal("2000"),
        )
        User = get_user_model()
        cls.dg = User.objects.create_superuser("dg-test", "dg@test.local", "clave-segura-123")

    def test_requiere_login(self):
        response = self.client.get(reverse("rentabilidad_reporte_mensual"))
        self.assertEqual(response.status_code, 302)

    def test_html_renderiza_tabla(self):
        self.client.force_login(self.dg)
        response = self.client.get(reverse("rentabilidad_reporte_mensual"), {"hasta": "2026-06"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Reporte mensual consolidado")
        self.assertContains(response, "Nómina total")

    def test_export_json(self):
        self.client.force_login(self.dg)
        response = self.client.get(
            reverse("rentabilidad_reporte_mensual"), {"hasta": "2026-06", "formato": "json"}
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        junio = [f for f in payload["filas"] if f["periodo"] == "2026-06-01"][0]
        self.assertEqual(junio["ingresos"], "10000.00")

    def test_export_csv(self):
        self.client.force_login(self.dg)
        response = self.client.get(
            reverse("rentabilidad_reporte_mensual"), {"hasta": "2026-06", "formato": "csv"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        self.assertIn("2026-06,TOTAL,10000.00", response.content.decode("utf-8"))
