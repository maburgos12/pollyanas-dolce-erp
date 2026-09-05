from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from core.models import Sucursal
from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from rrhh.models import Empleado, NominaLinea, NominaPeriodo
from rentabilidad.models_rentabilidad import EstadoRentabilidad, SucursalRentabilidad
from rentabilidad.tasks_rentabilidad import recalcular_rentabilidad_mensual
from reportes.services_rentabilidad_mensual import (
    aplicar_costos_en_memoria, costos_de_sucursal, leer_costos_mensuales,
)


class IntegracionGastosRecurrentesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sucursal = Sucursal.objects.create(codigo="REC-MAT", nombre="Matriz")
        cls.centro = CentroCosto.objects.create(codigo="REC-MAT", nombre="Ventas Matriz", tipo="SUCURSAL_VENTA", sucursal=cls.sucursal)
        cls.usuario = get_user_model().objects.create_superuser("recurrentes", password="test")

    def gasto(self, codigo="RENTA", monto="13242.00", **kwargs):
        cat, _ = CategoriaGasto.objects.get_or_create(codigo=codigo, defaults={"nombre": codigo})
        return GastoOperativoMensual.objects.create(
            categoria_gasto=cat, centro_costo=self.centro, periodo=date(2026, 6, 1),
            monto=Decimal(monto), archivo_soporte="recibo-test.pdf", **kwargs,
        )

    def nomina(self):
        empleado = Empleado.objects.create(codigo="REC-P", nombre="Nombre privado", departamento="VENTAS", sucursal_ref=self.sucursal)
        periodo = NominaPeriodo.objects.create(fecha_inicio=date(2026, 6, 1), fecha_fin=date(2026, 6, 15), estatus="PAGADA")
        return NominaLinea.objects.create(empleado=empleado, periodo=periodo, salario_base=1000, bonos=200)

    def test_recalculo_reemplaza_excel_por_erp_y_conserva_renta_proporcional(self):
        self.gasto()
        self.gasto("NOMINA", "9999")
        self.nomina()
        recalcular_rentabilidad_mensual(year=2026, month=6)
        rent = SucursalRentabilidad.objects.get(sucursal=self.sucursal, periodo=date(2026, 6, 1))
        self.assertEqual(rent.renta, Decimal("13242.00"))
        self.assertEqual(rent.nomina_directa, Decimal("1200.00"))
        # La cobertura parcial se rotula, ya no vacía el estado ni el PE.
        self.assertNotEqual(rent.estado, EstadoRentabilidad.SIN_DATOS)
        resumen = costos_de_sucursal(leer_costos_mensuales(rent.periodo), self.sucursal.pk)
        self.assertFalse(resumen["completo"])
        self.assertLess(resumen["cobertura_pct"], 100)
        recalcular_rentabilidad_mensual(year=2026, month=6)
        rent.refresh_from_db()
        self.assertEqual(rent.nomina_directa, Decimal("1200.00"))
        self.assertEqual(SucursalRentabilidad.objects.filter(sucursal=self.sucursal).count(), 1)

    def test_fuente_ausente_preserva_previo_y_marca_incompleto(self):
        rent = SucursalRentabilidad.objects.create(sucursal=self.sucursal, periodo=date(2026, 6, 1), renta=Decimal("800"), ventas_brutas=Decimal("2000"))
        recalcular_rentabilidad_mensual(year=2026, month=6)
        rent.refresh_from_db()
        self.assertEqual(rent.renta, Decimal("800"))
        aplicar_costos_en_memoria(rent, leer_costos_mensuales(rent.periodo))
        self.assertTrue(rent.fuente_gastos_incompleta)
        self.assertIn("Nómina ERP", rent.gastos_faltantes_display)

    def test_get_y_tarea_coinciden_sin_escribir_ni_mostrar_nomina_individual(self):
        self.gasto()
        self.nomina()
        recalcular_rentabilidad_mensual(year=2026, month=6)
        rent = SucursalRentabilidad.objects.get(sucursal=self.sucursal, periodo=date(2026, 6, 1))
        self.client.force_login(self.usuario)
        with CaptureQueriesContext(connection) as queries:
            response = self.client.get(reverse("rentabilidad_detalle", kwargs={"pk": rent.pk}))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["rent"].nomina_directa, Decimal("1200"))
        self.assertEqual(response.context["rent"].renta, rent.renta)
        self.assertContains(response, "No calculable")
        self.assertContains(response, "Nómina ERP")
        self.assertNotContains(response, "Nombre privado")
        self.assertContains(response, "sucursal asignada en RRHH")
        self.assertFalse([q["sql"] for q in queries if q["sql"].lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"}])

    def test_nomina_sola_calcula_pe_parcial_y_lo_rotula(self):
        """Con una sola familia cargada el PE se publica, pero marcado como parcial."""
        self.nomina()
        rent = SucursalRentabilidad.objects.create(sucursal=self.sucursal, periodo=date(2026, 6, 1), ventas_brutas=Decimal("2000"))
        self.client.force_login(self.usuario)
        response = self.client.get(reverse("rentabilidad_detalle", kwargs={"pk": rent.pk}))
        leido = response.context["rent"]
        self.assertEqual(leido.punto_equilibrio_mensual, Decimal("1200.00"))
        self.assertTrue(leido.fuente_gastos_incompleta)
        # 1 de 6 familias exigibles: cargas patronales no cuenta, no tiene fuente.
        self.assertEqual(leido.cobertura_gastos_pct, 17)
        self.assertContains(response, "17% de los gastos cargados")
        self.assertContains(response, "Falta confirmar")

    def test_snapshot_releido_recalcula_estado_desde_sus_importes(self):
        """Un SIN_DATOS heredado deja de ser pegajoso: manda la aritmética."""
        rent = SucursalRentabilidad.objects.create(
            sucursal=self.sucursal, periodo=date(2026, 6, 1),
            ventas_brutas=Decimal("2000"), renta=Decimal("100"),
        )
        SucursalRentabilidad.objects.filter(pk=rent.pk).update(estado=EstadoRentabilidad.SIN_DATOS)
        rent.refresh_from_db()
        self.assertEqual(rent.punto_equilibrio_mensual, Decimal("100.00"))
        rent.calcular_estado()
        self.assertNotEqual(rent.estado, EstadoRentabilidad.SIN_DATOS)

    def test_pendiente_sin_sucursal_no_baja_la_cobertura_de_la_sucursal(self):
        """Un origen sin sucursal es un problema del catálogo, no de esta sucursal."""
        resumen = costos_de_sucursal({
            "filas": [{"sucursal_id": self.sucursal.pk, "area": "gastos-venta", "familia": f,
                       "estado": "COMPLETO", "monto_mensual": Decimal("10")}
                      for f in ("renta", "electricidad", "telefono", "sistemas", "alarmas", "nomina")],
            "pendientes": [{"sucursal_id": None, "area": "gastos-venta", "familia": "electricidad",
                            "concepto": "Energía eléctrica", "estado": "PENDIENTE", "detalle": "sin sucursal"}],
        }, self.sucursal.pk)
        self.assertEqual(resumen["cobertura_pct"], 100)
        self.assertTrue(resumen["completo"])
        self.assertEqual(len(resumen["pendientes_globales"]), 1)
        # Lo único propio es el aviso de cargas patronales, que no bloquea.
        self.assertEqual([p["familia"] for p in resumen["pendientes"]], ["cargas_patronales"])

    def test_cargas_patronales_sin_fuente_no_bloquean_la_cobertura(self):
        """IMSS/RCV no tienen fuente automatizada: se informan, no reprueban."""
        resumen = costos_de_sucursal({
            "filas": [{"sucursal_id": self.sucursal.pk, "area": "gastos-venta", "familia": f,
                       "estado": "COMPLETO", "monto_mensual": Decimal("10")}
                      for f in ("renta", "electricidad", "telefono", "sistemas", "alarmas", "nomina")],
            "pendientes": [],
        }, self.sucursal.pk)
        self.assertEqual(resumen["familias_sin_fuente"], ["cargas_patronales"])
        self.assertEqual(resumen["cobertura_pct"], 100)
        self.assertTrue(resumen["completo"])

    def test_componente_ausente_no_borra_total_laboral_previo(self):
        from reportes.services_rentabilidad_mensual import aplicar_costos_en_memoria, leer_costos_mensuales
        self.nomina()
        rent = SucursalRentabilidad.objects.create(
            sucursal=self.sucursal, periodo=date(2026, 6, 1), nomina_directa=Decimal("1400"),
        )
        resumen = aplicar_costos_en_memoria(rent, leer_costos_mensuales(rent.periodo))
        self.assertEqual(rent.nomina_directa, Decimal("1400"))
        self.assertIn("nomina_directa", resumen["campos_previos_conservados"])
        self.assertEqual(resumen["totales"]["nomina"], Decimal("1200"))

    def test_detalle_enlaza_soporte_seguro_y_no_esquemas_ejecutables(self):
        gasto = self.gasto()
        gasto.archivo_soporte = "https://example.com/recibo.pdf"
        gasto.save(update_fields=["archivo_soporte"])
        rent = SucursalRentabilidad.objects.create(sucursal=self.sucursal, periodo=date(2026, 6, 1))
        self.client.force_login(self.usuario)
        response = self.client.get(reverse("rentabilidad_detalle", kwargs={"pk": rent.pk}))
        self.assertContains(response, 'href="https://example.com/recibo.pdf"')
        gasto.archivo_soporte = "javascript:alert(1)"
        gasto.save(update_fields=["archivo_soporte"])
        response = self.client.get(reverse("rentabilidad_detalle", kwargs={"pk": rent.pk}))
        self.assertNotContains(response, 'href="javascript:')

    def test_admin_parcial_no_borra_componentes_previos(self):
        from reportes.services_rentabilidad_mensual import aplicar_costos_en_memoria
        rent = SucursalRentabilidad(sucursal=self.sucursal, periodo=date(2026, 6, 1), gastos_admin_prorrateados=Decimal("150"))
        aplicar_costos_en_memoria(rent, {
            "filas": [{"sucursal_id": self.sucursal.pk, "area": "administracion", "familia": "sistemas", "estado": "COMPLETO", "monto_mensual": Decimal("100")}],
            "pendientes": [{"sucursal_id": self.sucursal.pk, "area": "administracion", "familia": "alarmas"}],
        })
        self.assertEqual(rent.gastos_admin_prorrateados, Decimal("150"))
