from datetime import date
from decimal import Decimal

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from reportes.models import AreaPresupuesto, LineaPresupuestoMensual, RubroPresupuesto
from rrhh.models import Empleado, NominaConceptoLinea, NominaLinea, NominaPeriodo


class PersonalMensualTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sucursal = Sucursal.objects.create(codigo="PER-MAT", nombre="Matriz")
        cls.area = AreaPresupuesto.objects.create(codigo="gastos-venta", nombre="Ventas")

    def nomina(self, *, estatus="PAGADA", departamento="VENTAS", sucursal=True):
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 6, 1), fecha_fin=date(2026, 6, 15), estatus=estatus,
        )
        empleado = Empleado.objects.create(
            nombre="No mostrar nombre personal", codigo=f"PERSONA-{periodo.pk}",
            departamento=departamento, sucursal_ref=self.sucursal if sucursal else None,
        )
        return NominaLinea.objects.create(
            empleado=empleado, periodo=periodo, salario_base=Decimal("1000"),
            bonos=Decimal("200"), descuentos=Decimal("75"),
        )

    def leer(self):
        from reportes.services_rentabilidad_personal import leer_personal_mensual
        return leer_personal_mensual(date(2026, 6, 1))

    def test_total_no_suma_componentes_ni_descuentos_y_advierte_asignacion_historica(self):
        linea = self.nomina()
        NominaConceptoLinea.objects.create(linea=linea, tipo="PERCEPCION", nombre="Sueldo", importe=1000)
        NominaConceptoLinea.objects.create(linea=linea, tipo="PERCEPCION", nombre="Bono", importe=200)
        NominaConceptoLinea.objects.create(linea=linea, tipo="DEDUCCION", nombre="ISR", importe=75)
        resultado = self.leer()
        filas = [f for f in resultado["filas"] if f["familia"] == "nomina"]
        self.assertEqual(sum(f["monto_mensual"] for f in filas), Decimal("1200"))
        self.assertEqual(filas[0]["sucursal_id"], self.sucursal.pk)
        self.assertEqual(filas[0]["estado"], "PARCIAL")
        self.assertIn("histórica", filas[0]["detalle"])
        self.assertNotIn(linea.empleado.nombre, str(resultado))

    def test_borrador_y_produccion_no_suman_al_costo_de_ventas(self):
        self.nomina(estatus="BORRADOR")
        self.nomina(departamento="PRODUCCION")
        self.assertFalse([f for f in self.leer()["filas"] if f["familia"] == "nomina"])

    def test_sin_sucursal_no_reparte_automaticamente(self):
        self.nomina(sucursal=False)
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertTrue(any(p["sucursal_id"] is None and "sucursal" in p["detalle"] for p in resultado["pendientes"]))

    def test_conceptos_en_desacuerdo_se_reportan_sin_sumarlos(self):
        linea = self.nomina()
        NominaConceptoLinea.objects.create(linea=linea, tipo="PERCEPCION", nombre="Sueldo", importe=1500)
        resultado = self.leer()
        self.assertEqual(resultado["filas"][0]["monto_mensual"], Decimal("1200"))
        self.assertTrue(any("conceptos" in p["detalle"] for p in resultado["pendientes"]))

    def test_sipare_mensual_no_se_divide_y_no_duplica_version(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="Infonavit-RCV", tipo="EGRESO",
        )
        for version in ("ORIGINAL", "REVISADO"):
            LineaPresupuestoMensual.objects.create(
                rubro=rubro, periodo=date(2026, 6, 1), version=version,
                monto_real=Decimal("50.01"), fuente_real="AUTO:SIPARE",
                metadata={"cedula_imss": {"tipo": "BIMESTRAL", "registro_patronal": "TEST"}},
            )
        resultado = self.leer()
        cargas = [f for f in resultado["filas"] if f["familia"] == "cargas_patronales"]
        self.assertEqual(sum(f["monto_mensual"] for f in cargas), Decimal("50.01"))
        self.assertTrue(any("IMSS" in p["detalle"] for p in resultado["pendientes"]))

    def test_lectura_no_modifica_fuentes(self):
        self.nomina()
        with CaptureQueriesContext(connection) as queries:
            self.leer()
        self.assertFalse([q["sql"] for q in queries if q["sql"].lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"}])

    def test_manual_no_certifica_cuota_patronal_sin_cedula(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="IMSS", tipo="EGRESO",
        )
        LineaPresupuestoMensual.objects.create(
            rubro=rubro, periodo=date(2026, 6, 1), monto_real=75, fuente_real="MANUAL:test",
        )
        fila = next(f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales")
        self.assertEqual(fila["estado"], "PARCIAL")
        self.assertIn("parte patronal", fila["detalle"])
        self.assertNotIn("no incluye retenciones", fila["detalle"])
