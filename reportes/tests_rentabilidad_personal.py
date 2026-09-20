from datetime import date
from decimal import Decimal

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from reportes.models import (
    AreaPresupuesto,
    ExpedienteCedulaIMSS,
    LineaPresupuestoMensual,
    RubroPresupuesto,
)
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

    def leer(self, periodo=date(2026, 6, 1)):
        from reportes.services_rentabilidad_personal import leer_personal_mensual
        return leer_personal_mensual(periodo)

    def test_nomina_cerrada_usa_total_oficial_y_sucursal_erp_como_fuente_completa(self):
        linea = self.nomina()
        NominaConceptoLinea.objects.create(linea=linea, tipo="PERCEPCION", nombre="Sueldo", importe=1000)
        NominaConceptoLinea.objects.create(linea=linea, tipo="PERCEPCION", nombre="Bono", importe=200)
        NominaConceptoLinea.objects.create(linea=linea, tipo="DEDUCCION", nombre="ISR", importe=75)
        resultado = self.leer()
        filas = [f for f in resultado["filas"] if f["familia"] == "nomina"]
        self.assertEqual(sum(f["monto_mensual"] for f in filas), Decimal("1200"))
        self.assertEqual(filas[0]["sucursal_id"], self.sucursal.pk)
        self.assertEqual(filas[0]["estado"], "COMPLETO")
        self.assertIn("sucursal asignada en RRHH", filas[0]["detalle"])
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

    def test_conceptos_en_desacuerdo_no_invalidan_total_oficial_de_nomina(self):
        linea = self.nomina()
        NominaConceptoLinea.objects.create(linea=linea, tipo="PERCEPCION", nombre="Sueldo", importe=1500)
        resultado = self.leer()
        self.assertEqual(resultado["filas"][0]["monto_mensual"], Decimal("1200"))
        self.assertEqual(resultado["filas"][0]["estado"], "COMPLETO")
        self.assertIn("conceptos", resultado["filas"][0]["detalle"])
        self.assertFalse(any(p["familia"] == "nomina" for p in resultado["pendientes"]))

    def test_sipare_mensual_no_se_divide_y_no_duplica_version(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="Infonavit-RCV", tipo="EGRESO",
        )
        for version in ("ORIGINAL", "REVISADO"):
            LineaPresupuestoMensual.objects.create(
                rubro=rubro, periodo=date(2026, 6, 1), version=version,
                monto_real=Decimal("50.01"), fuente_real="AUTO:SIPARE",
                metadata={
                    "cedula_imss": {
                        "tipo": "BIMESTRAL",
                        "registro_patronal": "E52-40157-10-0",
                    }
                },
            )
        resultado = self.leer()
        cargas = [f for f in resultado["filas"] if f["familia"] == "cargas_patronales"]
        self.assertEqual(sum(f["monto_mensual"] for f in cargas), Decimal("50.01"))
        self.assertTrue(any("IMSS" in p["detalle"] for p in resultado["pendientes"]))

    def test_sipare_exige_expediente_aplicado_y_no_suma_revision_reemplazada(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="IMSS", tipo="EGRESO",
        )
        aplicado = ExpedienteCedulaIMSS.objects.create(
            tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
            periodo=date(2026, 6, 1),
            registro_patronal="E5240157100",
            estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
            total_patronal=Decimal("1200.00"),
        )
        reemplazado = ExpedienteCedulaIMSS.objects.create(
            tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
            periodo=date(2026, 6, 1),
            registro_patronal="E5240157100",
            revision=2,
            estado=ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO,
            total_patronal=Decimal("9999.00"),
        )
        linea = LineaPresupuestoMensual.objects.create(
            rubro=rubro, periodo=date(2026, 6, 1), monto_real=Decimal("1200.00"),
            fuente_real="AUTO:SIPARE",
            metadata={"expediente_cedula_imss_id": aplicado.pk},
        )

        cargas = [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        self.assertEqual(sum(f["monto_mensual"] for f in cargas), Decimal("1200.00"))

        linea.metadata = {
            "expediente_cedula_imss_id": reemplazado.pk,
            "cedula_imss": {
                "tipo": "MENSUAL",
                "registro_patronal": "E52-40157-10-0",
            },
        }
        linea.save(update_fields=["metadata"])
        self.assertFalse(
            [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        )

    def test_sipare_legado_requiere_registro_patronal_verificable(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="IMSS", tipo="EGRESO",
        )
        linea = LineaPresupuestoMensual.objects.create(
            rubro=rubro, periodo=date(2026, 6, 1), monto_real=Decimal("75.00"),
            fuente_real="AUTO:SIPARE", metadata={"cedula_imss": {"tipo": "MENSUAL"}},
        )
        self.assertFalse(
            [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        )

        linea.metadata = {
            "cedula_imss": {"tipo": "MENSUAL", "registro_patronal": "TEST"}
        }
        linea.save(update_fields=["metadata"])
        self.assertFalse(
            [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        )

        linea.metadata = {
            "cedula_imss": {"registro_patronal": "E52-40157-10-0"}
        }
        linea.save(update_fields=["metadata"])
        self.assertFalse(
            [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        )

        linea.metadata = {
            "cedula_imss": {"tipo": "MENSUAL", "registro_patronal": "E52-40157-10-0"}
        }
        linea.save(update_fields=["metadata"])
        cargas = [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        self.assertEqual(sum(f["monto_mensual"] for f in cargas), Decimal("75.00"))

    def test_expediente_enlazado_valida_tipo_periodo_registro_y_metadata(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="IMSS", tipo="EGRESO",
        )
        linea = LineaPresupuestoMensual.objects.create(
            rubro=rubro, periodo=date(2026, 6, 1), monto_real=Decimal("75.00"),
            fuente_real="AUTO:SIPARE",
        )
        casos = {
            "tipo": ExpedienteCedulaIMSS.objects.create(
                tipo=ExpedienteCedulaIMSS.TIPO_BIMESTRAL,
                periodo=date(2026, 6, 1),
                registro_patronal="E5240157100",
                estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
                total_patronal=Decimal("150.00"),
            ),
            "periodo": ExpedienteCedulaIMSS.objects.create(
                tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
                periodo=date(2026, 5, 1),
                registro_patronal="E5240157100",
                estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
                total_patronal=Decimal("75.00"),
            ),
            "registro": ExpedienteCedulaIMSS.objects.create(
                tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
                periodo=date(2026, 6, 1),
                registro_patronal="OTRO999",
                estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
                total_patronal=Decimal("75.00"),
            ),
            "tipo_periodo_registro": ExpedienteCedulaIMSS.objects.create(
                tipo=ExpedienteCedulaIMSS.TIPO_BIMESTRAL,
                periodo=date(2026, 8, 1),
                registro_patronal="OTRO999",
                estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
                total_patronal=Decimal("150.00"),
            ),
        }
        for nombre, expediente in casos.items():
            with self.subTest(nombre=nombre):
                linea.metadata = {"expediente_cedula_imss_id": expediente.pk}
                linea.save(update_fields=["metadata"])
                self.assertFalse(
                    [
                        fila for fila in self.leer()["filas"]
                        if fila["familia"] == "cargas_patronales"
                    ]
                )

        valido = ExpedienteCedulaIMSS.objects.create(
            tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
            periodo=date(2026, 6, 1),
            registro_patronal="E5240157100",
            revision=2,
            estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
            total_patronal=Decimal("75.00"),
        )
        linea.metadata = {
            "expediente_cedula_imss_id": valido.pk,
            "cedula_imss": {"tipo": "MENSUAL", "registro_patronal": "OTRO999"},
        }
        linea.save(update_fields=["metadata"])
        self.assertFalse(
            [f for f in self.leer()["filas"] if f["familia"] == "cargas_patronales"]
        )

    def test_expediente_bimestral_cubre_mes_de_cierre_y_mes_anterior(self):
        expediente = ExpedienteCedulaIMSS.objects.create(
            tipo=ExpedienteCedulaIMSS.TIPO_BIMESTRAL,
            periodo=date(2026, 8, 1),
            registro_patronal="E5240157100",
            estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
            total_patronal=Decimal("100.01"),
        )
        rubro = RubroPresupuesto.objects.create(
            area=self.area, sucursal=self.sucursal, concepto="Infonavit", tipo="EGRESO",
        )
        for periodo, monto in (
            (date(2026, 7, 1), Decimal("50.00")),
            (date(2026, 8, 1), Decimal("50.01")),
        ):
            LineaPresupuestoMensual.objects.create(
                rubro=rubro, periodo=periodo, monto_real=monto,
                fuente_real="AUTO:SIPARE",
                metadata={"expediente_cedula_imss_id": expediente.pk},
            )
            cargas = [
                fila for fila in self.leer(periodo)["filas"]
                if fila["familia"] == "cargas_patronales"
            ]
            self.assertEqual(sum(fila["monto_mensual"] for fila in cargas), monto)

    def test_expedientes_enlazados_se_validan_en_consulta_masiva_constante(self):
        expediente = ExpedienteCedulaIMSS.objects.create(
            tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
            periodo=date(2026, 6, 1),
            registro_patronal="E5240157100",
            estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
            total_patronal=Decimal("300.00"),
        )

        def crear_linea(sucursal, sufijo):
            rubro = RubroPresupuesto.objects.create(
                area=self.area, sucursal=sucursal, concepto="IMSS",
                codigo_cuenta=f"IMSS-{sufijo}", tipo="EGRESO",
            )
            LineaPresupuestoMensual.objects.create(
                rubro=rubro, periodo=date(2026, 6, 1), monto_real=Decimal("75.00"),
                fuente_real="AUTO:SIPARE",
                metadata={"expediente_cedula_imss_id": expediente.pk},
            )

        crear_linea(self.sucursal, "BASE")
        with CaptureQueriesContext(connection) as una_linea:
            self.leer()
        for indice in range(3):
            sucursal = Sucursal.objects.create(
                codigo=f"PER-{indice}", nombre=f"Sucursal {indice}",
            )
            crear_linea(sucursal, indice)
        with CaptureQueriesContext(connection) as varias_lineas:
            self.leer()

        self.assertEqual(len(varias_lineas), len(una_linea))

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
