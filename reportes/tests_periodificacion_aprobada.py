"""El devengo aprobado conserva el recibo y no libera otras estimaciones."""
from datetime import date
from decimal import Decimal

from django.test import TestCase
from reportes import tests_rentabilidad_gastos as fixtures


class PeriodificacionAprobadaTests(TestCase):
    setUpTestData = classmethod(fixtures.LectorGastosMensualesTests.setUpTestData.__func__)
    gasto = fixtures.LectorGastosMensualesTests.gasto
    regla = fixtures.LectorGastosMensualesTests.regla
    leer = fixtures.LectorGastosMensualesTests.leer

    def preparar(self):
        self.categoria.codigo = "LUZ_SUC"
        self.categoria.save(update_fields=["codigo"])
        self.octubre = date(2026, 10, 1)
        self.noviembre = date(2026, 11, 1)
        self.partes = []
        aprobadas = {}
        for mes, importe in [(self.octubre, "9407.16"), (self.noviembre, "5158.76")]:
            clave = f"CFE:544260800272:BB000021278471:{mes:%Y%m}"
            self.partes.append(self.gasto(periodo=mes, monto=Decimal(importe),
                external_key=clave, es_estimado=True, fuente="IMPORTADA",
                cobertura_mes_inicio=mes, cobertura_mes_fin=mes,
                servicio_desde=date(2026, 9, 18), servicio_hasta=date(2026, 11, 18)))
            aprobadas[clave] = {"importe_facturado": "18510.86", "aprobacion": "DG-20261122",
                "sha256_pdf": "a" * 64, "servicio_desde": "2026-09-18", "servicio_hasta": "2026-11-18",
                "archivo_soporte": "recibos/renta.pdf"}
        self.aprobacion = self.regla(centro_costo=self.centro,
            filtros={"desde": "2026-10-01", "hasta": "2026-11-30",
                     "prorrateos_aprobados": aprobadas})

    def test_dos_porciones_aprobadas_no_se_reparten_de_nuevo(self):
        self.preparar()
        for mes, esperado in [(self.octubre, "9407.16"), (self.noviembre, "5158.76")]:
            filas = self.leer(mes)["filas"]
            self.assertEqual(len(filas), 1)
            self.assertEqual(filas[0]["monto_mensual"], Decimal(esperado))
            self.assertEqual(filas[0]["referencia_servicio"]["importe_facturado"], Decimal("18510.86"))
            self.assertEqual(filas[0]["referencia_servicio"]["referencia_mensual"], Decimal(esperado))
            self.assertEqual(filas[0]["referencia_servicio"]["dias_mes"], 31 if mes == self.octubre else 17)
        self.assertFalse(self.leer(date(2026, 9, 1))["filas"])
        self.assertFalse(self.leer(date(2026, 12, 1))["filas"])
        for g in self.partes:
            g.refresh_from_db()
            self.assertTrue(g.es_estimado)
            self.assertEqual(g.servicio_desde, date(2026, 9, 18))
            self.assertEqual(g.servicio_hasta, date(2026, 11, 18))

    def test_estimacion_no_autorizada_no_entra(self):
        self.preparar()
        self.gasto(periodo=self.octubre, monto=Decimal("100.00"), es_estimado=True)
        self.assertEqual(sum(f["monto_mensual"] for f in self.leer(self.octubre)["filas"]), Decimal("9407.16"))

    def test_importe_que_no_corresponde_al_servicio_no_entra(self):
        self.preparar()
        self.partes[0].monto += Decimal("0.01")
        self.partes[0].save(update_fields=["monto"])
        self.assertFalse(self.leer(self.octubre)["filas"])

    def test_aprobacion_fuera_de_vigencia_no_libera_estimacion(self):
        self.preparar()
        self.aprobacion.filtros["hasta"] = "2026-09-30"
        self.aprobacion.save(update_fields=["filtros"])
        self.assertFalse(self.leer(self.octubre)["filas"])

    def test_cambiar_fecha_y_monto_no_reutiliza_aprobacion_del_recibo(self):
        self.preparar()
        gasto = self.partes[0]
        gasto.servicio_desde = date(2026, 9, 19)
        gasto.monto = Decimal("9563.94")
        gasto.save(update_fields=["servicio_desde", "monto"])
        self.assertFalse(self.leer(self.octubre)["filas"])

    def test_cambiar_archivo_no_reutiliza_aprobacion(self):
        self.preparar()
        self.partes[0].archivo_soporte = "otro.pdf"
        self.partes[0].save(update_fields=["archivo_soporte"])
        self.assertFalse(self.leer(self.octubre)["filas"])

    def test_presupuesto_y_lector_usan_la_misma_porcion(self):
        from reportes.models import LineaPresupuestoMensual
        from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
        self.preparar()
        for mes, importe in [(self.octubre, "9407.16"), (self.noviembre, "5158.76")]:
            linea = LineaPresupuestoMensual.objects.create(rubro=self.rubro, periodo=mes)
            resumen = PresupuestoRealConsolidacionService().consolidar(periodo=mes, rubro_ids=[self.rubro.pk])
            self.assertFalse(resumen.errores)
            linea.refresh_from_db()
            self.assertEqual(linea.monto_real, Decimal(importe))
            self.assertEqual(linea.monto_real, sum(f["monto_mensual"] for f in self.leer(mes)["filas"]))
