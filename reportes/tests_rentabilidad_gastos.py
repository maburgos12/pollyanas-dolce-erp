"""Lectura económica de gastos: pruebas contra PostgreSQL, sin mocks de fuentes."""
from datetime import date
from decimal import Decimal
import importlib.util

from django.test import SimpleTestCase, TestCase
from django.db import connection
from django.test.utils import CaptureQueriesContext
from core.models import Sucursal
from .models import (AreaPresupuesto, CategoriaGasto, CentroCosto, GastoOperativoMensual,
                     GastoRecurrente, GastoRecurrenteVersion, LineaPresupuestoMensual,
                     ObligacionGasto, PagoObligacionGasto, ReglaFuenteRubro, RubroPresupuesto)


class ContratoLectorGastosTests(SimpleTestCase):
    def test_existe_lector_mensual_independiente_de_la_consolidacion(self):
        self.assertIsNotNone(importlib.util.find_spec("reportes.services_rentabilidad_gastos"))

    def test_importe_mensual_reserva_centavo_para_ultimo_mes(self):
        from . import services_rentabilidad_gastos as servicio
        self.assertTrue(callable(getattr(servicio, "_importe_mensual", None)))
        self.assertEqual(servicio._importe_mensual(Decimal("100.01"), date(2026, 1, 1), date(2026, 2, 1), date(2026, 1, 1)), Decimal("50.00"))
        self.assertEqual(servicio._importe_mensual(Decimal("100.01"), date(2026, 1, 1), date(2026, 2, 1), date(2026, 2, 1)), Decimal("50.01"))


class LectorGastosMensualesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.mes = date(2026, 1, 1)
        cls.sucursal = Sucursal.objects.create(codigo="LECTOR", nombre="Sucursal lector")
        cls.area = AreaPresupuesto.objects.create(codigo="gastos-venta", nombre="Venta lector")
        cls.produccion = AreaPresupuesto.objects.create(codigo="produccion", nombre="Producción lector")
        cls.categoria = CategoriaGasto.objects.create(codigo="RENTA", nombre="Renta", capa_objetivo="SUCURSAL")
        cls.centro = CentroCosto.objects.create(codigo="LECTOR", nombre="Sucursal", tipo="SUCURSAL_VENTA", sucursal=cls.sucursal)
        cls.compartido = CentroCosto.objects.create(codigo="LECTOR-COM", nombre="Compartido", tipo="COMPARTIDO")
        cls.rubro = RubroPresupuesto.objects.create(area=cls.area, concepto="Arrendamiento local", tipo="EGRESO", sucursal=cls.sucursal)

    def leer(self, mes=None):
        from .services_rentabilidad_gastos import leer_gastos_mensuales
        return leer_gastos_mensuales(mes or self.mes)

    def gasto(self, **extra):
        datos = dict(periodo=self.mes, centro_costo=self.centro, categoria_gasto=self.categoria,
                     monto=Decimal("13242.00"), archivo_soporte="recibos/renta.pdf")
        datos.update(extra)
        return GastoOperativoMensual.objects.create(**datos)

    def regla(self, **extra):
        datos = dict(rubro=self.rubro, tipo_fuente="GASTO_OPERATIVO", categoria_gasto=self.categoria,
                     filtros={"desde": "2026-01"})
        datos.update(extra)
        return ReglaFuenteRubro.objects.create(**datos)

    def obligacion(self, gasto=None, **extra):
        datos = dict(origen="VARIABLE", area=self.area, rubro=self.rubro, centro_costo=self.centro,
                     categoria_gasto=self.categoria, concepto="Renta", periodo=self.mes,
                     fecha_gasto=self.mes, fecha_vencimiento=date(2026, 3, 1),
                     monto_reconocido=Decimal("13242.00"), gasto_operativo=gasto,
                     archivo_soporte="recibos/renta.pdf")
        datos.update(extra)
        return ObligacionGasto.objects.create(**datos)

    def test_real_directo_no_reaplica_porcentaje(self):
        gasto = self.gasto()
        self.regla(modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 20})
        resultado = self.leer()
        self.assertEqual(len(resultado["filas"]), 1)
        fila = resultado["filas"][0]
        self.assertEqual(fila["registro_id"], gasto.pk)
        self.assertEqual(fila["monto_mensual"], Decimal("13242.00"))
        self.assertEqual(fila["familia"], "renta")

    def test_recibo_compartido_20_80_conserva_centavos_y_area(self):
        self.gasto(centro_costo=self.compartido, monto=Decimal("66210.12"))
        rubro_prod = RubroPresupuesto.objects.create(area=self.produccion, concepto="Renta complejo", tipo="EGRESO")
        self.regla(centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 20})
        self.regla(rubro=rubro_prod, centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 80})
        filas = self.leer()["filas"]
        self.assertEqual(sorted(f["monto_mensual"] for f in filas), [Decimal("13242.02"), Decimal("52968.10")])
        self.assertIsNone(next(f for f in filas if f["area"] == "produccion")["sucursal_id"])

    def test_cobertura_bimestral_devenga_con_residuo_no_fecha_pago(self):
        self.regla()
        self.gasto(periodo=date(2026, 3, 1), monto=Decimal("100.01"),
                   cobertura_mes_inicio=self.mes, cobertura_mes_fin=date(2026, 2, 1))
        self.assertEqual(self.leer()["filas"][0]["monto_mensual"], Decimal("50.00"))
        self.assertEqual(self.leer(date(2026, 2, 1))["filas"][0]["monto_mensual"], Decimal("50.01"))
        self.assertEqual(self.leer(date(2026, 3, 1))["filas"], [])

    def test_sin_cobertura_preserva_mes_capturado(self):
        self.regla()
        self.gasto(periodo=date(2026, 3, 1))
        self.assertFalse(self.leer()["filas"])
        self.assertEqual(self.leer(date(2026, 3, 1))["filas"][0]["monto_mensual"], Decimal("13242.00"))

    def test_obligacion_y_gasto_vinculado_son_una_fuente(self):
        self.regla(tipo_fuente="OBLIGACION_GASTO", categoria_gasto=None)
        gasto = self.gasto()
        obligacion = self.obligacion(gasto)
        filas = self.leer()["filas"]
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["monto_original"], obligacion.monto_reconocido)
        self.assertIn(str(obligacion.pk), str(filas[0]["soporte"]))

    def test_cancelada_excluye_tambien_gasto_vinculado(self):
        self.regla()
        self.obligacion(self.gasto(), estado="CANCELADO")
        self.assertEqual(self.leer()["filas"], [])

    def test_estimada_presupuesto_y_nomina_no_suman(self):
        self.regla()
        self.gasto(es_estimado=True)
        self.gasto(tipo_dato="PRESUPUESTO")
        categoria = CategoriaGasto.objects.create(codigo="NOMINA_SUC", nombre="Nómina", capa_objetivo="SUCURSAL")
        self.gasto(categoria_gasto=categoria, fuente="IMPORTADA")
        self.assertEqual(self.leer()["filas"], [])

    def test_control_no_se_suma_y_espejo_automatico_no_duplica(self):
        self.regla()
        self.regla(modo_asignacion="CONTROL")
        self.gasto()
        LineaPresupuestoMensual.objects.create(rubro=self.rubro, periodo=self.mes, monto_real=Decimal("13242.00"), fuente_real="AUTO:GASTO_OPERATIVO")
        self.assertEqual(len(self.leer()["filas"]), 1)

    def test_categoria_desconocida_no_se_convierte_en_otros(self):
        categoria = CategoriaGasto.objects.create(codigo="DESCONOCIDO", nombre="Desconocido", capa_objetivo="SUCURSAL")
        gasto = self.gasto(categoria_gasto=categoria)
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertEqual(resultado["pendientes"][0]["registro_id"], gasto.pk)

    def test_gasto_sin_regla_queda_pendiente(self):
        gasto = self.gasto(centro_costo=self.compartido)
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertEqual(resultado["pendientes"][0]["registro_id"], gasto.pk)

    def test_reparto_sin_historia_no_retroaplica(self):
        self.regla(centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"porcentaje": 20})
        self.gasto(centro_costo=self.compartido)
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertTrue(any("hist" in f["detalle"].lower() for f in resultado["pendientes"]))

    def test_regla_faltante_genera_pendiente_aunque_no_haya_recibo(self):
        regla = self.regla()
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertTrue(any(f["regla_id"] == regla.pk and f["monto_mensual"] is None for f in resultado["pendientes"]))

    def test_manual_inequivoco_solo_original(self):
        self.rubro.metadata = {"familia": "renta"}
        self.rubro.save(update_fields=["metadata"])
        for version, monto in [("ORIGINAL", "111.00"), ("REVISADO", "222.00")]:
            LineaPresupuestoMensual.objects.create(rubro=self.rubro, periodo=self.mes, version=version, monto_real=Decimal(monto), fuente_real="MANUAL:responsable")
        self.assertEqual([f["monto_mensual"] for f in self.leer()["filas"]], [Decimal("111.00")])

    def test_manual_posible_espejo_queda_pendiente(self):
        self.regla()
        self.gasto()
        self.rubro.metadata = {"familia": "renta"}
        self.rubro.save(update_fields=["metadata"])
        LineaPresupuestoMensual.objects.create(rubro=self.rubro, periodo=self.mes, monto_real=Decimal("13242.00"), fuente_real="MANUAL:responsable")
        resultado = self.leer()
        self.assertEqual(len(resultado["filas"]), 1)
        self.assertTrue(any(f["origen"] == "LINEA_PRESUPUESTO" for f in resultado["pendientes"]))

    def test_bimestral_conocido_sin_cobertura_queda_pendiente(self):
        recurrente = GastoRecurrente.objects.create(area=self.area, rubro=self.rubro, centro_costo=self.centro,
                                                   categoria_gasto=self.categoria, concepto="Servicio bimestral")
        version = GastoRecurrenteVersion.objects.create(gasto_recurrente=recurrente, vigencia_inicio=self.mes,
                                                       monto=Decimal("100.01"), periodicidad_meses=2)
        self.obligacion(self.gasto(monto=Decimal("100.01")), gasto_recurrente=recurrente,
                        version_recurrente=version, monto_reconocido=Decimal("100.01"))
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertTrue(any("cobertura" in f["detalle"].lower() for f in resultado["pendientes"]))

    def test_lectura_sin_ninguna_escritura_y_sin_soporte_no_completa(self):
        self.regla()
        self.gasto(archivo_soporte="", fuente="IMPORTADA")
        with CaptureQueriesContext(connection) as capturas:
            resultado = self.leer()
        self.assertFalse(any(q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for q in capturas))
        self.assertNotEqual(resultado["filas"][0]["estado"], "COMPLETO")

    def test_centro_sucursal_inequivoco_no_requiere_regla_extra(self):
        self.gasto()
        filas = self.leer()["filas"]
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["sucursal_id"], self.sucursal.pk)
        self.assertEqual(filas[0]["area"], "gastos-venta")

    def test_fabricacion_y_empaque_no_se_vuelven_gasto_fijo_sucursal(self):
        self.categoria.capa_objetivo = "FABRICACION"
        self.categoria.save(update_fields=["capa_objetivo"])
        self.gasto()
        empaque = CategoriaGasto.objects.create(codigo="EMPAQUE", nombre="Empaque", capa_objetivo="SUCURSAL", bucket="EMPAQUE_PROD")
        self.gasto(categoria_gasto=empaque)
        resultado = self.leer()
        self.assertEqual(resultado, {"filas": [], "pendientes": []})

    def test_categoria_generica_usa_rubro_exacto_no_keyword(self):
        categoria = CategoriaGasto.objects.create(codigo="SERVICIOS", nombre="Servicios", capa_objetivo="SUCURSAL")
        self.gasto(categoria_gasto=categoria)
        self.regla(categoria_gasto=categoria)
        self.assertEqual([f["familia"] for f in self.leer()["filas"]], ["renta"])

    def test_sistemas_y_alarmas_por_conceptos_exactos_observados(self):
        categoria = CategoriaGasto.objects.create(codigo="SERVICIOS", nombre="Servicios", capa_objetivo="SUCURSAL")
        self.gasto(categoria_gasto=categoria)
        self.regla(categoria_gasto=categoria)
        for concepto, familia in [("Licencias y servicios de sistemas", "sistemas"),
                                   ("Servicio de monitoreo de alarmas y seguridad", "alarmas")]:
            with self.subTest(concepto=concepto):
                self.rubro.concepto = concepto
                self.rubro.save(update_fields=["concepto"])
                self.assertEqual([f["familia"] for f in self.leer()["filas"]], [familia])

    def test_pago_es_evidencia_y_no_otro_gasto(self):
        gasto = self.gasto()
        obligacion = self.obligacion(gasto)
        pago = PagoObligacionGasto.objects.create(obligacion=obligacion, fecha_pago=date(2026, 3, 1), monto=Decimal("13242.00"), metodo_pago="TRANSFERENCIA", referencia="RECIBO")
        filas = self.leer()["filas"]
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["soporte"]["pagos"][0]["id"], pago.pk)
        self.assertEqual(filas[0]["soporte"]["pagos"][0]["fecha_pago"], date(2026, 3, 1))

    def test_regla_centro_explicito_precede_tipo_y_sucursal_heredada(self):
        self.gasto(centro_costo=self.compartido)
        self.regla(centro_costo=self.compartido, filtros={"desde": "2026-01", "centro_tipo": "NO_COINCIDE"})
        filas = self.leer()["filas"]
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["sucursal_id"], self.sucursal.pk)

    def test_contrato_sin_recibo_no_extrapola_su_monto(self):
        recurrente = GastoRecurrente.objects.create(area=self.area, rubro=self.rubro, centro_costo=self.centro,
                                                   categoria_gasto=self.categoria, concepto="Renta mensual")
        version = GastoRecurrenteVersion.objects.create(gasto_recurrente=recurrente, vigencia_inicio=self.mes, monto=Decimal("100.00"))
        resultado = self.leer()
        self.assertEqual(resultado["filas"], [])
        self.assertTrue(any(f["registro_id"] == version.pk and f["monto_mensual"] is None for f in resultado["pendientes"]))

    def test_reparto_incompleto_no_certifica_importe_completo(self):
        self.gasto(centro_costo=self.compartido)
        self.regla(centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 20})
        resultado = self.leer()
        self.assertTrue(resultado["pendientes"])
        self.assertEqual(resultado["filas"][0]["estado"], "PARCIAL")

    def test_primero_reparte_por_sucursal_despues_mensualiza(self):
        self.gasto(centro_costo=self.compartido, monto=Decimal("100.03"), cobertura_mes_inicio=self.mes, cobertura_mes_fin=date(2026, 2, 1))
        rubro_prod = RubroPresupuesto.objects.create(area=self.produccion, concepto="Renta complejo", tipo="EGRESO")
        self.regla(centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 20})
        self.regla(rubro=rubro_prod, centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 80})
        enero, febrero = self.leer()["filas"], self.leer(date(2026, 2, 1))["filas"]
        renta_branch = [f["monto_mensual"] for f in enero + febrero if f["sucursal_id"] == self.sucursal.pk]
        self.assertEqual(renta_branch, [Decimal("10.00"), Decimal("10.01")])
        self.assertEqual(sum(f["monto_mensual"] for f in enero + febrero), Decimal("100.03"))

    def test_cobertura_se_divide_por_vigencias_historicas_explicitas(self):
        self.gasto(centro_costo=self.compartido, monto=Decimal("100.03"),
                   cobertura_mes_inicio=self.mes, cobertura_mes_fin=date(2026, 2, 1))
        rubro_prod = RubroPresupuesto.objects.create(area=self.produccion, concepto="Renta complejo", tipo="EGRESO")
        for desde, hasta, venta in [("2026-01", "2026-01", 20), ("2026-02", "2026-02", 40)]:
            for rubro, porcentaje in [(self.rubro, venta), (rubro_prod, 100 - venta)]:
                self.regla(rubro=rubro, centro_costo=self.compartido, modo_asignacion="DISTRIBUCION",
                           filtros={"desde": desde, "hasta": hasta, "porcentaje": porcentaje})
        enero = self.leer()
        febrero = self.leer(date(2026, 2, 1))
        self.assertEqual(len(enero["filas"]), 2)
        self.assertEqual(len(febrero["filas"]), 2)
        self.assertEqual(next(f["monto_mensual"] for f in enero["filas"] if f["sucursal_id"]), Decimal("10.00"))
        self.assertEqual(next(f["monto_mensual"] for f in febrero["filas"] if f["sucursal_id"]), Decimal("20.01"))
        self.assertEqual(sum(f["monto_mensual"] for f in enero["filas"] + febrero["filas"]), Decimal("100.03"))
        self.assertEqual(enero["pendientes"] + febrero["pendientes"], [])

    def test_recibo_ambiguo_no_satisface_dos_contratos_distintos(self):
        versiones = []
        for numero in (1, 2):
            recurrente = GastoRecurrente.objects.create(area=self.area, rubro=self.rubro,
                                                       centro_costo=self.centro, categoria_gasto=self.categoria,
                                                       concepto=f"Contrato {numero}")
            versiones.append(GastoRecurrenteVersion.objects.create(gasto_recurrente=recurrente,
                              vigencia_inicio=self.mes, monto=Decimal("100.00")))
        self.gasto()
        resultado = self.leer()
        self.assertEqual({f["registro_id"] for f in resultado["pendientes"]
                          if f["origen"] == "GASTO_RECURRENTE_VERSION"}, {v.pk for v in versiones})

    def test_version_enlazada_solo_satisface_su_contrato(self):
        versiones = []
        for numero in (1, 2):
            recurrente = GastoRecurrente.objects.create(area=self.area, rubro=self.rubro,
                                                       centro_costo=self.centro, categoria_gasto=self.categoria,
                                                       concepto=f"Contrato {numero}")
            versiones.append(GastoRecurrenteVersion.objects.create(gasto_recurrente=recurrente,
                              vigencia_inicio=self.mes, monto=Decimal("100.00")))
        self.obligacion(self.gasto(), gasto_recurrente=versiones[0].gasto_recurrente,
                        version_recurrente=versiones[0])
        resultado = self.leer()
        self.assertEqual({f["registro_id"] for f in resultado["pendientes"]
                          if f["origen"] == "GASTO_RECURRENTE_VERSION"}, {versiones[1].pk})

    def test_regla_sin_historia_deja_pendiente_especifico_sucursal(self):
        regla = self.regla(centro_costo=self.compartido, modo_asignacion="DISTRIBUCION",
                           filtros={"porcentaje": 20})
        self.gasto(centro_costo=self.compartido)
        resultado = self.leer()
        self.assertEqual(resultado["filas"], [])
        self.assertTrue(any(f["sucursal_id"] == self.sucursal.pk and f["regla_id"] == regla.pk
                            for f in resultado["pendientes"]))

    def test_total_compartido_100_sin_historia_no_certifica_mes_anterior(self):
        self.gasto(centro_costo=self.compartido)
        self.regla(centro_costo=self.compartido, filtros={"porcentaje": 100})
        resultado = self.leer()
        self.assertFalse(resultado["filas"])
        self.assertTrue(any("historia" in f["detalle"] for f in resultado["pendientes"]))

    def test_fila_contrato_conserva_version_vigencia_y_base_asignada(self):
        recurrente = GastoRecurrente.objects.create(area=self.area, rubro=self.rubro,
                                                   centro_costo=self.centro, categoria_gasto=self.categoria,
                                                   concepto="Contrato documentado")
        version = GastoRecurrenteVersion.objects.create(gasto_recurrente=recurrente,
                                                        vigencia_inicio=self.mes, monto=Decimal("100.00"))
        self.obligacion(self.gasto(monto=Decimal("100.00")), gasto_recurrente=recurrente,
                        version_recurrente=version, monto_reconocido=Decimal("100.00"))
        filas = self.leer()["filas"]
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0].get("version_recurrente_id"), version.pk)
        self.assertEqual(filas[0].get("vigencia_inicio"), self.mes)
        self.assertEqual(filas[0].get("base_asignada"), Decimal("100.00"))

    def test_fila_regla_conserva_vigencia_y_base_asignada(self):
        self.gasto(centro_costo=self.compartido, monto=Decimal("100.03"),
                   cobertura_mes_inicio=self.mes, cobertura_mes_fin=date(2026, 2, 1))
        self.regla(centro_costo=self.compartido, filtros={"desde": "2026-01", "hasta": "2026-02", "porcentaje": 100})
        fila = self.leer()["filas"][0]
        self.assertEqual(fila.get("base_asignada"), Decimal("100.03"))
        self.assertEqual(fila.get("vigencia_inicio"), self.mes)
        self.assertEqual(fila.get("vigencia_fin"), date(2026, 2, 1))

    def test_otro_recibo_valido_no_oculta_pendiente_compartido_en_sucursal(self):
        otra = Sucursal.objects.create(codigo="LECTOR-2", nombre="Otra sucursal")
        centro = CentroCosto.objects.create(codigo="LECTOR-2", nombre="Otra", tipo="SUCURSAL_VENTA", sucursal=otra)
        rubro = RubroPresupuesto.objects.create(area=self.area, concepto="Arrendamiento local", tipo="EGRESO", sucursal=otra)
        self.regla(centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 20})
        regla_otra = self.regla(rubro=rubro, centro_costo=self.compartido, modo_asignacion="DISTRIBUCION", filtros={"desde": "2026-01", "porcentaje": 80})
        recurrente = GastoRecurrente.objects.create(area=self.area, rubro=self.rubro, centro_costo=self.compartido,
                                                   categoria_gasto=self.categoria, concepto="Contrato compartido")
        version = GastoRecurrenteVersion.objects.create(gasto_recurrente=recurrente, vigencia_inicio=self.mes,
                                                        monto=Decimal("100.00"), periodicidad_meses=2)
        self.obligacion(self.gasto(centro_costo=self.compartido), centro_costo=self.compartido,
                        gasto_recurrente=recurrente, version_recurrente=version)
        self.obligacion(self.gasto(centro_costo=centro), centro_costo=centro, rubro=rubro)
        resultado = self.leer()
        self.assertTrue(any(f["sucursal_id"] == otra.pk for f in resultado["filas"]))
        self.assertTrue(any(f["sucursal_id"] == otra.pk and f["regla_id"] == regla_otra.pk
                            and "cobertura" in f["detalle"].lower() for f in resultado["pendientes"]))

    def test_rubro_compuesto_no_oculta_regla_sin_su_fuente(self):
        self.regla()
        self.gasto()
        telefono = CategoriaGasto.objects.create(codigo="TELEFONO_SUC", nombre="Teléfono", capa_objetivo="SUCURSAL")
        regla_faltante = self.regla(categoria_gasto=telefono)
        resultado = self.leer()
        self.assertEqual(len(resultado["filas"]), 1)
        self.assertTrue(any(f["regla_id"] == regla_faltante.pk and f["sucursal_id"] == self.sucursal.pk
                            for f in resultado["pendientes"]))

    def test_regla_obligacion_y_regla_gasto_del_mismo_origen_no_exigen_otro_recibo(self):
        self.regla()
        self.regla(tipo_fuente="OBLIGACION_GASTO", categoria_gasto=None)
        self.obligacion(self.gasto())
        resultado = self.leer()
        self.assertEqual(len(resultado["filas"]), 1)
        self.assertEqual(resultado["pendientes"], [])
