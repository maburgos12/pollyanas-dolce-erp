"""Pruebas de consolidación del real en el presupuesto maestro."""

from datetime import date
from decimal import Decimal
from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch
from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact
from reportes.models import (
    AreaPresupuesto,
    CategoriaGasto,
    CentroCosto,
    EmpresaResultadoMensual,
    GastoOperativoMensual,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_presupuesto_maestro import PresupuestoMaestroService
from reportes.services_presupuesto_real import (
    PresupuestoRealConsolidacionService,
    migrar_fuentes_legadas,
)
from rrhh.models import Empleado, NominaLinea, NominaPeriodo


class PresupuestoRealConsolidacionTests(TestCase):
    """Valida las fuentes automáticas y la protección de capturas manuales."""

    @classmethod
    def setUpTestData(cls):
        cls.periodo = date(2026, 3, 1)
        cls.sucursal = Sucursal.objects.create(codigo="GVE01", nombre="Centro")
        cls.otra_sucursal = Sucursal.objects.create(codigo="GVE02", nombre="Norte")
        cls.area = AreaPresupuesto.objects.create(nombre="Pruebas", codigo="pruebas")
        cls.categoria = CategoriaGasto.objects.create(
            codigo="PRUEBA_REAL",
            nombre="Categoría prueba",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
        )
        cls.otra_categoria = CategoriaGasto.objects.create(
            codigo="OTRA_PRUEBA_REAL",
            nombre="Otra categoría",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
        )
        cls.centro = CentroCosto.objects.create(
            codigo="CC-GVE01",
            nombre="Centro sucursal",
            tipo=CentroCosto.TIPO_SUCURSAL,
            sucursal=cls.sucursal,
        )
        cls.otro_centro = CentroCosto.objects.create(
            codigo="CC-GVE02",
            nombre="Centro otra sucursal",
            tipo=CentroCosto.TIPO_SUCURSAL,
            sucursal=cls.otra_sucursal,
        )
        cls.corporativo = CentroCosto.objects.create(
            codigo="CC-CORP",
            nombre="Corporativo",
            tipo=CentroCosto.TIPO_CORPORATIVO,
        )

    def crear_linea(self, concepto="Rubro prueba", *, sucursal=None, monto_real=None, fuente_real="", area=None, tipo=None):
        rubro = RubroPresupuesto.objects.create(
            area=area or self.area,
            concepto=concepto,
            tipo=tipo or RubroPresupuesto.TIPO_EGRESO,
            sucursal=sucursal,
        )
        linea = LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=self.periodo,
            monto_presupuesto=Decimal("1000"),
            monto_real=monto_real,
            fuente_real=fuente_real,
        )
        return rubro, linea

    def crear_gasto(self, monto, *, periodo=None, centro=None, categoria=None, tipo_dato=None):
        return GastoOperativoMensual.objects.create(
            periodo=periodo or self.periodo,
            centro_costo=centro or self.centro,
            categoria_gasto=categoria or self.categoria,
            monto=Decimal(str(monto)),
            tipo_dato=tipo_dato or GastoOperativoMensual.TIPO_DATO_REAL,
        )

    def crear_regla_gasto(self, rubro, **kwargs):
        return ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            categoria_gasto=self.categoria,
            **kwargs,
        )

    def consolidar(self, **kwargs):
        return PresupuestoRealConsolidacionService().consolidar(periodo=self.periodo, **kwargs)

    def test_gasto_operativo_suma_solo_reales_del_periodo_categoria_y_sucursal(self):
        """GASTO_OPERATIVO ignora presupuesto, otros meses, categorías y sucursales."""
        rubro, linea = self.crear_linea(sucursal=self.sucursal)
        self.crear_regla_gasto(rubro)
        self.crear_gasto("100")
        self.crear_gasto("25")
        self.crear_gasto("900", tipo_dato=GastoOperativoMensual.TIPO_DATO_PRESUPUESTO)
        self.crear_gasto("800", periodo=date(2026, 2, 1))
        self.crear_gasto("700", centro=self.otro_centro)
        self.crear_gasto("600", categoria=self.otra_categoria)

        self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("125.00"))
        self.assertEqual(linea.fuente_real, "AUTO:GASTO_OPERATIVO")

    def test_gasto_operativo_filtra_centro_corporativo_sin_sucursal(self):
        """El filtro centro_tipo limita gastos de rubros sin sucursal."""
        rubro, linea = self.crear_linea(concepto="Corporativo")
        self.crear_regla_gasto(rubro, filtros={"centro_tipo": "CORPORATIVO"})
        self.crear_gasto("310", centro=self.corporativo)
        self.crear_gasto("999", centro=self.centro)

        self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("310.00"))

    def test_nomina_filtra_campo_estatus_mes_departamento_y_sucursal(self):
        """NOMINA suma salario base cerrado/pagado del departamento y sucursal."""
        rubro, linea = self.crear_linea(concepto="Sueldos ventas", sucursal=self.sucursal)
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA,
            filtros={"campo_monto": "salario_base", "departamento": "ventas"},
        )
        empleado_valido = Empleado.objects.create(
            codigo="EMP-REAL-1", nombre="Venta válida", departamento=Empleado.DEP_VENTAS, sucursal_ref=self.sucursal
        )
        empleado_otro_depto = Empleado.objects.create(
            codigo="EMP-REAL-2", nombre="Producción", departamento=Empleado.DEP_PRODUCCION, sucursal_ref=self.sucursal
        )
        empleado_otra_sucursal = Empleado.objects.create(
            codigo="EMP-REAL-3", nombre="Venta norte", departamento=Empleado.DEP_VENTAS, sucursal_ref=self.otra_sucursal
        )

        def agregar_linea(folio, fin, estatus, empleado, monto):
            periodo = NominaPeriodo.objects.create(
                folio=folio, fecha_inicio=fin.replace(day=1), fecha_fin=fin, estatus=estatus
            )
            NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal(str(monto)))

        agregar_linea("NOM-CERRADA", date(2026, 3, 15), NominaPeriodo.ESTATUS_CERRADA, empleado_valido, 100)
        agregar_linea("NOM-PAGADA", date(2026, 3, 31), NominaPeriodo.ESTATUS_PAGADA, empleado_valido, 150)
        agregar_linea("NOM-BORRADOR", date(2026, 3, 20), NominaPeriodo.ESTATUS_BORRADOR, empleado_valido, 900)
        agregar_linea("NOM-OTRO-MES", date(2026, 2, 28), NominaPeriodo.ESTATUS_PAGADA, empleado_valido, 800)
        agregar_linea("NOM-OTRO-DEP", date(2026, 3, 10), NominaPeriodo.ESTATUS_PAGADA, empleado_otro_depto, 700)
        agregar_linea("NOM-OTRA-SUC", date(2026, 3, 11), NominaPeriodo.ESTATUS_PAGADA, empleado_otra_sucursal, 600)

        self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("250.00"))

    def test_venta_pos_normaliza_categoria_producto_y_filtra_sucursal(self):
        """VENTA_POS compara categoría/producto sin distinguir caso ni acentos."""
        rubro, linea = self.crear_linea(concepto="Bollo chocolate", sucursal=self.sucursal)
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_VENTA_POS,
            filtros={"categoria_pos": "BÓLLO", "producto_pos": "CHOCOLÁTE"},
        )
        branch = PointBranch.objects.create(external_id="POINT-GVE01", name="Centro", erp_branch=self.sucursal)
        otro_branch = PointBranch.objects.create(external_id="POINT-GVE02", name="Norte", erp_branch=self.otra_sucursal)

        def venta(branch_obj, fecha, categoria, producto, monto):
            PointSalesDailyProductFact.objects.create(
                branch=branch_obj,
                sale_date=fecha,
                sucursal_nombre=branch_obj.name,
                categoria=categoria,
                producto_nombre_historico=producto,
                total_venta=Decimal(str(monto)),
                total_venta_neta=Decimal(str(monto)) - Decimal("1"),
            )

        venta(branch, date(2026, 3, 2), "bollo", "chocolate", 100)
        venta(branch, date(2026, 3, 9), "BÓLLO", "CHOCOLÁTE", 50)
        venta(branch, date(2026, 2, 9), "BOLLO", "CHOCOLATE", 800)
        venta(branch, date(2026, 3, 10), "BOLLO", "VAINILLA", 700)
        venta(otro_branch, date(2026, 3, 11), "BOLLO", "CHOCOLATE", 600)

        self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("150.00"))

    def test_signo_resta_y_reglas_distintas_forman_fuente_ordenada(self):
        """Dos tipos de fuente se suman con signo y generan fuente alfabética."""
        rubro, linea = self.crear_linea(concepto="Combinado", sucursal=self.sucursal)
        self.crear_regla_gasto(rubro)
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA,
            signo=-1,
            filtros={"campo_monto": "salario_base", "departamento": "VENTAS"},
        )
        self.crear_gasto("500")
        empleado = Empleado.objects.create(
            codigo="EMP-SIGNO", nombre="Empleado signo", departamento=Empleado.DEP_VENTAS, sucursal_ref=self.sucursal
        )
        nomina = NominaPeriodo.objects.create(
            folio="NOM-SIGNO", fecha_inicio=date(2026, 3, 1), fecha_fin=date(2026, 3, 31), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=nomina, empleado=empleado, salario_base=Decimal("120"))

        self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("380.00"))
        self.assertEqual(linea.fuente_real, "AUTO:GASTO_OPERATIVO+NOMINA")

    def test_linea_manual_nunca_cambia_y_se_cuenta_protegida(self):
        """Una captura MANUAL conserva monto, fuente y metadata."""
        rubro, linea = self.crear_linea(
            concepto="Manual", sucursal=self.sucursal, monto_real=Decimal("777"), fuente_real="MANUAL:johana"
        )
        linea.metadata = {"captura": "humana"}
        linea.save(update_fields=["metadata"])
        self.crear_regla_gasto(rubro)
        self.crear_gasto("100")

        summary = self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("777.00"))
        self.assertEqual(linea.fuente_real, "MANUAL:johana")
        self.assertEqual(linea.metadata, {"captura": "humana"})
        self.assertEqual(summary.protegidas_manual, 1)

    def test_segunda_consolidacion_es_idempotente(self):
        """La segunda corrida reconoce la línea AUTO sin cambios."""
        rubro, linea = self.crear_linea(concepto="Idempotente", sucursal=self.sucursal)
        self.crear_regla_gasto(rubro)
        self.crear_gasto("42")
        primera = self.consolidar()

        segunda = self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(primera.actualizadas, 1)
        self.assertEqual(segunda.sin_cambio, 1)
        self.assertEqual(segunda.actualizadas, 0)
        self.assertEqual(linea.monto_real, Decimal("42.00"))

    def test_dry_run_no_persiste_y_detalla_el_cambio(self):
        """El modo dry-run calcula el detalle sin modificar la línea."""
        rubro, linea = self.crear_linea(concepto="Simulación", sucursal=self.sucursal)
        self.crear_regla_gasto(rubro)
        self.crear_gasto("63")

        summary = self.consolidar(dry_run=True)

        linea.refresh_from_db()
        self.assertIsNone(linea.monto_real)
        self.assertEqual(linea.fuente_real, "")
        self.assertEqual(summary.actualizadas, 1)
        self.assertEqual(len(summary.detalle), 1)
        self.assertEqual(summary.detalle[0]["nuevo"], "63.00")

    def test_regla_sin_datos_guarda_cero_y_marca_metadata(self):
        """Una regla válida sin filas fuente persiste cero y la marca sin datos."""
        rubro, linea = self.crear_linea(concepto="Sin datos", sucursal=self.sucursal)
        self.crear_regla_gasto(rubro)

        summary = self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("0.00"))
        self.assertTrue(linea.metadata["sin_datos_fuente"])
        self.assertEqual(summary.sin_datos_fuente, 1)

    def test_rubro_sin_reglas_deja_linea_intacta(self):
        """Un rubro sin reglas se reporta y conserva todos sus valores."""
        _, linea = self.crear_linea(concepto="Sin regla", monto_real=Decimal("91"), fuente_real="fuente-anterior")
        linea.metadata = {"intacto": True}
        linea.save(update_fields=["metadata"])

        summary = self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(summary.sin_regla, 1)
        self.assertEqual(linea.monto_real, Decimal("91.00"))
        self.assertEqual(linea.fuente_real, "fuente-anterior")
        self.assertEqual(linea.metadata, {"intacto": True})

    def test_migrar_fuentes_legadas_clasifica_auto_y_manual(self):
        """La migración convierte venta legada en AUTO y CAPEX en MANUAL protegido."""
        rubro_auto, linea_auto = self.crear_linea(
            concepto="Venta legada", monto_real=Decimal("10"), fuente_real="PROYECCIO_N_VENTAS_2026_AUTORIZADA"
        )
        rubro_manual, linea_manual = self.crear_linea(
            concepto="Capex legado", monto_real=Decimal("20"), fuente_real="CAPEX_GUAMUCHIL_CONFIRMADO"
        )
        self.crear_regla_gasto(rubro_auto)
        self.crear_regla_gasto(rubro_manual)

        resultado = migrar_fuentes_legadas()

        linea_auto.refresh_from_db()
        linea_manual.refresh_from_db()
        self.assertEqual(resultado["PROYECCIO_N_VENTAS_2026_AUTORIZADA"], 1)
        self.assertEqual(resultado["CAPEX_GUAMUCHIL_CONFIRMADO"], 1)
        self.assertEqual(linea_auto.fuente_real, "AUTO:LEGADO")
        self.assertEqual(linea_manual.fuente_real, "MANUAL:legado")
        summary = self.consolidar()
        self.assertEqual(summary.actualizadas, 1)
        self.assertEqual(summary.protegidas_manual, 1)

    def test_seed_real_es_idempotente_respeta_admin_y_dry_run(self):
        """El seed real crea nómina/ventas, no duplica y respeta reglas ADMIN."""
        nomina = AreaPresupuesto.objects.create(nombre="Nómina seed", codigo="nomina")
        ventas = AreaPresupuesto.objects.create(nombre="Ventas seed", codigo="ventas")
        sueldo, _ = self.crear_linea(concepto="SUELDO", area=nomina)
        venta, _ = self.crear_linea(
            concepto="BOLLO · CHOCOLATE", area=ventas, sucursal=self.sucursal, tipo=RubroPresupuesto.TIPO_INGRESO
        )
        sueldo_admin = RubroPresupuesto.objects.create(
            area=nomina, concepto="Sueldo", codigo_cuenta="ADMIN", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        ReglaFuenteRubro.objects.create(
            rubro=sueldo_admin,
            tipo_fuente=ReglaFuenteRubro.FUENTE_MANUAL,
            origen=ReglaFuenteRubro.ORIGEN_ADMIN,
        )

        call_command("seed_reglas_fuente_rubro", dry_run=True, stdout=StringIO())
        self.assertFalse(ReglaFuenteRubro.objects.filter(origen=ReglaFuenteRubro.ORIGEN_SEED).exists())

        call_command("seed_reglas_fuente_rubro", stdout=StringIO())
        total_seed = ReglaFuenteRubro.objects.filter(origen=ReglaFuenteRubro.ORIGEN_SEED).count()
        self.assertTrue(
            ReglaFuenteRubro.objects.filter(
                rubro=sueldo, origen=ReglaFuenteRubro.ORIGEN_SEED, tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA
            ).exists()
        )
        regla_venta = ReglaFuenteRubro.objects.get(rubro=venta, origen=ReglaFuenteRubro.ORIGEN_SEED)
        self.assertEqual(regla_venta.tipo_fuente, ReglaFuenteRubro.FUENTE_VENTA_POS)
        self.assertEqual(regla_venta.filtros["categoria_pos"], "BOLLO")
        self.assertEqual(regla_venta.filtros["producto_pos"], "CHOCOLATE")
        self.assertEqual(venta.sucursal, self.sucursal)
        self.assertFalse(ReglaFuenteRubro.objects.filter(rubro=sueldo_admin, origen=ReglaFuenteRubro.ORIGEN_SEED).exists())

        call_command("seed_reglas_fuente_rubro", stdout=StringIO())
        self.assertEqual(ReglaFuenteRubro.objects.filter(origen=ReglaFuenteRubro.ORIGEN_SEED).count(), total_seed)
        self.assertEqual(ReglaFuenteRubro.objects.filter(rubro=sueldo_admin).count(), 1)

    def test_linea_auto_tiene_precedencia_sobre_resultado_empresa(self):
        """_line_actual usa el monto AUTO aunque exista un resultado mensual aplicable."""
        rubro, linea = self.crear_linea(
            concepto="Ventas", monto_real=Decimal("321.45"), fuente_real="AUTO:NOMINA", tipo=RubroPresupuesto.TIPO_INGRESO
        )
        rubro.metadata = {"actual_key": "ventas"}
        rubro.save(update_fields=["metadata"])
        EmpresaResultadoMensual.objects.create(periodo=self.periodo, venta_total=Decimal("9999"))
        linea.refresh_from_db()
        linea.rubro = rubro

        monto, fuente = PresupuestoMaestroService()._line_actual(
            linea, {"ventas": Decimal("9999")}, set()
        )

        self.assertEqual(monto, Decimal("321.45"))
        self.assertEqual(fuente, "AUTO:NOMINA")
