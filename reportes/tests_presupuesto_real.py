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

    def test_regla_sin_datos_no_modifica_la_linea(self):
        """Sin filas fuente en el mes, la línea NO se toca (un retraso de
        Point/nómina no debe borrar el último real consolidado)."""
        rubro, linea = self.crear_linea(
            concepto="Sin datos",
            sucursal=self.sucursal,
            monto_real=Decimal("500.00"),
            fuente_real="AUTO:GASTO_OPERATIVO",
        )
        self.crear_regla_gasto(rubro)

        summary = self.consolidar()

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("500.00"))
        self.assertEqual(linea.fuente_real, "AUTO:GASTO_OPERATIVO")
        self.assertEqual(summary.sin_datos_fuente, 1)
        self.assertEqual(summary.actualizadas, 0)
        # El valor retenido queda marcado visiblemente como fuente sin datos.
        self.assertTrue(linea.metadata["sin_datos_fuente"])
        self.assertIn("fuente_sin_datos_en", linea.metadata)
        # Y al volver datos de la fuente CON EL MISMO importe retenido, la
        # marca también se limpia (no queda un badge obsoleto por la ruta
        # "sin cambio").
        self.crear_gasto("500")
        self.consolidar()
        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("500.00"))
        self.assertFalse(linea.metadata.get("sin_datos_fuente"))
        self.assertNotIn("fuente_sin_datos_en", linea.metadata)

    def test_captura_manual_concurrente_no_se_pisa(self):
        """Si una usuaria captura entre la lectura y la escritura, el UPDATE
        condicional no coincide y la captura se conserva."""
        rubro, linea = self.crear_linea(concepto="Concurrente", sucursal=self.sucursal)
        self.crear_regla_gasto(rubro)
        self.crear_gasto("700")

        # Simula la carrera: la instancia en memoria tiene fuente_real="",
        # pero la base ya recibió una captura manual.
        LineaPresupuestoMensual.objects.filter(pk=linea.pk).update(
            monto_real=Decimal("123.45"), fuente_real="MANUAL:johana"
        )

        service = PresupuestoRealConsolidacionService()
        escrita = service._escribir_linea(linea, Decimal("700.00"), "AUTO:GASTO_OPERATIVO", {})

        self.assertFalse(escrita)
        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("123.45"))
        self.assertEqual(linea.fuente_real, "MANUAL:johana")

    def test_consolidacion_usa_consultas_acotadas(self):
        """Las fuentes se precargan agrupadas: el número de consultas no crece
        con el número de rubros."""
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        for i in range(12):
            rubro, _ = self.crear_linea(concepto=f"Rubro escala {i}", sucursal=self.sucursal)
            self.crear_regla_gasto(rubro)
        self.crear_gasto("100")

        with CaptureQueriesContext(connection) as ctx:
            self.consolidar(dry_run=True)
        # 12 rubros con regla: sin índices serían 12+ consultas de agregación.
        self.assertLess(len(ctx.captured_queries), 8)

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
        # Con datos de fuente, el legado AUTO se re-escribe y el MANUAL queda protegido.
        self.crear_gasto("55")
        summary = self.consolidar()
        self.assertEqual(summary.actualizadas, 1)
        self.assertEqual(summary.protegidas_manual, 1)
        linea_auto.refresh_from_db()
        linea_manual.refresh_from_db()
        self.assertEqual(linea_auto.monto_real, Decimal("55.00"))
        self.assertEqual(linea_manual.monto_real, Decimal("20"))

    def test_seed_real_es_idempotente_respeta_admin_y_dry_run(self):
        """El seed real crea nómina/ventas, no duplica y respeta reglas ADMIN."""
        # El CSV real referencia RENTA_SUC; debe existir o el comando aborta.
        CategoriaGasto.objects.create(
            codigo="RENTA", nombre="Renta sucursal", capa_objetivo=CategoriaGasto.CAPA_EMPRESA
        )
        nomina = AreaPresupuesto.objects.create(nombre="Nómina seed", codigo="nomina")
        ventas = AreaPresupuesto.objects.create(nombre="Ventas seed", codigo="ventas")
        sueldo, _ = self.crear_linea(concepto="SUELDO", area=nomina)
        venta, _ = self.crear_linea(
            concepto="BOLLO · CHOCOLATE", area=ventas, sucursal=self.sucursal, tipo=RubroPresupuesto.TIPO_INGRESO
        )
        # Nombres POS reales para el matching difuso del seed.
        branch_seed = PointBranch.objects.create(external_id="SEED-BR", name="Centro", erp_branch=self.sucursal)
        PointSalesDailyProductFact.objects.create(
            branch=branch_seed,
            sale_date=self.periodo,
            sucursal_nombre="Centro",
            categoria="Bollo",
            producto_nombre_historico="Bollo Chocolate",
            total_venta=Decimal("10"),
            total_venta_neta=Decimal("9"),
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
        # El matching difuso asigna el nombre POS REAL, no el texto del rubro.
        self.assertEqual(regla_venta.filtros["productos_pos"], ["Bollo Chocolate"])
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


class PresupuestoVsRealViewTests(TestCase):
    """Valida el tablero comparativo: RBAC, render y export."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.superuser = User.objects.create_superuser("dg_test", "dg@test.mx", "clave-test")
        cls.sin_permiso = User.objects.create_user("sin_permiso", "np@test.mx", "clave-test")

        cls.periodo = date(2026, 3, 1)
        cls.area = AreaPresupuesto.objects.create(nombre="Área tablero", codigo="tablero")
        cls.rubro = RubroPresupuesto.objects.create(
            area=cls.area, concepto="Concepto tablero", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        cls.linea = LineaPresupuestoMensual.objects.create(
            rubro=cls.rubro,
            periodo=cls.periodo,
            monto_presupuesto=Decimal("100.00"),
            monto_real=Decimal("80.00"),
            fuente_real="AUTO:NOMINA",
            metadata={"real_breakdown": [{"tipo_fuente": "NOMINA", "monto": "80.00"}]},
        )

    def test_requiere_permiso_de_reportes(self):
        """Un usuario sin acceso al módulo recibe 403."""
        self.client.force_login(self.sin_permiso)
        response = self.client.get("/reportes/presupuesto-vs-real/")
        self.assertEqual(response.status_code, 403)

    def test_render_muestra_detalle_y_fuente(self):
        """El tablero muestra el concepto, su varianza y el badge de fuente."""
        self.client.force_login(self.superuser)
        response = self.client.get("/reportes/presupuesto-vs-real/?year=2026&month=3")
        self.assertEqual(response.status_code, 200)
        contenido = response.content.decode()
        self.assertIn("Concepto tablero", contenido)
        self.assertIn("Automático · Nómina", contenido)
        detalle = response.context["detalle"]
        self.assertEqual(len(detalle), 1)
        self.assertEqual(detalle[0]["varianza"], Decimal("-20.00"))
        # Egreso gastando menos que presupuesto = verde
        self.assertEqual(detalle[0]["tone"], "success")

    def test_export_csv_incluye_encabezados_y_fila(self):
        """El export CSV trae encabezados y la línea del mes."""
        self.client.force_login(self.superuser)
        response = self.client.get("/reportes/presupuesto-vs-real/?year=2026&month=3&export=csv")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        cuerpo = response.content.decode()
        self.assertIn("Concepto tablero", cuerpo)
        self.assertIn("Varianza %", cuerpo)

    def test_export_xlsx_responde_archivo(self):
        """El export XLSX responde un adjunto de Excel."""
        self.client.force_login(self.superuser)
        response = self.client.get("/reportes/presupuesto-vs-real/?year=2026&month=3&export=xlsx")
        self.assertEqual(response.status_code, 200)
        self.assertIn("spreadsheetml", response["Content-Type"])
        self.assertIn("attachment", response["Content-Disposition"])


class PresupuestoRealFixesReviewTests(TestCase):
    """Cobertura de los hallazgos de la revisión adversarial."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        cls.superuser = get_user_model().objects.create_superuser(
            "dg_fixes", "dgf@test.mx", "clave-test"
        )
        cls.periodo = date(2026, 3, 1)
        cls.area_nomina = AreaPresupuesto.objects.create(nombre="Nómina", codigo="nomina")
        cls.area_ventas = AreaPresupuesto.objects.create(nombre="Gastos", codigo="gastos-venta")

        rubro_nomina = RubroPresupuesto.objects.create(
            area=cls.area_nomina, concepto="SUELDO", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        LineaPresupuestoMensual.objects.create(
            rubro=rubro_nomina, periodo=cls.periodo,
            monto_presupuesto=Decimal("100.00"), monto_real=Decimal("90.00"),
            fuente_real="AUTO:NOMINA",
        )
        rubro_gasto = RubroPresupuesto.objects.create(
            area=cls.area_ventas, concepto="=SUMA(A1:A9)", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        LineaPresupuestoMensual.objects.create(
            rubro=rubro_gasto, periodo=cls.periodo,
            monto_presupuesto=Decimal("50.00"), monto_real=Decimal("40.00"),
            fuente_real="AUTO:GASTO_OPERATIVO",
        )

    def test_kpi_global_excluye_area_nomina(self):
        """El área Nómina no se suma a los KPI globales (doble conteo)."""
        self.client.force_login(self.superuser)
        response = self.client.get("/reportes/presupuesto-vs-real/?year=2026&month=3")
        kpis = response.context["kpis"]
        self.assertEqual(kpis["presupuesto"], Decimal("50.00"))
        self.assertEqual(kpis["real"], Decimal("40.00"))
        # Con el área nómina seleccionada sí se muestra su propio total.
        response = self.client.get("/reportes/presupuesto-vs-real/?year=2026&month=3&area=nomina")
        self.assertEqual(response.context["kpis"]["presupuesto"], Decimal("100.00"))

    def test_export_neutraliza_formulas(self):
        """Un concepto que empieza con '=' se exporta neutralizado."""
        self.client.force_login(self.superuser)
        response = self.client.get("/reportes/presupuesto-vs-real/?year=2026&month=3&export=csv")
        cuerpo = response.content.decode()
        self.assertIn("'=SUMA(A1:A9)", cuerpo)
        self.assertNotIn("\n=SUMA", cuerpo.replace("\r", ""))

    def test_seed_elimina_reglas_obsoletas(self):
        """Una regla SEED cuyo rubro salió del mapeo se elimina al re-correr."""
        CategoriaGasto.objects.create(
            codigo="RENTA", nombre="Renta sucursal", capa_objetivo=CategoriaGasto.CAPA_EMPRESA
        )
        rubro_viejo = RubroPresupuesto.objects.create(
            area=self.area_ventas, concepto="Concepto retirado", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro_viejo,
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA,
            origen=ReglaFuenteRubro.ORIGEN_SEED,
            filtros={"campo_monto": "salario_base"},
        )
        salida = StringIO()
        call_command("seed_reglas_fuente_rubro", stdout=salida)
        self.assertFalse(ReglaFuenteRubro.objects.filter(rubro=rubro_viejo).exists())
        self.assertIn("seed obsoletas eliminadas: 1", salida.getvalue())

    def test_categoria_inexistente_aborta_sin_escribir(self):
        """Una categoria_gasto inválida en el CSV aborta el comando completo;
        las reglas SEED previas se conservan (no hay borrado degradado)."""
        import csv as csv_mod
        import tempfile

        from django.core.management.base import CommandError

        rubro = RubroPresupuesto.objects.create(
            area=self.area_ventas, concepto="Con regla previa", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        regla_previa = ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA,
            origen=ReglaFuenteRubro.ORIGEN_SEED,
            filtros={"campo_monto": "salario_base"},
        )
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
            writer = csv_mod.DictWriter(
                tmp, fieldnames=["area", "concepto", "tipo_fuente", "categoria_gasto", "filtros", "notas"]
            )
            writer.writeheader()
            writer.writerow(
                {
                    "area": "gastos-venta",
                    "concepto": "Con regla previa",
                    "tipo_fuente": "GASTO_OPERATIVO",
                    "categoria_gasto": "NO_EXISTE_XYZ",
                    "filtros": "",
                    "notas": "",
                }
            )
            ruta = tmp.name

        with self.assertRaises(CommandError):
            call_command("seed_reglas_fuente_rubro", csv=ruta, stdout=StringIO())
        self.assertTrue(ReglaFuenteRubro.objects.filter(pk=regla_previa.pk).exists())


class VentasPosMatchingTests(TestCase):
    """Matching difuso rubro de Ventas → nombres POS reales (casos de producción)."""

    @classmethod
    def setUpTestData(cls):
        # El CSV real referencia RENTA_SUC; debe existir o el comando aborta.
        CategoriaGasto.objects.create(
            codigo="RENTA", nombre="Renta sucursal", capa_objetivo=CategoriaGasto.CAPA_EMPRESA
        )
        cls.ventas = AreaPresupuesto.objects.create(nombre="Ventas", codigo="ventas")
        cls.branch = PointBranch.objects.create(external_id="MATCH-BR", name="Centro")

        def fact(categoria, producto):
            PointSalesDailyProductFact.objects.create(
                branch=cls.branch,
                sale_date=date(2026, 5, 3),
                sucursal_nombre="Centro",
                categoria=categoria,
                producto_nombre_historico=producto,
                total_venta=Decimal("10"),
                total_venta_neta=Decimal("9"),
            )

        fact("Pastel Mediano", "Pastel de 3 Pecados Mediano")
        fact("Pastel Mediano", "Pastel de Snickers Mediano")
        fact("Rebanada", "Pastel de 3 Pecados R")
        fact("TE", "Té helado 500ml")
        fact("Galletas", "Bolitas de Nuez 10 PZ")

    def _rubro(self, concepto):
        rubro = RubroPresupuesto.objects.create(
            area=self.ventas, concepto=concepto, tipo=RubroPresupuesto.TIPO_INGRESO
        )
        LineaPresupuestoMensual.objects.create(
            rubro=rubro, periodo=date(2026, 5, 1), monto_presupuesto=Decimal("1")
        )
        return rubro

    def _filtros_de(self, rubro):
        call_command("seed_reglas_fuente_rubro", stdout=StringIO())
        return ReglaFuenteRubro.objects.get(rubro=rubro, origen=ReglaFuenteRubro.ORIGEN_SEED).filtros

    def test_producto_con_orden_y_preposiciones_distintas(self):
        """'PASTEL MEDIANO · 3 PECADOS' cruza con 'Pastel de 3 Pecados Mediano'."""
        rubro = self._rubro("PASTEL MEDIANO · 3 PECADOS")
        filtros = self._filtros_de(rubro)
        self.assertEqual(filtros["productos_pos"], ["Pastel de 3 Pecados Mediano"])

    def test_apostrofe_y_abreviatura(self):
        """SNICKER'S cruza con Snickers; el sufijo R se expande a rebanada."""
        rubro_snickers = self._rubro("PASTEL MEDIANO · SNICKER'S")
        rubro_rebanada = self._rubro("PASTEL REBANADAS · 3 PECADOS")
        call_command("seed_reglas_fuente_rubro", stdout=StringIO())
        filtros_s = ReglaFuenteRubro.objects.get(rubro=rubro_snickers, origen="SEED").filtros
        filtros_r = ReglaFuenteRubro.objects.get(rubro=rubro_rebanada, origen="SEED").filtros
        self.assertEqual(filtros_s["productos_pos"], ["Pastel de Snickers Mediano"])
        self.assertEqual(filtros_r["productos_pos"], ["Pastel de 3 Pecados R"])

    def test_categoria_pos_completa(self):
        """'BEBIDAS/OTROS · TE' cruza con la categoría POS 'TE' completa."""
        rubro = self._rubro("BEBIDAS/OTROS · TE")
        filtros = self._filtros_de(rubro)
        self.assertEqual(filtros.get("categoria_pos"), "TE")
        self.assertNotIn("productos_pos", filtros)

    def test_sin_match_queda_reportado_y_sin_asignacion(self):
        """'BEBIDAS/OTROS · REFRESCO' no cruza con nada: regla sin asignación."""
        rubro = self._rubro("BEBIDAS/OTROS · REFRESCO")
        filtros = self._filtros_de(rubro)
        self.assertNotIn("productos_pos", filtros)
        self.assertNotIn("categoria_pos", filtros)

    def test_conflicto_de_producto_gana_el_mejor_score(self):
        """Dos rubros no pueden reclamar el mismo producto POS."""
        exacto = self._rubro("GALLETA · BOLITA DE NUEZ (10PZ)")
        parecido = self._rubro("GALLETA · BOLITAS DE NUEZ")
        call_command("seed_reglas_fuente_rubro", stdout=StringIO())
        con_producto = [
            r for r in ReglaFuenteRubro.objects.filter(rubro__in=[exacto, parecido])
            if r.filtros.get("productos_pos")
        ]
        self.assertEqual(len(con_producto), 1)


class VentasUnidadesTests(TestCase):
    """Comparativo de ventas por unidades × precio actual (regla de dirección)."""

    @classmethod
    def setUpTestData(cls):
        from recetas.models import PronosticoVenta, Receta

        cls.receta = Receta.objects.create(nombre="Bollo Chocolate", hash_contenido="test-vu-bollo")
        cls.sin_venta = Receta.objects.create(nombre="Rosca Dulce de Leche", hash_contenido="test-vu-rosca")
        PronosticoVenta.objects.create(receta=cls.receta, periodo="2026-05", cantidad=Decimal("100"), fuente="PRESUPUESTO_2026")
        PronosticoVenta.objects.create(receta=cls.sin_venta, periodo="2026-05", cantidad=Decimal("40"), fuente="PRESUPUESTO_2026")

        branch = PointBranch.objects.create(external_id="VU-BR", name="Centro")
        for day, qty, venta in [(3, 30, 300), (10, 50, 500)]:
            PointSalesDailyProductFact.objects.create(
                branch=branch,
                sale_date=date(2026, 5, day),
                sucursal_nombre="Centro",
                categoria="Bollo",
                producto_nombre_historico="Bollo Chocolate",
                receta=cls.receta,
                total_cantidad=Decimal(qty),
                total_venta=Decimal(venta),
                total_venta_neta=Decimal(venta),
            )

    def test_unidades_cumplimiento_e_importe_a_precio_actual(self):
        """80 de 100 unidades = 80% y el $ proyectado usa el ASP reciente ($10)."""
        from reportes.services_ventas_unidades import comparativo_ventas_unidades

        resultado = comparativo_ventas_unidades(date(2026, 5, 1), hoy=date(2026, 5, 31))
        fila = next(f for f in resultado["filas"] if f["receta_id"] == self.receta.id)
        self.assertEqual(fila["unidades_proyectadas"], Decimal("100"))
        self.assertEqual(fila["unidades_reales"], Decimal("80"))
        self.assertEqual(fila["cumplimiento_pct"], Decimal("80.0"))
        self.assertEqual(fila["precio_actual"], Decimal("10.00"))
        self.assertEqual(fila["importe_proyectado"], Decimal("1000.00"))
        self.assertEqual(fila["importe_real"], Decimal("800"))
        self.assertEqual(fila["varianza"], Decimal("-200.00"))

    def test_producto_sin_venta_ni_precio_queda_marcado(self):
        """Sin ventas recientes ni precio de lista: fila sin precio, contada."""
        from reportes.services_ventas_unidades import comparativo_ventas_unidades

        resultado = comparativo_ventas_unidades(date(2026, 5, 1), hoy=date(2026, 5, 31))
        fila = next(f for f in resultado["filas"] if f["receta_id"] == self.sin_venta.id)
        self.assertIsNone(fila["precio_actual"])
        self.assertIsNone(fila["importe_proyectado"])
        self.assertEqual(resultado["totales"]["sin_precio"], 1)


class ImportUnidadesProyeccionTests(TestCase):
    """El reimport de la proyección guarda las CANTIDADES en PronosticoVenta."""

    def _xlsx_proyeccion(self):
        import tempfile

        from openpyxl import Workbook

        wb = Workbook()
        hoja = wb.active
        hoja.title = "GENERAL"
        # Fila de meses + encabezados CANT/VENTA por bloque (proyección 2026).
        hoja.append(["", "ENERO", "", "", "FEBRERO", "", "", "MARZO", "", ""])
        hoja.append(["", "PROYECCIÓN 2026", "", "", "PROYECCIÓN 2026", "", "", "PROYECCIÓN 2026", "", ""])
        hoja.append(["CONCEPTO", "CANT", "VENTA", "", "CANT", "VENTA", "", "CANT", "VENTA", ""])
        hoja.append(["BOLLO", None, None, None, None, None, None, None, None, None])
        hoja.append(["CHOCOLATE", 120, 3600, None, 150, 4500, None, 130, 3900, None])
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        wb.save(tmp.name)
        return tmp.name

    def test_reimport_guarda_unidades_y_respeta_manual(self):
        from recetas.models import PronosticoVenta, Receta

        from reportes.services_presupuesto_maestro import PresupuestoMaestroImportService

        receta = Receta.objects.create(nombre="Bollo Chocolate", hash_contenido="test-imp-bollo")
        # Pronóstico manual previo de enero: NO debe pisarse.
        PronosticoVenta.objects.create(receta=receta, periodo="2026-01", cantidad=Decimal("999"), fuente="MANUAL")

        resumen = PresupuestoMaestroImportService().reimport_sales_projection(
            archivo=self._xlsx_proyeccion(), year=2026
        )

        enero = PronosticoVenta.objects.get(receta=receta, periodo="2026-01")
        febrero = PronosticoVenta.objects.get(receta=receta, periodo="2026-02")
        marzo = PronosticoVenta.objects.get(receta=receta, periodo="2026-03")
        self.assertEqual(enero.cantidad, Decimal("999"))
        self.assertEqual(enero.fuente, "MANUAL")
        self.assertEqual(febrero.cantidad, Decimal("150"))
        self.assertEqual(febrero.fuente, "PRESUPUESTO_2026")
        self.assertEqual(marzo.cantidad, Decimal("130"))
        self.assertEqual(resumen.unidades_upsertadas, 2)
