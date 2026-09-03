from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import TestCase

from core.models import UserModuleAccess
from core.navigation import build_nav_groups
from reportes.models import (
    AreaPresupuesto,
    AreaPresupuestoResponsable,
    CategoriaGasto,
    CentroCosto,
    GastoOperativoMensual,
    GastoRecurrente,
    LineaPresupuestoMensual,
    ObligacionGasto,
    PagoObligacionGasto,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_gastos_compromisos import (
    crear_gasto_recurrente,
    crear_gasto_variable,
    editar_gasto_recurrente,
    generar_obligacion_recurrente,
    registrar_pago,
)
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService


class PermisosCapturaPresupuestoTests(TestCase):
    """La lectura de reportes nunca concede escritura financiera global."""

    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.lectura = User.objects.create_user("lectura_reportes", password="x")
        cls.responsable = User.objects.create_user("responsable_area", password="x")
        cls.gestor = User.objects.create_user("gestor_reportes", password="x")
        cls.area = AreaPresupuesto.objects.create(nombre="Gastos de Venta", codigo="gastos-venta")
        UserModuleAccess.objects.create(
            user=cls.lectura,
            module="reportes",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        UserModuleAccess.objects.create(
            user=cls.gestor,
            module="reportes",
            access=UserModuleAccess.ACCESS_MANAGE,
        )
        AreaPresupuestoResponsable.objects.create(area=cls.area, usuario=cls.responsable)

    def _etiquetas(self, usuario):
        return [
            item["label"]
            for grupo in build_nav_groups(usuario, "/")
            for item in grupo["items"]
        ]

    def test_solo_lectura_no_ve_captura_financiera(self):
        self.assertNotIn("Captura de presupuesto", self._etiquetas(self.lectura))

    def test_solo_lectura_no_entra_directamente_a_captura_financiera(self):
        self.client.force_login(self.lectura)
        response = self.client.get("/reportes/presupuesto-real/captura/")
        self.assertEqual(response.status_code, 403)

    def test_responsable_ve_captura_solo_por_su_asignacion(self):
        self.assertIn("Captura de presupuesto", self._etiquetas(self.responsable))

    def test_gestor_de_reportes_conserva_captura_global(self):
        self.assertIn("Captura de presupuesto", self._etiquetas(self.gestor))

    def test_solo_lectura_no_puede_usar_captura_manual_general(self):
        self.client.force_login(self.lectura)
        response = self.client.post("/reportes/gastos-operativos/captura-manual/", {})
        self.assertEqual(response.status_code, 403)

    def test_solo_lectura_no_puede_importar_gastos(self):
        self.client.force_login(self.lectura)
        response = self.client.post("/reportes/gastos-operativos/importar/", {})
        self.assertEqual(response.status_code, 403)


class GastosCompromisosServiceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.responsable = User.objects.create_user("jefa_area", password="x")
        cls.ajena = User.objects.create_user("jefa_ajena", password="x")
        cls.area = AreaPresupuesto.objects.create(nombre="Gastos de Venta", codigo="gastos-venta")
        cls.otra_area = AreaPresupuesto.objects.create(nombre="Administración", codigo="administracion")
        AreaPresupuestoResponsable.objects.create(area=cls.area, usuario=cls.responsable)
        cls.rubro = RubroPresupuesto.objects.create(
            area=cls.area,
            concepto="Rentas",
            codigo_cuenta="RENTA",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        cls.centro = CentroCosto.objects.create(
            codigo="CORP-01",
            nombre="Corporativo",
            tipo=CentroCosto.TIPO_CORPORATIVO,
        )
        cls.categoria = CategoriaGasto.objects.create(
            codigo="RENTA",
            nombre="Rentas",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
        )

    def _datos_credito(self):
        return {
            "condicion_pago": ObligacionGasto.CONDICION_CREDITO,
            "tipo_credito": ObligacionGasto.CREDITO_DIFERIDO,
            "plazo_cantidad": 3,
            "plazo_unidad": ObligacionGasto.PLAZO_MESES,
            "numero_parcialidades": 3,
            "metodo_pago_previsto": ObligacionGasto.METODO_TRANSFERENCIA,
        }

    def test_gasto_variable_reconoce_gasto_independiente_del_pago(self):
        obligacion = crear_gasto_variable(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Reparación urgente",
            periodo=date(2026, 7, 1),
            fecha_gasto=date(2026, 7, 15),
            fecha_vencimiento=date(2026, 10, 15),
            monto=Decimal("9000.00"),
            **self._datos_credito(),
        )

        self.assertEqual(obligacion.estado, ObligacionGasto.ESTADO_PENDIENTE)
        self.assertEqual(obligacion.pagos.count(), 0)
        self.assertEqual(obligacion.parcialidades.count(), 3)
        self.assertEqual(obligacion.gasto_operativo.monto, Decimal("9000.00"))
        self.assertEqual(obligacion.gasto_operativo.tipo_dato, GastoOperativoMensual.TIPO_DATO_REAL)
        self.assertTrue(
            self.rubro.reglas_fuente.filter(
                tipo_fuente=ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO,
                activa=True,
            ).exists()
        )

    def test_consolidacion_suma_obligaciones_por_rubro_sin_cruzar_categorias(self):
        linea = LineaPresupuestoMensual.objects.create(
            rubro=self.rubro,
            periodo=date(2026, 7, 1),
            monto_presupuesto=Decimal("10000.00"),
        )
        crear_gasto_variable(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Reparación dirigida al rubro",
            periodo=date(2026, 7, 1),
            fecha_gasto=date(2026, 7, 15),
            fecha_vencimiento=date(2026, 7, 15),
            monto=Decimal("750.00"),
        )

        PresupuestoRealConsolidacionService().consolidar(periodo=date(2026, 7, 1))

        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("750.00"))
        self.assertEqual(linea.fuente_real, "AUTO:OBLIGACION_GASTO")

    def test_rubro_ya_mapeado_a_gasto_operativo_no_duplica_la_obligacion(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Servicios con fuente existente",
            codigo_cuenta="SERV-GO",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            categoria_gasto=self.categoria,
            centro_costo=self.centro,
        )
        linea = LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=date(2026, 7, 1),
            monto_presupuesto=Decimal("500.00"),
        )
        crear_gasto_variable(
            usuario=self.responsable,
            area=self.area,
            rubro=rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Servicio único",
            periodo=date(2026, 7, 1),
            fecha_gasto=date(2026, 7, 20),
            fecha_vencimiento=date(2026, 7, 20),
            monto=Decimal("125.00"),
        )

        self.assertFalse(
            rubro.reglas_fuente.filter(tipo_fuente=ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO).exists()
        )
        PresupuestoRealConsolidacionService().consolidar(periodo=date(2026, 7, 1))
        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("125.00"))
        self.assertEqual(linea.fuente_real, "AUTO:GASTO_OPERATIVO")

    def test_fuente_exacta_excluye_su_gasto_de_agregados_por_categoria(self):
        rubro_agregado = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Agregado de servicios",
            codigo_cuenta="AGREGADO-GO",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro_agregado,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            categoria_gasto=self.categoria,
            centro_costo=self.centro,
        )
        linea_exacta = LineaPresupuestoMensual.objects.create(
            rubro=self.rubro,
            periodo=date(2026, 9, 1),
            monto_presupuesto=Decimal("1000.00"),
        )
        linea_agregada = LineaPresupuestoMensual.objects.create(
            rubro=rubro_agregado,
            periodo=date(2026, 9, 1),
            monto_presupuesto=Decimal("1000.00"),
        )
        crear_gasto_variable(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Servicio dirigido",
            periodo=date(2026, 9, 1),
            fecha_gasto=date(2026, 9, 10),
            fecha_vencimiento=date(2026, 9, 10),
            monto=Decimal("400.00"),
        )

        PresupuestoRealConsolidacionService().consolidar(periodo=date(2026, 9, 1))

        linea_exacta.refresh_from_db()
        linea_agregada.refresh_from_db()
        self.assertEqual(linea_exacta.monto_real, Decimal("400.00"))
        self.assertIsNone(linea_agregada.monto_real)

    def test_rubro_de_otra_fuente_automatica_rechaza_captura_paralela(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Nómina automática",
            codigo_cuenta="NOM-AUTO",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA,
            filtros={"campo_monto": "salario_base"},
        )

        with self.assertRaisesMessage(ValidationError, "otra fuente automática"):
            crear_gasto_variable(
                usuario=self.responsable,
                area=self.area,
                rubro=rubro,
                centro_costo=self.centro,
                categoria_gasto=self.categoria,
                concepto="No debe duplicar nómina",
                periodo=date(2026, 7, 1),
                fecha_gasto=date(2026, 7, 20),
                fecha_vencimiento=date(2026, 7, 20),
                monto=Decimal("125.00"),
            )

    def test_usuario_ajeno_no_registra_gasto_en_el_area(self):
        with self.assertRaises(PermissionDenied):
            crear_gasto_variable(
                usuario=self.ajena,
                area=self.area,
                rubro=self.rubro,
                centro_costo=self.centro,
                categoria_gasto=self.categoria,
                concepto="No autorizado",
                periodo=date(2026, 7, 1),
                fecha_gasto=date(2026, 7, 10),
                fecha_vencimiento=date(2026, 7, 10),
                monto=Decimal("100.00"),
            )

    def test_editar_gasto_fijo_crea_version_y_conserva_historial(self):
        recurrente = crear_gasto_recurrente(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Renta local Centro",
            vigencia_inicio=date(2026, 1, 1),
            monto=Decimal("18000.00"),
            dia_vencimiento=5,
            condicion_pago=ObligacionGasto.CONDICION_CONTADO,
            metodo_pago_previsto=ObligacionGasto.METODO_TRANSFERENCIA,
            motivo="Contrato inicial",
        )
        version_inicial = recurrente.versiones.get()

        nueva = editar_gasto_recurrente(
            usuario=self.responsable,
            recurrente=recurrente,
            vigencia_inicio=date(2026, 8, 1),
            monto=Decimal("19500.00"),
            dia_vencimiento=5,
            condicion_pago=ObligacionGasto.CONDICION_CONTADO,
            metodo_pago_previsto=ObligacionGasto.METODO_TRANSFERENCIA,
            motivo="Incremento anual acordado",
        )

        version_inicial.refresh_from_db()
        self.assertEqual(version_inicial.monto, Decimal("18000.00"))
        self.assertEqual(version_inicial.vigencia_fin, date(2026, 7, 31))
        self.assertEqual(nueva.monto, Decimal("19500.00"))
        self.assertEqual(recurrente.versiones.count(), 2)

    def test_generar_gasto_fijo_es_idempotente(self):
        recurrente = crear_gasto_recurrente(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Renta local Centro",
            vigencia_inicio=date(2026, 1, 1),
            monto=Decimal("18000.00"),
            dia_vencimiento=5,
            condicion_pago=ObligacionGasto.CONDICION_CONTADO,
            metodo_pago_previsto=ObligacionGasto.METODO_TRANSFERENCIA,
        )

        primera, creada = generar_obligacion_recurrente(
            usuario=self.responsable, recurrente=recurrente, periodo=date(2026, 7, 1)
        )
        segunda, creada_otra_vez = generar_obligacion_recurrente(
            usuario=self.responsable, recurrente=recurrente, periodo=date(2026, 7, 1)
        )

        self.assertTrue(creada)
        self.assertFalse(creada_otra_vez)
        self.assertEqual(primera.pk, segunda.pk)
        self.assertEqual(GastoOperativoMensual.objects.filter(external_key=f"OBLIGACION-GASTO-{primera.pk}").count(), 1)

    def test_gasto_fijo_propaga_referencia_de_soporte_al_gasto_real(self):
        recurrente = crear_gasto_recurrente(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Renta local Centro",
            vigencia_inicio=date(2026, 1, 1),
            monto=Decimal("18000.00"),
            dia_vencimiento=5,
            condicion_pago=ObligacionGasto.CONDICION_CONTADO,
            archivo_soporte="CFDI:11111111-2222-3333-4444-555555555555",
        )

        obligacion, _ = generar_obligacion_recurrente(
            usuario=self.responsable, recurrente=recurrente, periodo=date(2026, 7, 1)
        )

        self.assertEqual(
            obligacion.archivo_soporte,
            "CFDI:11111111-2222-3333-4444-555555555555",
        )
        self.assertEqual(obligacion.gasto_operativo.archivo_soporte, obligacion.archivo_soporte)

    def test_pagos_guardan_medio_y_actualizan_saldo_sin_sobregiro(self):
        obligacion = crear_gasto_variable(
            usuario=self.responsable,
            area=self.area,
            rubro=self.rubro,
            centro_costo=self.centro,
            categoria_gasto=self.categoria,
            concepto="Licencia anual",
            periodo=date(2026, 7, 1),
            fecha_gasto=date(2026, 7, 1),
            fecha_vencimiento=date(2026, 7, 31),
            monto=Decimal("1200.00"),
            condicion_pago=ObligacionGasto.CONDICION_CREDITO,
            tipo_credito=ObligacionGasto.CREDITO_UNICO,
            plazo_cantidad=30,
            plazo_unidad=ObligacionGasto.PLAZO_DIAS,
        )

        pago = registrar_pago(
            usuario=self.responsable,
            obligacion=obligacion,
            fecha_pago=date(2026, 7, 15),
            monto=Decimal("500.00"),
            metodo_pago=PagoObligacionGasto.METODO_TARJETA,
            referencia="VISA 1234",
        )
        obligacion.refresh_from_db()
        self.assertEqual(pago.metodo_pago, PagoObligacionGasto.METODO_TARJETA)
        self.assertEqual(obligacion.estado, ObligacionGasto.ESTADO_PARCIAL)
        self.assertEqual(obligacion.saldo_pendiente, Decimal("700.00"))

        with self.assertRaises(ValidationError):
            registrar_pago(
                usuario=self.responsable,
                obligacion=obligacion,
                fecha_pago=date(2026, 7, 16),
                monto=Decimal("701.00"),
                metodo_pago=PagoObligacionGasto.METODO_EFECTIVO,
            )


class CapturaGastosViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.usuario = User.objects.create_user("capturista_area", password="x")
        cls.area = AreaPresupuesto.objects.create(nombre="Administración", codigo="administracion")
        AreaPresupuestoResponsable.objects.create(area=cls.area, usuario=cls.usuario)
        cls.rubro = RubroPresupuesto.objects.create(
            area=cls.area,
            concepto="Servicios generales",
            codigo_cuenta="SERV",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        cls.centro = CentroCosto.objects.create(
            codigo="CORP-VIEW",
            nombre="Corporativo",
            tipo=CentroCosto.TIPO_CORPORATIVO,
        )
        cls.categoria = CategoriaGasto.objects.create(
            codigo="SERV-VIEW",
            nombre="Servicios generales",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
        )

    def test_pantalla_explica_gasto_fijo_credito_y_medio_de_pago(self):
        self.client.force_login(self.usuario)
        response = self.client.get(
            "/reportes/presupuesto-real/captura/?area=administracion&year=2026&month=7"
        )
        self.assertContains(response, "Registrar gasto variable")
        self.assertContains(response, "Agregar gasto fijo")
        self.assertContains(response, "Crédito")
        self.assertContains(response, "Transferencia")
        self.assertContains(response, "Obligaciones y pagos")

    def test_post_variable_crea_obligacion_y_responde_toast_json(self):
        self.client.force_login(self.usuario)
        response = self.client.post(
            "/reportes/presupuesto-real/gastos/variables/",
            {
                "area_id": self.area.id,
                "rubro_id": self.rubro.id,
                "centro_costo_id": self.centro.id,
                "categoria_gasto_id": self.categoria.id,
                "concepto": "Mensajería extraordinaria",
                "periodo": "2026-07-01",
                "fecha_gasto": "2026-07-20",
                "fecha_vencimiento": "2026-07-20",
                "monto": "850.50",
                "condicion_pago": ObligacionGasto.CONDICION_CONTADO,
                "metodo_pago_previsto": ObligacionGasto.METODO_EFECTIVO,
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertTrue(response.json()["reload"])
        self.assertTrue(response.json()["redirect"].endswith("#gastos-area"))
        obligacion = ObligacionGasto.objects.get(concepto="Mensajería extraordinaria")
        self.assertEqual(obligacion.monto_reconocido, Decimal("850.50"))
