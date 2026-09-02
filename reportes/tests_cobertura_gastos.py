from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase

from reportes.models import (
    AreaPresupuesto, CategoriaGasto, CentroCosto, GastoOperativoMensual,
    ObligacionGasto, RubroPresupuesto,
)
from reportes.services_gastos_compromisos import (
    crear_gasto_recurrente, crear_gasto_variable, editar_gasto_recurrente, generar_obligacion_recurrente,
)


class CoberturaGastosTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.usuario = get_user_model().objects.create_superuser("cobertura", password="test")
        cls.area = AreaPresupuesto.objects.create(codigo="gastos-venta", nombre="Ventas")
        cls.rubro = RubroPresupuesto.objects.create(
            area=cls.area, concepto="Electricidad", tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        cls.centro = CentroCosto.objects.create(
            codigo="COBERTURA", nombre="Centro", tipo=CentroCosto.TIPO_SUCURSAL,
        )
        cls.categoria = CategoriaGasto.objects.create(codigo="LUZ_SUC", nombre="Electricidad")

    def gasto(self, **kwargs):
        return GastoOperativoMensual(
            periodo=date(2026, 3, 1), centro_costo=self.centro,
            categoria_gasto=self.categoria, monto=Decimal("100.01"), **kwargs,
        )

    def recurrente(self, **kwargs):
        vigencia_inicio = kwargs.pop("vigencia_inicio", date(2026, 1, 1))
        return crear_gasto_recurrente(
            usuario=self.usuario, area=self.area, rubro=self.rubro,
            centro_costo=self.centro, categoria_gasto=self.categoria,
            concepto="Electricidad", vigencia_inicio=vigencia_inicio,
            monto=Decimal("100.01"), dia_vencimiento=5, condicion_pago="CONTADO",
            **kwargs,
        )

    def test_cobertura_nula_conserva_compatibilidad(self):
        gasto = self.gasto()
        self.assertIsNone(gasto.cobertura_mes_inicio)
        self.assertIsNone(gasto.cobertura_mes_fin)
        gasto.full_clean()

    def test_cobertura_exige_par_ordenado_y_primer_dia(self):
        for inicio, fin in [
            (date(2026, 1, 1), None), (None, date(2026, 2, 1)),
            (date(2026, 2, 1), date(2026, 1, 1)),
            (date(2026, 1, 2), date(2026, 2, 1)),
            (date(2026, 1, 1), date(2026, 2, 2)),
        ]:
            with self.subTest(inicio=inicio, fin=fin), self.assertRaises(ValidationError):
                self.gasto(cobertura_mes_inicio=inicio, cobertura_mes_fin=fin).full_clean()

    def test_cobertura_no_cambia_importe_ni_periodo_original(self):
        gasto = self.gasto(cobertura_mes_inicio=date(2026, 1, 1), cobertura_mes_fin=date(2026, 2, 1))
        gasto.full_clean()
        gasto.save()
        gasto.refresh_from_db()
        self.assertEqual(gasto.monto, Decimal("100.01"))
        self.assertEqual(gasto.periodo, date(2026, 3, 1))

    def test_bimestral_solo_genera_en_inicio_de_ciclo(self):
        recurrente = self.recurrente(periodicidad_meses=2)
        enero, creada = generar_obligacion_recurrente(
            usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 1, 1),
        )
        self.assertTrue(creada)
        self.assertEqual(enero.gasto_operativo.cobertura_mes_inicio, date(2026, 1, 1))
        self.assertEqual(enero.gasto_operativo.cobertura_mes_fin, date(2026, 2, 1))
        self.assertEqual(enero.monto_reconocido, Decimal("100.01"))
        self.assertEqual(enero.saldo_pendiente, Decimal("100.01"))
        self.assertEqual(enero.fecha_vencimiento, date(2026, 1, 5))
        with self.assertRaises(ValidationError):
            generar_obligacion_recurrente(
                usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 2, 1),
            )
        misma, creada = generar_obligacion_recurrente(
            usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 1, 1),
        )
        self.assertFalse(creada)
        self.assertEqual(misma.pk, enero.pk)
        marzo, creada = generar_obligacion_recurrente(
            usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 3, 1),
        )
        self.assertTrue(creada)
        self.assertEqual(marzo.gasto_operativo.cobertura_mes_fin, date(2026, 4, 1))
        self.assertEqual(ObligacionGasto.objects.count(), 2)

    def test_periodicidad_invalida_no_crea_contrato(self):
        with self.assertRaises(ValidationError):
            self.recurrente(periodicidad_meses=3)

    def test_bimestral_rechaza_inicio_parcial_al_crear_y_editar(self):
        with self.assertRaisesMessage(ValidationError, "primer día"):
            self.recurrente(periodicidad_meses=2, vigencia_inicio=date(2026, 1, 15))
        recurrente = self.recurrente(periodicidad_meses=2)
        with self.assertRaisesMessage(ValidationError, "primer día"):
            editar_gasto_recurrente(
                usuario=self.usuario, recurrente=recurrente, vigencia_inicio=date(2026, 3, 15),
                monto=120, dia_vencimiento=5, condicion_pago="CONTADO", motivo="Cambio de tarifa",
            )
        self.assertEqual(recurrente.versiones.count(), 1)

    def test_no_cambia_vigencia_dentro_de_bimestre_reconocido(self):
        recurrente = self.recurrente(periodicidad_meses=2)
        generar_obligacion_recurrente(usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 1, 1))
        with self.assertRaises(ValidationError):
            editar_gasto_recurrente(
                usuario=self.usuario, recurrente=recurrente, vigencia_inicio=date(2026, 2, 1),
                monto=Decimal("120"), dia_vencimiento=5, condicion_pago="CONTADO",
                periodicidad_meses=2, motivo="Cambio dentro del ciclo",
            )
        self.assertEqual(recurrente.versiones.count(), 1)

    def test_edicion_no_corta_ciclo_aun_sin_obligaciones(self):
        recurrente = self.recurrente(periodicidad_meses=2)
        with self.assertRaisesMessage(ValidationError, "ciclo bimestral"):
            editar_gasto_recurrente(
                usuario=self.usuario, recurrente=recurrente, vigencia_inicio=date(2026, 2, 1),
                monto=120, dia_vencimiento=5, condicion_pago="CONTADO",
                periodicidad_meses=1, motivo="Cambio antes de generar enero",
            )
        self.assertEqual(recurrente.versiones.count(), 1)
        obligacion, creada = generar_obligacion_recurrente(
            usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 1, 1),
        )
        self.assertTrue(creada)
        self.assertEqual(obligacion.gasto_operativo.cobertura_mes_fin, date(2026, 2, 1))

    def test_generacion_rechaza_solapamiento_de_versiones(self):
        from reportes.models import GastoRecurrenteVersion
        recurrente = self.recurrente(periodicidad_meses=2)
        generar_obligacion_recurrente(usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 1, 1))
        GastoRecurrenteVersion.objects.create(
            gasto_recurrente=recurrente, vigencia_inicio=date(2026, 2, 1),
            periodicidad_meses=2, monto=120, dia_vencimiento=5,
        )
        with self.assertRaises(ValidationError):
            generar_obligacion_recurrente(usuario=self.usuario, recurrente=recurrente, periodo=date(2026, 2, 1))
        self.assertEqual(ObligacionGasto.objects.count(), 1)

    def test_mensual_sigue_generando_cada_mes(self):
        recurrente = self.recurrente()
        self.assertEqual(recurrente.versiones.get().periodicidad_meses, 1)
        for mes in (1, 2):
            obligacion, creada = generar_obligacion_recurrente(
                usuario=self.usuario, recurrente=recurrente, periodo=date(2026, mes, 1),
            )
            self.assertTrue(creada)
            self.assertEqual(obligacion.gasto_operativo.cobertura_mes_fin, date(2026, mes, 1))

    def test_gasto_variable_guarda_cobertura_antes_del_pago(self):
        obligacion = crear_gasto_variable(
            usuario=self.usuario, area=self.area, rubro=self.rubro,
            centro_costo=self.centro, categoria_gasto=self.categoria,
            concepto="Recibo CFE", periodo=date(2026, 3, 1),
            fecha_gasto=date(2026, 3, 5), fecha_vencimiento=date(2026, 3, 15),
            monto=Decimal("100.01"), cobertura_mes_inicio=date(2026, 1, 1),
            cobertura_mes_fin=date(2026, 2, 1),
        )
        self.assertEqual(obligacion.pagos.count(), 0)
        self.assertEqual(obligacion.gasto_operativo.cobertura_mes_fin, date(2026, 2, 1))

    def test_formulario_guarda_periodicidad_bimestral(self):
        from django.urls import reverse
        self.client.force_login(self.usuario)
        response = self.client.post(reverse("reportes:presupuesto_gasto_recurrente_crear"), {
            "area_id": self.area.pk, "rubro_id": self.rubro.pk,
            "centro_costo_id": self.centro.pk, "categoria_gasto_id": self.categoria.pk,
            "concepto": "CFE", "vigencia_inicio": "2026-01-01", "monto": "100.01",
            "dia_vencimiento": "5", "periodicidad_meses": "2",
        }, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.rubro.gastos_recurrentes.get().versiones.get().periodicidad_meses, 2)

    def test_formulario_muestra_cobertura_y_periodicidad(self):
        from django.urls import reverse
        self.client.force_login(self.usuario)
        response = self.client.get(reverse("reportes:presupuesto_real_captura"), {"area": self.area.codigo})
        self.assertContains(response, 'name="cobertura_mes_inicio"')
        self.assertContains(response, 'name="cobertura_mes_fin"')
        self.assertContains(response, 'name="periodicidad_meses"')
