from datetime import date
from decimal import Decimal

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase

from rrhh.models import (
    AjusteAsistencia,
    AsistenciaEmpleado,
    Empleado,
    PrenominaCorte,
    PrenominaEmpleadoResumen,
    PrenominaEquivalenciaCONTPAQi,
    PrenominaMovimiento,
)


class PrenominaModelTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-prenomina")
        self.empleado = Empleado.objects.create(
            codigo="347",
            nombre="ANAYA BERNAL CARLOS EZEQUIEL",
            fecha_ingreso=date(2026, 6, 10),
            activo=True,
            sucursal="Matriz",
        )
        self.otro_empleado = Empleado.objects.create(
            codigo="348",
            nombre="BARRAZA LOPEZ MARIA",
            fecha_ingreso=date(2026, 6, 10),
            activo=True,
            sucursal="Matriz",
        )

    def test_corte_genera_folio_y_resumen_por_empleado(self):
        corte = PrenominaCorte.objects.create(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            tipo_periodo=PrenominaCorte.TIPO_QUINCENAL,
            creado_por=self.user,
        )
        resumen = PrenominaEmpleadoResumen.objects.create(
            corte=corte,
            empleado=self.empleado,
            dias_periodo=15,
            dias_laborables=6,
            dias_no_laborados_pre_ingreso=9,
            estado=PrenominaEmpleadoResumen.ESTADO_LISTO,
        )

        self.assertTrue(corte.folio.startswith("PRE-202606-"))
        self.assertEqual(str(resumen), f"{corte.folio} · {self.empleado.nombre}")

    def test_corte_genera_folio_secuencial_por_sufijo(self):
        PrenominaCorte.objects.create(
            folio="PRE-202606-009",
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            creado_por=self.user,
        )
        corte = PrenominaCorte.objects.create(
            fecha_inicio=date(2026, 6, 16),
            fecha_fin=date(2026, 6, 30),
            fecha_corte=date(2026, 6, 30),
            creado_por=self.user,
        )

        self.assertEqual(corte.folio, "PRE-202606-010")

    def test_movimiento_requiere_equivalencia_para_exportar(self):
        corte = PrenominaCorte.objects.create(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            creado_por=self.user,
        )
        movimiento = PrenominaMovimiento.objects.create(
            corte=corte,
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA,
            valor=Decimal("1"),
        )

        self.assertEqual(movimiento.estado, PrenominaMovimiento.ESTADO_PENDIENTE_CONFIGURACION)

        PrenominaEquivalenciaCONTPAQi.objects.create(
            tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA,
            clave_contpaqi="F",
            descripcion="Falta",
            aplica_valor=True,
            activo=True,
        )
        movimiento.aplicar_equivalencia()
        movimiento.save(update_fields=["clave_contpaqi", "estado"])

        self.assertEqual(movimiento.clave_contpaqi, "F")
        self.assertEqual(movimiento.estado, PrenominaMovimiento.ESTADO_LISTO)

    def test_movimiento_no_duplica_misma_fuente_tipo_y_corte(self):
        corte = PrenominaCorte.objects.create(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            creado_por=self.user,
        )
        base = {
            "corte": corte,
            "empleado": self.empleado,
            "fecha": date(2026, 6, 11),
            "tipo_movimiento_erp": PrenominaMovimiento.TIPO_FALTA,
            "valor": Decimal("1"),
            "fuente_modelo": "IncidenciaAsistencia",
            "fuente_id": "123",
        }
        PrenominaMovimiento.objects.create(**base)

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                PrenominaMovimiento.objects.create(**base)

    def test_ajuste_asistencia_guarda_valores_anteriores_y_propuestos(self):
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
        )
        ajuste = AjusteAsistencia.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            asistencia=asistencia,
            tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
            estado=AjusteAsistencia.ESTADO_PENDIENTE,
            valores_anteriores={"salida": None},
            valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
            motivo="Olvido checar salida.",
            solicitado_por=self.user,
        )

        self.assertEqual(ajuste.estado, AjusteAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(ajuste.valores_anteriores["salida"], None)

    def test_ajuste_asistencia_rechaza_asistencia_de_otro_empleado_o_fecha(self):
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.otro_empleado,
            fecha=date(2026, 6, 11),
        )

        with self.assertRaises(ValidationError):
            AjusteAsistencia.objects.create(
                empleado=self.empleado,
                fecha=date(2026, 6, 11),
                asistencia=asistencia,
                tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
                valores_anteriores={"salida": None},
                valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
                motivo="Asistencia de otro empleado.",
                solicitado_por=self.user,
            )

        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 12),
        )
        with self.assertRaises(ValidationError):
            AjusteAsistencia.objects.create(
                empleado=self.empleado,
                fecha=date(2026, 6, 11),
                asistencia=asistencia,
                tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
                valores_anteriores={"salida": None},
                valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
                motivo="Fecha distinta.",
                solicitado_por=self.user,
            )
