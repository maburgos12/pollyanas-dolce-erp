from datetime import date, datetime, time
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from rrhh.models import (
    AjusteAsistencia,
    AsistenciaEmpleado,
    Empleado,
    PrenominaCorte,
    PrenominaEmpleadoResumen,
    PrenominaEquivalenciaCONTPAQi,
    PrenominaMovimiento,
)
from rrhh.services_ajustes_asistencia import (
    aprobar_ajuste_asistencia,
    crear_ajuste_asistencia,
    rechazar_ajuste_asistencia,
)


def aware(fecha: date, hora: time):
    return timezone.make_aware(datetime.combine(fecha, hora), timezone.get_current_timezone())


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


class AjusteAsistenciaServiceTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-prenomina")
        self.autorizador = User.objects.create_user(username="rrhh-autorizador")
        self.fecha = date(2026, 6, 11)
        self.empleado = Empleado.objects.create(
            codigo="349",
            nombre="CASTRO LOPEZ ANA",
            fecha_ingreso=date(2026, 6, 1),
            activo=True,
            sucursal="Matriz",
        )

    @patch("rrhh.services_ajustes_asistencia.evaluar_dia_empleado")
    def test_aprobar_ajuste_de_salida_actualiza_asistencia_y_guarda_historial(self, evaluar_mock):
        salida_original = aware(self.fecha, time(17, 45))
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=self.fecha,
            entrada=aware(self.fecha, time(9, 0)),
            salida=salida_original,
        )
        ajuste = crear_ajuste_asistencia(
            empleado=self.empleado,
            fecha=self.fecha,
            tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
            valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
            motivo="Olvido checar salida correcta.",
            solicitado_por=self.user,
        )

        aprobado = aprobar_ajuste_asistencia(ajuste, self.autorizador, comentario="Validado contra checador.")

        asistencia.refresh_from_db()
        aprobado.refresh_from_db()
        self.assertEqual(ajuste.asistencia_id, asistencia.id)
        self.assertEqual(ajuste.valores_anteriores, {"salida": salida_original.isoformat()})
        self.assertEqual(timezone.localtime(asistencia.salida).time(), time(18, 5))
        self.assertEqual(aprobado.estado, AjusteAsistencia.ESTADO_APLICADO)
        self.assertEqual(aprobado.autorizado_por, self.autorizador)
        self.assertEqual(aprobado.aplicado_por, self.autorizador)
        self.assertIsNotNone(aprobado.autorizado_en)
        self.assertIsNotNone(aprobado.aplicado_en)
        self.assertEqual(aprobado.valores_aplicados["salida"], "2026-06-11T18:05:00-07:00")
        self.assertEqual(aprobado.valores_aplicados["comentario"], "Validado contra checador.")
        evaluar_mock.assert_called_once_with(self.empleado, self.fecha)

    @patch("rrhh.services_ajustes_asistencia.evaluar_dia_empleado")
    def test_rechazar_ajuste_no_modifica_asistencia(self, evaluar_mock):
        salida_original = aware(self.fecha, time(17, 45))
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=self.fecha,
            salida=salida_original,
        )
        ajuste = crear_ajuste_asistencia(
            empleado=self.empleado,
            fecha=self.fecha,
            tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
            valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
            motivo="Solicitud por aclarar.",
            solicitado_por=self.user,
        )

        rechazado = rechazar_ajuste_asistencia(ajuste, self.autorizador, comentario="No procede.")

        asistencia.refresh_from_db()
        rechazado.refresh_from_db()
        self.assertEqual(asistencia.salida, salida_original)
        self.assertEqual(rechazado.estado, AjusteAsistencia.ESTADO_RECHAZADO)
        self.assertEqual(rechazado.autorizado_por, self.autorizador)
        self.assertIsNotNone(rechazado.autorizado_en)
        self.assertEqual(rechazado.valores_aplicados["comentario"], "No procede.")
        evaluar_mock.assert_not_called()

    @patch("rrhh.services_ajustes_asistencia.evaluar_dia_empleado")
    def test_no_se_puede_aprobar_dos_veces(self, evaluar_mock):
        AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=self.fecha,
        )
        ajuste = crear_ajuste_asistencia(
            empleado=self.empleado,
            fecha=self.fecha,
            tipo_ajuste=AjusteAsistencia.TIPO_ENTRADA,
            valores_propuestos={"entrada": "2026-06-11T09:03:00-07:00"},
            motivo="Entrada capturada manualmente.",
            solicitado_por=self.user,
        )
        aprobar_ajuste_asistencia(ajuste, self.autorizador)

        with self.assertRaises(ValidationError):
            aprobar_ajuste_asistencia(ajuste, self.autorizador)

        self.assertEqual(evaluar_mock.call_count, 1)

    def test_crear_ajuste_requiere_motivo(self):
        with self.assertRaises(ValidationError):
            crear_ajuste_asistencia(
                empleado=self.empleado,
                fecha=self.fecha,
                tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
                valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
                motivo=" ",
                solicitado_por=self.user,
            )
