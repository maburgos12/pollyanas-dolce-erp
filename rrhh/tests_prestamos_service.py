from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from rest_framework.exceptions import ValidationError

from rrhh.models import Empleado, Prestamo
from rrhh.services_prestamos import crear_solicitud_prestamo


class CrearSolicitudPrestamoTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model().objects
        cls.carolina_user = users.create_user(username="carolina.cayetano", password="test12345")
        cls.carolina = Empleado.objects.create(
            codigo="CAR-PRE",
            nombre="CAYETANO VALENZUELA CAROLINA",
            departamento=Empleado.DEP_PRODUCCION,
            activo=True,
            usuario_erp=cls.carolina_user,
        )
        cls.operadora = users.create_user(username="rosa.cervantes", password="test12345")
        cls.empleado = Empleado.objects.create(
            codigo="EMP-PRE",
            nombre="COLABORADORA PRODUCCION",
            departamento=Empleado.DEP_PRODUCCION,
            activo=True,
            jefe_directo=cls.carolina,
        )

    def _crear(self, **overrides):
        params = {
            "empleado": self.empleado,
            "actor": self.operadora,
            "concepto": "Emergencia familiar",
            "metodo_pago": Prestamo.METODO_TRANSFERENCIA,
            "importe": Decimal("3000.00"),
            "num_quincenas": 6,
            "fecha_deposito": date(2026, 8, 15),
        }
        params.update(overrides)
        return crear_solicitud_prestamo(**params)

    @patch("rrhh.services_prestamos.notificar_prestamo_solicitado")
    def test_crea_solicitud_con_calculo_jefa_actor_y_notificacion(self, notificar):
        prestamo = self._crear()

        self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
        self.assertEqual(prestamo.descuento_quincenal, Decimal("500.00"))
        self.assertEqual(prestamo.saldo_actual, Decimal("3000.00"))
        self.assertEqual(prestamo.jefe_directo, self.carolina_user)
        self.assertEqual(prestamo.creado_por, self.operadora)
        self.assertEqual(prestamo.fecha_solicitud, date.today())
        notificar.assert_called_once_with(prestamo, actor=self.operadora)

    def test_rechaza_deuda_vigente_sin_crear_segundo_prestamo(self):
        vigente = self._crear()

        with self.assertRaisesMessage(ValidationError, vigente.folio):
            self._crear(concepto="Segundo intento")

        self.assertEqual(Prestamo.objects.count(), 1)

    def test_rechaza_jefa_sin_usuario_activo(self):
        self.carolina_user.is_active = False
        self.carolina_user.save(update_fields=["is_active"])

        with self.assertRaisesMessage(ValidationError, "jefa directa activa"):
            self._crear()

        self.assertFalse(Prestamo.objects.exists())

    def test_rechaza_empleado_sin_jefa(self):
        self.empleado.jefe_directo = None
        self.empleado.save(update_fields=["jefe_directo"])

        with self.assertRaisesMessage(ValidationError, "jefa directa activa"):
            self._crear()

        self.assertFalse(Prestamo.objects.exists())

    def test_rechaza_importe_y_quincenas_no_positivos(self):
        casos = (
            {"importe": Decimal("0.00")},
            {"importe": Decimal("-1.00")},
            {"num_quincenas": 0},
            {"num_quincenas": -1},
        )
        for caso in casos:
            with self.subTest(caso=caso), self.assertRaises(ValidationError):
                self._crear(**caso)

        self.assertFalse(Prestamo.objects.exists())

    def test_rechaza_concepto_vacio_y_metodo_invalido(self):
        with self.assertRaises(ValidationError):
            self._crear(concepto="   ")
        with self.assertRaises(ValidationError):
            self._crear(metodo_pago="bitcoin")

        self.assertFalse(Prestamo.objects.exists())
