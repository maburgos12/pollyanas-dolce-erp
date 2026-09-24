from datetime import date
from decimal import Decimal as D

from django.db import IntegrityError, transaction
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from core.models import Sucursal
from reportes.models import DistribucionISNEmpleado, ExpedienteISN
from reportes.services_isn import calcular_isn_sinaloa, prorratear_isn
from rrhh.models import Empleado
from sat_client.models import CfdiDescargado


class ISNMathTests(SimpleTestCase):
    def test_tarifa_progresiva_sinaloa(self):
        self.assertEqual(calcular_isn_sinaloa(D("500000.00")), D("12000.00"))
        self.assertEqual(calcular_isn_sinaloa(D("660307.70")), D("16168.00"))
        self.assertEqual(calcular_isn_sinaloa(D("707079.82")), D("17398.23"))
        self.assertEqual(calcular_isn_sinaloa(D("1000000.00")), D("25800.00"))

    def test_prorrateo_cierra_exactamente_a_centavos(self):
        result = prorratear_isn({3: D("1"), 1: D("1"), 2: D("1")}, D("10.00"))

        self.assertEqual(sum(result.values(), D("0")), D("10.00"))
        self.assertEqual(result, {1: D("3.34"), 2: D("3.33"), 3: D("3.33")})


class ISNModelTests(TestCase):
    def _crear_cfdi(self, uuid):
        return CfdiDescargado.objects.create(
            uuid=uuid,
            rfc_emisor="GES8101015I7",
            rfc_receptor="GEF211230KR2",
            subtotal=D("100.00"),
            total=D("100.00"),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
            fecha_emision=timezone.now(),
        )

    def _crear_expediente(self, *, cfdi, uuid="A", revision=1):
        return ExpedienteISN.objects.create(
            periodo=date(2026, 8, 1),
            revision=revision,
            uuid=uuid,
            cfdi=cfdi,
            importe_pagado=D("100.00"),
            base_gravada_calculada=D("4000.00"),
            estado=ExpedienteISN.ESTADO_APLICADO,
        )

    def test_solo_hay_un_expediente_aplicado_por_periodo(self):
        self._crear_expediente(cfdi=self._crear_cfdi("CFDI-A"))

        with self.assertRaises(IntegrityError), transaction.atomic():
            self._crear_expediente(
                cfdi=self._crear_cfdi("CFDI-B"),
                uuid="B",
                revision=2,
            )

    def test_empleado_no_se_duplica_en_un_expediente(self):
        sucursal = Sucursal.objects.create(codigo="S1", nombre="Sucursal 1")
        empleado = Empleado.objects.create(codigo="E1", nombre="Empleado 1")
        expediente = self._crear_expediente(cfdi=self._crear_cfdi("CFDI-C"))
        datos = {
            "expediente": expediente,
            "empleado": empleado,
            "base_gravada": D("4000.00"),
            "monto_isn": D("100.00"),
            "area_codigo": "VENTAS",
            "sucursal": sucursal,
        }
        DistribucionISNEmpleado.objects.create(**datos)

        with self.assertRaises(IntegrityError), transaction.atomic():
            DistribucionISNEmpleado.objects.create(**datos)
