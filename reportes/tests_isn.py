from datetime import date
from decimal import Decimal as D

from django.db import IntegrityError, transaction
from django.db.models.deletion import ProtectedError
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

    def _datos_expediente(self, *, cfdi, **overrides):
        datos = {
            "periodo": date(2026, 8, 1),
            "revision": 1,
            "uuid": "A",
            "cfdi": cfdi,
            "importe_pagado": D("100.00"),
            "base_gravada_calculada": D("4000.00"),
            "estado": ExpedienteISN.ESTADO_APLICADO,
            "aplicado_en": timezone.now(),
        }
        datos.update(overrides)
        return datos

    def _crear_expediente(self, *, cfdi, **overrides):
        return ExpedienteISN.objects.create(
            **self._datos_expediente(cfdi=cfdi, **overrides)
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

    def test_periodo_debe_ser_el_primer_dia_del_mes(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-PERIODO"),
            periodo=date(2026, 8, 15),
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_revision_debe_ser_mayor_o_igual_a_uno(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-REVISION"),
            revision=0,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_importe_pagado_debe_ser_positivo(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-IMPORTE"),
            importe_pagado=D("0.00"),
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_bases_del_expediente_no_admiten_negativos(self):
        casos = (
            ("base_gravada_calculada", "CFDI-BASE-CALCULADA"),
            ("base_declarada", "CFDI-BASE-DECLARADA"),
        )
        for revision, (campo, cfdi_uuid) in enumerate(casos, start=1):
            with self.subTest(campo=campo):
                datos = self._datos_expediente(
                    cfdi=self._crear_cfdi(cfdi_uuid),
                    uuid=cfdi_uuid,
                    revision=revision,
                    estado=ExpedienteISN.ESTADO_VALIDO,
                    aplicado_en=None,
                    **{campo: D("-0.01")},
                )
                with self.assertRaises(IntegrityError), transaction.atomic():
                    ExpedienteISN.objects.create(**datos)

    def test_estado_debe_ser_un_valor_permitido(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-ESTADO"),
            estado="INVENTADO",
            aplicado_en=None,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_estado_aplicado_exige_fecha_de_aplicacion(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-APLICADO-SIN-FECHA"),
            aplicado_en=None,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_estado_no_aplicado_rechaza_fecha_de_aplicacion(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-VALIDO-CON-FECHA"),
            estado=ExpedienteISN.ESTADO_VALIDO,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_distribucion_no_admite_montos_negativos(self):
        sucursal = Sucursal.objects.create(codigo="S2", nombre="Sucursal 2")
        expediente = self._crear_expediente(cfdi=self._crear_cfdi("CFDI-DISTRIBUCION"))
        for numero, campo in enumerate(("base_gravada", "monto_isn"), start=1):
            with self.subTest(campo=campo):
                empleado = Empleado.objects.create(
                    codigo=f"E2-{numero}",
                    nombre=f"Empleado {numero}",
                )
                datos = {
                    "expediente": expediente,
                    "empleado": empleado,
                    "base_gravada": D("100.00"),
                    "monto_isn": D("10.00"),
                    "area_codigo": "VENTAS",
                    "sucursal": sucursal,
                }
                invalidos = {**datos, campo: D("-0.01")}
                with self.assertRaises(IntegrityError), transaction.atomic():
                    DistribucionISNEmpleado.objects.create(**invalidos)

    def test_uuid_se_sincroniza_desde_el_cfdi(self):
        cfdi = self._crear_cfdi("CFDI-UUID-CANONICO")

        expediente = self._crear_expediente(cfdi=cfdi, uuid="UUID-DIVERGENTE")

        expediente.refresh_from_db()
        self.assertEqual(expediente.uuid, cfdi.uuid)

    def test_cfdi_de_un_expediente_esta_protegido(self):
        cfdi = self._crear_cfdi("CFDI-PROTEGIDO")
        self._crear_expediente(
            cfdi=cfdi,
            estado=ExpedienteISN.ESTADO_VALIDO,
            aplicado_en=None,
        )

        with self.assertRaises(ProtectedError):
            cfdi.delete()
