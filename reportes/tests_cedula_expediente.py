from datetime import date
from decimal import Decimal

from django.db import IntegrityError, models, transaction
from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase, TestCase

from reportes import models as reportes_models


class ExpedienteCedulaIMSSSchemaTests(SimpleTestCase):
    def test_define_los_tres_modelos_normalizados(self):
        for nombre_modelo in (
            "ExpedienteCedulaIMSS",
            "DocumentoCedulaIMSS",
            "DetalleCedulaIMSS",
        ):
            with self.subTest(modelo=nombre_modelo):
                self.assertTrue(
                    hasattr(reportes_models, nombre_modelo),
                    f"Falta definir {nombre_modelo}",
                )

    def test_configura_relaciones_criticas_como_protect(self):
        relaciones = (
            (reportes_models.ExpedienteCedulaIMSS, "aplicado_por"),
            (reportes_models.DocumentoCedulaIMSS, "expediente"),
            (reportes_models.DetalleCedulaIMSS, "expediente"),
            (reportes_models.DetalleCedulaIMSS, "empleado"),
            (reportes_models.DetalleCedulaIMSS, "sucursal"),
        )

        for modelo, campo in relaciones:
            with self.subTest(modelo=modelo.__name__, campo=campo):
                self.assertIs(modelo._meta.get_field(campo).remote_field.on_delete, models.PROTECT)


class ExpedienteCedulaIMSSDatabaseTests(TestCase):
    def crear_expediente(self, **overrides):
        datos = {
            "tipo": reportes_models.ExpedienteCedulaIMSS.TIPO_MENSUAL,
            "periodo": date(2026, 8, 1),
            "registro_patronal": "Y5467890101",
            "razon_social": "Pollyana's Dolce, S.A. de C.V.",
            "total_patronal": Decimal("1250.50"),
        }
        datos.update(overrides)
        return reportes_models.ExpedienteCedulaIMSS.objects.create(**datos)

    def test_no_permite_repetir_tipo_periodo_registro_y_revision(self):
        self.crear_expediente()

        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_expediente()

    def test_sha256_identifica_un_documento_de_forma_unica(self):
        expediente = self.crear_expediente()
        datos = {
            "expediente": expediente,
            "clase": reportes_models.DocumentoCedulaIMSS.CLASE_SUA_XLS,
            "nombre_original": "SUA_agosto.xlsx",
            "archivo": "reportes/cedulas-imss/2026/08/SUA_agosto.xlsx",
            "sha256": "a" * 64,
            "tamano": 2048,
            "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        reportes_models.DocumentoCedulaIMSS.objects.create(**datos)

        with self.assertRaises(IntegrityError), transaction.atomic():
            reportes_models.DocumentoCedulaIMSS.objects.create(**datos)

    def test_no_permite_repetir_nss_dentro_del_mismo_expediente(self):
        expediente = self.crear_expediente()
        datos = {
            "expediente": expediente,
            "nss": "12345678901",
            "nombre_origen": "Persona de prueba",
            "dias": Decimal("30.00"),
            "sdi": Decimal("350.00"),
            "cuota_patronal": Decimal("450.25"),
            "cruce_estado": reportes_models.DetalleCedulaIMSS.CRUCE_SIN_CRUCE,
        }
        reportes_models.DetalleCedulaIMSS.objects.create(**datos)

        with self.assertRaises(IntegrityError), transaction.atomic():
            reportes_models.DetalleCedulaIMSS.objects.create(**datos)

    def test_documento_protege_el_expediente_de_borrado(self):
        expediente = self.crear_expediente()
        reportes_models.DocumentoCedulaIMSS.objects.create(
            expediente=expediente,
            clase=reportes_models.DocumentoCedulaIMSS.CLASE_EMA_PDF,
            nombre_original="EMA_agosto.pdf",
            archivo="reportes/cedulas-imss/2026/08/EMA_agosto.pdf",
            sha256="b" * 64,
            tamano=1024,
            mime_type="application/pdf",
        )

        with self.assertRaises(ProtectedError):
            expediente.delete()
