from datetime import date
from decimal import Decimal

from django.core.exceptions import FieldDoesNotExist
from django.db import IntegrityError, models, transaction
from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from reportes import models as reportes_models
from rrhh.models import Empleado


class CedulaIMSSParserTests(SimpleTestCase):
    def test_mensual_parsea_registro_inline_y_suma_baja_reingreso_sin_totales(self):
        from reportes.services_cedula_imss import parsear_cedula

        filas = [
            ["SISTEMA UNICO DE AUTODETERMINACION"] + [""] * 21,
            ["Periodo de Proceso: Agosto-2026"] + [""] * 21,
            ["Registro Patronal: E52-40157-10-0", "POLLYANA'S DOLCE"] + [""] * 20,
            ["Clave", "Movimiento", "Fecha", "Dias", "SDI", "Lic.", "Inc.", "Aus.",
             "C.F.", "Exc.Pat.", "Exc. Obr.", "P.D. Pat.", "P.D. Obr.", "G.M.P. Pat.",
             "G.M.P. Obr.", "R.T.", "I.V. Pat.", "I.V. Obr", "G.P.S.", "Patronal",
             "Obrera", "SubTotal"],
            ["23-91-73-3507-9", "", "", "", "", "ACOSTA FLORES MARIA"] + [""] * 16,
            ["", "BAJA", "2026-08-10", 10, 331.44] + [0] * 14 + [350.25, 45.10, 395.35],
            ["", "REINGRESO", "2026-08-11", 20, 345.67] + [0] * 14 + [710.35, 90.20, 800.55],
            ["23-98-80-5185-2", "", "", "", "", "PEREZ ADMIN JUAN"] + [""] * 16,
            ["", "NORMAL", "", 30, 400.10] + [0] * 14 + [900.40, 100.00, 1000.40],
            ["TOTAL REGISTRO PATRONAL", "", "", 60, ""] + [0] * 14 + [9999.99, 235.30, 10235.29],
        ]

        parseada = parsear_cedula(filas)

        self.assertEqual(parseada.registro_patronal, "E52-40157-10-0")
        self.assertEqual(len(parseada.trabajadores), 2)
        primero, segundo = parseada.trabajadores
        self.assertEqual(primero.dias, Decimal("30.00"))
        self.assertEqual(primero.sdi, Decimal("345.67"))
        self.assertEqual(primero.patronal, Decimal("1060.60"))
        self.assertEqual(primero.retiro, Decimal("0"))
        self.assertEqual(primero.cesantia_patronal, Decimal("0"))
        self.assertEqual(primero.aportacion_vivienda, Decimal("0"))
        self.assertEqual(segundo.patronal, Decimal("900.40"))

    def test_registro_patronal_en_celda_adyacente(self):
        from reportes.services_cedula_imss import parsear_cedula

        filas = [
            ["Periodo de Proceso: Agosto-2026", "", "", "", ""],
            ["Registro Patronal:", "Y54-67890-10-1", "", "", ""],
            ["Clave", "Nombre", "Dias", "SDI", "Patronal"],
            ["12-12-12-1212-1", "TRABAJADORA UNO", "", "", ""],
            ["", "NORMAL", 30, 350.25, 500.75],
        ]

        parseada = parsear_cedula(filas)

        self.assertEqual(parseada.registro_patronal, "Y54-67890-10-1")

    def test_bimestral_desglosa_componentes_patronales_y_excluye_obrera_creditos(self):
        from reportes.services_cedula_imss import parsear_cedula

        filas = [
            ["Bimestre de Proceso: Agosto-2026"] + [""] * 13,
            ["Registro Patronal:", "E52-40157-10-0"] + [""] * 12,
            ["Clave", "Movimiento", "Fecha", "Dias", "SDI", "Retiro", "Patronal",
             "Obrera", "Suma", "Aportacion Patronal", "Amortizacion", "Credito Vivienda",
             "Tipo", "Total"],
            ["11-11-11-1111-1", "", "", "", "", "EMPLEADA BIMESTRAL"] + [""] * 8,
            ["", "BAJA", "2026-07-15", 15, 300.00, 100.10, 200.20, 80.80, 381.10,
             150.30, 999.99, 888.88, "02", 2420.27],
            ["", "REINGRESO", "2026-07-16", 46, 320.00, 110.11, 210.21, 90.90, 411.22,
             160.31, 777.77, 666.66, "03", 2016.87],
            ["TOTAL", "", "", 61, "", 9999, 9999, 9999, 9999, 9999, 9999, 9999, "", 9999],
        ]

        trabajador = parsear_cedula(filas).trabajadores[0]

        self.assertEqual(trabajador.dias, Decimal("61.00"))
        self.assertEqual(trabajador.sdi, Decimal("320.00"))
        self.assertEqual(trabajador.retiro, Decimal("210.21"))
        self.assertEqual(trabajador.cesantia_patronal, Decimal("410.41"))
        self.assertEqual(trabajador.aportacion_vivienda, Decimal("310.61"))
        self.assertEqual(trabajador.patronal, Decimal("931.23"))


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
            (reportes_models.DetalleCedulaIMSS, "documento"),
            (reportes_models.DetalleCedulaIMSS, "empleado"),
            (reportes_models.DetalleCedulaIMSS, "sucursal"),
        )

        for modelo, campo in relaciones:
            with self.subTest(modelo=modelo.__name__, campo=campo):
                self.assertIs(modelo._meta.get_field(campo).remote_field.on_delete, models.PROTECT)

    def test_respeta_metadatos_e_indices_del_contrato(self):
        expediente = reportes_models.ExpedienteCedulaIMSS
        documento = reportes_models.DocumentoCedulaIMSS
        detalle = reportes_models.DetalleCedulaIMSS

        campos = (
            (expediente, "tipo", {"max_length": 12}),
            (expediente, "periodo", {"db_index": True}),
            (expediente, "registro_patronal", {"max_length": 20, "db_index": True}),
            (expediente, "razon_social", {"blank": True, "default": ""}),
            (expediente, "estado", {"max_length": 16, "default": models.NOT_PROVIDED}),
            (expediente, "creado_en", {"default": timezone.now, "auto_now_add": False}),
            (documento, "clase", {"max_length": 12}),
            (documento, "mime_type", {"max_length": 100}),
            (detalle, "dias", {"max_digits": 6, "default": 0}),
            (detalle, "sdi", {"max_digits": 12, "default": 0}),
            (detalle, "area_codigo", {"max_length": 50}),
            (detalle, "cruce_estado", {"max_length": 16}),
        )
        for modelo, campo_nombre, atributos in campos:
            campo = modelo._meta.get_field(campo_nombre)
            for atributo, esperado in atributos.items():
                with self.subTest(modelo=modelo.__name__, campo=campo_nombre, atributo=atributo):
                    self.assertEqual(getattr(campo, atributo), esperado)

        self.assertEqual(dict(documento.CLASE_CHOICES)[documento.CLASE_SUA_XLS], "SUA XLS")
        self.assertIn("uniq_cedula_imss_revision", {c.name for c in expediente._meta.constraints})
        self.assertIn("uniq_cedula_imss_aplicada", {c.name for c in expediente._meta.constraints})
        self.assertIn("cedula_imss_reg_period_idx", {i.name for i in expediente._meta.indexes})
        self.assertIn("uniq_cedula_sua_expediente", {c.name for c in documento._meta.constraints})
        self.assertIn("uniq_cedula_imss_nss", {c.name for c in detalle._meta.constraints})
        self.assertIn("cedula_imss_emp_doc_idx", {i.name for i in detalle._meta.indexes})

        documento_fk = detalle._meta.get_field("documento")
        self.assertFalse(documento_fk.null)
        self.assertEqual(documento_fk.remote_field.related_name, "detalles")

    def test_detalle_no_duplica_referencia_al_expediente(self):
        with self.assertRaises(FieldDoesNotExist):
            reportes_models.DetalleCedulaIMSS._meta.get_field("expediente")


class ExpedienteCedulaIMSSDatabaseTests(TestCase):
    def crear_expediente(self, **overrides):
        datos = {
            "tipo": reportes_models.ExpedienteCedulaIMSS.TIPO_MENSUAL,
            "periodo": date(2026, 8, 1),
            "registro_patronal": "Y5467890101",
            "razon_social": "Pollyana's Dolce, S.A. de C.V.",
            "estado": reportes_models.ExpedienteCedulaIMSS.ESTADO_VALIDO,
            "total_patronal": Decimal("1250.50"),
        }
        datos.update(overrides)
        return reportes_models.ExpedienteCedulaIMSS.objects.create(**datos)

    def crear_documento_sua(self, expediente, **overrides):
        datos = {
            "expediente": expediente,
            "clase": reportes_models.DocumentoCedulaIMSS.CLASE_SUA_XLS,
            "nombre_original": "SUA_agosto.xlsx",
            "archivo": "reportes/cedulas-imss/2026/08/SUA_agosto.xlsx",
            "sha256": "a" * 64,
            "tamano": 2048,
            "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        datos.update(overrides)
        return reportes_models.DocumentoCedulaIMSS.objects.create(**datos)

    def crear_detalle(self, documento, **overrides):
        datos = {
            "documento": documento,
            "nss": "12345678901",
            "nombre_origen": "Persona de prueba",
            "dias": Decimal("30.00"),
            "sdi": Decimal("350.00"),
            "cuota_patronal": Decimal("450.25"),
            "cruce_estado": reportes_models.DetalleCedulaIMSS.CRUCE_SIN_CRUCE,
        }
        datos.update(overrides)
        return reportes_models.DetalleCedulaIMSS.objects.create(**datos)

    def test_no_permite_repetir_tipo_periodo_registro_y_revision(self):
        self.crear_expediente()

        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_expediente()

    def test_solo_permite_una_revision_aplicada_por_periodo_y_registro(self):
        self.crear_expediente(estado=reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO)

        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_expediente(
                revision=2,
                estado=reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO,
            )

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

    def test_solo_permite_un_sua_por_expediente(self):
        expediente = self.crear_expediente()
        self.crear_documento_sua(expediente)

        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_documento_sua(
                expediente,
                nombre_original="SUA_corregido.xlsx",
                sha256="c" * 64,
            )

    def test_documentos_ema_y_eba_coexisten_en_el_mismo_expediente(self):
        expediente = self.crear_expediente()
        ema = self.crear_documento_sua(
            expediente,
            clase=reportes_models.DocumentoCedulaIMSS.CLASE_EMA_PDF,
            nombre_original="EMA_agosto.pdf",
            sha256="d" * 64,
            mime_type="application/pdf",
        )
        eba = self.crear_documento_sua(
            expediente,
            clase=reportes_models.DocumentoCedulaIMSS.CLASE_EBA_PDF,
            nombre_original="EBA_agosto.pdf",
            sha256="e" * 64,
            mime_type="application/pdf",
        )

        self.assertEqual(expediente.documentos.filter(pk__in=[ema.pk, eba.pk]).count(), 2)

    def test_no_permite_repetir_nss_dentro_del_mismo_documento(self):
        expediente = self.crear_expediente()
        documento = self.crear_documento_sua(expediente)
        self.crear_detalle(documento)

        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_detalle(documento)

    def test_detalle_traza_y_protege_documento_sua(self):
        documento = self.crear_documento_sua(self.crear_expediente())
        detalle = self.crear_detalle(documento)

        self.assertEqual(detalle.documento, documento)
        self.assertEqual(documento.detalles.get(), detalle)
        with self.assertRaises(ProtectedError):
            documento.delete()

    def test_rechaza_cruce_sin_empleado_y_sin_cruce_con_empleado(self):
        documento = self.crear_documento_sua(self.crear_expediente())
        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_detalle(
                documento,
                cruce_estado=reportes_models.DetalleCedulaIMSS.CRUCE_CRUZADO,
            )

        empleado = Empleado.objects.create(codigo="EMP-CED-001", nombre="Persona cruzada")
        with self.assertRaises(IntegrityError), transaction.atomic():
            self.crear_detalle(documento, empleado=empleado)

    def test_str_enmascara_nss_y_conserva_solo_ultimos_cuatro(self):
        detalle = self.crear_detalle(self.crear_documento_sua(self.crear_expediente()))

        representacion = str(detalle)

        self.assertNotIn("12345678901", representacion)
        self.assertNotIn("1234567", representacion)
        self.assertIn("8901", representacion)

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
