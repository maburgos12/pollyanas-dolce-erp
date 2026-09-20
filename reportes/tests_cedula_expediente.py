from datetime import date
from decimal import Decimal
from io import BytesIO
from pathlib import Path
import tempfile
import threading
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.core.exceptions import FieldDoesNotExist
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import IntegrityError, close_old_connections, connection, models, transaction
from django.db.models import Sum
from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase, TestCase, TransactionTestCase
from django.utils import timezone

from core.models import AuditLog
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
        self.assertEqual(
            dict(expediente.ESTADO_CHOICES)[expediente.ESTADO_REEMPLAZADO],
            "Reemplazado",
        )
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


class PersistenciaExpedienteTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user("aplica_cedula", password="x")
        cls.adm = reportes_models.AreaPresupuesto.objects.create(
            nombre="Administración", codigo="administracion"
        )
        cls.nom = reportes_models.AreaPresupuesto.objects.create(nombre="Nómina", codigo="nomina")
        cls.prod = reportes_models.AreaPresupuesto.objects.create(
            nombre="Producción", codigo="produccion"
        )
        for area in (cls.adm, cls.nom, cls.prod):
            reportes_models.RubroPresupuesto.objects.create(
                area=area,
                concepto="IMSS",
                tipo=reportes_models.RubroPresupuesto.TIPO_EGRESO,
            )
        Empleado.objects.create(
            codigo="CED-001",
            nombre="Persona cédula",
            nss="12-12-12-1212-1",
            departamento=Empleado.DEP_ADMINISTRACION,
        )

    def setUp(self):
        self._media = tempfile.TemporaryDirectory()
        self._settings = self.settings(MEDIA_ROOT=self._media.name)
        self._settings.enable()
        self.addCleanup(self._settings.disable)
        self.addCleanup(self._media.cleanup)

    @staticmethod
    def _filas(*, detalle=Decimal("150.25"), control=Decimal("150.25")):
        return [
            ["SISTEMA UNICO DE AUTODETERMINACION"] + [""] * 21,
            ["Período de Proceso: Agosto-2026"] + [""] * 21,
            ["Registro Patronal: E52-40157-10-0", "POLLYANA'S DOLCE"] + [""] * 20,
            ["Clave", "Movimiento", "Fecha", "Dias", "SDI", "Lic.", "Inc.", "Aus.",
             "C.F.", "Exc.Pat.", "Exc. Obr.", "P.D. Pat.", "P.D. Obr.", "G.M.P. Pat.",
             "G.M.P. Obr.", "R.T.", "I.V. Pat.", "I.V. Obr", "G.P.S.", "Patronal",
             "Obrera", "SubTotal"],
            ["12-12-12-1212-1", "", "", "", "", "PERSONA CEDULA"] + [""] * 16,
            ["", "NORMAL", "", 30, 350] + [0] * 14 + [detalle, Decimal("20"), detalle + 20],
            ["TOTAL REGISTRO PATRONAL", "", "", 30, ""] + [0] * 14
            + [control, Decimal("20"), control + 20],
        ]

    @staticmethod
    def _sua(nombre="SUA_agosto.xls", contenido=b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1xls"):
        return SimpleUploadedFile(nombre, contenido, content_type="application/vnd.ms-excel")

    @staticmethod
    def _pdf(nombre="EMA_agosto.pdf"):
        from reportlab.pdfgen import canvas

        etiqueta = "EBA EMISION BIMESTRAL ANTICIPADA" if "EBA" in nombre.upper() else "EMA EMISION MENSUAL ANTICIPADA"
        salida = BytesIO()
        pdf = canvas.Canvas(salida)
        pdf.drawString(72, 720, etiqueta)
        pdf.showPage()
        pdf.save()
        return SimpleUploadedFile(
            nombre,
            salida.getvalue(),
            content_type="text/plain",
        )

    def _preview(self, *, filas=None, archivos=None):
        from reportes.services_cedula_expediente import preparar_expediente

        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls",
            return_value=filas or self._filas(),
        ):
            return preparar_expediente(archivos or [self._sua()], usuario=self.user)

    def test_preview_no_escribe_y_prepara_cruce(self):
        preview = self._preview(archivos=[self._sua(), self._pdf()])

        self.assertEqual(preview.total_detalle, Decimal("150.25"))
        self.assertEqual(preview.total_patronal, Decimal("150.25"))
        self.assertEqual(preview.detalles[0].empleado_id, Empleado.objects.get(codigo="CED-001").pk)
        self.assertEqual(preview.detalles[0].area_codigo, "administracion")
        self.assertEqual(reportes_models.ExpedienteCedulaIMSS.objects.count(), 0)
        self.assertEqual(reportes_models.DocumentoCedulaIMSS.objects.count(), 0)
        self.assertEqual(reportes_models.DetalleCedulaIMSS.objects.count(), 0)
        self.assertEqual(list(Path(self._media.name).rglob("*")), [])

    def test_preview_exige_un_solo_sua_y_firmas_validas(self):
        with self.assertRaisesRegex(ValueError, "exactamente un"):
            self._preview(archivos=[self._pdf()])
        with self.assertRaisesRegex(ValueError, "firma válida"):
            self._preview(archivos=[self._sua(contenido=b"xls falso")])

    def test_preview_bloquea_total_control_ausente(self):
        from reportes.services_cedula_expediente import CedulaDiscrepante

        filas = self._filas()[:-1]
        with self.assertRaisesRegex(CedulaDiscrepante, "control inequívoco"):
            self._preview(filas=filas)

    def test_preview_no_confunde_palabras_con_siglas_ema_eba(self):
        from reportlab.pdfgen import canvas

        salida = BytesIO()
        documento = canvas.Canvas(salida)
        documento.drawString(72, 720, "Documento sin clasificacion IMSS")
        documento.showPage()
        documento.save()
        pdf = SimpleUploadedFile(
            "sistema_prueba.pdf",
            salida.getvalue(),
            content_type="application/pdf",
        )
        with self.assertRaisesRegex(ValueError, "clasificar inequívocamente"):
            self._preview(archivos=[self._sua(), pdf])

    def test_preview_rechaza_pdf_falso_y_archivo_sobredimensionado(self):
        pdf_falso = SimpleUploadedFile(
            "EMA_falso.pdf",
            b"%PDF-1.4\nEMA EMISION MENSUAL ANTICIPADA\n%%EOF",
            content_type="application/pdf",
        )
        with self.assertRaisesRegex(ValueError, "PDF válido"):
            self._preview(archivos=[self._sua(), pdf_falso])
        with self.assertRaisesRegex(ValueError, "10 MiB"):
            self._preview(
                archivos=[self._sua(contenido=b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"x" * (10 * 1024 * 1024))]
            )

    def test_aplica_documentos_detalle_metadata_y_auditoria(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        expediente = aplicar_expediente(
            self._preview(archivos=[self._sua(), self._pdf()]), usuario=self.user
        )

        self.assertEqual(expediente.estado, reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO)
        self.assertEqual(expediente.documentos.count(), 2)
        sua = expediente.documentos.get(clase=reportes_models.DocumentoCedulaIMSS.CLASE_SUA_XLS)
        self.assertEqual(
            sua.detalles.aggregate(total=Sum("cuota_patronal"))["total"], Decimal("150.25")
        )
        detalle = sua.detalles.get()
        self.assertEqual(detalle.area_codigo, "administracion")
        self.assertEqual(detalle.empleado.codigo, "CED-001")
        lineas = reportes_models.LineaPresupuestoMensual.objects.filter(fuente_real="AUTO:SIPARE")
        self.assertEqual(lineas.count(), 2)
        for linea in lineas:
            self.assertEqual(linea.metadata["expediente_cedula_imss_id"], expediente.pk)
            self.assertEqual(linea.metadata["documento_cedula_imss_id"], sua.pk)
        self.assertTrue(
            AuditLog.objects.filter(
                action="CEDULA_IMSS_APLICADA",
                model="reportes.ExpedienteCedulaIMSS",
                object_id=str(expediente.pk),
                user=self.user,
            ).exists()
        )

    def test_sha_sua_es_idempotente_y_no_duplica(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        expediente = aplicar_expediente(self._preview(), usuario=self.user)
        repetido = aplicar_expediente(self._preview(), usuario=self.user)

        self.assertEqual(repetido.pk, expediente.pk)
        self.assertEqual(reportes_models.ExpedienteCedulaIMSS.objects.count(), 1)
        self.assertEqual(reportes_models.DocumentoCedulaIMSS.objects.count(), 1)
        self.assertEqual(AuditLog.objects.filter(action="CEDULA_IMSS_APLICADA").count(), 1)

    def test_nueva_huella_reemplaza_revision_aplicada(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        anterior = aplicar_expediente(self._preview(), usuario=self.user)
        nueva = aplicar_expediente(
            self._preview(archivos=[self._sua(contenido=self._sua().read() + b"correccion")]),
            usuario=self.user,
        )

        anterior.refresh_from_db()
        self.assertEqual(anterior.estado, reportes_models.ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO)
        self.assertEqual(nueva.estado, reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO)
        self.assertEqual((anterior.revision, nueva.revision), (1, 2))
        self.assertEqual(
            reportes_models.ExpedienteCedulaIMSS.objects.filter(
                tipo=nueva.tipo,
                periodo=nueva.periodo,
                registro_patronal=nueva.registro_patronal,
                estado=reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO,
            ).count(),
            1,
        )

    def test_aplicar_rehace_snapshot_y_materializa_desde_detalle_final(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        preview = self._preview()
        empleado = Empleado.objects.get(codigo="CED-001")
        empleado.departamento = Empleado.DEP_PRODUCCION
        empleado.save(update_fields=["departamento"])

        expediente = aplicar_expediente(preview, usuario=self.user)
        detalle = reportes_models.DetalleCedulaIMSS.objects.get(
            documento__expediente=expediente
        )

        self.assertEqual(detalle.area_codigo, "produccion")
        self.assertTrue(
            reportes_models.LineaPresupuestoMensual.objects.filter(
                rubro__area=self.prod,
                monto_real=Decimal("150.25"),
                fuente_real="AUTO:SIPARE",
            ).exists()
        )
        self.assertFalse(
            reportes_models.LineaPresupuestoMensual.objects.filter(
                rubro__area=self.adm,
                fuente_real="AUTO:SIPARE",
            ).exists()
        )

    def test_revision_pone_en_cero_destino_anterior_ausente_sin_doble_conteo(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        anterior = aplicar_expediente(self._preview(), usuario=self.user)
        empleado = Empleado.objects.get(codigo="CED-001")
        empleado.departamento = Empleado.DEP_PRODUCCION
        empleado.save(update_fields=["departamento"])
        contenido_corregido = self._sua().read() + b"movido-produccion"
        nuevo = aplicar_expediente(
            self._preview(archivos=[self._sua(contenido=contenido_corregido)]),
            usuario=self.user,
        )

        administracion = reportes_models.LineaPresupuestoMensual.objects.get(
            rubro__area=self.adm, periodo=date(2026, 8, 1)
        )
        produccion = reportes_models.LineaPresupuestoMensual.objects.get(
            rubro__area=self.prod, periodo=date(2026, 8, 1)
        )
        nomina = reportes_models.LineaPresupuestoMensual.objects.get(
            rubro__area=self.nom, periodo=date(2026, 8, 1)
        )
        self.assertEqual(administracion.monto_real, Decimal("0.00"))
        self.assertEqual(produccion.monto_real, Decimal("150.25"))
        self.assertEqual(nomina.monto_real, Decimal("150.25"))
        self.assertEqual(administracion.metadata["expediente_cedula_imss_id"], nuevo.pk)
        self.assertEqual(
            reportes_models.LineaPresupuestoMensual.objects.filter(
                fuente_real="AUTO:SIPARE",
                monto_real__gt=0,
            ).exclude(
                rubro__area=self.nom,
            ).aggregate(total=Sum("monto_real"))["total"],
            Decimal("150.25"),
        )
        auditoria = AuditLog.objects.get(
            action="CEDULA_IMSS_APLICADA", object_id=str(nuevo.pk)
        )
        self.assertEqual(auditoria.payload["lineas_actualizadas"], 3)
        anterior.refresh_from_db()
        self.assertEqual(anterior.estado, reportes_models.ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO)

    def test_sha_valido_completa_aplicacion_y_sha_aplicado_adjunta_pdf_nuevo(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        preview = self._preview()
        pendiente = reportes_models.ExpedienteCedulaIMSS.objects.create(
            tipo=preview.parseada.tipo,
            periodo=preview.parseada.periodo,
            registro_patronal=preview.parseada.registro_patronal,
            estado=reportes_models.ExpedienteCedulaIMSS.ESTADO_VALIDO,
            total_patronal=preview.total_patronal,
        )
        sua = reportes_models.DocumentoCedulaIMSS(
            expediente=pendiente,
            clase=reportes_models.DocumentoCedulaIMSS.CLASE_SUA_XLS,
            nombre_original=preview.sua.nombre_original,
            sha256=preview.sua.sha256,
            tamano=preview.sua.tamano,
            mime_type="application/vnd.ms-excel",
            total_visible=preview.total_patronal,
        )
        sua.archivo.save("pendiente.xls", ContentFile(preview.sua.contenido), save=False)
        sua.save()

        completado = aplicar_expediente(preview, usuario=self.user)
        self.assertEqual(completado.pk, pendiente.pk)
        completado.refresh_from_db()
        self.assertEqual(completado.estado, reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO)
        self.assertEqual(sua.detalles.count(), 1)

        con_pdf = self._preview(archivos=[self._sua(), self._pdf()])
        repetido = aplicar_expediente(con_pdf, usuario=self.user)
        self.assertEqual(repetido.pk, pendiente.pk)
        self.assertEqual(pendiente.documentos.count(), 2)
        evento = AuditLog.objects.get(
            action="CEDULA_IMSS_EVIDENCIA_AGREGADA", object_id=str(pendiente.pk)
        )
        pdf = pendiente.documentos.get(clase=reportes_models.DocumentoCedulaIMSS.CLASE_EMA_PDF)
        self.assertEqual(evento.user, self.user)
        self.assertEqual(evento.payload["sha256"], [pdf.sha256])
        self.assertEqual(evento.payload["documento_ids"], [pdf.pk])
        aplicar_expediente(con_pdf, usuario=self.user)
        self.assertEqual(pendiente.documentos.count(), 2)
        self.assertEqual(
            AuditLog.objects.filter(
                action="CEDULA_IMSS_EVIDENCIA_AGREGADA", object_id=str(pendiente.pk)
            ).count(),
            1,
        )

    def test_carrera_sha_relee_ganador_despues_del_rollback_y_conserva_su_blob(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        preview = self._preview()
        ganador = reportes_models.ExpedienteCedulaIMSS.objects.create(
            tipo=reportes_models.ExpedienteCedulaIMSS.TIPO_MENSUAL,
            periodo=date(2026, 7, 1),
            registro_patronal="GANADOR",
            estado=reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO,
            total_patronal=preview.total_patronal,
        )
        documento_ganador = reportes_models.DocumentoCedulaIMSS(
            expediente=ganador,
            clase=reportes_models.DocumentoCedulaIMSS.CLASE_SUA_XLS,
            nombre_original="ganador.xls",
            sha256=preview.sua.sha256,
            tamano=len(preview.sua.contenido),
            mime_type="application/vnd.ms-excel",
            total_visible=preview.total_patronal,
        )
        documento_ganador.archivo.save(
            "ganador.xls", ContentFile(preview.sua.contenido), save=False
        )
        documento_ganador.save()
        blob_ganador = documento_ganador.archivo.name

        consulta_inicial_oculta = MagicMock()
        consulta_inicial_oculta.filter.return_value.select_related.return_value.first.return_value = None

        def reutilizar_nombre_ganador(field_file, name, content, save=True):
            field_file.name = blob_ganador
            field_file._committed = True

        with patch.object(
            reportes_models.DocumentoCedulaIMSS.objects,
            "select_for_update",
            return_value=consulta_inicial_oculta,
        ), patch(
            "django.db.models.fields.files.FieldFile.save",
            new=reutilizar_nombre_ganador,
        ):
            resultado = aplicar_expediente(preview, usuario=self.user)

        self.assertEqual(resultado.pk, ganador.pk)
        self.assertTrue(default_storage.exists(blob_ganador))
        self.assertEqual(reportes_models.ExpedienteCedulaIMSS.objects.count(), 1)
        self.assertEqual(reportes_models.DocumentoCedulaIMSS.objects.count(), 1)

    def test_total_discordante_hace_rollback_sin_lineas_ni_archivos(self):
        from reportes.services_cedula_expediente import CedulaDiscrepante, aplicar_expediente

        preview = self._preview(filas=self._filas(detalle=Decimal("150.25"), control=Decimal("151.25")))
        with self.assertRaises(CedulaDiscrepante):
            aplicar_expediente(preview, usuario=self.user)

        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertFalse(reportes_models.LineaPresupuestoMensual.objects.exists())
        self.assertEqual([p for p in Path(self._media.name).rglob("*") if p.is_file()], [])

    def test_pdf_es_evidencia_y_no_incrementa_importes(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        expediente = aplicar_expediente(
            self._preview(archivos=[self._sua(), self._pdf("EBA_bimestre.pdf")]),
            usuario=self.user,
        )

        self.assertEqual(expediente.total_patronal, Decimal("150.25"))
        self.assertEqual(
            expediente.documentos.get(clase=reportes_models.DocumentoCedulaIMSS.CLASE_EBA_PDF).total_visible,
            None,
        )
        self.assertEqual(
            reportes_models.DetalleCedulaIMSS.objects.aggregate(total=Sum("cuota_patronal"))["total"],
            Decimal("150.25"),
        )

    def test_nss_duplicado_activo_bloquea_sin_escrituras(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        Empleado.objects.create(
            codigo="CED-002",
            nombre="Duplicada",
            nss="12121212121",
            departamento=Empleado.DEP_ADMINISTRACION,
        )
        preview = self._preview()

        with self.assertRaisesRegex(ValueError, "NSS duplicados"):
            aplicar_expediente(preview, usuario=self.user)
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertFalse(reportes_models.LineaPresupuestoMensual.objects.exists())

    def test_elimina_blobs_si_falla_la_base(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        preview = self._preview(archivos=[self._sua(), self._pdf()])
        with patch.object(
            reportes_models.DetalleCedulaIMSS.objects,
            "bulk_create",
            side_effect=IntegrityError("falla forzada"),
        ):
            with self.assertRaises(IntegrityError):
                aplicar_expediente(preview, usuario=self.user)

        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertEqual([p for p in Path(self._media.name).rglob("*") if p.is_file()], [])

    def test_cleanup_continua_si_un_delete_falla_y_preserva_error_original(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        preview = self._preview(archivos=[self._sua(), self._pdf()])
        with patch.object(
            reportes_models.DetalleCedulaIMSS.objects,
            "bulk_create",
            side_effect=IntegrityError("falla original"),
        ), patch.object(
            default_storage,
            "delete",
            side_effect=[OSError("storage caído"), None],
        ) as eliminar:
            with self.assertLogs("reportes.services_cedula_expediente", level="WARNING"):
                with self.assertRaisesRegex(IntegrityError, "falla original"):
                    aplicar_expediente(preview, usuario=self.user)

        self.assertEqual(eliminar.call_count, 2)

    def test_respeta_manual_y_conflicto_auto(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        rubro_adm = reportes_models.RubroPresupuesto.objects.get(area=self.adm, concepto="IMSS")
        rubro_nom = reportes_models.RubroPresupuesto.objects.get(area=self.nom, concepto="IMSS")
        manual = reportes_models.LineaPresupuestoMensual.objects.create(
            rubro=rubro_adm,
            periodo=date(2026, 8, 1),
            monto_real=Decimal("999"),
            fuente_real="MANUAL:paula",
        )
        auto = reportes_models.LineaPresupuestoMensual.objects.create(
            rubro=rubro_nom,
            periodo=date(2026, 8, 1),
            monto_real=Decimal("888"),
            fuente_real="AUTO:GASTO_OPERATIVO",
        )

        aplicar_expediente(self._preview(), usuario=self.user)

        manual.refresh_from_db()
        auto.refresh_from_db()
        self.assertEqual((manual.monto_real, manual.fuente_real), (Decimal("999"), "MANUAL:paula"))
        self.assertEqual(
            (auto.monto_real, auto.fuente_real),
            (Decimal("888"), "AUTO:GASTO_OPERATIVO"),
        )

    def test_auditoria_incluye_sha_documentos_y_contadores(self):
        from reportes.services_cedula_expediente import aplicar_expediente

        expediente = aplicar_expediente(
            self._preview(archivos=[self._sua(), self._pdf()]), usuario=self.user
        )
        payload = AuditLog.objects.get(
            action="CEDULA_IMSS_APLICADA", object_id=str(expediente.pk)
        ).payload

        self.assertEqual(payload["sha256_sua"], expediente.metadata["sha256_sua"])
        self.assertEqual(len(payload["documento_ids"]), 2)
        self.assertIn("lineas_actualizadas", payload)
        self.assertIn("protegidas_manual", payload)
        self.assertIn("conflictos_auto", payload)


class ConcurrenciaExpedienteTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self._media = tempfile.TemporaryDirectory()
        self._settings = self.settings(MEDIA_ROOT=self._media.name)
        self._settings.enable()
        self.addCleanup(self._settings.disable)
        self.addCleanup(self._media.cleanup)
        self.user = get_user_model().objects.create_user("concurrente_cedula", password="x")
        adm = reportes_models.AreaPresupuesto.objects.create(
            nombre="Administración", codigo="administracion"
        )
        nom = reportes_models.AreaPresupuesto.objects.create(nombre="Nómina", codigo="nomina")
        for area in (adm, nom):
            reportes_models.RubroPresupuesto.objects.create(
                area=area,
                concepto="IMSS",
                tipo=reportes_models.RubroPresupuesto.TIPO_EGRESO,
            )
        Empleado.objects.create(
            codigo="CONC-001",
            nombre="Concurrente",
            nss="12121212121",
            departamento=Empleado.DEP_ADMINISTRACION,
        )

    def test_dos_conexiones_mismo_sha_retornan_un_expediente(self):
        self.assertEqual(connection.vendor, "postgresql")
        from reportes.services_cedula_expediente import aplicar_expediente, preparar_expediente

        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls",
            return_value=PersistenciaExpedienteTests._filas(),
        ):
            preview = preparar_expediente([PersistenciaExpedienteTests._sua()], usuario=self.user)

        barrera = threading.Barrier(2, timeout=10)
        resultados = []
        errores = []
        candado = threading.Lock()

        def ejecutar():
            close_old_connections()
            try:
                barrera.wait()
                pk = aplicar_expediente(preview, usuario=self.user).pk
                with candado:
                    resultados.append(pk)
            except Exception as exc:
                with candado:
                    errores.append(exc)
            finally:
                close_old_connections()

        hilos = [threading.Thread(target=ejecutar) for _ in range(2)]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join(timeout=15)

        self.assertFalse(any(hilo.is_alive() for hilo in hilos))
        self.assertEqual(errores, [])
        self.assertEqual(len(resultados), 2)
        self.assertEqual(len(set(resultados)), 1)
        self.assertEqual(reportes_models.ExpedienteCedulaIMSS.objects.count(), 1)
        self.assertEqual(reportes_models.DocumentoCedulaIMSS.objects.count(), 1)

    def test_dos_sha_distintos_se_serializan_en_revisiones_sin_error(self):
        from reportes.services_cedula_expediente import aplicar_expediente, preparar_expediente

        previews = []
        for sufijo in (b"revision-a", b"revision-b"):
            archivo = PersistenciaExpedienteTests._sua(
                contenido=PersistenciaExpedienteTests._sua().read() + sufijo
            )
            with patch(
                "reportes.services_cedula_expediente.cargar_filas_xls",
                return_value=PersistenciaExpedienteTests._filas(),
            ):
                previews.append(preparar_expediente([archivo], usuario=self.user))

        barrera = threading.Barrier(2, timeout=10)
        resultados = []
        errores = []
        candado = threading.Lock()

        def ejecutar(preview):
            close_old_connections()
            try:
                barrera.wait()
                expediente = aplicar_expediente(preview, usuario=self.user)
                with candado:
                    resultados.append(expediente.pk)
            except Exception as exc:
                with candado:
                    errores.append(exc)
            finally:
                close_old_connections()

        hilos = [threading.Thread(target=ejecutar, args=(preview,)) for preview in previews]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join(timeout=15)

        self.assertFalse(any(hilo.is_alive() for hilo in hilos))
        self.assertEqual(errores, [])
        self.assertEqual(len(set(resultados)), 2)
        expedientes = list(
            reportes_models.ExpedienteCedulaIMSS.objects.order_by("revision")
        )
        self.assertEqual([e.revision for e in expedientes], [1, 2])
        self.assertEqual(
            [e.estado for e in expedientes],
            [
                reportes_models.ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO,
                reportes_models.ExpedienteCedulaIMSS.ESTADO_APLICADO,
            ],
        )
