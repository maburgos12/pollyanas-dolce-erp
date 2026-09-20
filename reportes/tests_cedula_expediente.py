from datetime import date
from decimal import Decimal
from io import BytesIO, StringIO
from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest import skipUnless
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.core.exceptions import FieldDoesNotExist
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import IntegrityError, close_old_connections, connection, models, transaction
from django.db.models import Sum
from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase, TestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from core.models import AuditLog
from reportes import models as reportes_models
from rrhh.models import Empleado


class RegularizacionCedulasTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user("regulariza_cedula", password="x")
        cls.adm = reportes_models.AreaPresupuesto.objects.create(
            nombre="Administración", codigo="administracion"
        )
        cls.nom = reportes_models.AreaPresupuesto.objects.create(nombre="Nómina", codigo="nomina")
        for area in (cls.adm, cls.nom):
            reportes_models.RubroPresupuesto.objects.create(
                area=area,
                concepto="IMSS",
                tipo=reportes_models.RubroPresupuesto.TIPO_EGRESO,
            )
        Empleado.objects.create(
            codigo="REG-CED-001",
            nombre="Persona histórica",
            nss="12-12-12-1212-1",
            departamento=Empleado.DEP_ADMINISTRACION,
        )

    def setUp(self):
        self._root = tempfile.TemporaryDirectory()
        self.addCleanup(self._root.cleanup)
        self._media = tempfile.TemporaryDirectory()
        self.addCleanup(self._media.cleanup)
        self._settings = self.settings(MEDIA_ROOT=self._media.name)
        self._settings.enable()
        self.addCleanup(self._settings.disable)
        self.xls = Path(self._root.name) / "SUA_agosto.xls"
        self.xls.write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1historico")

    def _crear_lineas_historicas(self):
        lineas = []
        for area, monto in ((self.adm, "149.25"), (self.nom, "150.25")):
            lineas.append(
                reportes_models.LineaPresupuestoMensual.objects.create(
                    rubro=reportes_models.RubroPresupuesto.objects.get(area=area, concepto="IMSS"),
                    periodo=date(2026, 8, 1),
                    monto_real=Decimal(monto),
                    fuente_real="AUTO:LEGADO",
                    metadata={"source_file": "presupuesto-historico.xlsx"},
                )
            )
        return lineas

    def _ejecutar(self, *args, stdout=None):
        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls",
            return_value=PersistenciaExpedienteTests._filas(),
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss._cargar_filas_bytes",
            return_value=PersistenciaExpedienteTests._filas(),
        ):
            return call_command(
                "regularizar_expedientes_cedulas_imss",
                "--root",
                self._root.name,
                *args,
                stdout=stdout or StringIO(),
            )

    def test_dry_run_es_predeterminado_y_no_escribe(self):
        lineas = self._crear_lineas_historicas()
        salida = StringIO()

        self._ejecutar(stdout=salida)

        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertFalse(reportes_models.DocumentoCedulaIMSS.objects.exists())
        for linea in lineas:
            linea.refresh_from_db()
            self.assertNotIn("expediente_cedula_imss_id", linea.metadata)
        self.assertIn("archivo", salida.getvalue())
        self.assertIn("150.25", salida.getvalue())
        self.assertIn("DRY-RUN", salida.getvalue())

    def test_dry_run_no_usa_temporales_storage_ni_escrituras_sql(self):
        self._crear_lineas_historicas()
        with patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss._cargar_filas_bytes",
            return_value=PersistenciaExpedienteTests._filas(),
            create=True,
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss.preparar_expediente",
            side_effect=AssertionError("dry-run no debe preparar persistencia"),
        ), patch(
            "tempfile.NamedTemporaryFile",
            side_effect=AssertionError("dry-run no debe crear temporales"),
        ), patch(
            "django.db.models.fields.files.FieldFile.save",
            side_effect=AssertionError("dry-run no debe escribir storage"),
        ), CaptureQueriesContext(connection) as consultas:
            call_command(
                "regularizar_expedientes_cedulas_imss",
                "--root",
                self._root.name,
                stdout=StringIO(),
            )

        escrituras = [
            consulta["sql"]
            for consulta in consultas.captured_queries
            if re.match(r"^\s*(INSERT|UPDATE|DELETE)\b", consulta["sql"], re.IGNORECASE)
        ]
        self.assertEqual(escrituras, [])

    def test_apply_enlaza_sin_cambiar_montos_y_segunda_ejecucion_es_idempotente(self):
        lineas = self._crear_lineas_historicas()
        originales = {linea.pk: linea.monto_real for linea in lineas}

        self._ejecutar("--apply")

        expediente = reportes_models.ExpedienteCedulaIMSS.objects.get()
        for linea in lineas:
            linea.refresh_from_db()
            self.assertEqual(linea.monto_real, originales[linea.pk])
            self.assertEqual(linea.fuente_real, "AUTO:LEGADO")
            self.assertEqual(linea.metadata["expediente_cedula_imss_id"], expediente.pk)
        self.assertEqual(
            reportes_models.LineaPresupuestoMensual.objects.count(), len(lineas)
        )

        salida = StringIO()
        self._ejecutar("--apply", stdout=salida)

        self.assertEqual(reportes_models.ExpedienteCedulaIMSS.objects.count(), 1)
        self.assertIn("YA_ENLAZADO", salida.getvalue())

    def test_apply_no_borra_ni_altera_linea_preexistente_con_fuente_vacia(self):
        rubro_adm = reportes_models.RubroPresupuesto.objects.get(
            area=self.adm, concepto="IMSS"
        )
        linea_vacia = reportes_models.LineaPresupuestoMensual.objects.create(
            rubro=rubro_adm,
            periodo=date(2026, 8, 1),
            monto_presupuesto=Decimal("888.00"),
            monto_real=Decimal("77.00"),
            fuente_real="",
            metadata={"conservar": {"valor": True}},
        )
        rubro_nom = reportes_models.RubroPresupuesto.objects.get(
            area=self.nom, concepto="IMSS"
        )
        reportes_models.LineaPresupuestoMensual.objects.create(
            rubro=rubro_nom,
            periodo=date(2026, 8, 1),
            monto_real=Decimal("150.25"),
            fuente_real="AUTO:LEGADO",
        )
        creado_en = linea_vacia.creado_en
        actualizado_en = linea_vacia.actualizado_en

        self._ejecutar("--apply")

        linea_vacia.refresh_from_db()
        self.assertEqual(linea_vacia.monto_presupuesto, Decimal("888.00"))
        self.assertEqual(linea_vacia.monto_real, Decimal("77.00"))
        self.assertEqual(linea_vacia.fuente_real, "")
        self.assertEqual(linea_vacia.metadata, {"conservar": {"valor": True}})
        self.assertEqual(linea_vacia.creado_en, creado_en)
        self.assertEqual(linea_vacia.actualizado_en, actualizado_en)

    def test_total_discordante_aborta_sin_aplicacion_parcial(self):
        lineas = self._crear_lineas_historicas()
        control = next(linea for linea in lineas if linea.rubro.area_id == self.nom.pk)
        control.monto_real = Decimal("999.99")
        control.save(update_fields=["monto_real"])

        with self.assertRaisesRegex(CommandError, "no coincide"):
            self._ejecutar("--apply")

        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertFalse(reportes_models.DocumentoCedulaIMSS.objects.exists())

    def test_sha_historico_distinto_bloquea_antes_de_escribir(self):
        lineas = self._crear_lineas_historicas()
        linea = lineas[0]
        linea.metadata = {"cedula_imss_documento": {"sha256": "f" * 64}}
        linea.save(update_fields=["metadata"])

        with self.assertRaisesRegex(CommandError, "SHA-256 distinta"):
            self._ejecutar("--apply")

        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

    def test_bimestral_asocia_pdf_del_mismo_directorio_y_preserva_dos_meses(self):
        from reportlab.pdfgen import canvas

        self.xls.unlink()
        carpeta = Path(self._root.name) / "julio-agosto"
        carpeta.mkdir()
        (carpeta / "SUA_bimestral.xls").write_bytes(
            b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1bimestral"
        )
        salida_pdf = BytesIO()
        pdf = canvas.Canvas(salida_pdf, invariant=1)
        pdf.drawString(72, 720, "EBA EMISION BIMESTRAL ANTICIPADA")
        pdf.drawString(72, 700, "E52-40157-10-0")
        pdf.drawString(72, 680, "BIMESTRE 04-2026")
        pdf.showPage()
        pdf.save()
        (carpeta / "EBA.pdf").write_bytes(salida_pdf.getvalue())
        for area in (self.adm, self.nom):
            rubro = reportes_models.RubroPresupuesto.objects.create(
                area=area,
                concepto="Infonavit",
                tipo=reportes_models.RubroPresupuesto.TIPO_EGRESO,
            )
            for periodo, monto in ((date(2026, 7, 1), "50.01"), (date(2026, 8, 1), "50.00")):
                reportes_models.LineaPresupuestoMensual.objects.create(
                    rubro=rubro,
                    periodo=periodo,
                    monto_real=Decimal(monto),
                    fuente_real="AUTO:LEGADO",
                )
        filas = [
            ["Bimestre de Proceso: Agosto-2026"] + [""] * 13,
            ["Registro Patronal:", "E52-40157-10-0"] + [""] * 12,
            ["Clave", "Movimiento", "Fecha", "Dias", "SDI", "Retiro", "Patronal",
             "Obrera", "Suma", "Aportacion Patronal", "Amortizacion", "Credito Vivienda",
             "Tipo", "Total"],
            ["12-12-12-1212-1", "", "", "", "", "PERSONA HISTORICA"] + [""] * 8,
            ["", "NORMAL", "", 61, 350, 20, 30, 0, 50, Decimal("50.01"), 0, 0, "", 100.01],
            ["TOTAL", "", "", 61, "", 20, 30, 0, 50, Decimal("50.01"), 0, 0, "", 100.01],
        ]

        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls", return_value=filas
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss._cargar_filas_bytes",
            return_value=filas,
        ):
            call_command(
                "regularizar_expedientes_cedulas_imss",
                "--root",
                self._root.name,
                "--apply",
                stdout=StringIO(),
            )

        expediente = reportes_models.ExpedienteCedulaIMSS.objects.get()
        self.assertEqual(expediente.tipo, reportes_models.ExpedienteCedulaIMSS.TIPO_BIMESTRAL)
        self.assertEqual(expediente.documentos.count(), 2)
        self.assertEqual(reportes_models.LineaPresupuestoMensual.objects.count(), 4)
        self.assertEqual(
            sum(
                reportes_models.LineaPresupuestoMensual.objects.filter(
                    rubro__area=self.nom
                ).values_list("monto_real", flat=True),
                Decimal("0"),
            ),
            Decimal("100.01"),
        )

    def test_fallo_en_segundo_expediente_revierte_bd_y_blobs_del_primero(self):
        from reportes.services_cedula_expediente import aplicar_expediente as aplicar_real

        lineas = self._crear_lineas_historicas()
        self.xls.unlink()
        for indice in (1, 2):
            carpeta = Path(self._root.name) / f"expediente-{indice}"
            carpeta.mkdir()
            (carpeta / f"SUA_agosto_{indice}.xls").write_bytes(
                b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + bytes([indice])
            )
        llamadas = 0

        def aplicar_y_contar(preview, usuario):
            nonlocal llamadas
            llamadas += 1
            return aplicar_real(preview, usuario)

        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls",
            return_value=PersistenciaExpedienteTests._filas(),
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss._cargar_filas_bytes",
            return_value=PersistenciaExpedienteTests._filas(),
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss.aplicar_expediente",
            side_effect=aplicar_y_contar,
        ):
            with self.assertRaisesRegex(CommandError, "SHA-256 distinta"):
                call_command(
                    "regularizar_expedientes_cedulas_imss",
                    "--root",
                    self._root.name,
                    "--apply",
                    stdout=StringIO(),
                )

        self.assertEqual(llamadas, 1)
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertFalse(reportes_models.DocumentoCedulaIMSS.objects.exists())
        self.assertEqual([p for p in Path(self._media.name).rglob("*") if p.is_file()], [])
        for linea in lineas:
            linea.refresh_from_db()
            self.assertNotIn("expediente_cedula_imss_id", linea.metadata)

    def test_fallo_inmediato_tras_servicio_limpia_blob_nuevo_y_conserva_preexistente(self):
        from django.db.models.query import QuerySet
        from reportes.services_cedula_expediente import aplicar_expediente as aplicar_real

        self._crear_lineas_historicas()
        preexistente = Path(self._media.name) / "reportes/cedulas-imss/preexistente.xls"
        preexistente.parent.mkdir(parents=True)
        preexistente.write_bytes(b"no borrar")
        servicio_retorno = False
        get_real = QuerySet.get

        def aplicar_y_marcar(preview, usuario):
            nonlocal servicio_retorno
            resultado = aplicar_real(preview, usuario)
            servicio_retorno = True
            return resultado

        def fallar_primera_consulta_documento(queryset, *args, **kwargs):
            if servicio_retorno and queryset.model is reportes_models.DocumentoCedulaIMSS:
                raise RuntimeError("falla inmediata post-servicio")
            return get_real(queryset, *args, **kwargs)

        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls",
            return_value=PersistenciaExpedienteTests._filas(),
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss._cargar_filas_bytes",
            return_value=PersistenciaExpedienteTests._filas(),
        ), patch(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss.aplicar_expediente",
            side_effect=aplicar_y_marcar,
        ), patch.object(QuerySet, "get", new=fallar_primera_consulta_documento):
            with self.assertRaisesRegex(RuntimeError, "falla inmediata post-servicio"):
                call_command(
                    "regularizar_expedientes_cedulas_imss",
                    "--root",
                    self._root.name,
                    "--apply",
                    stdout=StringIO(),
                )

        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())
        self.assertFalse(reportes_models.DocumentoCedulaIMSS.objects.exists())
        self.assertEqual(
            [p.relative_to(self._media.name) for p in Path(self._media.name).rglob("*") if p.is_file()],
            [Path("reportes/cedulas-imss/preexistente.xls")],
        )

    def test_rechaza_root_inexistente_y_symlinks(self):
        with self.assertRaises(CommandError):
            call_command(
                "regularizar_expedientes_cedulas_imss",
                "--root",
                str(Path(self._root.name) / "inexistente"),
                stdout=StringIO(),
            )
        enlace = Path(self._root.name) / "enlace.xls"
        enlace.symlink_to(self.xls)
        with self.assertRaisesRegex(CommandError, "simbólico"):
            self._ejecutar()

    def test_lectura_segura_rechaza_swap_a_symlink_y_archivo_mayor_a_10_mib(self):
        from reportes.management.commands.regularizar_expedientes_cedulas_imss import (
            _leer_archivo_seguro,
        )

        real_open = os.open
        destino = Path(self._root.name) / "destino.xls"
        destino.write_bytes(b"xls")
        original = self.xls

        def intercambiar(path, flags):
            original.unlink()
            original.symlink_to(destino)
            return real_open(path, flags)

        with patch("os.open", side_effect=intercambiar):
            with self.assertRaises(CommandError):
                _leer_archivo_seguro(original, Path(self._root.name))

        grande = Path(self._root.name) / "grande.xls"
        with grande.open("wb") as archivo:
            archivo.truncate(10 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(CommandError, "10 MiB"):
            _leer_archivo_seguro(grande, Path(self._root.name))

    def test_pdf_de_tipo_periodo_incorrecto_no_se_asocia_por_directorio(self):
        from reportlab.pdfgen import canvas

        salida = BytesIO()
        pdf = canvas.Canvas(salida, invariant=1)
        pdf.drawString(72, 720, "EBA EMISION BIMESTRAL ANTICIPADA")
        pdf.drawString(72, 700, "E52-40157-10-0")
        pdf.drawString(72, 680, "BIMESTRE 04-2026")
        pdf.showPage()
        pdf.save()
        (Path(self._root.name) / "EBA.pdf").write_bytes(salida.getvalue())

        with self.assertRaisesRegex(CommandError, "tipo, registro y periodo"):
            self._ejecutar()

    def test_limpieza_continua_y_preserva_error_primario(self):
        from reportes.management.commands.regularizar_expedientes_cedulas_imss import (
            _limpiar_blobs,
        )

        storage = MagicMock()
        storage.delete.side_effect = [OSError("storage caído"), None]
        with self.assertLogs(
            "reportes.management.commands.regularizar_expedientes_cedulas_imss",
            level="WARNING",
        ):
            _limpiar_blobs([(storage, "uno"), (storage, "dos")])
        self.assertEqual(storage.delete.call_count, 2)

    def test_preflight_aplica_limite_agregado_de_30_mib(self):
        from reportes.management.commands.regularizar_expedientes_cedulas_imss import (
            ArchivoInspeccionado,
            _agrupar_inspecciones,
        )

        periodo = date(2026, 8, 1)
        sua = ArchivoInspeccionado(
            self.xls, "a" * 64, 10 * 1024 * 1024, "SUA_XLS",
            "E52-40157-10-0", periodo, SimpleNamespace(tipo="MENSUAL"), Decimal("1"),
        )
        pdfs = tuple(
            ArchivoInspeccionado(
                Path(self._root.name) / f"EMA-{indice}.pdf",
                str(indice) * 64,
                10 * 1024 * 1024,
                "EMA_PDF",
                "E52-40157-10-0",
                periodo,
            )
            for indice in range(1, 4)
        )
        with self.assertRaisesRegex(CommandError, "30 MiB"):
            _agrupar_inspecciones((sua, *pdfs))

    def test_apply_toma_advisory_antes_de_bloquear_lineas(self):
        from reportes.management.commands import regularizar_expedientes_cedulas_imss as comando

        self._crear_lineas_historicas()
        orden = []
        advisory_real = comando._bloquear_familia
        lineas_real = comando._lineas_objetivo

        def advisory(preview):
            orden.append("advisory")
            return advisory_real(preview)

        def lineas(parseada, *, bloquear=False):
            if bloquear:
                orden.append("filas")
            return lineas_real(parseada, bloquear=bloquear)

        with patch.object(comando, "_bloquear_familia", side_effect=advisory), patch.object(
            comando, "_lineas_objetivo", side_effect=lineas
        ):
            self._ejecutar("--apply")

        self.assertLess(orden.index("advisory"), orden.index("filas"))


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

        etiqueta = (
            "EBA EMISION BIMESTRAL ANTICIPADA E52-40157-10-0 BIMESTRE 04-2026"
            if "EBA" in nombre.upper()
            else "EMA EMISION MENSUAL ANTICIPADA E52-40157-10-0 PERIODO 08-2026"
        )
        salida = BytesIO()
        pdf = canvas.Canvas(salida, invariant=1)
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

    def test_dependencia_pdf_ausente_no_se_reporta_como_archivo_invalido(self):
        importar_real = __import__

        def importar_sin_pdfplumber(nombre, *args, **kwargs):
            if nombre == "pdfplumber":
                raise ModuleNotFoundError("pdfplumber ausente")
            return importar_real(nombre, *args, **kwargs)

        with patch("builtins.__import__", side_effect=importar_sin_pdfplumber):
            with self.assertRaisesRegex(RuntimeError, "Dependencia pdfplumber"):
                self._preview(archivos=[self._sua(), self._pdf()])

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


class PantallaExpedienteTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_superuser(
            "direccion_cedulas", "direccion@example.com", "x"
        )
        cls.sin_permiso = get_user_model().objects.create_user("consulta_cedulas", password="x")
        cls.otro_admin = get_user_model().objects.create_superuser(
            "otra_direccion_cedulas", "otra-direccion@example.com", "x"
        )
        cls.adm = reportes_models.AreaPresupuesto.objects.create(
            nombre="Administración", codigo="administracion"
        )
        cls.nom = reportes_models.AreaPresupuesto.objects.create(nombre="Nómina", codigo="nomina")
        for area in (cls.adm, cls.nom):
            reportes_models.RubroPresupuesto.objects.create(
                area=area,
                concepto="IMSS",
                tipo=reportes_models.RubroPresupuesto.TIPO_EGRESO,
            )
        Empleado.objects.create(
            codigo="CED-UI-001",
            nombre="Persona cédula UI",
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
    def _archivos():
        return {
            "documentos": [
                PersistenciaExpedienteTests._sua(),
                PersistenciaExpedienteTests._pdf(),
            ],
        }

    def _post(self, accion, *, json=False, token="", archivos=None):
        headers = {
            "HTTP_ACCEPT": "application/json",
            "HTTP_X_REQUESTED_WITH": "XMLHttpRequest",
        } if json else {}
        with patch(
            "reportes.services_cedula_expediente.cargar_filas_xls",
            return_value=PersistenciaExpedienteTests._filas(),
        ):
            return self.client.post(
                reverse("reportes:cedula_imss_importar"),
                {
                    **(archivos or self._archivos()),
                    accion: "1",
                    **({"preview_token": token} if token else {}),
                },
                **headers,
            )

    def _preview_token(self, *, json=True):
        response = self._post("previsualizar", json=json)
        return response.json()["preview_token"] if json else response.context["preview_token"]

    def test_preview_no_guarda_y_aplicar_redirige_al_expediente(self):
        self.client.force_login(self.user)

        preview = self._post("previsualizar")

        self.assertContains(preview, "Total conciliado")
        self.assertContains(preview, "E52-40157-10-0")
        self.assertContains(preview, "agosto de 2026")
        self.assertContains(preview, "Hasta 2 líneas presupuestales")
        self.assertContains(preview, "•••• 2121")
        self.assertNotContains(preview, "12121212121")
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

        self.assertContains(preview, 'name="preview_token"')
        self.assertContains(preview, "Vuelve a seleccionar exactamente los mismos documentos")
        self.assertNotContains(preview, 'id="cedula-aplicar" type="submit" name="aplicar" value="1" data-pending-label="Aplicando…" disabled')
        aplicado = self._post("aplicar", token=preview.context["preview_token"])
        expediente = reportes_models.ExpedienteCedulaIMSS.objects.get()
        self.assertRedirects(
            aplicado,
            reverse("reportes:cedula_imss_detalle", args=[expediente.pk]),
        )

    def test_preview_json_conserva_archivos_en_el_formulario_y_habilita_aplicar(self):
        self.client.force_login(self.user)
        pantalla = self.client.get(reverse("reportes:cedula_imss_importar"))
        self.assertContains(pantalla, 'name="documentos"')
        self.assertContains(pantalla, "multiple")
        self.assertContains(pantalla, "data-async-action")
        self.assertContains(pantalla, 'name="aplicar"')
        self.assertContains(pantalla, "disabled")

        response = self._post("previsualizar", json=True)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["preview"]["total_patronal"], "150.25")
        self.assertEqual(payload["preview"]["registro_patronal"], "E52-40157-10-0")
        self.assertEqual(
            payload["preview"]["meses"],
            [{"iso": "2026-08", "label": "agosto de 2026"}],
        )
        self.assertEqual(payload["preview"]["efecto_estimado"]["lineas_maximas"], 2)
        self.assertFalse(payload["preview"]["documentos"][0]["duplicado"])
        self.assertTrue(payload["preview_token"])
        self.assertEqual(payload["preview"]["detalles"][0]["nss"], "•••• 2121")
        self.assertNotContains(response, "12121212121")
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

        self._post("aplicar", token=payload["preview_token"])
        repetida = self._post("previsualizar", json=True).json()["preview"]
        self.assertTrue(repetida["documentos"][0]["duplicado"])
        self.assertEqual(
            repetida["documentos"][0]["expediente_id"],
            reportes_models.ExpedienteCedulaIMSS.objects.get().pk,
        )

    def test_aplicar_json_con_token_valido_devuelve_toast_y_redireccion(self):
        self.client.force_login(self.user)
        token = self._preview_token()

        response = self._post("aplicar", json=True, token=token)

        expediente = reportes_models.ExpedienteCedulaIMSS.objects.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["redirect"],
            reverse("reportes:cedula_imss_detalle", args=[expediente.pk]),
        )
        self.assertEqual(response.json()["toast"]["type"], "success")

    def test_aplicar_directo_sin_preview_es_rechazado(self):
        self.client.force_login(self.user)

        response = self._post("aplicar", json=True)

        self.assertEqual(response.status_code, 400)
        self.assertIn("previsualiza", response.json()["toast"]["message"].lower())
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

    def test_token_de_otro_usuario_o_archivos_distintos_es_rechazado(self):
        self.client.force_login(self.user)
        token = self._preview_token()

        self.client.force_login(self.otro_admin)
        otro_usuario = self._post("aplicar", json=True, token=token)
        self.assertEqual(otro_usuario.status_code, 400)

        self.client.force_login(self.user)
        sua_distinta = PersistenciaExpedienteTests._sua(
            contenido=PersistenciaExpedienteTests._sua().read() + b"correccion"
        )
        otros_archivos = {
            "documentos": [sua_distinta, PersistenciaExpedienteTests._pdf()]
        }
        otros_documentos = self._post(
            "aplicar", json=True, token=token, archivos=otros_archivos
        )
        self.assertEqual(otros_documentos.status_code, 400)
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

    def test_token_expirado_es_rechazado(self):
        self.client.force_login(self.user)
        token = self._preview_token()

        with patch("django.core.signing.time.time", return_value=time.time() + 3600):
            response = self._post("aplicar", json=True, token=token)

        self.assertEqual(response.status_code, 400)
        self.assertIn("expiró", response.json()["toast"]["message"])
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

    def test_error_de_nss_duplicado_tambien_se_enmascara(self):
        Empleado.objects.create(
            codigo="CED-UI-DUP",
            nombre="Persona duplicada",
            nss="12121212121",
            departamento=Empleado.DEP_ADMINISTRACION,
        )
        self.client.force_login(self.user)
        token = self._preview_token()

        response = self._post("aplicar", json=True, token=token)

        self.assertEqual(response.status_code, 400)
        self.assertNotContains(response, "12121212121", status_code=400)
        self.assertIn("•••• 2121", response.json()["toast"]["message"])
        self.assertFalse(reportes_models.ExpedienteCedulaIMSS.objects.exists())

    @override_settings(DEBUG=False)
    def test_descarga_requiere_permiso_y_detalle_enmascara_nss(self):
        self.client.force_login(self.user)
        self._post("aplicar", token=self._preview_token())
        expediente = reportes_models.ExpedienteCedulaIMSS.objects.get()
        documento = expediente.documentos.get(
            clase=reportes_models.DocumentoCedulaIMSS.CLASE_SUA_XLS
        )

        self.client.logout()
        self.assertEqual(self.client.get(documento.archivo.url).status_code, 404)
        ruta = documento.archivo.url.removeprefix("/media/")
        aliases = (
            f"/media/./{ruta}",
            f"/media/segmento/../{ruta}",
            f"/media/%2E/{ruta}",
            f"/media/segmento/%2E%2E/{ruta}",
            documento.archivo.url.replace("reportes/", "reportes%2F", 1),
        )
        for alias in aliases:
            with self.subTest(alias=alias):
                self.assertEqual(self.client.get(alias).status_code, 404)
        self.client.force_login(self.sin_permiso)
        self.assertEqual(self.client.get(documento.archivo.url).status_code, 404)
        self.assertEqual(
            self.client.get(
                reverse("reportes:cedula_imss_detalle", args=[expediente.pk])
            ).status_code,
            403,
        )

        self.client.force_login(self.user)
        detalle = self.client.get(reverse("reportes:cedula_imss_detalle", args=[expediente.pk]))
        self.assertContains(detalle, f"Expediente #{expediente.pk}")
        self.assertContains(detalle, "•••• 2121")
        self.assertNotContains(detalle, "12121212121")
        descarga = self.client.get(documento.archivo.url)
        self.assertEqual(descarga.status_code, 200)
        self.assertEqual(descarga["Cache-Control"], "private, no-store")

    @override_settings(DEBUG=False)
    def test_todos_los_prefijos_privados_pasan_por_el_gate_unico(self):
        rutas = (
            "fallas/evidencias/prueba.txt",
            "fallas/seguimiento/prueba.txt",
            "activos/facturas/prueba.txt",
            "activos/evidencias/prueba.txt",
            "logistica/reportes/prueba.txt",
            "servicios_unidad/prueba.txt",
            "reparaciones_unidad/prueba.txt",
            "compras/departamentales/prueba.txt",
            "compras/cotizaciones/prueba.txt",
            "reportes/cedulas-imss/prueba.txt",
        )
        for ruta in rutas:
            archivo = Path(self._media.name) / ruta
            archivo.parent.mkdir(parents=True, exist_ok=True)
            archivo.write_text("privado")
            for url in (f"/media/{ruta}", f"/media/publico/../{ruta}"):
                with self.subTest(url=url):
                    self.assertEqual(self.client.get(url).status_code, 404)


@skipUnless(shutil.which("node"), "Node.js es requerido para validar la carrera de preview")
class CedulaPreviewJavaScriptTests(SimpleTestCase):
    def test_respuesta_obsoleta_no_habilita_aplicar_para_otro_filelist(self):
        plantilla = (
            Path(__file__).parent
            / "templates"
            / "reportes"
            / "cedula_imss_importar.html"
        )
        fuente = plantilla.read_text()
        script = re.search(r"<script>(.*?)</script>", fuente, flags=re.DOTALL).group(1)
        escenario = r"""
const handlers={};
function element(name){return {name,disabled:false,hidden:true,textContent:'',dataset:{},files:[],addEventListener:(type,fn)=>{handlers[name+':'+type]=fn},querySelector:()=>element('child'),replaceChildren:()=>{},appendChild:()=>{},setAttribute:()=>{},scrollIntoView:()=>{}}}
const form=element('form'),input=element('input'),apply=element('apply'),preview=element('preview'),region=element('region'),token=element('token');
form.reportValidity=()=>true;form.action='/preview';
global.document={getElementById:(id)=>({"cedula-form":form,"id_documentos":input,"cedula-aplicar":apply,"cedula-preview":preview,"cedula-preview-token":token,"erp-toast-region":region}[id]),createElement:()=>element('created')};
global.window={location:{href:'http://test/preview'},matchMedia:()=>({matches:true}),setTimeout:()=>{}};
global.FormData=class{set(){} delete(){}};
let resolveFetch;global.fetch=()=>new Promise((resolve)=>{resolveFetch=resolve});
""" + script + r"""
(async()=>{
  input.files=[{name:'A.xls',size:10,lastModified:1,type:'application/vnd.ms-excel'}];
  const pending=handlers['form:submit']({submitter:{name:'previsualizar',textContent:'Previsualizar',dataset:{}},preventDefault(){},stopImmediatePropagation(){}});
  input.files=[{name:'B.xls',size:20,lastModified:2,type:'application/vnd.ms-excel'}];
  handlers['input:change']();
  resolveFetch({ok:true,json:async()=>({ok:true,preview_token:'obsoleto',preview:{total_patronal:'1',trabajadores:1,cruzados:1,sin_cruce:0,documentos:[],detalles:[]},toast:{type:'info',message:'lista'}})});
  await pending;
  if(!apply.disabled||preview.hidden!==true||token.value){throw new Error('La preview obsoleta habilitó Aplicar para otro FileList');}
  const current=handlers['form:submit']({submitter:{name:'previsualizar',textContent:'Previsualizar',dataset:{}},preventDefault(){},stopImmediatePropagation(){}});
  resolveFetch({ok:true,json:async()=>({ok:true,preview_token:'vigente',preview:{registro_patronal:'E52',meses:[],efecto_estimado:{lineas_maximas:2},total_patronal:'1',trabajadores:1,cruzados:1,sin_cruce:0,documentos:[],detalles:[]},toast:{type:'info',message:'lista'}})});
  await current;
  if(apply.disabled||token.value!=='vigente'){throw new Error('La preview vigente no habilitó Aplicar con su comprobante');}
  input.files=[{name:'C.xls',size:30,lastModified:3,type:'application/vnd.ms-excel'}];
  let prevented=false;
  await handlers['form:submit']({submitter:{name:'aplicar'},preventDefault(){prevented=true},stopImmediatePropagation(){}});
  if(!prevented||!apply.disabled){throw new Error('Aplicar no quedó ligado al FileList previsualizado');}
})().catch((error)=>{console.error(error);process.exit(1)});
"""
        resultado = subprocess.run(
            [shutil.which("node")],
            input=escenario,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(resultado.returncode, 0, resultado.stderr)


class RutasMediaPrivadaTests(SimpleTestCase):
    def test_normaliza_aliases_antes_de_clasificar_ruta_privada(self):
        from core.private_operational_media import (
            _is_private_operational_media_path,
            _normalize_operational_media_path,
        )

        aliases = (
            "./reportes/cedulas-imss/prueba.pdf",
            "publico/../reportes/cedulas-imss/prueba.pdf",
            "%2E/reportes/cedulas-imss/prueba.pdf",
            "publico/%2E%2E/reportes/cedulas-imss/prueba.pdf",
            "reportes%2Fcedulas-imss%2Fprueba.pdf",
            "reportes%252Fcedulas-imss%252Fprueba.pdf",
        )
        for alias in aliases:
            with self.subTest(alias=alias):
                canonical = _normalize_operational_media_path(alias)
                self.assertEqual(canonical, "reportes/cedulas-imss/prueba.pdf")
                self.assertTrue(_is_private_operational_media_path(canonical))


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
