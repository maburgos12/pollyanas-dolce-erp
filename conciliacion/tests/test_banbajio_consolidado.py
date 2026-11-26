from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from io import StringIO
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from conciliacion.models import ImportacionBancaria
from conciliacion.services.reglas_contables import _descripcion_contiene_cuenta_propia
from conciliacion.services.importador import (
    ImportacionBancariaError,
    MovimientoNormalizado,
    PreviewImportacion,
    _read_pdf_dataframe,
    _pdf_rows_from_banbajio_consolidado,
    confirmar_importacion,
    generar_preview,
    resumen_periodo_conciliacion,
)
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class _FakePdfPage:
    def __init__(self, lines):
        self._lines = lines

    def extract_text_lines(self):
        return self._lines

    def extract_text(self, **_kwargs):
        return "\n".join(line["text"] for line in self._lines)


class _FakePdf:
    def __init__(self, pages):
        self.pages = [_FakePdfPage(lines) for lines in pages]

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _pdf_line(text: str, *, amount_x: list[float] | None = None):
    amount_x = list(amount_x or [])
    chars = []
    dollar_index = 0
    for index, char in enumerate(text):
        x0 = float(index)
        if char == "$" and dollar_index < len(amount_x):
            x0 = amount_x[dollar_index]
            dollar_index += 1
        chars.append({"text": char, "x0": x0})
    return {"text": text, "chars": chars}


class CuentasBanbajioConsolidadasTests(TestCase):
    def test_banbajio_admite_cuenta_principal_y_apartados(self):
        principal = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Cuenta principal",
            id_site_syncfy="site-bajio",
            numero_cuenta="0410641890201",
            tipo_cuenta=CuentaBancaria.TIPO_PRINCIPAL,
        )

        gasto = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Gasto operativo",
            id_site_syncfy="",
            origen=CuentaBancaria.ORIGEN_MANUAL,
            numero_cuenta="0410641890205",
            tipo_cuenta=CuentaBancaria.TIPO_APARTADO,
            cuenta_principal=principal,
        )

        self.assertEqual(CuentaBancaria.objects.filter(banco=CuentaBancaria.BANCO_BANBAJIO).count(), 2)
        self.assertEqual(gasto.cuenta_principal, principal)
        self.assertEqual(gasto.tipo_cuenta, CuentaBancaria.TIPO_APARTADO)
        self.assertTrue(
            _descripcion_contiene_cuenta_propia(
                "traspaso a la cuenta conecta banbajio 410641890205"
            )
        )


class ParserPdfBanbajioConsolidadoTests(TestCase):
    def _pdf_con_traspaso(self):
        return _FakePdf(
            [
                [
                    _pdf_line("PERIODO: 1 DE JUNIO AL 30 DE JUNIO DE 2026"),
                    _pdf_line("DETALLE DE LA CUENTA: CUENTA CONECTA BANBAJIO #0410641890201"),
                    _pdf_line(
                        "1 JUN 7415750 TRASPASO DE RECURSOS A LA CUENTA CONECTA BANBAJIO# 410641890205 $ 61,027.77 $ 110,987.54",
                        amount_x=[486.0, 560.0],
                    ),
                ],
                [
                    _pdf_line("DETALLE DE LA CUENTA: CUENTA CONECTA BANBAJIO #0410641890205"),
                    _pdf_line(
                        "1 JUN 7415760 TRASPASO DE RECURSOS DE LA CUENTA CONECTA BANBAJIO# 410641890201 $ 61,027.77 $ 525,162.94",
                        amount_x=[416.0, 560.0],
                    ),
                ],
            ]
        )

    def test_separa_movimientos_por_cuenta_y_columna_deposito_retiro(self):
        pdf = self._pdf_con_traspaso()

        rows = _pdf_rows_from_banbajio_consolidado(pdf)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows.iloc[0]["cuenta_numero"], "0410641890201")
        self.assertEqual(rows.iloc[0]["cargo"], "61027.77")
        self.assertEqual(rows.iloc[0]["fecha"], "2026-06-01")
        self.assertEqual(rows.iloc[1]["cuenta_numero"], "0410641890205")
        self.assertEqual(rows.iloc[1]["abono"], "61027.77")

    def test_lector_pdf_entrega_filas_y_no_nombres_de_columnas(self):
        with patch("pdfplumber.open", return_value=self._pdf_con_traspaso()):
            rows = _read_pdf_dataframe(b"%PDF-sintetico")

        self.assertEqual(len(rows), 2)
        self.assertEqual(list(rows["cuenta_numero"]), ["0410641890201", "0410641890205"])


class ConfirmacionBanbajioConsolidadoTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_bajio_consolidado",
            email="admin-bajio@example.com",
            password="x",
        )
        self.principal = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Cuenta principal",
            id_site_syncfy="site-bajio",
            numero_cuenta="0410641890201",
            tipo_cuenta=CuentaBancaria.TIPO_PRINCIPAL,
        )
        self.gasto = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Gasto operativo",
            id_site_syncfy="",
            origen=CuentaBancaria.ORIGEN_MANUAL,
            numero_cuenta="0410641890205",
            tipo_cuenta=CuentaBancaria.TIPO_APARTADO,
            cuenta_principal=self.principal,
        )

    def test_confirma_en_cuenta_detectada_y_empareja_traspaso_interno(self):
        fecha = timezone.make_aware(datetime(2026, 6, 1, 12, 0))
        preview = PreviewImportacion(
            cuenta_id=self.principal.pk,
            archivo_nombre="estado_banbajio_junio.pdf",
            archivo_hash="a" * 64,
            fuente=ImportacionBancaria.FUENTE_MANUAL_PDF,
            movimientos=[
                MovimientoNormalizado(
                    fecha=fecha,
                    descripcion="TRASPASO DE RECURSOS A LA CUENTA CONECTA BANBAJIO 410641890205",
                    monto=Decimal("61027.77"),
                    tipo=MovimientoBancario.TIPO_CARGO,
                    moneda="MXN",
                    referencia="7415750",
                    saldo=Decimal("110987.54"),
                    fila=1,
                    raw={},
                    cuenta_numero="0410641890201",
                ),
                MovimientoNormalizado(
                    fecha=fecha,
                    descripcion="TRASPASO DE RECURSOS DE LA CUENTA CONECTA BANBAJIO 410641890201",
                    monto=Decimal("61027.77"),
                    tipo=MovimientoBancario.TIPO_ABONO,
                    moneda="MXN",
                    referencia="7415760",
                    saldo=Decimal("525162.94"),
                    fila=2,
                    raw={},
                    cuenta_numero="0410641890205",
                ),
            ],
            errores=[],
        )

        confirmar_importacion(preview=preview, user=self.user)

        salida = MovimientoBancario.objects.get(cuenta=self.principal)
        entrada = MovimientoBancario.objects.get(cuenta=self.gasto)
        self.assertEqual(salida.tipo_conciliacion, MovimientoBancario.CONCILIACION_TRASPASO)
        self.assertEqual(entrada.tipo_conciliacion, MovimientoBancario.CONCILIACION_TRASPASO)
        self.assertEqual(salida.movimiento_relacionado, entrada)
        self.assertEqual(entrada.movimiento_relacionado, salida)
        self.assertTrue(salida.conciliado)
        self.assertTrue(entrada.conciliado)

        resumen = resumen_periodo_conciliacion(year=2026, month=6)
        self.assertEqual(resumen["movimientos_total"], 2)
        self.assertEqual(resumen["movimientos_cargos"]["total"], Decimal("0"))
        self.assertEqual(resumen["movimientos_abonos"]["total"], Decimal("0"))
        self.assertEqual(resumen["traspasos_internos"]["conteo"], 2)

    def test_rechaza_cuenta_desconocida_sin_importacion_parcial(self):
        preview = PreviewImportacion(
            cuenta_id=self.principal.pk,
            archivo_nombre="estado_banbajio_desconocido.pdf",
            archivo_hash="b" * 64,
            fuente=ImportacionBancaria.FUENTE_MANUAL_PDF,
            movimientos=[
                MovimientoNormalizado(
                    fecha=timezone.make_aware(datetime(2026, 6, 2, 12, 0)),
                    descripcion="MOVIMIENTO EN CUENTA NO CONFIGURADA",
                    monto=Decimal("100.00"),
                    tipo=MovimientoBancario.TIPO_CARGO,
                    moneda="MXN",
                    referencia="desconocida",
                    saldo=None,
                    fila=1,
                    raw={},
                    cuenta_numero="0410641890999",
                )
            ],
            errores=[],
        )

        with self.assertRaisesMessage(ImportacionBancariaError, "no esta registrada"):
            confirmar_importacion(preview=preview, user=self.user)

        self.assertFalse(ImportacionBancaria.objects.exists())
        self.assertFalse(MovimientoBancario.objects.exists())

    def test_pantalla_explica_pdf_consolidado_y_xml_fiscal(self):
        self.client.force_login(self.user)

        response = self.client.get("/conciliacion/bancaria/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "PDF consolidado")
        self.assertContains(response, "XML fiscales")
        self.assertContains(response, "Gasto operativo")
        self.assertContains(response, "Traspasos internos")


class XmlFiscalBanbajioTests(TestCase):
    def test_cfdi_banbajio_no_se_importa_como_movimiento_bancario(self):
        cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Cuenta principal",
            id_site_syncfy="site-bajio",
            numero_cuenta="0410641890201",
            tipo_cuenta=CuentaBancaria.TIPO_PRINCIPAL,
        )
        archivo = SimpleUploadedFile(
            "cfdi_comisiones_banbajio.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Version="4.0" '
                'Fecha="2026-07-01T20:01:58" Moneda="MXN" TipoDeComprobante="I">'
                '<cfdi:Emisor Rfc="BBA940707IE1" Nombre="BANCO DEL BAJIO" />'
                '<cfdi:Receptor Rfc="GEF211230KR2" Nombre="GRUPO EMPRESARIAL FONSMA" />'
                "<cfdi:Conceptos>"
                '<cfdi:Concepto Descripcion="COMISION BANCARIA, de la cuenta 0410641890201" '
                'Importe="100.00" NoIdentificacion="20260601" />'
                "</cfdi:Conceptos>"
                "<cfdi:Addenda>"
                '<EstadoDeCuentaBajio numeroCuenta="0410641890201" '
                'periodo="01 de Junio de 2026 al 30 de Junio de 2026" />'
                "</cfdi:Addenda>"
                "</cfdi:Comprobante>"
            ).encode("utf-8"),
            content_type="application/xml",
        )

        with self.assertRaisesMessage(
            ImportacionBancariaError,
            "es evidencia fiscal",
        ):
            generar_preview(cuenta=cuenta, uploaded_file=archivo)


class SimulacionHistoricaTraspasosTests(TestCase):
    def test_comando_es_solo_lectura_y_reporta_candidatos(self):
        principal = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Cuenta principal",
            id_site_syncfy="site-bajio",
            numero_cuenta="0410641890201",
            tipo_cuenta=CuentaBancaria.TIPO_PRINCIPAL,
        )
        CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="Gasto operativo",
            id_site_syncfy="",
            origen=CuentaBancaria.ORIGEN_MANUAL,
            numero_cuenta="0410641890205",
            tipo_cuenta=CuentaBancaria.TIPO_APARTADO,
            cuenta_principal=principal,
        )
        movimiento = MovimientoBancario.objects.create(
            id_transaction="historico-traspaso-205",
            cuenta=principal,
            descripcion="TRASPASO DE RECURSOS A LA CUENTA CONECTA BANBAJIO 410641890205",
            monto=Decimal("1000.00"),
            tipo=MovimientoBancario.TIPO_CARGO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 6, 10, 12, 0)),
            fecha_refresh=timezone.now(),
        )
        stdout = StringIO()

        call_command("simular_traspasos_propios", banco="banbajio", stdout=stdout)

        resultado = json.loads(stdout.getvalue())
        movimiento.refresh_from_db()
        self.assertEqual(resultado["movimientos_candidatos"], 1)
        self.assertEqual(resultado["pares_detectados"], 0)
        self.assertEqual(resultado["sin_contraparte"], 1)
        self.assertEqual(movimiento.tipo_conciliacion, "")
        self.assertFalse(movimiento.conciliado)
