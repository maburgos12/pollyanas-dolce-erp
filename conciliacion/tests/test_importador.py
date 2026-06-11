from __future__ import annotations

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from conciliacion.models import ImportacionBancaria
from conciliacion.services.importador import ImportacionBancariaError, confirmar_importacion, generar_preview
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class ImportadorBancarioTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_conciliacion",
            email="admin@example.com",
            password="x",
        )
        self.cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="BanBajio Principal",
            id_site_syncfy="site-1",
            numero_cuenta="410641890201",
        )

    def test_generar_preview_normalizes_cargo_and_abono_csv(self):
        archivo = SimpleUploadedFile(
            "banbajio.csv",
            (
                "Fecha,Concepto,Cargo,Abono,Saldo,Referencia\n"
                "09/06/2026,SPEI RECIBIDO,,1500.50,2000.00,ABC123\n"
                "10/06/2026,PAGO PROVEEDOR,300.00,,1700.00,DEF456\n"
            ).encode("utf-8"),
            content_type="text/csv",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(len(preview.movimientos), 2)
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("1500.50"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("300.00"))
        self.assertEqual(preview.errores, [])

    def test_generar_preview_normalizes_banbajio_detallado_csv_without_header(self):
        archivo = SimpleUploadedFile(
            "ExcelDetallado_41064189_20260610200219.csv",
            (
                '         ,          ,                          ,Saldo Inicial      ,     ,     ,                  ,                  ,"0.00",\n'
                '41064189,30/05/2026,030741900036812905,2643314013215,IVA Comision informativa SPEI,2121,0.0,0,"49,365.12",300965487\n'
                '41064189,31/05/2026,030741900036812905,                               ,IVA Comision por Emision de Chequera,2182,2.08,0,"84,041.06",54875114\n'
                '41064189,31/05/2026,030741900036812905,9425978001,Deposito Negocios Afiliados por 830.00 mxn,2180,0,830.00,"84,871.06",4066700006001\n'
            ).encode("latin-1"),
            content_type="text/csv",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(len(preview.movimientos), 2)
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("2.08"))
        self.assertEqual(preview.movimientos[0].saldo, Decimal("84041.06"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("830.00"))
        self.assertEqual(preview.movimientos[1].referencia, "9425978001")
        self.assertEqual(preview.errores, [])

    def test_generar_preview_normalizes_banbajio_xml(self):
        archivo = SimpleUploadedFile(
            "estado_banbajio.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<EstadoCuenta>"
                "  <Movimientos>"
                "    <Movimiento>"
                "      <fechaOperacion>09/06/2026</fechaOperacion>"
                "      <concepto>SPEI RECIBIDO</concepto>"
                "      <cargoAbono>ABONO</cargoAbono>"
                "      <importeMovimiento>1500.50</importeMovimiento>"
                "      <saldo>2000.00</saldo>"
                "      <referencia>ABC123</referencia>"
                "    </Movimiento>"
                "    <Movimiento>"
                "      <fechaOperacion>10/06/2026</fechaOperacion>"
                "      <concepto>PAGO PROVEEDOR</concepto>"
                "      <cargoAbono>CARGO</cargoAbono>"
                "      <importeMovimiento>300.00</importeMovimiento>"
                "      <saldo>1700.00</saldo>"
                "      <referencia>DEF456</referencia>"
                "    </Movimiento>"
                "  </Movimientos>"
                "</EstadoCuenta>"
            ).encode("utf-8"),
            content_type="application/xml",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(len(preview.movimientos), 2)
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("1500.50"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("300.00"))
        self.assertEqual(preview.errores, [])

    def test_generar_preview_reads_text_pdf_statement(self):
        archivo = SimpleUploadedFile(
            "bbva_estado.pdf",
            _minimal_text_pdf(
                [
                    "Estado de cuenta BBVA",
                    "09/06/2026 SPEI RECIBIDO REF ABC123 1,500.50 2,000.00",
                    "10/06/2026 PAGO PROVEEDOR REF DEF456 300.00 1,700.00",
                ]
            ),
            content_type="application/pdf",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(preview.fuente, ImportacionBancaria.FUENTE_MANUAL_PDF)
        self.assertEqual(len(preview.movimientos), 2)
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("1500.50"))
        self.assertEqual(preview.movimientos[0].saldo, Decimal("2000.00"))
        self.assertEqual(preview.movimientos[0].referencia, "ABC123")
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("300.00"))
        self.assertEqual(preview.movimientos[1].saldo, Decimal("1700.00"))
        self.assertEqual(preview.errores, [])

    def test_generar_preview_reads_bbva_maestra_pyme_pdf_statement(self):
        archivo = SimpleUploadedFile(
            "00741744000120753084CH.pdf",
            _minimal_text_pdf(
                [
                    "Estado de Cuenta",
                    "MAESTRA PYME BBVA",
                    "Periodo DEL 01/05/2026 AL 31/05/2026",
                    "Detalle de Movimientos Realizados",
                    "FECHA SALDO",
                    "OPER LIQ COD. DESCRIPCIÓN REFERENCIA CARGOS ABONOS OPERACIÓN LIQUIDACIÓN",
                    "04/MAY 01/MAY Y45 MORA SPEI NORMABANXICO 0.27 6,160.73 6,160.73",
                    "Ref. COMP SPEI",
                    "07/MAY 07/MAY S39 SERV BANCA INTERNET 250.00",
                    "Ref. ADMON RENTA",
                    "08/MAY 08/MAY T20 SPEI RECIBIDOBAJIO 4,000.00",
                    "0164054TRASPASO PAGO FONACOT Ref. 0145607982 030",
                    "00030741900036812905",
                ]
            ),
            content_type="application/pdf",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(preview.fuente, ImportacionBancaria.FUENTE_MANUAL_PDF)
        self.assertEqual(len(preview.movimientos), 3)
        self.assertEqual(preview.movimientos[0].fecha.date().isoformat(), "2026-05-04")
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("0.27"))
        self.assertEqual(preview.movimientos[0].saldo, Decimal("6160.73"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("250.00"))
        self.assertEqual(preview.movimientos[1].referencia, "Ref. ADMON RENTA")
        self.assertEqual(preview.movimientos[2].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[2].monto, Decimal("4000.00"))
        self.assertIn("0145607982", preview.movimientos[2].referencia)
        self.assertEqual(preview.errores, [])

    def test_generar_preview_reads_american_express_pdf_statement(self):
        amex, _ = CuentaBancaria.objects.update_or_create(
            banco=CuentaBancaria.BANCO_AMEX,
            defaults={
                "nombre_display": "American Express Business Gold",
                "id_site_syncfy": "",
                "origen": CuentaBancaria.ORIGEN_MANUAL,
                "numero_cuenta": "01005",
            },
        )
        archivo = SimpleUploadedFile(
            "9_may_2026_-_8_jun_2026.pdf",
            _minimal_text_pdf(
                [
                    "americanexpress.com.mx",
                    "American Express",
                    "Tarjetahabiente 3401-061022-01005 de Corte de Corte",
                    "MAURICIO ANTONIO BURGOS FONSECA 08-Jun-2026 08-Jul-2026",
                    "Fecha y Detalle de las operaciones Importe en MN.",
                    "27 de Mayo GRACIAS POR SU PAGO EN LINEA 49,600.00",
                    "CR",
                    "12 de Mayo TELEFONOS DE MEXICO SAB CIUDAD DE MEXIC 6,552.00",
                    "RFCTME840315KT6 /REF101899993457",
                    "20 de Mayo OPENAI *CHATGPT SUBSCR SAN FRANCISCO 3,552.14",
                    "Dólar U.S.A. 200.00",
                    "Resumen de Planes de Pagos Diferidos con Intereses y Meses sin Intereses",
                    "AMAZON MX MSI MKT*AMAZO MEXICO 12 deEne 10,199.00 0.00% 1,699.80 5 de6 1,699.84",
                ]
            ),
            content_type="application/pdf",
        )

        preview = generar_preview(cuenta=amex, uploaded_file=archivo)

        self.assertEqual(preview.fuente, ImportacionBancaria.FUENTE_MANUAL_PDF)
        self.assertEqual(len(preview.movimientos), 3)
        self.assertEqual(preview.movimientos[0].fecha.date().isoformat(), "2026-05-27")
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("49600.00"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("6552.00"))
        self.assertEqual(preview.movimientos[1].referencia, "101899993457")
        self.assertEqual(preview.movimientos[2].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[2].monto, Decimal("3552.14"))
        self.assertEqual(preview.errores, [])

    def test_generar_preview_normalizes_xml_spreadsheet_table(self):
        archivo = SimpleUploadedFile(
            "estado_banbajio.xml",
            (
                '<?xml version="1.0"?>'
                '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" '
                'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">'
                "<Worksheet>"
                "<Table>"
                "<Row><Cell><Data>Estado de cuenta BanBajio</Data></Cell></Row>"
                "<Row>"
                "<Cell><Data>Fecha</Data></Cell>"
                "<Cell><Data>Concepto</Data></Cell>"
                "<Cell><Data>Cargo</Data></Cell>"
                "<Cell><Data>Abono</Data></Cell>"
                "<Cell><Data>Saldo</Data></Cell>"
                "<Cell><Data>Referencia</Data></Cell>"
                "</Row>"
                "<Row>"
                "<Cell><Data>09/06/2026</Data></Cell>"
                "<Cell><Data>SPEI RECIBIDO</Data></Cell>"
                "<Cell><Data></Data></Cell>"
                "<Cell><Data>1500.50</Data></Cell>"
                "<Cell><Data>2000.00</Data></Cell>"
                "<Cell><Data>ABC123</Data></Cell>"
                "</Row>"
                "<Row>"
                "<Cell><Data>10/06/2026</Data></Cell>"
                "<Cell><Data>PAGO PROVEEDOR</Data></Cell>"
                "<Cell><Data>300.00</Data></Cell>"
                "<Cell><Data></Data></Cell>"
                "<Cell><Data>1700.00</Data></Cell>"
                "<Cell><Data>DEF456</Data></Cell>"
                "</Row>"
                "</Table>"
                "</Worksheet>"
                "</Workbook>"
            ).encode("utf-8"),
            content_type="application/xml",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(len(preview.movimientos), 2)
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_ABONO)
        self.assertEqual(preview.movimientos[0].monto, Decimal("1500.50"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].monto, Decimal("300.00"))
        self.assertEqual(preview.errores, [])

    def test_generar_preview_rejects_cfdi_xml_as_bank_statement(self):
        archivo = SimpleUploadedFile(
            "factura_banbajio.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" '
                'Version="4.0" Fecha="2026-06-01T21:52:20" Total="28297.28" '
                'Moneda="MXN" TipoDeComprobante="I" Sello="abc" Certificado="xyz">'
                '<cfdi:Emisor Rfc="BBA940707IE1" Nombre="BANCO DEL BAJIO"/>'
                '<cfdi:Receptor Rfc="GEF211230KR2" Nombre="GRUPO EMPRESARIAL FONSMA"/>'
                '<cfdi:Conceptos>'
                '<cfdi:Concepto Descripcion="COMISION EMI.CHEQ" Importe="13.00"/>'
                "</cfdi:Conceptos>"
                "</cfdi:Comprobante>"
            ).encode("utf-8"),
            content_type="application/xml",
        )

        with self.assertRaisesMessage(ImportacionBancariaError, "Este XML es un CFDI/factura"):
            generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

    def test_generar_preview_reads_banbajio_statement_cfdi_addenda_concepts(self):
        archivo = SimpleUploadedFile(
            "2031_041064189_2.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" '
                'Version="4.0" Fecha="2026-06-01T21:52:20" SubTotal="64.23" '
                'Total="74.51" Moneda="MXN" TipoDeComprobante="I" Sello="abc" Certificado="xyz">'
                '<cfdi:Emisor Rfc="BBA940707IE1" Nombre="BANCO DEL BAJIO"/>'
                '<cfdi:Receptor Rfc="GEF211230KR2" Nombre="GRUPO EMPRESARIAL FONSMA"/>'
                '<cfdi:Conceptos>'
                '<cfdi:Concepto NoIdentificacion="0120260501001856124006001000000000002" '
                'Descripcion="COMISION APLICACION DE TASAS DE DESCUENTO DE CR; VENTAS AL DETALLE" '
                'Importe="47.36">'
                '<cfdi:Impuestos><cfdi:Traslados><cfdi:Traslado Importe="7.58"/></cfdi:Traslados></cfdi:Impuestos>'
                '</cfdi:Concepto>'
                '<cfdi:Concepto NoIdentificacion="0120260502001856293006001000000000002" '
                'Descripcion="COMISION APLICACION DE TASAS DE DESCUENTO DE DB; VENTAS AL DETALLE" '
                'Importe="16.87">'
                '<cfdi:Impuestos><cfdi:Traslados><cfdi:Traslado Importe="2.70"/></cfdi:Traslados></cfdi:Impuestos>'
                '</cfdi:Concepto>'
                '</cfdi:Conceptos>'
                '<cfdi:Addenda><EstadoDeCuentaBajio numeroCuenta="0410641890201" periodo="01 de Mayo de 2026 al 31 de Mayo de 2026"/></cfdi:Addenda>'
                '</cfdi:Comprobante>'
            ).encode("utf-8"),
            content_type="application/xml",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(len(preview.movimientos), 2)
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[0].fecha.date().isoformat(), "2026-05-01")
        self.assertEqual(preview.movimientos[0].monto, Decimal("54.94"))
        self.assertEqual(preview.movimientos[1].tipo, MovimientoBancario.TIPO_CARGO)
        self.assertEqual(preview.movimientos[1].fecha.date().isoformat(), "2026-05-02")
        self.assertEqual(preview.movimientos[1].monto, Decimal("19.57"))
        self.assertEqual(preview.errores, [])

    def test_generar_preview_keeps_iso_dates_in_year_month_day_order(self):
        archivo = SimpleUploadedFile(
            "banbajio.csv",
            "Fecha,Concepto,Cargo,Abono\n2026-06-01T21:52:20,COMISION,13.00,\n".encode("utf-8"),
            content_type="text/csv",
        )

        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        self.assertEqual(preview.movimientos[0].fecha.date().isoformat(), "2026-06-01")
        self.assertEqual(preview.movimientos[0].tipo, MovimientoBancario.TIPO_CARGO)

    def test_confirmar_importacion_is_idempotent_by_manual_hash(self):
        archivo = SimpleUploadedFile(
            "bbva.csv",
            "Fecha,Descripcion,Monto,Referencia\n2026-06-09,DEPOSITO CLIENTE,900.00,R1\n".encode("utf-8"),
            content_type="text/csv",
        )
        preview = generar_preview(cuenta=self.cuenta, uploaded_file=archivo)

        first = confirmar_importacion(preview=preview, user=self.user)
        second = confirmar_importacion(preview=preview, user=self.user)

        self.assertEqual(first.movimientos_nuevos, 1)
        self.assertEqual(first.movimientos_duplicados, 0)
        self.assertEqual(second.movimientos_nuevos, 0)
        self.assertEqual(second.movimientos_duplicados, 1)
        self.assertEqual(MovimientoBancario.objects.count(), 1)
        self.assertEqual(ImportacionBancaria.objects.count(), 2)
        movimiento = MovimientoBancario.objects.get()
        self.assertTrue(movimiento.id_transaction.startswith("manual:"))
        self.assertEqual(movimiento.extra_raw["source"], ImportacionBancaria.FUENTE_MANUAL_CSV)


def _minimal_text_pdf(lines: list[str]) -> bytes:
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    text_commands = ["BT", "/F1 10 Tf", "50 760 Td"]
    for index, line in enumerate(lines):
        if index:
            text_commands.append("0 -16 Td")
        text_commands.append(f"({escape(line)}) Tj")
    text_commands.append("ET")
    stream = "\n".join(text_commands).encode("latin-1")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream",
    ]
    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    startxref = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
            f"startxref\n{startxref}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(pdf)
