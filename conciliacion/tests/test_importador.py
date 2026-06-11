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
