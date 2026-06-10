from __future__ import annotations

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from conciliacion.models import ImportacionBancaria
from conciliacion.services.importador import confirmar_importacion, generar_preview
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
