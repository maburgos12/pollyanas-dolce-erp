from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.utils import timezone

from conciliacion.models import CfdiSucursalResolucion, ImportacionBancaria
from core.models import Sucursal
from sat_client.models import CfdiDescargado, CfdiPagoRelacionado, LogDescargaSat
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class ConciliacionBancariaViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="admin_conciliacion_view",
            email="admin-view@example.com",
            password="x",
        )
        self.cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BBVA,
            nombre_display="BBVA Empresas",
            id_site_syncfy="site-bbva",
            numero_cuenta="00741744000120753084",
        )
        self.client.force_login(self.user)

    def test_get_bancaria_renders_upload_screen(self):
        response = self.client.get("/conciliacion/bancaria/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Conciliacion bancaria")
        self.assertContains(response, "BBVA Empresas")
        self.assertContains(response, "Formatos aceptados: PDF, XML, CSV, XLS, XLSX o XLSM")
        self.assertNotContains(response, "accept=")

    @override_settings(SAT_DESCARGA_ENABLED=True)
    def test_get_bancaria_shows_sat_error_status_when_last_log_failed(self):
        LogDescargaSat.objects.create(nivel=LogDescargaSat.NIVEL_ERROR, mensaje="Error SAT: HTTP 500")

        response = self.client.get("/conciliacion/bancaria/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Descarga SAT con error")
        self.assertNotContains(response, "Descarga SAT activa")

    def test_get_bancaria_shows_period_bank_and_sat_summary(self):
        MovimientoBancario.objects.create(
            id_transaction="mayo-1",
            cuenta=self.cuenta,
            descripcion="DEPOSITO EN EFECTIVO MATRIZ MAYO",
            monto=Decimal("1250.00"),
            tipo=MovimientoBancario.TIPO_ABONO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 1, 12, 0)),
            fecha_refresh=timezone.now(),
            extra_raw={"archivo_nombre": "mayo.csv"},
        )
        MovimientoBancario.objects.create(
            id_transaction="mayo-31",
            cuenta=self.cuenta,
            descripcion="COMISION MAYO",
            monto=Decimal("15.00"),
            tipo=MovimientoBancario.TIPO_CARGO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 31, 12, 0)),
            fecha_refresh=timezone.now(),
            extra_raw={"archivo_nombre": "mayo.csv"},
        )
        CfdiDescargado.objects.create(
            uuid="11111111-1111-1111-1111-111111111111",
            rfc_emisor="AAA010101AAA",
            rfc_receptor="GEF211230KR2",
            subtotal=Decimal("1000.00"),
            total=Decimal("1160.00"),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
            fecha_emision=timezone.make_aware(datetime(2026, 5, 15, 10, 0)),
        )
        matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        cfdi_emitido = CfdiDescargado.objects.create(
            uuid="22222222-2222-2222-2222-222222222222",
            rfc_emisor="GEF211230KR2",
            rfc_receptor="XAXX010101000",
            subtotal=Decimal("500.00"),
            total=Decimal("500.00"),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
            forma_pago="01",
            fecha_emision=timezone.make_aware(datetime(2026, 5, 16, 10, 0)),
        )
        CfdiSucursalResolucion.objects.create(
            cfdi=cfdi_emitido,
            sucursal=matriz,
            fuente=CfdiSucursalResolucion.FUENTE_XML_CONCEPTO,
            confianza=95,
            texto_detectado="VENTAS DEL DIA",
        )
        cfdi_credito = CfdiDescargado.objects.create(
            uuid="33333333-3333-3333-3333-333333333333",
            rfc_emisor="GEF211230KR2",
            rfc_receptor="CLI010101AAA",
            subtotal=Decimal("1000.00"),
            total=Decimal("1000.00"),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
            metodo_pago="PPD",
            forma_pago="99",
            fecha_emision=timezone.make_aware(datetime(2026, 4, 20, 10, 0)),
        )
        cfdi_pago = CfdiDescargado.objects.create(
            uuid="44444444-4444-4444-4444-444444444444",
            rfc_emisor="GEF211230KR2",
            rfc_receptor="CLI010101AAA",
            subtotal=Decimal("0.00"),
            total=Decimal("0.00"),
            moneda="XXX",
            tipo_comprobante="P",
            tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
            fecha_emision=timezone.make_aware(datetime(2026, 6, 3, 10, 0)),
        )
        CfdiPagoRelacionado.objects.create(
            cfdi_pago=cfdi_pago,
            uuid_relacionado=cfdi_credito.uuid,
            fecha_pago=timezone.make_aware(datetime(2026, 5, 31, 18, 45)),
            monto=Decimal("400.00"),
            moneda="MXN",
            forma_pago="03",
            num_parcialidad="1",
            importe_saldo_anterior=Decimal("1000.00"),
            importe_saldo_insoluto=Decimal("600.00"),
        )

        response = self.client.get("/conciliacion/bancaria/?periodo=2026-05")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Revision del periodo: mayo 2026")
        self.assertContains(response, "Estado del cierre")
        self.assertContains(response, "Bancos cargados")
        self.assertContains(response, "SAT del mes cargado")
        self.assertContains(response, "Matriz de reglas de conciliacion fiscal")
        self.assertContains(response, "Paquete fiscal dirigido a conciliacion")
        self.assertContains(response, "ordena ingresos por sucursal y forma de pago")
        self.assertContains(response, "Efectivo por sucursal y dia")
        self.assertContains(response, "CFDI de ingreso emitido por sucursal")
        self.assertContains(response, "factura de ingreso contra deposito bancario")
        self.assertContains(response, "Tarjetas: deposito neto")
        self.assertContains(response, "Ingreso facturado por sucursal")
        self.assertContains(response, "Complementos de pago")
        self.assertContains(response, "Resumen ejecutivo")
        self.assertContains(response, "Trabajo de conciliacion")
        self.assertContains(response, "Siguiente accion")
        self.assertContains(response, "mayo.csv")
        self.assertContains(response, "2026-05-01")
        self.assertContains(response, "2026-05-31")
        self.assertContains(response, "CFDI SAT del periodo")
        self.assertContains(response, "11111111-1111-1111-1111-111111111111")
        self.assertContains(response, "CFDI emitidos por sucursal")
        self.assertContains(response, "Matriz")
        self.assertContains(response, "$500.00")
        self.assertContains(response, "Mesa de ingresos para trabajar")
        self.assertContains(response, "Cruza lo facturado en SAT contra los abonos del banco")
        self.assertContains(response, "Depositos")
        self.assertContains(response, "Revisar diferencia")
        self.assertContains(response, "Ver depositos")
        self.assertContains(response, "Banco contra SAT por canal")
        self.assertContains(response, "Efectivo en ventanilla")
        self.assertContains(response, "Comparar factura/corte de efectivo por sucursal contra depositos")
        self.assertContains(response, "$1,250.00")
        self.assertContains(response, "$750.00")
        self.assertContains(response, "Alcance fiscal de conciliacion")
        self.assertContains(response, "Pagos cobrados del mes")
        self.assertContains(response, "$400.00")
        self.assertContains(response, "Credito clientes pendiente")
        self.assertContains(response, "$600.00")

    def test_get_bancaria_shows_all_accounts_and_filters_movements(self):
        banbajio = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BANBAJIO,
            nombre_display="BanBajio Empresas",
            id_site_syncfy="site-banbajio",
            numero_cuenta="410641890201",
        )
        amex, _ = CuentaBancaria.objects.update_or_create(
            banco=CuentaBancaria.BANCO_AMEX,
            defaults={
                "nombre_display": "American Express Business Gold",
                "id_site_syncfy": "site-amex",
                "numero_cuenta": "01005",
            },
        )
        MovimientoBancario.objects.create(
            id_transaction="banbajio-mayo",
            cuenta=banbajio,
            descripcion="DEPOSITO SUCURSAL MATRIZ",
            monto=Decimal("2500.00"),
            tipo=MovimientoBancario.TIPO_ABONO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 31, 12, 0)),
            fecha_refresh=timezone.now(),
        )
        MovimientoBancario.objects.create(
            id_transaction="bbva-mayo",
            cuenta=self.cuenta,
            descripcion="TRANSFERENCIA CLIENTE MAYORISTA",
            monto=Decimal("8300.00"),
            tipo=MovimientoBancario.TIPO_ABONO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 23, 12, 0)),
            fecha_refresh=timezone.now(),
        )
        MovimientoBancario.objects.create(
            id_transaction="amex-mayo",
            cuenta=amex,
            descripcion="AMERICAN EXPRESS CARGO",
            monto=Decimal("1900.00"),
            tipo=MovimientoBancario.TIPO_CARGO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 12, 12, 0)),
            fecha_refresh=timezone.now(),
        )

        response = self.client.get("/conciliacion/bancaria/?periodo=2026-05")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cuentas cargadas del periodo")
        self.assertContains(response, "BanBajio Empresas")
        self.assertContains(response, "BBVA Empresas")
        self.assertContains(response, "American Express Business Gold")
        self.assertContains(response, "Mesa de movimientos")
        self.assertContains(response, "Regla fiscal")
        self.assertContains(response, "Filtrar movimientos")
        self.assertContains(response, "Mostrando 1-3 de 3 movimientos")

        filtered = self.client.get(f"/conciliacion/bancaria/?periodo=2026-05&cuenta={amex.pk}")

        self.assertEqual(filtered.status_code, 200)
        self.assertContains(filtered, "Mostrando 1-1 de 1 movimientos")
        self.assertContains(filtered, "AMERICAN EXPRESS CARGO")
        self.assertNotContains(filtered, "TRANSFERENCIA CLIENTE MAYORISTA")

    def test_preview_and_confirm_import_movements(self):
        archivo = SimpleUploadedFile(
            "bbva.csv",
            "Fecha,Descripcion,Monto,Referencia\n2026-06-09,DEPOSITO CLIENTE,900.00,R1\n".encode("utf-8"),
            content_type="text/csv",
        )

        preview_response = self.client.post(
            "/conciliacion/bancaria/",
            {"action": "preview", "cuenta": self.cuenta.pk, "archivo": archivo},
        )
        self.assertEqual(preview_response.status_code, 200)
        self.assertContains(preview_response, "DEPOSITO CLIENTE")

        confirm_response = self.client.post("/conciliacion/bancaria/", {"action": "confirm"})

        self.assertEqual(confirm_response.status_code, 302)
        self.assertEqual(MovimientoBancario.objects.count(), 1)
        self.assertEqual(ImportacionBancaria.objects.count(), 1)
