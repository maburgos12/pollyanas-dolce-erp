from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.utils import timezone

from conciliacion.models import CfdiSucursalResolucion, ImportacionBancaria
from core.models import Sucursal
from sat_client.models import CfdiDescargado, LogDescargaSat
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
        self.assertContains(response, "Formatos aceptados: XML, CSV, XLS, XLSX o XLSM")
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
            descripcion="DEPOSITO MAYO",
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

        response = self.client.get("/conciliacion/bancaria/?periodo=2026-05")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Revision del periodo: mayo 2026")
        self.assertContains(response, "mayo.csv")
        self.assertContains(response, "2026-05-01")
        self.assertContains(response, "2026-05-31")
        self.assertContains(response, "CFDI SAT del periodo")
        self.assertContains(response, "11111111-1111-1111-1111-111111111111")
        self.assertContains(response, "CFDI emitidos por sucursal")
        self.assertContains(response, "Matriz")
        self.assertContains(response, "$500.00")

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
