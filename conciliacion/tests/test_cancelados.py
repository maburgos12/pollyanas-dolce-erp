from decimal import Decimal
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from conciliacion.models import CfdiSucursalResolucion
from conciliacion.services.importador import resumen_periodo_conciliacion, sugerir_cfdis_para_movimientos
from conciliacion.views import _paquete_conciliacion
from core.models import Sucursal
from sat_client.models import CfdiDescargado, CfdiPagoRelacionado
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class CfdiCanceladosTests(TestCase):
    def cfdi(self, **kwargs):
        data = dict(uuid=str(uuid4()).upper(), rfc_emisor="GEF211230KR2",
                    rfc_receptor="XAXX010101000", subtotal=100, total=100,
                    tipo_comprobante="I", tipo_cfdi="emitido", forma_pago="03",
                    fecha_emision="2026-05-16T10:00:00-07:00")
        data.update(kwargs)
        cfdi = CfdiDescargado.objects.create(**data)
        cfdi.refresh_from_db()
        return cfdi

    def movimiento(self):
        cuenta = CuentaBancaria.objects.create(banco="BBVA", nombre_display="Prueba", id_site_syncfy="cancelados")
        movimiento = MovimientoBancario.objects.create(
            id_transaction="cancelados", cuenta=cuenta, descripcion="SPEI TRANSFERENCIA",
            monto=100, tipo=MovimientoBancario.TIPO_ABONO,
            fecha_transaccion="2026-05-16T12:00:00-07:00", fecha_refresh=timezone.now())
        movimiento.refresh_from_db()
        return movimiento

    def test_resumen_excluye_cancelados_en_totales_canales_nomina_y_sucursales(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        for estado in ["vigente", "cancelado"]:
            ingreso = self.cfdi(estatus=estado)
            CfdiSucursalResolucion.objects.create(cfdi=ingreso, sucursal=sucursal,
                                                  fuente=CfdiSucursalResolucion.FUENTE_XML_CONCEPTO)
            self.cfdi(estatus=estado, tipo_cfdi="recibido")
            self.cfdi(estatus=estado, tipo_comprobante="N")
        resumen = resumen_periodo_conciliacion(year=2026, month=5)
        self.assertEqual(resumen["cfdi_total"], 3)
        self.assertEqual(resumen["cfdi_emitidos"], {"conteo": 2, "total": Decimal("200")})
        self.assertEqual(resumen["cfdi_recibidos"], {"conteo": 1, "total": Decimal("100")})
        self.assertTrue(all(c.estatus == "vigente" for c in resumen["cfdi_rows"]))
        self.assertEqual(sum(row["sat_total"] for row in resumen["canales_comparativo"]), Decimal("100"))
        self.assertEqual(resumen["cfdi_sucursales"]["sucursales"][0]["total"], Decimal("100"))
        self.assertEqual(resumen["mesa_ingresos"]["total_sat"], Decimal("100"))
        nomina = next(row for row in resumen["trabajo_conciliacion"] if row["concepto"] == "Nomina pagada por banco vs timbrada")
        self.assertEqual(nomina["sat_total"], Decimal("100"))
        self.assertEqual(CfdiDescargado.objects.filter(estatus="cancelado").count(), 3)

    def test_pagos_y_ppd_excluyen_facturas_y_complementos_cancelados(self):
        for direccion in ["emitido", "recibido"]:
            vigente = self.cfdi(tipo_cfdi=direccion, metodo_pago="PPD", fecha_emision="2026-04-20T10:00:00-07:00")
            self.cfdi(tipo_cfdi=direccion, estatus="cancelado", metodo_pago="PPD", fecha_emision="2026-04-20T10:00:00-07:00")
            for estado, monto in [("vigente", 20), ("cancelado", 70)]:
                pago = self.cfdi(tipo_cfdi=direccion, estatus=estado, tipo_comprobante="P", total=0,
                                 fecha_emision="2026-06-03T10:00:00-07:00")
                CfdiPagoRelacionado.objects.create(cfdi_pago=pago, uuid_relacionado=vigente.uuid,
                    fecha_pago="2026-05-31T12:00:00-07:00", monto=monto)
        alcance = resumen_periodo_conciliacion(year=2026, month=5)["alcance_fiscal"]
        for direccion in ["emitidos", "recibidos"]:
            self.assertEqual(alcance[f"pagos_{direccion}"], {"conteo": 1, "total": Decimal("20")})
            self.assertEqual(alcance[f"ppd_{direccion}_abiertos"],
                             {"conteo": 1, "total": Decimal("100"), "pagado": Decimal("20"), "saldo": Decimal("80")})

    def test_sugerencias_excluyen_cancelados(self):
        movimiento = self.movimiento()
        vigente = self.cfdi()
        self.cfdi(estatus="cancelado")
        self.assertEqual(sugerir_cfdis_para_movimientos([movimiento])[movimiento.pk], [vigente])

    def test_paquete_excluye_cancelados_de_detalle_y_conteos(self):
        vigente = self.cfdi()
        self.cfdi(estatus="cancelado", conciliado=True)
        paquete = _paquete_conciliacion(year=2026, month=5)
        self.assertEqual(paquete["cfdis"], [vigente])
        self.assertEqual(paquete["resumen"]["cfdis_total"], 1)
        self.assertEqual(paquete["resumen"]["cfdis_conciliados"], 0)
        self.assertEqual(paquete["resumen"]["cfdis_pendientes"], 1)

    def test_vinculo_manual_rechaza_cancelado_y_permite_vigente(self):
        user = get_user_model().objects.create_superuser(username="cancelados", email="test@example.com", password="x")
        self.client.force_login(user)
        movimiento = self.movimiento()
        cancelado = self.cfdi(estatus="cancelado")
        data = dict(action="conciliar_movimiento", periodo="2026-05", movimiento_id=movimiento.pk,
                    tipo_conciliacion=MovimientoBancario.CONCILIACION_CFDI, cfdi_uuid=cancelado.uuid)
        response = self.client.post("/conciliacion/bancaria/", data)
        self.assertEqual(response.status_code, 302)
        movimiento.refresh_from_db(); cancelado.refresh_from_db()
        self.assertFalse(movimiento.conciliado)
        self.assertIsNone(movimiento.cfdi_relacionado_id)
        self.assertFalse(cancelado.conciliado)
        vigente = self.cfdi()
        data["cfdi_uuid"] = vigente.uuid
        self.client.post("/conciliacion/bancaria/", data)
        movimiento.refresh_from_db()
        self.assertTrue(movimiento.conciliado)
        self.assertEqual(movimiento.cfdi_relacionado_id, vigente.pk)

    def test_vinculo_historico_cancelado_queda_como_excepcion_sin_importe_fiscal(self):
        from conciliacion.views import _banco_factura_row, _documento_conciliacion
        movimiento = self.movimiento()
        cancelado = self.cfdi(estatus="cancelado", conciliado=True)
        movimiento.cfdi_relacionado = cancelado
        movimiento.conciliado = True
        movimiento.tipo_conciliacion = MovimientoBancario.CONCILIACION_CFDI
        movimiento.save()
        documento = _documento_conciliacion(movimiento)
        self.assertFalse(documento["estado_expediente"]["completo"])
        self.assertTrue(any("cancelado" in texto.lower() for texto in documento["evidencia_pendiente"]))
        row = _banco_factura_row(documento)
        self.assertEqual(row["TotalFactura"], "")
        self.assertEqual(row["DiferenciaBancoFactura"], "")
        movimiento.refresh_from_db()
        self.assertEqual(movimiento.cfdi_relacionado_id, cancelado.pk)
        self.assertTrue(movimiento.conciliado)
