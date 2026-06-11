from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from conciliacion.models import CfdiSucursalResolucion, SucursalIdentificadorFiscal
from conciliacion.services.sucursal_cfdi import (
    extraer_textos_cfdi,
    guardar_resolucion_sucursal_cfdi,
    normalizar_texto,
    resolver_sucursal_cfdi,
)
from core.models import Sucursal
from sat_client.models import CfdiDescargado


def cfdi_xml(descripcion: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="100.00">
  <cfdi:Emisor Rfc="GEF211230KR2" Nombre="GRUPO EMPRESARIAL FONSMA"/>
  <cfdi:Receptor Rfc="XAXX010101000" Nombre="PUBLICO EN GENERAL"/>
  <cfdi:Conceptos>
    <cfdi:Concepto Descripcion="{descripcion}" ValorUnitario="100.00" Importe="100.00"/>
  </cfdi:Conceptos>
</cfdi:Comprobante>
"""


class SucursalCfdiServiceTests(TestCase):
    def setUp(self):
        self.matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz")
        self.payan = Sucursal.objects.create(codigo="PAYAN", nombre="Payan")
        SucursalIdentificadorFiscal.objects.create(
            sucursal=self.matriz,
            patron="VENTAS DEL DIA(?!.*SUC)",
            tipo=SucursalIdentificadorFiscal.TIPO_REGEX,
            prioridad=40,
        )
        SucursalIdentificadorFiscal.objects.create(
            sucursal=self.payan,
            patron="SUC PAYAN",
            tipo=SucursalIdentificadorFiscal.TIPO_TEXTO,
            prioridad=10,
        )

    def _cfdi(self, uuid: str, descripcion: str) -> CfdiDescargado:
        return CfdiDescargado.objects.create(
            uuid=uuid,
            rfc_emisor="GEF211230KR2",
            nombre_emisor="GRUPO EMPRESARIAL FONSMA",
            rfc_receptor="XAXX010101000",
            nombre_receptor="PUBLICO EN GENERAL",
            subtotal=Decimal("100.00"),
            total=Decimal("100.00"),
            moneda="MXN",
            tipo_cambio=Decimal("1"),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
            metodo_pago="PUE",
            forma_pago="01",
            fecha_emision=timezone.now(),
            xml_raw=cfdi_xml(descripcion),
        )

    def test_normaliza_acentos_y_puntuacion(self):
        self.assertEqual(normalizar_texto("SUC. PAYÁN / Plaza"), "SUC PAYAN PLAZA")

    def test_extrae_descripcion_de_concepto_xml(self):
        textos = extraer_textos_cfdi(cfdi_xml("VENTAS DEL DIA SUC PAYAN"))

        self.assertIn("VENTAS DEL DIA SUC PAYAN", textos)

    def test_resuelve_sucursal_por_concepto_xml(self):
        cfdi = self._cfdi("11111111-1111-1111-1111-111111111111", "VENTAS DEL DIA SUC PAYAN")

        match = resolver_sucursal_cfdi(cfdi)

        self.assertEqual(match.sucursal_id, self.payan.id)
        self.assertEqual(match.sucursal_codigo, "PAYAN")
        self.assertEqual(match.fuente, CfdiSucursalResolucion.FUENTE_XML_CONCEPTO)
        self.assertGreaterEqual(match.confianza, 90)

    def test_matriz_no_toma_factura_con_suc_explicitamente_marcada(self):
        cfdi = self._cfdi("22222222-2222-2222-2222-222222222222", "VENTAS DEL DIA SUC PAYAN")

        match = resolver_sucursal_cfdi(cfdi)

        self.assertNotEqual(match.sucursal_id, self.matriz.id)

    def test_matriz_se_resuelve_cuando_no_hay_marca_suc(self):
        cfdi = self._cfdi("33333333-3333-3333-3333-333333333333", "VENTAS DEL DIA")

        match = resolver_sucursal_cfdi(cfdi)

        self.assertEqual(match.sucursal_id, self.matriz.id)
        self.assertEqual(match.sucursal_codigo, "MATRIZ")

    def test_guarda_resolucion_idempotente(self):
        cfdi = self._cfdi("44444444-4444-4444-4444-444444444444", "VENTAS DEL DIA SUC PAYAN")

        first = guardar_resolucion_sucursal_cfdi(cfdi)
        second = guardar_resolucion_sucursal_cfdi(cfdi)

        self.assertEqual(first.id, second.id)
        self.assertEqual(CfdiSucursalResolucion.objects.count(), 1)
        self.assertEqual(second.sucursal_id, self.payan.id)

    def test_sin_patron_queda_sin_coincidencia(self):
        cfdi = self._cfdi("55555555-5555-5555-5555-555555555555", "VENTA ESPECIAL SIN SUCURSAL")

        match = resolver_sucursal_cfdi(cfdi)

        self.assertIsNone(match.sucursal_id)
        self.assertEqual(match.fuente, CfdiSucursalResolucion.FUENTE_SIN_COINCIDENCIA)
