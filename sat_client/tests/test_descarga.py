from __future__ import annotations

import base64
import io
import zipfile
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from lxml import etree

from sat_client.models import CfdiDescargado, CfdiPagoRelacionado, SolicitudDescarga
from sat_client.services.base import SAT_DOWNLOAD_NS, SatCredentials, find_first_element
from sat_client.services.descarga import (
    _build_descarga_envelope,
    extraer_xmls_de_zip_base64,
    guardar_cfdis_xml,
    parse_cfdi_xml,
)


CFDI_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4"
                  xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital"
                  Version="4.0"
                  Fecha="2026-05-31T12:30:00"
                  SubTotal="100.00"
                  Descuento="5.00"
                  Moneda="MXN"
                  TipoCambio="1"
                  Total="95.00"
                  TipoDeComprobante="I"
                  MetodoPago="PUE"
                  FormaPago="03">
  <cfdi:Emisor Rfc="AAA010101AAA" Nombre="EMISOR SA DE CV"/>
  <cfdi:Receptor Rfc="BBB010101BBB" Nombre="RECEPTOR SA DE CV" UsoCFDI="G03"/>
  <cfdi:Complemento>
    <tfd:TimbreFiscalDigital UUID="550E8400-E29B-41D4-A716-446655440000"
                             FechaTimbrado="2026-05-31T12:31:00"/>
  </cfdi:Complemento>
</cfdi:Comprobante>
"""

CFDI_PAGO_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4"
                  xmlns:pago20="http://www.sat.gob.mx/Pagos20"
                  xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital"
                  Version="4.0"
                  Fecha="2026-06-03T10:00:00"
                  SubTotal="0"
                  Moneda="XXX"
                  Total="0"
                  TipoDeComprobante="P">
  <cfdi:Emisor Rfc="AAA010101AAA" Nombre="EMISOR SA DE CV"/>
  <cfdi:Receptor Rfc="BBB010101BBB" Nombre="RECEPTOR SA DE CV" UsoCFDI="CP01"/>
  <cfdi:Complemento>
    <pago20:Pagos Version="2.0">
      <pago20:Pago FechaPago="2026-05-31T18:45:00"
                   FormaDePagoP="03"
                   MonedaP="MXN"
                   Monto="400.00">
        <pago20:DoctoRelacionado IdDocumento="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
                                  MonedaDR="MXN"
                                  NumParcialidad="1"
                                  ImpSaldoAnt="1000.00"
                                  ImpPagado="400.00"
                                  ImpSaldoInsoluto="600.00"/>
      </pago20:Pago>
    </pago20:Pagos>
    <tfd:TimbreFiscalDigital UUID="99999999-9999-9999-9999-999999999999"
                             FechaTimbrado="2026-06-03T10:01:00"/>
  </cfdi:Complemento>
</cfdi:Comprobante>
"""


class SatDescargaServiceTests(TestCase):
    def test_build_descarga_envelope_uses_current_sat_contract(self):
        credentials = SatCredentials(
            cer_path="/tmp/fiel.cer",
            key_path="/tmp/fiel.key",
            password="secret",
            rfc="AAA010101AAA",
        )
        captured = {}

        def fake_signed_request(tag_name, attributes, credentials_arg):
            captured["tag_name"] = tag_name
            captured["attributes"] = attributes
            captured["credentials"] = credentials_arg
            return etree.Element(tag_name)

        with (
            patch("sat_client.services.descarga.get_sat_credentials", return_value=credentials),
            patch("sat_client.services.descarga.build_signed_sat_request", side_effect=fake_signed_request),
        ):
            envelope = _build_descarga_envelope("PAQUETE_01")

        operation = find_first_element(envelope, "PeticionDescargaMasivaTercerosEntrada")
        self.assertIsNotNone(operation)
        self.assertEqual(etree.QName(captured["tag_name"]).namespace, SAT_DOWNLOAD_NS)
        self.assertEqual(etree.QName(captured["tag_name"]).localname, "peticionDescarga")
        self.assertEqual(captured["attributes"], {"IdPaquete": "PAQUETE_01", "RfcSolicitante": "AAA010101AAA"})

    def test_parse_cfdi_xml_extracts_key_fields(self):
        parsed = parse_cfdi_xml(CFDI_XML)

        self.assertEqual(parsed.uuid, "550E8400-E29B-41D4-A716-446655440000")
        self.assertEqual(parsed.rfc_emisor, "AAA010101AAA")
        self.assertEqual(parsed.rfc_receptor, "BBB010101BBB")
        self.assertEqual(parsed.total, Decimal("95.00"))
        self.assertEqual(parsed.subtotal, Decimal("100.00"))
        self.assertEqual(parsed.descuento, Decimal("5.00"))
        self.assertEqual(parsed.tipo_comprobante, "I")
        self.assertEqual(parsed.uso_cfdi, "G03")

    def test_parse_cfdi_xml_extracts_payment_complement_details(self):
        parsed = parse_cfdi_xml(CFDI_PAGO_XML)

        self.assertEqual(parsed.uuid, "99999999-9999-9999-9999-999999999999")
        self.assertEqual(parsed.tipo_comprobante, "P")
        self.assertEqual(len(parsed.pagos), 1)
        pago = parsed.pagos[0]
        self.assertEqual(pago.uuid_relacionado, "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE")
        self.assertEqual(pago.monto, Decimal("400.00"))
        self.assertEqual(pago.forma_pago, "03")
        self.assertEqual(pago.moneda, "MXN")
        self.assertEqual(pago.num_parcialidad, "1")
        self.assertEqual(pago.importe_saldo_anterior, Decimal("1000.00"))
        self.assertEqual(pago.importe_saldo_insoluto, Decimal("600.00"))

    def test_guardar_cfdis_xml_is_idempotent_by_uuid(self):
        solicitud = SolicitudDescarga.objects.create(
            id_solicitud="sol-1",
            fecha_inicial=date(2026, 5, 1),
            fecha_final=date(2026, 5, 31),
            rfc_solicitante="BBB010101BBB",
            direccion=SolicitudDescarga.DIRECCION_RECIBIDOS,
        )

        total_1, nuevos_1 = guardar_cfdis_xml(
            [CFDI_XML],
            solicitud=solicitud,
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
        )
        total_2, nuevos_2 = guardar_cfdis_xml(
            [CFDI_XML],
            solicitud=solicitud,
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
        )

        self.assertEqual((total_1, nuevos_1), (1, 1))
        self.assertEqual((total_2, nuevos_2), (1, 0))
        self.assertEqual(CfdiDescargado.objects.count(), 1)

    def test_guardar_cfdis_xml_persists_payment_complement_details_idempotently(self):
        solicitud = SolicitudDescarga.objects.create(
            id_solicitud="sol-pago-1",
            fecha_inicial=date(2026, 6, 1),
            fecha_final=date(2026, 6, 5),
            rfc_solicitante="AAA010101AAA",
            direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
        )

        guardar_cfdis_xml([CFDI_PAGO_XML], solicitud=solicitud, tipo_cfdi=CfdiDescargado.TIPO_EMITIDO)
        guardar_cfdis_xml([CFDI_PAGO_XML], solicitud=solicitud, tipo_cfdi=CfdiDescargado.TIPO_EMITIDO)

        self.assertEqual(CfdiDescargado.objects.count(), 1)
        self.assertEqual(CfdiPagoRelacionado.objects.count(), 1)
        pago = CfdiPagoRelacionado.objects.get()
        self.assertEqual(pago.cfdi_pago.uuid, "99999999-9999-9999-9999-999999999999")
        self.assertEqual(pago.uuid_relacionado, "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE")
        self.assertEqual(pago.monto, Decimal("400.00"))

    def test_extraer_xmls_de_zip_base64_returns_only_xml_files(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zip_file:
            zip_file.writestr("cfdi.xml", CFDI_XML)
            zip_file.writestr("readme.txt", "ignored")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")

        xmls = extraer_xmls_de_zip_base64(encoded)

        self.assertEqual(xmls, [CFDI_XML])
