from __future__ import annotations

import base64
import io
import zipfile
from datetime import date
from decimal import Decimal

from django.test import TestCase

from sat_client.models import CfdiDescargado, SolicitudDescarga
from sat_client.services.descarga import extraer_xmls_de_zip_base64, guardar_cfdis_xml, parse_cfdi_xml


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


class SatDescargaServiceTests(TestCase):
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

    def test_extraer_xmls_de_zip_base64_returns_only_xml_files(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zip_file:
            zip_file.writestr("cfdi.xml", CFDI_XML)
            zip_file.writestr("readme.txt", "ignored")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")

        xmls = extraer_xmls_de_zip_base64(encoded)

        self.assertEqual(xmls, [CFDI_XML])
