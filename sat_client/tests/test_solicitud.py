from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.test import SimpleTestCase
from lxml import etree

from sat_client.services.base import SAT_DOWNLOAD_NS, SatCredentials, find_first_element
from sat_client.services.solicitud import _build_solicitud_envelope


class SatSolicitudEnvelopeTests(SimpleTestCase):
    def setUp(self):
        self.credentials = SatCredentials(
            cer_path="/tmp/fiel.cer",
            key_path="/tmp/fiel.key",
            password="secret",
            rfc="AAA010101AAA",
        )

    def test_build_emitidos_uses_current_sat_contract(self):
        captured = {}

        def fake_signed_request(tag_name, attributes, credentials):
            captured["tag_name"] = tag_name
            captured["attributes"] = attributes
            captured["credentials"] = credentials
            return etree.Element(tag_name)

        with (
            patch("sat_client.services.solicitud.get_sat_credentials", return_value=self.credentials),
            patch("sat_client.services.solicitud.build_signed_sat_request", side_effect=fake_signed_request),
        ):
            envelope = _build_solicitud_envelope(
                fecha_inicial=date(2026, 6, 1),
                fecha_final=date(2026, 6, 1),
                direccion="emitidos",
                tipo_solicitud="CFDI",
            )

        operation = find_first_element(envelope, "SolicitaDescargaEmitidos")
        self.assertIsNotNone(operation)
        self.assertEqual(etree.QName(captured["tag_name"]).namespace, SAT_DOWNLOAD_NS)
        self.assertEqual(etree.QName(captured["tag_name"]).localname, "solicitud")
        self.assertEqual(captured["attributes"]["RfcEmisor"], "AAA010101AAA")
        self.assertEqual(captured["attributes"]["EstadoComprobante"], "Vigente")
        self.assertNotIn("RfcReceptor", captured["attributes"])

    def test_build_recibidos_uses_current_sat_contract(self):
        captured = {}

        def fake_signed_request(tag_name, attributes, credentials):
            captured["tag_name"] = tag_name
            captured["attributes"] = attributes
            captured["credentials"] = credentials
            return etree.Element(tag_name)

        with (
            patch("sat_client.services.solicitud.get_sat_credentials", return_value=self.credentials),
            patch("sat_client.services.solicitud.build_signed_sat_request", side_effect=fake_signed_request),
        ):
            envelope = _build_solicitud_envelope(
                fecha_inicial=date(2026, 6, 1),
                fecha_final=date(2026, 6, 1),
                direccion="recibidos",
                tipo_solicitud="CFDI",
            )

        operation = find_first_element(envelope, "SolicitaDescargaRecibidos")
        self.assertIsNotNone(operation)
        self.assertEqual(etree.QName(captured["tag_name"]).namespace, SAT_DOWNLOAD_NS)
        self.assertEqual(etree.QName(captured["tag_name"]).localname, "solicitud")
        self.assertEqual(captured["attributes"]["RfcReceptor"], "AAA010101AAA")
        self.assertEqual(captured["attributes"]["EstadoComprobante"], "Vigente")
        self.assertNotIn("RfcEmisor", captured["attributes"])
