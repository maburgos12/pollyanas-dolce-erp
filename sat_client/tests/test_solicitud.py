from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase, override_settings
from lxml import etree

from sat_client.models import SolicitudDescarga
from sat_client.services.base import SAT_DOWNLOAD_NS, SatCredentials, find_first_element
from sat_client.services.solicitud import _build_solicitud_envelope, solicitar_descarga_periodo


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


class SatSolicitudCuotaAgotadaTests(TestCase):
    @override_settings(SAT_SOLICITUD_URL="https://sat.test/solicitud")
    def test_codigo_5002_registra_rechazo_definitivo_sin_lanzar(self):
        credentials = SatCredentials(
            cer_path="/tmp/fiel.cer",
            key_path="/tmp/fiel.key",
            password="secret",
            rfc="AAA010101AAA",
        )
        respuesta = (
            b'<root><SolicitaDescargaEmitidosResult CodEstatus="5002" '
            b'Mensaje="Se han agotado las solicitudes de por vida"/></root>'
        )

        with (
            patch("sat_client.services.solicitud.get_sat_credentials", return_value=credentials),
            patch(
                "sat_client.services.solicitud._build_solicitud_envelope",
                return_value=etree.Element("envelope"),
            ),
            patch("sat_client.services.solicitud.post_soap", return_value=respuesta),
        ):
            solicitud = solicitar_descarga_periodo(
                fecha_inicial=date(2026, 5, 1),
                fecha_final=date(2026, 5, 1),
                direccion=SolicitudDescarga.DIRECCION_EMITIDOS,
                token="tok",
            )

        self.assertEqual(solicitud.estado, SolicitudDescarga.ESTADO_RECHAZADA)
        self.assertEqual(solicitud.codigo_estado, "5002")
        self.assertIsNone(solicitud.id_solicitud)
        self.assertEqual(SolicitudDescarga.objects.count(), 1)
