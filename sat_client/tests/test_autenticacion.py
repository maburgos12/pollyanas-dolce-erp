from __future__ import annotations

from django.test import SimpleTestCase

from sat_client.services.base import extract_access_token


class SatAutenticacionParserTests(SimpleTestCase):
    def test_extract_access_token_prefers_autentica_result_over_timestamp(self):
        response = b"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
          <s:Header>
            <Security>
              <Timestamp>
                <Created>2026-06-09T13:06:51.735Z</Created>
                <Expires>2026-06-09T13:11:51.735Z</Expires>
              </Timestamp>
            </Security>
          </s:Header>
          <s:Body>
            <AutenticaResponse xmlns="http://DescargaMasivaTerceros.gob.mx">
              <AutenticaResult>token.real.sat</AutenticaResult>
            </AutenticaResponse>
          </s:Body>
        </s:Envelope>"""

        self.assertEqual(extract_access_token(response), "token.real.sat")
