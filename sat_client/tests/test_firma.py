from __future__ import annotations

from django.test import SimpleTestCase
from lxml import etree

from cryptography.hazmat.primitives.asymmetric import rsa

from sat_client.services.base import WSU_NS
from sat_client.services.firma import build_signature


class SatFirmaServiceTests(SimpleTestCase):
    def test_build_signature_includes_x509_key_info_for_sat_requests(self):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        request = etree.Element("solicitud", nsmap={"u": WSU_NS})
        request.set(etree.QName(WSU_NS, "Id"), "_0")
        request.set("RfcSolicitante", "AAA010101AAA")

        signature = build_signature(request, private_key, certificate="CERTIFICADOBASE64")

        x509_certificate = None
        for element in signature.iter():
            if etree.QName(element).localname == "X509Certificate":
                x509_certificate = element
                break

        self.assertIsNotNone(x509_certificate)
        self.assertEqual(x509_certificate.text, "CERTIFICADOBASE64")
