from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from django.conf import settings
from lxml import etree

if TYPE_CHECKING:
    from zeep.transports import Transport

from sat_client.services.base import (
    SAT_AUTH_NS,
    build_envelope,
    extract_access_token,
    get_endpoint,
    get_sat_credentials,
    post_soap,
)
from sat_client.services.firma import build_security_header

AUTENTICACION_ACTION = "http://DescargaMasivaTerceros.gob.mx/IAutenticacion/Autentica"


def build_autenticacion_envelope(now: datetime | None = None) -> etree._Element:
    credentials = get_sat_credentials()
    autentica = etree.Element(etree.QName(SAT_AUTH_NS, "Autentica"), nsmap={None: SAT_AUTH_NS})
    return build_envelope(autentica, build_security_header(credentials, now=now))


def obtener_token(*, transport: Transport | None = None) -> str:
    envelope = build_autenticacion_envelope()
    content = post_soap(
        get_endpoint("SAT_AUTENTICACION_URL"),
        envelope,
        soap_action=getattr(settings, "SAT_AUTENTICACION_ACTION", AUTENTICACION_ACTION),
        transport=transport,
    )
    return extract_access_token(content)
