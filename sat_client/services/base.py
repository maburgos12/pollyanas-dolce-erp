from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from django.conf import settings
from lxml import etree

if TYPE_CHECKING:
    from zeep.transports import Transport

SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
SAT_AUTH_NS = "http://DescargaMasivaTerceros.gob.mx"
SAT_DOWNLOAD_NS = "http://DescargaMasivaTerceros.sat.gob.mx"
WSU_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"


class SatConfigurationError(RuntimeError):
    """Raised when SAT client settings are incomplete."""


class SatServiceError(RuntimeError):
    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class SatRequestLimitExceeded(SatServiceError):
    """SAT returned 5003: request has too many CFDIs."""


@dataclass(frozen=True)
class SatCredentials:
    cer_path: str
    key_path: str
    password: str
    rfc: str


def get_sat_credentials() -> SatCredentials:
    credentials = SatCredentials(
        cer_path=(getattr(settings, "SAT_EFIRMA_CER_PATH", "") or "").strip(),
        key_path=(getattr(settings, "SAT_EFIRMA_KEY_PATH", "") or "").strip(),
        password=getattr(settings, "SAT_EFIRMA_PASSWORD", "") or "",
        rfc=(getattr(settings, "SAT_RFC", "") or "").strip().upper(),
    )
    missing = [
        name
        for name, value in (
            ("SAT_EFIRMA_CER_PATH", credentials.cer_path),
            ("SAT_EFIRMA_KEY_PATH", credentials.key_path),
            ("SAT_EFIRMA_PASSWORD", credentials.password),
            ("SAT_RFC", credentials.rfc),
        )
        if not value
    ]
    if missing:
        raise SatConfigurationError("Configuracion SAT incompleta: " + ", ".join(missing))
    return credentials


def get_endpoint(name: str) -> str:
    value = (getattr(settings, name, "") or "").strip()
    if not value:
        raise SatConfigurationError(f"Configuracion SAT incompleta: {name}")
    return value


def build_envelope(body_child: etree._Element, header_child: etree._Element | None = None) -> etree._Element:
    envelope = etree.Element(etree.QName(SOAP_NS, "Envelope"), nsmap={"s": SOAP_NS})
    if header_child is not None:
        header = etree.SubElement(envelope, etree.QName(SOAP_NS, "Header"))
        header.append(header_child)
    body = etree.SubElement(envelope, etree.QName(SOAP_NS, "Body"))
    body.append(body_child)
    return envelope


def format_authorization_header(token: str) -> str:
    token = token.strip()
    if token.startswith("WRAP "):
        return token
    return f'WRAP access_token="{token}"'


def post_soap(
    url: str,
    envelope: etree._Element,
    *,
    soap_action: str,
    token: str | None = None,
    transport: Transport | None = None,
    timeout_seconds: int = 60,
) -> bytes:
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": f'"{soap_action}"',
    }
    if token:
        headers["Authorization"] = format_authorization_header(token)

    if transport is None:
        try:
            from zeep.transports import Transport
        except ModuleNotFoundError as exc:
            raise SatConfigurationError(
                "Zeep no esta disponible en este interprete; instala dependencias en un runtime compatible."
            ) from exc
        client_transport = Transport(timeout=timeout_seconds, operation_timeout=timeout_seconds)
    else:
        client_transport = transport
    response = client_transport.post_xml(url, envelope, headers)
    status_code = getattr(response, "status_code", 200)
    content = getattr(response, "content", b"")
    if status_code >= 400:
        raise SatServiceError(f"SAT respondio HTTP {status_code}", code=str(status_code))
    return content


def parse_xml(content: bytes | str) -> etree._Element:
    if isinstance(content, str):
        content = content.encode("utf-8")
    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=True)
    return etree.fromstring(content, parser=parser)


def local_name(element: etree._Element) -> str:
    return etree.QName(element).localname


def find_first_element(root: etree._Element, name: str) -> etree._Element | None:
    for element in root.iter():
        if local_name(element) == name:
            return element
    return None


def find_all_text(root: etree._Element, name: str) -> list[str]:
    values: list[str] = []
    for element in root.iter():
        if local_name(element) == name and element.text:
            values.append(element.text.strip())
    return values


def find_result_attributes(content: bytes | str, preferred_name: str) -> dict[str, Any]:
    root = parse_xml(content)
    preferred = find_first_element(root, preferred_name)
    if preferred is not None:
        return dict(preferred.attrib)
    for element in root.iter():
        if "CodEstatus" in element.attrib:
            return dict(element.attrib)
    fault = find_first_element(root, "Fault")
    if fault is not None:
        text = " ".join(part.strip() for part in fault.itertext() if part.strip())
        raise SatServiceError(text or "SAT devolvio un SOAP Fault")
    return {}


def extract_access_token(content: bytes | str) -> str:
    raw = content.decode("utf-8", errors="ignore") if isinstance(content, bytes) else content
    match = re.search(r'access_token="([^"]+)"', raw)
    if match:
        return match.group(1)
    root = parse_xml(content)
    result = find_first_element(root, "AutenticaResult")
    if result is not None and result.text:
        text = result.text.strip()
        if text.startswith("WRAP "):
            match = re.search(r'access_token="([^"]+)"', text)
            if match:
                return match.group(1)
        if text:
            return text
    raise SatServiceError("La respuesta del SAT no contiene access_token")
