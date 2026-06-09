from __future__ import annotations

import base64
import uuid
from datetime import datetime, timedelta, timezone as dt_timezone

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from lxml import etree

from sat_client.services.base import WSU_NS, SatCredentials

DS_NS = "http://www.w3.org/2000/09/xmldsig#"
WSS_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
X509_VALUE_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-x509-token-profile-1.0#X509v3"
)
BASE64_ENCODING_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-soap-message-security-1.0#Base64Binary"
)


def _read_certificate_der(path: str) -> bytes:
    raw = open(path, "rb").read()
    if b"BEGIN CERTIFICATE" in raw:
        return x509.load_pem_x509_certificate(raw).public_bytes(serialization.Encoding.DER)
    return x509.load_der_x509_certificate(raw).public_bytes(serialization.Encoding.DER)


def certificate_base64(path: str) -> str:
    return base64.b64encode(_read_certificate_der(path)).decode("ascii")


def load_private_key(path: str, password: str):
    raw = open(path, "rb").read()
    password_bytes = password.encode("utf-8") if password else None
    if b"BEGIN " in raw:
        return serialization.load_pem_private_key(raw, password=password_bytes)
    return serialization.load_der_private_key(raw, password=password_bytes)


def utc_timestamp(value: datetime | None = None) -> str:
    value = value or datetime.now(dt_timezone.utc)
    value = value.astimezone(dt_timezone.utc)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonicalize(element: etree._Element) -> bytes:
    return etree.tostring(element, method="c14n", exclusive=True, with_comments=False)


def _digest_sha1(data: bytes) -> str:
    digest = hashes.Hash(hashes.SHA1())
    digest.update(data)
    return base64.b64encode(digest.finalize()).decode("ascii")


def _sign_sha1(private_key, data: bytes) -> str:
    signature = private_key.sign(data, padding.PKCS1v15(), hashes.SHA1())
    return base64.b64encode(signature).decode("ascii")


def build_signature(
    reference_element: etree._Element,
    private_key,
    *,
    token_reference_id: str | None = None,
) -> etree._Element:
    reference_id = reference_element.get(etree.QName(WSU_NS, "Id"))
    if not reference_id:
        reference_id = "_0"
        reference_element.set(etree.QName(WSU_NS, "Id"), reference_id)

    signature = etree.Element(etree.QName(DS_NS, "Signature"), nsmap={None: DS_NS})
    signed_info = etree.SubElement(signature, etree.QName(DS_NS, "SignedInfo"))
    etree.SubElement(
        signed_info,
        etree.QName(DS_NS, "CanonicalizationMethod"),
        Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#",
    )
    etree.SubElement(
        signed_info,
        etree.QName(DS_NS, "SignatureMethod"),
        Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1",
    )
    reference = etree.SubElement(signed_info, etree.QName(DS_NS, "Reference"), URI=f"#{reference_id}")
    transforms = etree.SubElement(reference, etree.QName(DS_NS, "Transforms"))
    etree.SubElement(
        transforms,
        etree.QName(DS_NS, "Transform"),
        Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#",
    )
    etree.SubElement(
        reference,
        etree.QName(DS_NS, "DigestMethod"),
        Algorithm="http://www.w3.org/2000/09/xmldsig#sha1",
    )
    digest_value = etree.SubElement(reference, etree.QName(DS_NS, "DigestValue"))
    digest_value.text = _digest_sha1(canonicalize(reference_element))

    signature_value = etree.SubElement(signature, etree.QName(DS_NS, "SignatureValue"))
    signature_value.text = _sign_sha1(private_key, canonicalize(signed_info))

    if token_reference_id:
        key_info = etree.SubElement(signature, etree.QName(DS_NS, "KeyInfo"))
        token_reference = etree.SubElement(
            key_info,
            etree.QName(WSS_NS, "SecurityTokenReference"),
            nsmap={"o": WSS_NS},
        )
        etree.SubElement(
            token_reference,
            etree.QName(WSS_NS, "Reference"),
            URI=f"#{token_reference_id}",
            ValueType=X509_VALUE_TYPE,
        )
    return signature


def build_security_header(credentials: SatCredentials, *, now: datetime | None = None) -> etree._Element:
    now = now or datetime.now(dt_timezone.utc)
    token_id = f"uuid-{uuid.uuid4()}-4"
    private_key = load_private_key(credentials.key_path, credentials.password)

    security = etree.Element(
        etree.QName(WSS_NS, "Security"),
        nsmap={"o": WSS_NS, "u": WSU_NS},
    )
    security.set("{http://schemas.xmlsoap.org/soap/envelope/}mustUnderstand", "1")

    timestamp = etree.SubElement(security, etree.QName(WSU_NS, "Timestamp"))
    timestamp.set(etree.QName(WSU_NS, "Id"), "_0")
    created = etree.SubElement(timestamp, etree.QName(WSU_NS, "Created"))
    created.text = utc_timestamp(now)
    expires = etree.SubElement(timestamp, etree.QName(WSU_NS, "Expires"))
    expires.text = utc_timestamp(now + timedelta(minutes=5))

    binary_token = etree.SubElement(
        security,
        etree.QName(WSS_NS, "BinarySecurityToken"),
        ValueType=X509_VALUE_TYPE,
        EncodingType=BASE64_ENCODING_TYPE,
    )
    binary_token.set(etree.QName(WSU_NS, "Id"), token_id)
    binary_token.text = certificate_base64(credentials.cer_path)

    security.append(build_signature(timestamp, private_key, token_reference_id=token_id))
    return security


def build_signed_sat_request(
    tag_name: str,
    attributes: dict[str, str],
    credentials: SatCredentials,
) -> etree._Element:
    private_key = load_private_key(credentials.key_path, credentials.password)
    element = etree.Element(tag_name, nsmap={"u": WSU_NS})
    element.set(etree.QName(WSU_NS, "Id"), "_0")
    for key, value in attributes.items():
        if value not in (None, ""):
            element.set(key, str(value))
    element.append(build_signature(element, private_key))
    return element
