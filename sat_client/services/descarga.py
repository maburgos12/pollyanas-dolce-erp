from __future__ import annotations

import base64
import io
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from django.conf import settings
from django.utils import dateparse, timezone
from lxml import etree

if TYPE_CHECKING:
    from zeep.transports import Transport

from sat_client.models import CfdiDescargado, SolicitudDescarga
from sat_client.services.base import (
    SAT_DOWNLOAD_NS,
    SatServiceError,
    build_envelope,
    find_first_element,
    get_endpoint,
    get_sat_credentials,
    parse_xml,
    post_soap,
)
from sat_client.services.firma import build_signed_sat_request

DESCARGA_ACTION = "http://DescargaMasivaTerceros.sat.gob.mx/IDescargaMasivaTercerosService/Descargar"


@dataclass(frozen=True)
class CfdiParsed:
    uuid: str
    rfc_emisor: str
    nombre_emisor: str
    rfc_receptor: str
    nombre_receptor: str
    subtotal: Decimal
    total: Decimal
    descuento: Decimal
    moneda: str
    tipo_cambio: Decimal
    tipo_comprobante: str
    uso_cfdi: str
    metodo_pago: str
    forma_pago: str
    fecha_emision: object
    fecha_timbrado: object | None
    estatus: str


def _decimal(value: str | None, default: str = "0") -> Decimal:
    return Decimal(value if value not in (None, "") else default)


def _datetime(value: str | None):
    if not value:
        return None
    parsed = dateparse.parse_datetime(value)
    if parsed is None:
        raise SatServiceError(f"Fecha CFDI invalida: {value}")
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _child_by_local_name(root: etree._Element, name: str) -> etree._Element | None:
    for child in root.iter():
        if etree.QName(child).localname == name:
            return child
    return None


def parse_cfdi_xml(xml_content: bytes | str) -> CfdiParsed:
    root = parse_xml(xml_content)
    emisor = _child_by_local_name(root, "Emisor")
    receptor = _child_by_local_name(root, "Receptor")
    timbre = _child_by_local_name(root, "TimbreFiscalDigital")
    if emisor is None or receptor is None or timbre is None:
        raise SatServiceError("XML CFDI incompleto: faltan Emisor, Receptor o TimbreFiscalDigital")

    uuid = (timbre.get("UUID") or "").upper()
    if not uuid:
        raise SatServiceError("XML CFDI sin UUID")

    fecha_emision = _datetime(root.get("Fecha"))
    if fecha_emision is None:
        raise SatServiceError("XML CFDI sin Fecha")

    return CfdiParsed(
        uuid=uuid,
        rfc_emisor=(emisor.get("Rfc") or "").upper(),
        nombre_emisor=emisor.get("Nombre") or "",
        rfc_receptor=(receptor.get("Rfc") or "").upper(),
        nombre_receptor=receptor.get("Nombre") or "",
        subtotal=_decimal(root.get("SubTotal")),
        total=_decimal(root.get("Total")),
        descuento=_decimal(root.get("Descuento")),
        moneda=root.get("Moneda") or "MXN",
        tipo_cambio=_decimal(root.get("TipoCambio"), "1"),
        tipo_comprobante=root.get("TipoDeComprobante") or "",
        uso_cfdi=receptor.get("UsoCFDI") or "",
        metodo_pago=root.get("MetodoPago") or "",
        forma_pago=root.get("FormaPago") or "",
        fecha_emision=fecha_emision,
        fecha_timbrado=_datetime(timbre.get("FechaTimbrado")),
        estatus="vigente",
    )


def extraer_xmls_de_zip_base64(paquete_base64: str) -> list[bytes]:
    try:
        zip_bytes = base64.b64decode(paquete_base64)
    except Exception as exc:  # noqa: BLE001
        raise SatServiceError("Paquete SAT no es base64 valido") from exc

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
            xmls = []
            for name in zip_file.namelist():
                if not name.lower().endswith(".xml"):
                    continue
                xmls.append(zip_file.read(name))
            return xmls
    except zipfile.BadZipFile as exc:
        raise SatServiceError("Paquete SAT no es un ZIP valido") from exc


def guardar_cfdis_xml(
    xml_documents: list[bytes | str],
    *,
    solicitud: SolicitudDescarga | None,
    tipo_cfdi: str,
    guardar_xml_raw: bool = True,
) -> tuple[int, int]:
    total = 0
    nuevos = 0
    for xml_content in xml_documents:
        parsed = parse_cfdi_xml(xml_content)
        _, created = CfdiDescargado.objects.get_or_create(
            uuid=parsed.uuid,
            defaults={
                "solicitud": solicitud,
                "rfc_emisor": parsed.rfc_emisor,
                "nombre_emisor": parsed.nombre_emisor,
                "rfc_receptor": parsed.rfc_receptor,
                "nombre_receptor": parsed.nombre_receptor,
                "subtotal": parsed.subtotal,
                "total": parsed.total,
                "descuento": parsed.descuento,
                "moneda": parsed.moneda,
                "tipo_cambio": parsed.tipo_cambio,
                "tipo_comprobante": parsed.tipo_comprobante,
                "tipo_cfdi": tipo_cfdi,
                "uso_cfdi": parsed.uso_cfdi,
                "metodo_pago": parsed.metodo_pago,
                "forma_pago": parsed.forma_pago,
                "fecha_emision": parsed.fecha_emision,
                "fecha_timbrado": parsed.fecha_timbrado,
                "estatus": parsed.estatus,
                "xml_raw": (
                    xml_content.decode("utf-8", errors="replace")
                    if isinstance(xml_content, bytes)
                    else xml_content
                )
                if guardar_xml_raw
                else "",
            },
        )
        total += 1
        if created:
            nuevos += 1
    return total, nuevos


def _build_descarga_envelope(id_paquete: str) -> etree._Element:
    credentials = get_sat_credentials()
    operation = etree.Element(
        etree.QName(SAT_DOWNLOAD_NS, "PeticionDescargaMasivaTercerosEntrada"),
        nsmap={None: SAT_DOWNLOAD_NS},
    )
    operation.append(
        build_signed_sat_request(
            etree.QName(SAT_DOWNLOAD_NS, "peticionDescarga"),
            {
                "IdPaquete": id_paquete,
                "RfcSolicitante": credentials.rfc,
            },
            credentials,
        )
    )
    return build_envelope(operation)


def descargar_paquete(
    id_paquete: str,
    *,
    token: str,
    transport: Transport | None = None,
) -> str:
    content = post_soap(
        get_endpoint("SAT_DESCARGA_URL"),
        _build_descarga_envelope(id_paquete),
        soap_action=getattr(settings, "SAT_DESCARGA_ACTION", DESCARGA_ACTION),
        token=token,
        transport=transport,
    )
    root = parse_xml(content)
    paquete = find_first_element(root, "Paquete")
    if paquete is not None and paquete.text:
        return paquete.text.strip()
    result = find_first_element(root, "DescargarResult")
    if result is not None:
        code = result.get("CodEstatus")
        message = result.get("Mensaje") or "SAT no devolvio Paquete"
        raise SatServiceError(message, code=code)
    raise SatServiceError("Respuesta SAT de descarga sin Paquete")
