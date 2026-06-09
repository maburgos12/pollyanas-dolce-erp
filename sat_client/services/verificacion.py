from __future__ import annotations

import time
from typing import TYPE_CHECKING, Callable

from django.conf import settings
from lxml import etree

if TYPE_CHECKING:
    from zeep.transports import Transport

from sat_client.models import SolicitudDescarga
from sat_client.services.autenticacion import obtener_token
from sat_client.services.base import (
    SAT_DOWNLOAD_NS,
    SatServiceError,
    build_envelope,
    find_all_text,
    find_result_attributes,
    get_endpoint,
    get_sat_credentials,
    parse_xml,
    post_soap,
)
from sat_client.services.firma import build_signed_sat_request

VERIFICACION_ACTION = (
    "http://DescargaMasivaTerceros.sat.gob.mx/"
    "IVerificaSolicitudDescargaService/VerificaSolicitudDescarga"
)


def _build_verificacion_envelope(solicitud: SolicitudDescarga) -> etree._Element:
    credentials = get_sat_credentials()
    operation = etree.Element(
        etree.QName(SAT_DOWNLOAD_NS, "VerificaSolicitudDescarga"),
        nsmap={None: SAT_DOWNLOAD_NS},
    )
    operation.append(
        build_signed_sat_request(
            "solicitud",
            {
                "IdSolicitud": solicitud.id_solicitud or "",
                "RfcSolicitante": credentials.rfc,
            },
            credentials,
        )
    )
    return build_envelope(operation)


def verificar_solicitud(
    solicitud: SolicitudDescarga,
    *,
    token: str,
    transport: Transport | None = None,
) -> SolicitudDescarga:
    if not solicitud.id_solicitud:
        raise SatServiceError("No se puede verificar una solicitud sin IdSolicitud")

    content = post_soap(
        get_endpoint("SAT_VERIFICACION_URL"),
        _build_verificacion_envelope(solicitud),
        soap_action=getattr(settings, "SAT_VERIFICACION_ACTION", VERIFICACION_ACTION),
        token=token,
        transport=transport,
    )
    attrs = find_result_attributes(content, "VerificaSolicitudDescargaResult")
    root = parse_xml(content)
    ids_paquetes = find_all_text(root, "IdsPaquetes")
    estado = attrs.get("EstadoSolicitud")
    numero_cfdis = attrs.get("NumeroCFDIs")

    if estado:
        solicitud.estado = int(estado)
    solicitud.codigo_estado = attrs.get("CodigoEstadoSolicitud") or attrs.get("CodEstatus") or solicitud.codigo_estado
    solicitud.numero_cfdis = int(numero_cfdis) if numero_cfdis not in (None, "") else solicitud.numero_cfdis
    if ids_paquetes:
        solicitud.ids_paquetes = ids_paquetes
    solicitud.error_detalle = attrs.get("Mensaje") or solicitud.error_detalle
    solicitud.save(update_fields=["estado", "codigo_estado", "numero_cfdis", "ids_paquetes", "error_detalle", "actualizado_en"])
    return solicitud


def verificar_hasta_terminar(
    solicitud: SolicitudDescarga,
    *,
    obtener_token_func: Callable[[], str] = obtener_token,
    sleep_func: Callable[[float], None] = time.sleep,
    max_intentos: int | None = None,
    intervalo_segundos: int | None = None,
    transport: Transport | None = None,
) -> SolicitudDescarga:
    if max_intentos is None:
        max_intentos = getattr(settings, "SAT_POLL_MAX_ATTEMPTS", 12)
    if intervalo_segundos is None:
        intervalo_segundos = getattr(settings, "SAT_POLL_INTERVAL_SECONDS", 600)
    estados_finales = {
        SolicitudDescarga.ESTADO_TERMINADA,
        SolicitudDescarga.ESTADO_ERROR,
        SolicitudDescarga.ESTADO_RECHAZADA,
        SolicitudDescarga.ESTADO_VENCIDA,
    }

    for intento in range(max_intentos):
        token = obtener_token_func()
        solicitud = verificar_solicitud(solicitud, token=token, transport=transport)
        if solicitud.estado in estados_finales:
            return solicitud
        if intento < max_intentos - 1:
            sleep_func(intervalo_segundos)

    solicitud.estado = SolicitudDescarga.ESTADO_ERROR
    solicitud.error_detalle = "Timeout esperando terminacion de solicitud SAT"
    solicitud.save(update_fields=["estado", "error_detalle", "actualizado_en"])
    return solicitud
