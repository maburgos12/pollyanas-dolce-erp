from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING

from django.conf import settings
from django.utils import timezone
from lxml import etree

if TYPE_CHECKING:
    from zeep.transports import Transport

from sat_client.models import SolicitudDescarga
from sat_client.services.base import (
    SAT_DOWNLOAD_NS,
    SatRequestLimitExceeded,
    SatServiceError,
    build_envelope,
    find_result_attributes,
    get_endpoint,
    get_sat_credentials,
    post_soap,
)
from sat_client.services.firma import build_signed_sat_request

SOLICITUD_ACTION = "http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescarga"


def _fecha_inicio(fecha: date) -> str:
    return datetime.combine(fecha, time.min).strftime("%Y-%m-%dT%H:%M:%S")


def _fecha_fin(fecha: date) -> str:
    return datetime.combine(fecha, time.max.replace(microsecond=0)).strftime("%Y-%m-%dT%H:%M:%S")


def buscar_solicitud_vigente(
    *,
    fecha_inicial: date,
    fecha_final: date,
    rfc_solicitante: str,
    tipo_solicitud: str,
    direccion: str,
) -> SolicitudDescarga | None:
    limite_vigencia = timezone.now() - timedelta(hours=72)
    return (
        SolicitudDescarga.objects.filter(
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final,
            rfc_solicitante=rfc_solicitante,
            tipo_solicitud=tipo_solicitud,
            direccion=direccion,
            estado__in=[
                SolicitudDescarga.ESTADO_ACEPTADA,
                SolicitudDescarga.ESTADO_EN_PROCESO,
                SolicitudDescarga.ESTADO_TERMINADA,
            ],
            creado_en__gte=limite_vigencia,
        )
        .exclude(id_solicitud="")
        .exclude(id_solicitud__isnull=True)
        .order_by("-creado_en")
        .first()
    )


def _build_solicitud_envelope(
    *,
    fecha_inicial: date,
    fecha_final: date,
    direccion: str,
    tipo_solicitud: str,
) -> etree._Element:
    credentials = get_sat_credentials()
    attributes = {
        "RfcSolicitante": credentials.rfc,
        "FechaInicial": _fecha_inicio(fecha_inicial),
        "FechaFinal": _fecha_fin(fecha_final),
        "TipoSolicitud": tipo_solicitud,
    }
    if direccion == SolicitudDescarga.DIRECCION_EMITIDOS:
        attributes["RfcEmisor"] = credentials.rfc
    else:
        attributes["RfcReceptor"] = credentials.rfc

    operation = etree.Element(etree.QName(SAT_DOWNLOAD_NS, "SolicitaDescarga"), nsmap={None: SAT_DOWNLOAD_NS})
    operation.append(build_signed_sat_request("solicitud", attributes, credentials))
    return build_envelope(operation)


def solicitar_descarga_periodo(
    *,
    fecha_inicial: date,
    fecha_final: date,
    direccion: str,
    token: str,
    tipo_solicitud: str = SolicitudDescarga.TIPO_CFDI,
    transport: Transport | None = None,
) -> SolicitudDescarga:
    credentials = get_sat_credentials()
    vigente = buscar_solicitud_vigente(
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        rfc_solicitante=credentials.rfc,
        tipo_solicitud=tipo_solicitud,
        direccion=direccion,
    )
    if vigente:
        return vigente

    envelope = _build_solicitud_envelope(
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        direccion=direccion,
        tipo_solicitud=tipo_solicitud,
    )
    content = post_soap(
        get_endpoint("SAT_SOLICITUD_URL"),
        envelope,
        soap_action=getattr(settings, "SAT_SOLICITUD_ACTION", SOLICITUD_ACTION),
        token=token,
        transport=transport,
    )
    attrs = find_result_attributes(content, "SolicitaDescargaResult")
    code = attrs.get("CodEstatus", "")
    message = attrs.get("Mensaje") or attrs.get("MensajeError") or ""
    id_solicitud = attrs.get("IdSolicitud") or attrs.get("IdSolicitud")

    if code == "5003":
        raise SatRequestLimitExceeded(message or "SAT reporto tope maximo de CFDIs", code=code)
    if code == "5005":
        existente = buscar_solicitud_vigente(
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final,
            rfc_solicitante=credentials.rfc,
            tipo_solicitud=tipo_solicitud,
            direccion=direccion,
        )
        if existente:
            return existente
    if code and code != "5000":
        raise SatServiceError(message or f"SAT rechazo solicitud con codigo {code}", code=code)
    if not id_solicitud:
        raise SatServiceError("SAT no devolvio IdSolicitud", code=code or None)

    return SolicitudDescarga.objects.create(
        id_solicitud=id_solicitud,
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        rfc_solicitante=credentials.rfc,
        tipo_solicitud=tipo_solicitud,
        direccion=direccion,
        codigo_estado=code,
        estado=SolicitudDescarga.ESTADO_ACEPTADA,
        error_detalle=message,
    )
