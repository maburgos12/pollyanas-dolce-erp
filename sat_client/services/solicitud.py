from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from django.conf import settings
from django.db import connection, transaction
from django.db.models import Q
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

SOLICITUD_ACTION_EMITIDOS = (
    "http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaEmitidos"
)
SOLICITUD_ACTION_RECIBIDOS = (
    "http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaRecibidos"
)


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
    folio: str = "",
) -> SolicitudDescarga | None:
    limite_vigencia = timezone.now() - timedelta(
        days=7 if tipo_solicitud == "Metadata" else 3
    )
    return (
        SolicitudDescarga.objects.filter(
            **(
                {"folio": folio}
                if folio
                else {
                    "fecha_inicial": fecha_inicial,
                    "fecha_final": fecha_final,
                    "folio": "",
                }
            ),
            rfc_solicitante=rfc_solicitante,
            tipo_solicitud=tipo_solicitud,
            direccion=direccion,
        )
        .filter(
            Q(estado__in=[1, 2])
            | Q(estado=4, error_detalle__startswith="Timeout esperando")
            | Q(estado=3, creado_en__gte=limite_vigencia),
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
    folio: str = "",
) -> etree._Element:
    credentials = get_sat_credentials()
    attributes = {
        "RfcSolicitante": credentials.rfc,
        "FechaInicial": _fecha_inicio(fecha_inicial),
        "FechaFinal": _fecha_fin(fecha_final),
        "TipoSolicitud": tipo_solicitud,
    }
    if tipo_solicitud == SolicitudDescarga.TIPO_CFDI:
        attributes["EstadoComprobante"] = "Vigente"
    if direccion == SolicitudDescarga.DIRECCION_EMITIDOS:
        operation_name = "SolicitaDescargaEmitidos"
        attributes["RfcEmisor"] = credentials.rfc
    else:
        operation_name = "SolicitaDescargaRecibidos"
        attributes["RfcReceptor"] = credentials.rfc

    if folio:
        operation_name = "SolicitaDescargaFolio"
        attributes = {"RfcSolicitante": credentials.rfc, "Folio": folio}
    operation = etree.Element(
        etree.QName(SAT_DOWNLOAD_NS, operation_name), nsmap={None: SAT_DOWNLOAD_NS}
    )
    operation.append(
        build_signed_sat_request(
            etree.QName(SAT_DOWNLOAD_NS, "solicitud"), attributes, credentials
        )
    )
    return build_envelope(operation)


def _soap_action_for_direccion(direccion: str) -> str:
    if direccion == SolicitudDescarga.DIRECCION_EMITIDOS:
        return SOLICITUD_ACTION_EMITIDOS
    return SOLICITUD_ACTION_RECIBIDOS


def _result_name_for_direccion(direccion: str) -> str:
    if direccion == SolicitudDescarga.DIRECCION_EMITIDOS:
        return "SolicitaDescargaEmitidosResult"
    return "SolicitaDescargaRecibidosResult"


@transaction.atomic
def solicitar_descarga_periodo(
    *,
    fecha_inicial: date,
    fecha_final: date,
    direccion: str,
    token: str,
    tipo_solicitud: str = SolicitudDescarga.TIPO_CFDI,
    folio: str = "",
    transport: Transport | None = None,
) -> SolicitudDescarga:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_xact_lock(74521014)")
        if not cursor.fetchone()[0]:
            raise SatServiceError("Otra descarga SAT esta en ejecucion")
    if direccion not in ("emitidos", "recibidos") or tipo_solicitud not in (
        "CFDI",
        "Metadata",
    ):
        raise SatServiceError("Direccion o tipo de solicitud SAT invalido")
    if folio:
        try:
            folio = str(UUID(folio)).upper()
        except ValueError as exc:
            raise SatServiceError("Folio SAT invalido") from exc
        if tipo_solicitud != "CFDI":
            raise SatServiceError("La consulta por folio requiere CFDI")
    credentials = get_sat_credentials()
    vigente = buscar_solicitud_vigente(
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        rfc_solicitante=credentials.rfc,
        tipo_solicitud=tipo_solicitud,
        direccion=direccion,
        folio=folio,
    )
    if vigente:
        return vigente

    bloqueada = (
        SolicitudDescarga.objects.filter(
            rfc_solicitante=credentials.rfc,
            direccion=direccion,
            tipo_solicitud=tipo_solicitud,
            **(
                {"folio": folio}
                if folio
                else {
                    "fecha_inicial": fecha_inicial,
                    "fecha_final": fecha_final,
                    "folio": "",
                }
            ),
            estado=5,
        )
        .filter(
            Q(codigo_estado="5002")
            | Q(codigo_estado="5004", creado_en__gte=timezone.now() - timedelta(days=7))
        )
        .first()
    )
    if bloqueada:
        return bloqueada

    envelope = _build_solicitud_envelope(
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        direccion=direccion,
        tipo_solicitud=tipo_solicitud,
        folio=folio,
    )
    content = post_soap(
        get_endpoint("SAT_SOLICITUD_URL"),
        envelope,
        soap_action=(
            "http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/SolicitaDescargaFolio"
            if folio
            else (
                getattr(settings, "SAT_SOLICITUD_ACTION", "")
                or _soap_action_for_direccion(direccion)
            )
        ),
        token=token,
        transport=transport,
    )
    attrs = find_result_attributes(
        content,
        (
            "SolicitaDescargaFolioResult"
            if folio
            else _result_name_for_direccion(direccion)
        ),
    )
    code = attrs.get("CodEstatus", "")
    message = attrs.get("Mensaje") or attrs.get("MensajeError") or ""
    id_solicitud = attrs.get("IdSolicitud") or attrs.get("IdSolicitud")

    if code == "5003":
        raise SatRequestLimitExceeded(
            message or "SAT reporto tope maximo de CFDIs", code=code
        )
    if code == "5002":
        # Cuota "de por vida" agotada para este periodo exacto: registrarlo como
        # rechazo definitivo para que nunca se vuelva a solicitar (reintentar
        # solo quema mas cuota y no puede prosperar).
        return SolicitudDescarga.objects.create(
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final,
            rfc_solicitante=credentials.rfc,
            tipo_solicitud=tipo_solicitud,
            direccion=direccion,
            folio=folio,
            codigo_estado=code,
            estado=SolicitudDescarga.ESTADO_RECHAZADA,
            error_detalle=message or "Se han agotado las solicitudes de por vida",
        )
    if code == "5005":
        existente = buscar_solicitud_vigente(
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final,
            rfc_solicitante=credentials.rfc,
            tipo_solicitud=tipo_solicitud,
            direccion=direccion,
            folio=folio,
        )
        if existente:
            return existente
    if code and code != "5000":
        raise SatServiceError(
            message or f"SAT rechazo solicitud con codigo {code}", code=code
        )
    if not id_solicitud:
        raise SatServiceError("SAT no devolvio IdSolicitud", code=code or None)

    return SolicitudDescarga.objects.create(
        id_solicitud=id_solicitud,
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        rfc_solicitante=credentials.rfc,
        tipo_solicitud=tipo_solicitud,
        direccion=direccion,
        folio=folio,
        codigo_estado=code,
        estado=SolicitudDescarga.ESTADO_ACEPTADA,
        error_detalle=message,
    )
