from __future__ import annotations

import time
from datetime import date, timedelta

from celery import shared_task
from django.conf import settings
from django.core.mail import send_mail

from sat_client.models import CfdiDescargado, LogDescargaSat, SolicitudDescarga
from sat_client.services.autenticacion import obtener_token
from sat_client.services.base import SatConfigurationError, SatRequestLimitExceeded, SatServiceError
from sat_client.services.descarga import descargar_paquete, extraer_xmls_de_zip_base64, guardar_cfdis_xml
from sat_client.services.solicitud import solicitar_descarga_periodo
from sat_client.services.verificacion import verificar_hasta_terminar


def _restar_meses(fecha: date, meses: int) -> date:
    year = fecha.year
    month = fecha.month - meses
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def periodos_mensuales_a_descargar(meses_atras: int, *, hoy: date | None = None) -> list[tuple[date, date]]:
    hoy = hoy or date.today()
    primer_dia_mes_actual = hoy.replace(day=1)
    periodos: list[tuple[date, date]] = []
    for offset in range(meses_atras, 0, -1):
        inicio = _restar_meses(primer_dia_mes_actual, offset)
        siguiente = _restar_meses(primer_dia_mes_actual, offset - 1)
        periodos.append((inicio, siguiente - timedelta(days=1)))
    return periodos


def _tipo_cfdi_desde_direccion(direccion: str) -> str:
    return CfdiDescargado.TIPO_EMITIDO if direccion == SolicitudDescarga.DIRECCION_EMITIDOS else CfdiDescargado.TIPO_RECIBIDO


def _alertar_error(mensaje: str) -> None:
    recipients = [
        email.strip()
        for email in getattr(settings, "SAT_ALERT_EMAILS", ["maburgos12@pollyanasdolce.com"])
        if email and email.strip()
    ]
    if not recipients:
        return
    send_mail(
        subject="Error descarga SAT ERP",
        message=mensaje,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "erp@pollyanasdolce.com"),
        recipient_list=recipients,
        fail_silently=True,
    )


def _procesar_periodo_direccion(fecha_inicial: date, fecha_final: date, direccion: str) -> dict[str, int | str]:
    token = obtener_token()
    solicitud = solicitar_descarga_periodo(
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        direccion=direccion,
        token=token,
    )
    if solicitud.estado != SolicitudDescarga.ESTADO_TERMINADA:
        solicitud = verificar_hasta_terminar(solicitud)

    if solicitud.estado != SolicitudDescarga.ESTADO_TERMINADA:
        LogDescargaSat.objects.create(
            nivel=LogDescargaSat.NIVEL_WARN,
            mensaje=f"Solicitud SAT no termino: estado={solicitud.estado}",
            solicitud=solicitud,
        )
        return {"solicitud_id": solicitud.id_solicitud or "", "descargados": 0, "nuevos": 0}

    descargados = 0
    nuevos = 0
    for id_paquete in solicitud.ids_paquetes:
        paquete_base64 = descargar_paquete(id_paquete, token=obtener_token())
        xmls = extraer_xmls_de_zip_base64(paquete_base64)
        total_paquete, nuevos_paquete = guardar_cfdis_xml(
            xmls,
            solicitud=solicitud,
            tipo_cfdi=_tipo_cfdi_desde_direccion(direccion),
        )
        descargados += total_paquete
        nuevos += nuevos_paquete

    LogDescargaSat.objects.create(
        nivel=LogDescargaSat.NIVEL_INFO,
        mensaje=f"Descarga SAT completada para {direccion} {fecha_inicial:%Y-%m}",
        solicitud=solicitud,
        cfdis_descargados=descargados,
        cfdis_nuevos=nuevos,
    )
    return {"solicitud_id": solicitud.id_solicitud or "", "descargados": descargados, "nuevos": nuevos}


def _procesar_con_split(fecha_inicial: date, fecha_final: date, direccion: str) -> list[dict[str, int | str]]:
    try:
        return [_procesar_periodo_direccion(fecha_inicial, fecha_final, direccion)]
    except SatRequestLimitExceeded:
        mitad = fecha_inicial + ((fecha_final - fecha_inicial) // 2)
        if mitad <= fecha_inicial or mitad >= fecha_final:
            raise
        return [
            *_procesar_con_split(fecha_inicial, mitad, direccion),
            *_procesar_con_split(mitad + timedelta(days=1), fecha_final, direccion),
        ]


@shared_task(name="sat_client.ejecutar_descarga_sat_nocturna", bind=True, max_retries=2, default_retry_delay=300)
def ejecutar_descarga_sat_nocturna(self):
    inicio = time.monotonic()
    if not getattr(settings, "SAT_DESCARGA_ENABLED", False):
        return {"status": "deshabilitada"}

    meses_atras = max(1, int(getattr(settings, "SAT_DESCARGA_MESES_ATRAS", 1)))
    resultados: list[dict[str, int | str]] = []
    try:
        for fecha_inicial, fecha_final in periodos_mensuales_a_descargar(meses_atras):
            for direccion in (SolicitudDescarga.DIRECCION_EMITIDOS, SolicitudDescarga.DIRECCION_RECIBIDOS):
                resultados.extend(_procesar_con_split(fecha_inicial, fecha_final, direccion))
    except SatConfigurationError as exc:
        mensaje = f"Descarga SAT no configurada: {exc}"
        LogDescargaSat.objects.create(
            nivel=LogDescargaSat.NIVEL_ERROR,
            mensaje=mensaje,
            duracion_segundos=int(time.monotonic() - inicio),
        )
        _alertar_error(mensaje)
        return {"status": "configuracion_incompleta", "error": str(exc)}
    except SatServiceError as exc:
        mensaje = f"Error SAT: {exc}"
        LogDescargaSat.objects.create(
            nivel=LogDescargaSat.NIVEL_ERROR,
            mensaje=mensaje,
            duracion_segundos=int(time.monotonic() - inicio),
        )
        _alertar_error(mensaje)
        raise self.retry(exc=exc)

    descargados = sum(int(item["descargados"]) for item in resultados)
    nuevos = sum(int(item["nuevos"]) for item in resultados)
    duracion = int(time.monotonic() - inicio)
    LogDescargaSat.objects.create(
        nivel=LogDescargaSat.NIVEL_INFO,
        mensaje=f"Descarga SAT nocturna finalizada: {nuevos}/{descargados} CFDIs nuevos",
        cfdis_descargados=descargados,
        cfdis_nuevos=nuevos,
        duracion_segundos=duracion,
    )
    return {
        "status": "ok",
        "periodos": len(periodos_mensuales_a_descargar(meses_atras)),
        "descargados": descargados,
        "nuevos": nuevos,
        "duracion_segundos": duracion,
    }
