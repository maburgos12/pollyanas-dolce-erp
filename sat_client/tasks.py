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


def periodos_diarios_a_descargar(meses_atras: int, *, hoy: date | None = None) -> list[tuple[date, date]]:
    hoy = hoy or date.today()
    primer_dia_mes_actual = hoy.replace(day=1)
    fecha_final = hoy - timedelta(days=1)
    fecha = _restar_meses(primer_dia_mes_actual, max(0, meses_atras - 1))
    if fecha > fecha_final:
        fecha = fecha_final
    periodos: list[tuple[date, date]] = []
    while fecha <= fecha_final:
        periodos.append((fecha, fecha))
        fecha += timedelta(days=1)
    return periodos


def _tipo_cfdi_desde_direccion(direccion: str) -> str:
    return CfdiDescargado.TIPO_EMITIDO if direccion == SolicitudDescarga.DIRECCION_EMITIDOS else CfdiDescargado.TIPO_RECIBIDO


# Rechazos definitivos del SAT: 5004 = sin CFDIs en el periodo (resultado final),
# 5002 = cuota "de por vida" agotada. Reintentarlos quema cuota sin poder prosperar.
CODIGOS_RECHAZO_DEFINITIVO = ("5002", "5004")


def _solicitud_periodo_registrada(fecha_inicial: date, fecha_final: date, direccion: str) -> bool:
    rfc = (getattr(settings, "SAT_RFC", "") or "").strip().upper()
    if not rfc:
        return False
    # Cobertura por rango: una solicitud mensual (backfill) cubre sus dias.
    base = SolicitudDescarga.objects.filter(
        fecha_inicial__lte=fecha_inicial,
        fecha_final__gte=fecha_final,
        rfc_solicitante=rfc,
        tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
        direccion=direccion,
    )
    activa = (
        base.filter(
            estado__in=[
                SolicitudDescarga.ESTADO_ACEPTADA,
                SolicitudDescarga.ESTADO_EN_PROCESO,
                SolicitudDescarga.ESTADO_TERMINADA,
            ]
        )
        .exclude(id_solicitud="")
        .exclude(id_solicitud__isnull=True)
        .exists()
    )
    if activa:
        return True
    return base.filter(
        estado=SolicitudDescarga.ESTADO_RECHAZADA,
        codigo_estado__in=CODIGOS_RECHAZO_DEFINITIVO,
    ).exists()


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
    if solicitud.estado == SolicitudDescarga.ESTADO_RECHAZADA and not solicitud.id_solicitud:
        # Rechazo directo del SAT (ej. 5002 cuota agotada): no hay nada que verificar.
        LogDescargaSat.objects.create(
            nivel=LogDescargaSat.NIVEL_WARN,
            mensaje=(
                f"SAT rechazo solicitud {direccion} {fecha_inicial:%Y-%m-%d}: "
                f"codigo={solicitud.codigo_estado} {solicitud.error_detalle or ''}".strip()
            ),
            solicitud=solicitud,
        )
        return {"solicitud_id": "", "descargados": 0, "nuevos": 0}
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
    periodos = periodos_diarios_a_descargar(meses_atras)
    resultados: list[dict[str, int | str]] = []
    omitidos = 0
    fallidos: list[str] = []
    for fecha_inicial, fecha_final in periodos:
        for direccion in (SolicitudDescarga.DIRECCION_EMITIDOS, SolicitudDescarga.DIRECCION_RECIBIDOS):
            if _solicitud_periodo_registrada(fecha_inicial, fecha_final, direccion):
                omitidos += 1
                continue
            try:
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
                # Un dia fallido no debe bloquear el resto del run: registrar y seguir.
                detalle = f"{direccion} {fecha_inicial:%Y-%m-%d}: {exc}"
                fallidos.append(detalle)
                LogDescargaSat.objects.create(
                    nivel=LogDescargaSat.NIVEL_ERROR,
                    mensaje=f"Error SAT en {detalle}",
                )

    descargados = sum(int(item["descargados"]) for item in resultados)
    nuevos = sum(int(item["nuevos"]) for item in resultados)
    duracion = int(time.monotonic() - inicio)
    LogDescargaSat.objects.create(
        nivel=LogDescargaSat.NIVEL_INFO,
        mensaje=(
            f"Descarga SAT nocturna finalizada: {nuevos}/{descargados} CFDIs nuevos, "
            f"{omitidos} dias omitidos, {len(fallidos)} fallidos"
        ),
        cfdis_descargados=descargados,
        cfdis_nuevos=nuevos,
        duracion_segundos=duracion,
    )
    if fallidos:
        _alertar_error(
            "Descarga SAT nocturna con periodos fallidos:\n" + "\n".join(fallidos)
        )
    return {
        "status": "ok",
        "periodos": len(periodos),
        "omitidos": omitidos,
        "fallidos": len(fallidos),
        "descargados": descargados,
        "nuevos": nuevos,
        "duracion_segundos": duracion,
    }
