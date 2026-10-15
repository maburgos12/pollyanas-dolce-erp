from __future__ import annotations

import time
import calendar
from datetime import date, timedelta

from celery import shared_task
from django.conf import settings
from django.core.mail import send_mail
from django.db import connection, transaction
from django.db.models import F, Min, Q
from django.utils import timezone

from sat_client.models import (
    CfdiDescargado,
    CfdiPagoRelacionado,
    LogDescargaSat,
    SolicitudDescarga,
)
from sat_client.services.autenticacion import obtener_token
from sat_client.services.base import SatConfigurationError, SatRequestLimitExceeded, SatServiceError
from sat_client.services.descarga import (
    descargar_paquete,
    extraer_xmls_de_zip_base64,
    guardar_cfdis_xml,
    extraer_metadata,
    parse_cfdi_xml,
)
from sat_client.services.solicitud import solicitar_descarga_periodo
from sat_client.services.verificacion import verificar_hasta_terminar


def _restar_meses(fecha: date, meses: int) -> date:
    year = fecha.year
    month = fecha.month - meses
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def periodos_diarios_a_descargar(
    meses_atras: int, *, hoy: date | None = None
) -> list[tuple[date, date]]:
    hoy = hoy or timezone.localdate()
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


def _solicitud_periodo_registrada(
    fecha_inicial: date, fecha_final: date, direccion: str
) -> bool:
    rfc = (getattr(settings, "SAT_RFC", "") or "").strip().upper()
    if not rfc:
        return False
    # Evitar repetir el XML inicial; la metadata renueva la cobertura fiscal.
    base = SolicitudDescarga.objects.filter(
        fecha_inicial__lte=fecha_inicial,
        fecha_final__gte=fecha_final,
        rfc_solicitante=rfc,
        tipo_solicitud=SolicitudDescarga.TIPO_CFDI,
        direccion=direccion,
        folio="",
    )
    terminada_importada = (
        base.filter(
            estado=SolicitudDescarga.ESTADO_TERMINADA,
            logs__nivel=LogDescargaSat.NIVEL_INFO,
            logs__cfdis_descargados__gte=F("numero_cfdis"),
        )
        .exclude(id_solicitud="")
        .exclude(id_solicitud__isnull=True)
        .exists()
    )
    if terminada_importada:
        return True
    return base.filter(
        estado=SolicitudDescarga.ESTADO_RECHAZADA,
        codigo_estado="5004",
    ).exists()


def _alerta_repetida_reciente(mensaje: str, *, horas: int = 24) -> bool:
    """Evita reenviar por correo el mismo error ya alertado en las últimas horas."""
    limite = timezone.now() - timedelta(hours=horas)
    return LogDescargaSat.objects.filter(
        nivel=LogDescargaSat.NIVEL_ERROR,
        mensaje=mensaje,
        creado_en__gte=limite,
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


def procesar_solicitud(solicitud: SolicitudDescarga) -> dict:
    """Un paso reanudable: verificar una vez e importar paquetes pendientes."""
    if solicitud.estado in (1, 2, 4) and solicitud.id_solicitud:
        solicitud = verificar_hasta_terminar(solicitud, max_intentos=1)
    result = {
        "solicitud_id": solicitud.id_solicitud or "",
        "descargados": 0,
        "nuevos": 0,
        "estado": "pendiente",
    }
    if solicitud.estado != 3:
        if solicitud.estado in (5, 6):
            result["estado"] = "bloqueada"
        return result
    if solicitud.numero_cfdis is None or (
        solicitud.numero_cfdis and not solicitud.ids_paquetes
    ):
        raise SatServiceError("Solicitud terminada sin inventario de paquetes/CFDI")
    # Las solicitudes anteriores al checkpoint conservan su evidencia de importacion.
    if (
        solicitud.tipo_solicitud == "CFDI"
        and solicitud.logs.filter(
            nivel="INFO", cfdis_descargados__gte=solicitud.numero_cfdis
        ).exists()
    ):
        result["estado"] = "completa"
        return result
    for paquete in solicitud.ids_paquetes:
        with transaction.atomic():
            solicitud = SolicitudDescarga.objects.select_for_update().get(
                pk=solicitud.pk
            )
            if paquete in solicitud.paquetes_procesados:
                continue
            if paquete not in solicitud.paquetes_pendientes:
                try:
                    contenido = descargar_paquete(paquete, token=obtener_token())
                except SatServiceError as exc:
                    if exc.code != "5008":
                        raise
                    solicitud.estado = SolicitudDescarga.ESTADO_VENCIDA
                    solicitud.codigo_estado = exc.code
                    solicitud.error_detalle = str(exc)
                    solicitud.save(update_fields=[
                        "estado", "codigo_estado", "error_detalle", "actualizado_en"
                    ])
                    result["estado"] = "bloqueada"
                    return result
                solicitud.paquetes_pendientes[paquete] = contenido
                solicitud.save(update_fields=["paquetes_pendientes", "actualizado_en"])
        # Guardar el paquete antes de interpretar: SAT limita sus descargas.
        with transaction.atomic():
            solicitud = SolicitudDescarga.objects.select_for_update().get(pk=solicitud.pk)
            if paquete in solicitud.paquetes_procesados:
                continue
            contenido = solicitud.paquetes_pendientes[paquete]
            if solicitud.tipo_solicitud == "Metadata":
                rows = extraer_metadata(
                    contenido,
                    rfc=solicitud.rfc_solicitante,
                    direccion=solicitud.direccion,
                )
                solicitud.metadata = (solicitud.metadata or []) + rows
                total, nuevos = len(rows), 0
            else:
                xmls = extraer_xmls_de_zip_base64(contenido)
                for xml in xmls:
                    parsed = parse_cfdi_xml(xml)
                    rfc = (
                        parsed.rfc_emisor
                        if solicitud.direccion == "emitidos"
                        else parsed.rfc_receptor
                    )
                    if rfc != solicitud.rfc_solicitante or (
                        solicitud.folio and parsed.uuid != solicitud.folio
                    ):
                        raise SatServiceError(
                            "XML no corresponde al RFC/folio solicitado"
                        )
                total, nuevos = guardar_cfdis_xml(
                    xmls,
                    solicitud=solicitud,
                    tipo_cfdi=_tipo_cfdi_desde_direccion(solicitud.direccion),
                )
            solicitud.paquetes_procesados[paquete] = total
            del solicitud.paquetes_pendientes[paquete]
            solicitud.save(
                update_fields=["paquetes_procesados", "paquetes_pendientes", "metadata", "actualizado_en"]
            )
            result["descargados"] += total
            result["nuevos"] += nuevos
    total = sum(solicitud.paquetes_procesados.values())
    if total != solicitud.numero_cfdis:
        raise SatServiceError(
            f"Paquetes incompletos: {total} de {solicitud.numero_cfdis}"
        )
    if solicitud.tipo_solicitud == "Metadata" and solicitud.metadata is None:
        solicitud.metadata = []
        solicitud.save(update_fields=["metadata"])
    LogDescargaSat.objects.get_or_create(
        solicitud=solicitud,
        nivel="INFO",
        cfdis_descargados=total,
        defaults={
            "mensaje": f"Importacion SAT completa: {solicitud.tipo_solicitud} {solicitud.direccion}",
            "cfdis_nuevos": result["nuevos"],
        },
    )
    result["estado"] = "completa"
    return result


def _procesar_periodo_direccion(
    fecha_inicial: date, fecha_final: date, direccion: str
) -> dict:
    solicitud = solicitar_descarga_periodo(
        fecha_inicial=fecha_inicial,
        fecha_final=fecha_final,
        direccion=direccion,
        token=obtener_token(),
    )
    result = procesar_solicitud(solicitud)
    if result["estado"] != "completa":
        LogDescargaSat.objects.create(
            nivel="WARN",
            solicitud=solicitud,
            mensaje=f"SAT pendiente: {direccion} {fecha_inicial} estado={solicitud.estado} codigo={solicitud.codigo_estado}",
        )
    return result


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


def conciliar_metadata(solicitudes) -> dict:
    inventario = {}
    for solicitud in sorted(solicitudes, key=lambda s: s.creado_en):
        if (
            solicitud.estado != 3
            or solicitud.metadata is None
            or sum(solicitud.paquetes_procesados.values()) != solicitud.numero_cfdis
        ):
            continue
        for row in solicitud.metadata:
            inventario[row["uuid"]] = (
                row,
                solicitud.direccion,
                solicitud.rfc_solicitante,
            )
    originales = {
        c.uuid: c
        for c in CfdiDescargado.objects.filter(uuid__in=inventario).defer("xml_raw")
    }
    con_xml = set(
        CfdiDescargado.objects.filter(uuid__in=inventario)
        .exclude(xml_raw="")
        .exclude(xml_raw__isnull=True)
        .values_list("uuid", flat=True)
    )
    faltantes = {}
    for uuid, (row, direccion, rfc) in inventario.items():
        original = originales.get(uuid)
        if original:
            own_rfc = (
                original.rfc_emisor
                if direccion == "emitidos"
                else original.rfc_receptor
            )
            if own_rfc != rfc:
                raise SatServiceError("Inventario SAT no corresponde al RFC del XML")
            if original.estatus != row["estatus"]:
                CfdiDescargado.objects.filter(pk=original.pk).update(
                    estatus=row["estatus"]
                )
        if uuid not in con_xml:
            faltantes[uuid] = (row, direccion)
    result = {
        "inventario": len(inventario),
        "faltantes": len(faltantes),
        "cancelados_sin_xml": 0,
        "nuevos": 0,
        "errores": [],
    }
    for uuid, (row, direccion) in faltantes.items():
        if row["estatus"] == "cancelado":
            result["cancelados_sin_xml"] += 1
            continue
        try:
            fecha = date.fromisoformat(row["fecha_emision"][:10])
            solicitud = solicitar_descarga_periodo(
                fecha_inicial=fecha,
                fecha_final=fecha,
                direccion=direccion,
                folio=uuid,
                token=obtener_token(),
            )
            processed = procesar_solicitud(solicitud)
            result["nuevos"] += processed["nuevos"]
        except SatServiceError as exc:
            result["errores"].append(f"{uuid}: {exc}")
    presentes = set(
        CfdiDescargado.objects.filter(uuid__in=faltantes)
        .exclude(xml_raw="")
        .exclude(xml_raw__isnull=True)
        .values_list("uuid", flat=True)
    )
    result["faltantes"] = len(set(faltantes) - presentes)
    return result


def actualizar_cobertura_sat(*, desde: date | None = None) -> dict:
    """Metadata mensual renovable y recuperacion selectiva; nunca repite XML completos."""
    rfc = (getattr(settings, "SAT_RFC", "") or "").strip().upper()
    if not rfc:
        raise SatConfigurationError("Falta SAT_RFC")
    ayer = timezone.localdate() - timedelta(days=1)
    primera = CfdiDescargado.objects.aggregate(fecha=Min("fecha_emision"))["fecha"]
    inicio = (
        desde
        or (
            timezone.localtime(primera).date()
            if primera
            else ayer.replace(month=1, day=1)
        )
    ).replace(day=1)
    pendientes, errores = [], []
    while inicio <= ayer:
        fin = min(
            inicio.replace(day=calendar.monthrange(inicio.year, inicio.month)[1]), ayer
        )
        for direccion in ("emitidos", "recibidos"):
            try:
                latest = SolicitudDescarga.objects.filter(
                    rfc_solicitante=rfc,
                    direccion=direccion,
                    tipo_solicitud="Metadata",
                    fecha_inicial=inicio,
                    folio="",
                ).first()
                reciente = latest and latest.estado != 6 and latest.creado_en >= timezone.now() - timedelta(
                    days=7
                )
                reanudable = latest and (
                    latest.estado in (1, 2)
                    or (
                        latest.estado == 4
                        and (latest.error_detalle or "").startswith("Timeout esperando")
                    )
                )
                if not reanudable and (not reciente or latest.fecha_final < fin):
                    latest = solicitar_descarga_periodo(
                        fecha_inicial=inicio,
                        fecha_final=fin,
                        direccion=direccion,
                        tipo_solicitud="Metadata",
                        token=obtener_token(),
                    )
                if latest and (reanudable or latest.estado == 3):
                    processed = procesar_solicitud(latest)
                    if processed["estado"] != "completa":
                        pendientes.append(latest.pk)
                elif latest and not (
                    latest.estado == 5 and latest.codigo_estado == "5004"
                ):
                    pendientes.append(latest.pk)
            except SatServiceError as exc:
                errores.append(f"Metadata {direccion} {inicio}: {exc}")
        inicio = fin.replace(day=1) + timedelta(days=32)
        inicio = inicio.replace(day=1)
    snapshots = SolicitudDescarga.objects.filter(
        rfc_solicitante=rfc, tipo_solicitud="Metadata", estado=3, metadata__isnull=False
    )
    result = conciliar_metadata(snapshots)
    # Un complemento puede referir una factura anterior al inventario consultado.
    presentes = (
        CfdiDescargado.objects.exclude(xml_raw="")
        .exclude(xml_raw__isnull=True)
        .values("uuid")
    )
    refs = CfdiPagoRelacionado.objects.exclude(
        uuid_relacionado__in=presentes
    ).select_related("cfdi_pago")
    vistos = {
        row["uuid"] for snapshot in snapshots for row in (snapshot.metadata or [])
    }
    for pago in refs:
        if pago.uuid_relacionado in vistos:
            continue
        vistos.add(pago.uuid_relacionado)
        try:
            direccion = (
                "emitidos" if pago.cfdi_pago.tipo_cfdi == "emitido" else "recibidos"
            )
            # En consulta por folio estas fechas son referencia de pago, no emision inferida.
            fecha = timezone.localtime(pago.fecha_pago).date()
            solicitud = solicitar_descarga_periodo(
                fecha_inicial=fecha,
                fecha_final=fecha,
                direccion=direccion,
                folio=pago.uuid_relacionado,
                token=obtener_token(),
            )
            processed = procesar_solicitud(solicitud)
            result["nuevos"] += processed["nuevos"]
            if processed["estado"] != "completa":
                pendientes.append(solicitud.pk)
        except SatServiceError as exc:
            errores.append(f"Referencia {pago.uuid_relacionado}: {exc}")
    result.update(
        pendientes=sorted(set(pendientes)), errores=result["errores"] + errores
    )
    LogDescargaSat.objects.create(
        nivel=(
            "WARN"
            if result["faltantes"] or result["pendientes"] or result["errores"]
            else "INFO"
        ),
        mensaje=f"Cobertura SAT: {result['inventario']} UUID inventariados; {result['faltantes']} sin XML; {len(result['pendientes'])} solicitudes pendientes; {len(result['errores'])} errores",
        cfdis_nuevos=result["nuevos"],
    )
    return result


@shared_task(
    name="sat_client.ejecutar_descarga_sat_nocturna",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def ejecutar_descarga_sat_nocturna(self):
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(74521014)")
        locked = cursor.fetchone()[0]
    if not locked:
        return {"status": "en_ejecucion"}
    try:
        return _ejecutar_descarga_sat_nocturna()
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(74521014)")


def _ejecutar_descarga_sat_nocturna():
    inicio = time.monotonic()
    if not getattr(settings, "SAT_DESCARGA_ENABLED", False):
        return {"status": "deshabilitada"}

    meses_atras = max(1, int(getattr(settings, "SAT_DESCARGA_MESES_ATRAS", 1)))
    periodos = periodos_diarios_a_descargar(meses_atras)
    resultados: list[dict[str, int | str]] = []
    omitidos = 0
    fallidos: list[str] = []
    pendientes_anteriores = SolicitudDescarga.objects.filter(
        rfc_solicitante=(getattr(settings, "SAT_RFC", "") or "").strip().upper(),
        tipo_solicitud="CFDI",
    ).filter(
        Q(estado__in=[1, 2])
        | Q(estado=4, error_detalle__startswith="Timeout esperando")
    )
    for solicitud in pendientes_anteriores:
        try:
            resultados.append(procesar_solicitud(solicitud))
        except SatServiceError as exc:
            fallidos.append(f"Solicitud {solicitud.pk}: {exc}")
    for fecha_inicial, fecha_final in periodos:
        for direccion in (
            SolicitudDescarga.DIRECCION_EMITIDOS,
            SolicitudDescarga.DIRECCION_RECIBIDOS,
        ):
            if _solicitud_periodo_registrada(fecha_inicial, fecha_final, direccion):
                omitidos += 1
                continue
            try:
                resultados.extend(
                    _procesar_con_split(fecha_inicial, fecha_final, direccion)
                )
            except SatConfigurationError as exc:
                mensaje = f"Descarga SAT no configurada: {exc}"
                # Consultar ANTES de crear el log de hoy, para no encontrarnos a nosotros mismos.
                alerta_repetida = _alerta_repetida_reciente(mensaje)
                LogDescargaSat.objects.create(
                    nivel=LogDescargaSat.NIVEL_ERROR,
                    mensaje=mensaje,
                    duracion_segundos=int(time.monotonic() - inicio),
                )
                if not alerta_repetida:
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

    pendientes = sum(
        item.get("estado") in ("pendiente", "bloqueada") for item in resultados
    )
    cobertura = actualizar_cobertura_sat()
    descargados = sum(int(item["descargados"]) for item in resultados)
    nuevos = sum(int(item["nuevos"]) for item in resultados) + cobertura.get("nuevos", 0)
    descargados += cobertura.get("nuevos", 0)
    duracion = int(time.monotonic() - inicio)
    parcial = bool(
        fallidos
        or pendientes
        or cobertura["faltantes"]
        or cobertura["pendientes"]
        or cobertura["errores"]
    )
    LogDescargaSat.objects.create(
        nivel="WARN" if parcial else "INFO",
        mensaje=(
            f"Descarga SAT nocturna finalizada: {nuevos}/{descargados} CFDIs nuevos, "
            f"{omitidos} dias omitidos, {len(fallidos)} fallidos, {pendientes} pendientes; "
            f"metadata: {cobertura['faltantes']} sin XML, {len(cobertura['errores'])} errores"
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
        "status": "parcial" if parcial else "ok",
        "cobertura": cobertura,
        "pendientes": pendientes,
        "periodos": len(periodos),
        "omitidos": omitidos,
        "fallidos": len(fallidos),
        "descargados": descargados,
        "nuevos": nuevos,
        "duracion_segundos": duracion,
    }
