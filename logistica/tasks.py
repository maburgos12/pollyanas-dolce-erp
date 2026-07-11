from datetime import date, timedelta

from celery import shared_task
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.db import OperationalError, transaction
from django.utils import timezone
from django.utils.html import strip_tags

from core.access import can_view_module, group_name_variants
from core.email_rendering import render_email_to_string

from .models import (
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    ConfigAlertaFlota,
    DocumentoUnidad,
    EventoRuta,
    LavadoUnidad,
    ReporteUnidad,
    RutaEntrega,
    ServicioRealizadoUnidad,
    Unidad,
)
from .services_combustible_auditoria import auditar_carga_combustible
from .services_auditoria_entregas import auditar_entregas_ruta
from .services_rutas_control import detectar_gps_perdido


def _emails_de_grupo(nombre_grupo: str) -> list[str]:
    return list(
        get_user_model()
        .objects.filter(groups__name__in=group_name_variants(nombre_grupo), email__isnull=False)
        .exclude(email="")
        .values_list("email", flat=True)
        .distinct()
    )


def _emails_con_acceso_modulo(modulo: str) -> list[str]:
    usuarios = (
        get_user_model()
        .objects.filter(is_active=True)
        .exclude(email__isnull=True)
        .exclude(email="")
    )
    return sorted({usuario.email for usuario in usuarios if can_view_module(usuario, modulo)})


def _emails_de_usuarios(usuarios) -> list[str]:
    return list(
        usuarios.exclude(email__isnull=True)
        .exclude(email="")
        .values_list("email", flat=True)
        .distinct()
    )


def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")


@shared_task
def auditar_ticket_combustible(carga_id):
    try:
        return auditar_carga_combustible(carga_id)
    except CargaCombustibleUnidad.DoesNotExist:
        return {"estado": "no_encontrado", "carga_id": carga_id}


@shared_task
def detectar_gps_perdido_rutas(umbral_minutos: int = 10):
    fecha = timezone.localdate()
    rutas = RutaEntrega.objects.filter(
        fecha_ruta=fecha,
        estatus=RutaEntrega.ESTATUS_EN_RUTA,
    ).order_by("id")
    revisadas = rutas.count()
    eventos = []

    for ruta in rutas:
        with transaction.atomic():
            ruta_bloqueada = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
            evento = detectar_gps_perdido(ruta_bloqueada, umbral_minutos=umbral_minutos)
        if evento:
            eventos.append(evento.id)

    return {
        "fecha": fecha.isoformat(),
        "rutas_revisadas": revisadas,
        "eventos_gps_perdido": len(set(eventos)),
    }


@shared_task(
    name="logistica.tasks.auditar_entregas_ruta_task",
    autoretry_for=(OperationalError,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    max_retries=3,
)
def auditar_entregas_ruta_task(ruta_id: int | None = None, fecha: str | None = None):
    if ruta_id is not None or fecha:
        fecha_ruta = date.fromisoformat(fecha) if fecha else None
        return auditar_entregas_ruta(ruta_id=ruta_id, fecha=fecha_ruta)
    hoy = timezone.localdate()
    resultados = [
        auditar_entregas_ruta(fecha=hoy - timedelta(days=1)),
        auditar_entregas_ruta(fecha=hoy),
    ]
    return {
        "rutas_revisadas": sum(row["rutas_revisadas"] for row in resultados),
        "paradas_revisadas": sum(row["paradas_revisadas"] for row in resultados),
        "hallazgos": [item for row in resultados for item in row["hallazgos"]],
        "alertas_creadas": sum(row["alertas_creadas"] for row in resultados),
        "dry_run": False,
        "ventana_fechas": [(hoy - timedelta(days=1)).isoformat(), hoy.isoformat()],
    }


@shared_task
def notificar_reporte_nuevo(reporte_id):
    try:
        reporte = ReporteUnidad.objects.select_related("unidad", "repartidor__user").get(pk=reporte_id)
    except ReporteUnidad.DoesNotExist:
        return {"enviado": False, "motivo": "reporte_no_encontrado", "reporte_id": reporte_id}

    context = {
        "reporte": reporte,
        "ticket_url": f"/logistica/tickets/?ticket={reporte.id}",
    }
    html_message = render_email_to_string("logistica/emails/reporte_nuevo.html", context)
    plain_message = strip_tags(html_message)
    from_email = _from_email()

    compras_emails = _emails_de_grupo("compras_logistica")
    dg_emails = _emails_de_grupo("dg") if reporte.severidad in ["urgente", "critico"] else []

    enviados = 0
    if compras_emails:
        send_mail(
            subject=f"Nuevo reporte de unidad {reporte.unidad.codigo}",
            message=plain_message,
            from_email=from_email,
            recipient_list=compras_emails,
            html_message=html_message,
            fail_silently=False,
        )
        enviados += 1

    if dg_emails:
        send_mail(
            subject=f"Reporte {reporte.get_severidad_display()} de unidad {reporte.unidad.codigo}",
            message=plain_message,
            from_email=from_email,
            recipient_list=dg_emails,
            html_message=html_message,
            fail_silently=False,
        )
        enviados += 1

    return {"enviado": bool(enviados), "grupos_notificados": enviados, "reporte_id": reporte.id}


@shared_task
def notificar_desvio_ruta_automatico(evento_id):
    try:
        evento = EventoRuta.objects.select_related("ruta__repartidor__user", "ruta__unidad_operativa").get(pk=evento_id)
    except EventoRuta.DoesNotExist:
        return {"enviado": False, "motivo": "evento_no_encontrado", "evento_id": evento_id}

    context = {
        "evento": evento,
        "control_url": "/logistica/rutas/control/",
    }
    html_message = render_email_to_string("logistica/emails/desvio_ruta.html", context)
    plain_message = strip_tags(html_message)
    from_email = _from_email()

    destinatarios = sorted(set(_emails_de_grupo("dg")) | set(_emails_con_acceso_modulo("logistica")))
    if not destinatarios:
        return {"enviado": False, "motivo": "sin_destinatarios", "evento_id": evento.id}

    send_mail(
        subject=f"Desvío de ruta detectado · {evento.ruta.folio}",
        message=plain_message,
        from_email=from_email,
        recipient_list=destinatarios,
        html_message=html_message,
        fail_silently=False,
    )
    return {"enviado": True, "destinatarios": len(destinatarios), "evento_id": evento.id}


@shared_task
def escalar_tickets_sin_respuesta():
    limite = timezone.now() - timedelta(hours=2)
    tickets = ReporteUnidad.objects.select_related("unidad", "repartidor__user").filter(
        estatus=ReporteUnidad.ESTATUS_ABIERTO,
        severidad__in=[ReporteUnidad.SEVERIDAD_URGENTE, ReporteUnidad.SEVERIDAD_CRITICO],
        fecha_reporte__lte=limite,
        notificacion_escalada=False,
    )
    ticket_ids = list(tickets.values_list("id", flat=True))
    if not ticket_ids:
        return {"escalados": 0}

    dg_emails = _emails_de_grupo("dg")
    if dg_emails:
        html_message = render_email_to_string("logistica/emails/escalado.html", {"tickets": tickets})
        send_mail(
            subject="Escalado de tickets de logística sin respuesta",
            message=strip_tags(html_message),
            from_email=_from_email(),
            recipient_list=dg_emails,
            html_message=html_message,
            fail_silently=False,
        )

    actualizados = ReporteUnidad.objects.filter(id__in=ticket_ids).update(notificacion_escalada=True)
    return {"escalados": actualizados}


def _config_alerta(tipo: str):
    try:
        return ConfigAlertaFlota.objects.prefetch_related("destinatarios").get(tipo=tipo, activa=True)
    except ConfigAlertaFlota.DoesNotExist:
        return None


def _dias_configurados(config: ConfigAlertaFlota) -> list[int]:
    return sorted({config.dias_anticipacion_1, config.dias_anticipacion_2, config.dias_anticipacion_3}, reverse=True)


@shared_task
def alertar_documentos_por_vencer():
    config = _config_alerta("documento_vencimiento")
    if not config:
        return {"enviadas": 0, "vencidos_marcados": 0, "motivo": "config_no_activa"}

    hoy = timezone.localdate()
    emails = _emails_de_usuarios(config.destinatarios.all())
    enviadas = 0

    if emails:
        for dias in _dias_configurados(config):
            fecha_objetivo = hoy + timedelta(days=dias)
            documentos = DocumentoUnidad.objects.select_related("unidad").filter(
                fecha_vencimiento=fecha_objetivo,
                vigente=True,
            )
            for documento in documentos:
                html_message = render_email_to_string(
                    "logistica/emails/alerta_documento.html",
                    {
                        "documento": documento,
                        "dias_restantes": dias,
                        "unidad": documento.unidad,
                    },
                )
                subject = (
                    f"VENCE HOY - {documento.unidad.codigo} · {documento.get_tipo_display()}"
                    if dias == 0
                    else f"Documento por vencer - {documento.unidad.codigo} · {documento.get_tipo_display()} · {dias} días"
                )
                send_mail(
                    subject=subject,
                    message=strip_tags(html_message),
                    from_email=_from_email(),
                    recipient_list=emails,
                    html_message=html_message,
                    fail_silently=False,
                )
                enviadas += 1

    vencidos_marcados = DocumentoUnidad.objects.filter(fecha_vencimiento__lt=hoy, vigente=True).update(vigente=False)
    return {"enviadas": enviadas, "vencidos_marcados": vencidos_marcados}


@shared_task
def alertar_servicios_proximos():
    config = _config_alerta("servicio_proximo")
    if not config:
        return {"enviadas": 0, "motivo": "config_no_activa"}

    hoy = timezone.localdate()
    emails = _emails_de_usuarios(config.destinatarios.all())
    if not emails:
        return {"enviadas": 0, "motivo": "sin_destinatarios"}

    enviadas = 0
    for dias in _dias_configurados(config):
        fecha_objetivo = hoy + timedelta(days=dias)
        servicios = ServicioRealizadoUnidad.objects.select_related("unidad", "tipo_servicio").filter(
            proxima_fecha=fecha_objetivo
        )
        for servicio in servicios:
            html_message = render_email_to_string(
                "logistica/emails/alerta_servicio.html",
                {
                    "servicio": servicio,
                    "dias_restantes": dias,
                    "unidad": servicio.unidad,
                    "km_actual": None,
                    "alerta_tipo": "fecha",
                },
            )
            subject = (
                f"SERVICIO HOY - {servicio.unidad.codigo} · {servicio.tipo_servicio.nombre}"
                if dias == 0
                else f"Servicio próximo - {servicio.unidad.codigo} · {servicio.tipo_servicio.nombre} · en {dias} días"
            )
            send_mail(
                subject=subject,
                message=strip_tags(html_message),
                from_email=_from_email(),
                recipient_list=emails,
                html_message=html_message,
                fail_silently=False,
            )
            enviadas += 1

    servicios_por_km = ServicioRealizadoUnidad.objects.select_related("unidad", "tipo_servicio").filter(
        proximos_km__isnull=False
    )
    for servicio in servicios_por_km:
        km_actual = (
            BitacoraSalidaLlegada.objects.filter(
                unidad=servicio.unidad,
                cerrada=True,
                km_llegada__isnull=False,
            )
            .order_by("-hora_llegada", "-fecha")
            .values_list("km_llegada", flat=True)
            .first()
        )
        if km_actual is None or km_actual < servicio.proximos_km - 500:
            continue

        vencido = km_actual >= servicio.proximos_km
        html_message = render_email_to_string(
            "logistica/emails/alerta_servicio.html",
            {
                "servicio": servicio,
                "dias_restantes": None,
                "unidad": servicio.unidad,
                "km_actual": km_actual,
                "alerta_tipo": "km",
            },
        )
        subject = (
            f"Servicio VENCIDO por km - {servicio.unidad.codigo} · {servicio.tipo_servicio.nombre}"
            if vencido
            else f"Servicio próximo por km - {servicio.unidad.codigo} · {servicio.tipo_servicio.nombre}"
        )
        send_mail(
            subject=subject,
            message=strip_tags(html_message),
            from_email=_from_email(),
            recipient_list=emails,
            html_message=html_message,
            fail_silently=False,
        )
        enviadas += 1

    return {"enviadas": enviadas}


@shared_task
def alertar_lavados_pendientes():
    config = _config_alerta("lavado_pendiente")
    if not config:
        return {"enviadas": 0, "motivo": "config_no_activa"}

    emails = _emails_de_usuarios(config.destinatarios.all())
    if not emails:
        return {"enviadas": 0, "motivo": "sin_destinatarios"}

    hoy = timezone.localdate()
    limite = hoy - timedelta(days=15)
    enviadas = 0

    for unidad in Unidad.objects.filter(activa=True).order_by("codigo"):
        ultimo_lavado = LavadoUnidad.objects.filter(unidad=unidad).order_by("-fecha").first()
        if ultimo_lavado and ultimo_lavado.fecha >= limite:
            continue

        dias_sin_lavar = (hoy - ultimo_lavado.fecha).days if ultimo_lavado else None
        html_message = render_email_to_string(
            "logistica/emails/alerta_lavado.html",
            {
                "unidad": unidad,
                "dias_sin_lavar": dias_sin_lavar,
                "ultimo_lavado": ultimo_lavado.fecha if ultimo_lavado else None,
            },
        )
        subject = (
            f"Lavado pendiente - {unidad.codigo} · {dias_sin_lavar} días sin lavar"
            if ultimo_lavado
            else f"Sin registro de lavado - {unidad.codigo}"
        )
        send_mail(
            subject=subject,
            message=strip_tags(html_message),
            from_email=_from_email(),
            recipient_list=emails,
            html_message=html_message,
            fail_silently=False,
        )
        enviadas += 1

    return {"enviadas": enviadas}
