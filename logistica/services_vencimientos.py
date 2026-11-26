"""Avisos recuperables desde las fuentes de Logística, sin otra captura de documentos."""
import hashlib
import logging
from datetime import timedelta
from email.utils import parseaddr
from smtplib import SMTPResponseException, SMTPRecipientsRefused

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.mail import send_mail
from django.core.validators import validate_email
from django.db import connection
from django.db.models import F, Q
from django.utils import timezone
from django.utils.html import strip_tags

from core.email_rendering import render_email_to_string
from .models import AvisoVencimientoFlota, ConfigAlertaFlota, DocumentoUnidad, Repartidor

logger = logging.getLogger(__name__)
TIPO = ConfigAlertaFlota.TIPO_DOCUMENTO_VENCIMIENTO
BLOQUEO = 2026112601
MOTIVOS = {
    "config_no_activa": "Los avisos de vencimiento están desactivados.",
    "sin_destinatarios_institucionales": "No hay destinatarios institucionales activos y válidos. Revisa la configuración de alertas de flota.",
    "remitente_no_institucional": "El remitente de correo no es institucional o no es válido.",
    "correo_no_operativo": "El servicio de correo está configurado para pruebas y no puede notificar al personal.",
    "sin_ejecucion": "Todavía no se ha completado una revisión automática de vencimientos.",
    "revision_atrasada": "La revisión automática lleva más de 26 horas sin ejecutarse. Revisa el programador y el trabajador de avisos.",
    "procesando": "La revisión de vencimientos no ha terminado. Si persiste, revisa el trabajador de avisos.",
    "error_ejecucion": "La revisión automática falló. Revisa el trabajador de avisos; los pendientes se conservan.",
    "avisos_pendientes": "Hay avisos sin aceptación del servicio de correo. Los fallidos se reintentan en la próxima revisión; los resultados inciertos requieren cotejo con el proveedor.",
}


def _institucional(valor):
    correo = (valor or "").strip().lower()
    try:
        validate_email(correo)
    except ValidationError:
        return ""
    return correo if correo.rsplit("@", 1)[-1] == "pollyanasdolce.com" else ""


def configuracion_envio(config):
    seleccion = list(config.destinatarios.all()) if config else []
    candidatos = [u.email for u in seleccion if u.is_active] if seleccion else getattr(
        settings, "LOGISTICA_VENCIMIENTOS_DESTINATARIOS", []
    )
    emails = sorted({email for candidato in candidatos if (email := _institucional(candidato))})
    remitente = getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")
    if settings.EMAIL_BACKEND == "config.email_backends.ResendEmailBackend":
        remitente = getattr(settings, "RESEND_FROM", "") or remitente
    motivo = ""
    if config and not config.activa:
        motivo = "config_no_activa"
    elif not emails:
        motivo = "sin_destinatarios_institucionales"
    elif not _institucional(parseaddr(remitente)[1]):
        motivo = "remitente_no_institucional"
    elif not settings.IS_DEVELOPMENT_ENV and settings.EMAIL_BACKEND in {
        "django.core.mail.backends.console.EmailBackend", "django.core.mail.backends.filebased.EmailBackend",
        "django.core.mail.backends.dummy.EmailBackend", "django.core.mail.backends.locmem.EmailBackend",
    }:
        motivo = "correo_no_operativo"
    return {"destinatarios": emails, "remitente": remitente, "motivo": motivo, "respaldo": not seleccion}


def vencimientos_actuales(config=None):
    hoy = timezone.localdate()
    umbrales = {0, 30, 15} if config is None else {
        0, config.dias_anticipacion_1, config.dias_anticipacion_2, config.dias_anticipacion_3,
    }
    limite = hoy + timedelta(days=max(umbrales))
    licencias = Repartidor.objects.select_related("user").filter(user__is_active=True).exclude(
        tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA,
    ).filter(Q(licencia_expiracion__lte=limite) | Q(licencia_expiracion__isnull=True))
    documentos = DocumentoUnidad.objects.select_related("unidad").filter(
        vigente=True, unidad__activa=True, fecha_vencimiento__lte=limite,
    )
    eventos = []
    fuentes = [("licencia", r, r.licencia_expiracion, f"Licencia · {r}") for r in licencias]
    fuentes += [("documento", d, d.fecha_vencimiento, f"{d.get_tipo_display()} · {d.unidad.codigo}") for d in documentos]
    for fuente, objeto, fecha, titulo in fuentes:
        dias = (fecha - hoy).days if fecha else None
        umbral = min(u for u in umbrales if dias <= u) if fecha else 0
        estado = "Sin fecha registrada" if fecha is None else (
            "Vencido" if dias < 0 else "Vence hoy" if dias == 0 else f"Vence en {dias} días"
        )
        eventos.append({
            "fuente": fuente, "objeto_id": objeto.pk, "fecha_vencimiento": fecha,
            "umbral": umbral, "titulo": titulo, "estado": estado, "dias": dias,
        })
    return sorted(eventos, key=lambda e: (e["fecha_vencimiento"] or hoy - timedelta(days=36500), e["titulo"]))


def _clave(evento, email):
    valor = f'{evento["fuente"]}:{evento["objeto_id"]}:{evento["fecha_vencimiento"]}:{evento["umbral"]}:{email}'
    return hashlib.sha256(valor.encode()).hexdigest()


def resumen_avisos_vencimiento():
    """Lectura sin efectos: también detecta un programador caído o sin primera ejecución."""
    config = ConfigAlertaFlota.objects.prefetch_related("destinatarios").filter(tipo=TIPO).first()
    envio = configuracion_envio(config)
    eventos = vencimientos_actuales(config)
    claves = [_clave(e, email) for e in eventos for email in envio["destinatarios"]]
    estados = list(AvisoVencimientoFlota.objects.filter(clave__in=claves).values_list("estado_envio", flat=True))
    pendientes = len(claves) - estados.count("aceptado")
    motivo = envio["motivo"]
    if not motivo:
        if not config or not config.ultima_ejecucion:
            motivo = "sin_ejecucion"
        elif config.ultima_ejecucion < timezone.now() - timedelta(hours=26):
            motivo = "revision_atrasada"
        elif config.ultimo_resultado.get("motivo"):
            motivo = config.ultimo_resultado["motivo"]
        elif pendientes:
            motivo = "avisos_pendientes"
    return {
        **envio, "motivo": motivo, "mensaje": MOTIVOS.get(motivo, "La revisión requiere atención." if motivo else ""),
        "eventos": eventos, "pendientes": pendientes, "inciertos": estados.count("incierto"),
        "ultima_ejecucion": config.ultima_ejecucion if config else None,
    }


def _enviar_evento(evento, email, remitente):
    # La revisión puede tardar varios envíos: respetar renovaciones, bajas y
    # cambios de destinatarios ocurridos desde que se hizo el barrido inicial.
    config = ConfigAlertaFlota.objects.filter(tipo=TIPO).first()
    envio = configuracion_envio(config)
    if envio["motivo"] or email not in envio["destinatarios"]:
        return False
    if evento["fuente"] == "licencia":
        vigente = Repartidor.objects.filter(
            pk=evento["objeto_id"], licencia_expiracion=evento["fecha_vencimiento"], user__is_active=True,
        ).exclude(tipo_identidad=Repartidor.TIPO_CUENTA_TECNICA).exists()
    else:
        vigente = DocumentoUnidad.objects.filter(
            pk=evento["objeto_id"], fecha_vencimiento=evento["fecha_vencimiento"], vigente=True, unidad__activa=True,
        ).exists()
    if not vigente:
        return False
    registro, _ = AvisoVencimientoFlota.objects.get_or_create(
        clave=_clave(evento, email), defaults={
            **{campo: evento[campo] for campo in ("fuente", "objeto_id", "fecha_vencimiento", "umbral")},
            "destinatario": email,
        },
    )
    if registro.estado_envio in {"aceptado", "incierto"}:
        return False
    # Un proceso interrumpido pudo alcanzar al proveedor: no reenviar a ciegas.
    if registro.estado_envio == "enviando":
        if registro.actualizado_en < timezone.now() - timedelta(minutes=15):
            AvisoVencimientoFlota.objects.filter(pk=registro.pk, estado_envio="enviando").update(
                estado_envio="incierto", error="envio_interrumpido", actualizado_en=timezone.now(),
            )
        return False
    reclamado = AvisoVencimientoFlota.objects.filter(pk=registro.pk, estado_envio__in=["pendiente", "fallido"]).update(
        estado_envio="enviando", intentos=F("intentos") + 1, actualizado_en=timezone.now(), error="",
    )
    if not reclamado:
        return False
    try:
        html = render_email_to_string("logistica/emails/aviso_vencimiento.html", {"evento": evento})
        aceptadas = send_mail(
            subject=f'{evento["estado"]} · {evento["titulo"]}', message=strip_tags(html),
            from_email=remitente, recipient_list=[email], html_message=html, fail_silently=False,
        )
    except Exception as exc:
        # No guardar texto de excepciones del transporte: puede contener credenciales.
        logger.error("No se pudo enviar aviso de vencimiento id=%s", registro.pk)
        rechazo = isinstance(exc, (SMTPResponseException, SMTPRecipientsRefused)) or (
            isinstance(exc, RuntimeError) and (
                str(exc) == "RESEND_API_KEY no configurado."
                or str(exc).startswith("Resend API error (4")
            )
        )
        AvisoVencimientoFlota.objects.filter(pk=registro.pk).update(
            estado_envio="fallido" if rechazo else "incierto",
            error="correo_rechazado" if rechazo else "resultado_desconocido", actualizado_en=timezone.now(),
        )
        return False
    AvisoVencimientoFlota.objects.filter(pk=registro.pk).update(
        estado_envio="aceptado" if aceptadas == 1 else "fallido",
        aceptado_en=timezone.now() if aceptadas == 1 else None,
        error="" if aceptadas == 1 else "correo_no_aceptado", actualizado_en=timezone.now(),
    )
    return aceptadas == 1


def procesar_avisos_vencimiento():
    # Candado PostgreSQL de sesión: sobrevive a los commits de la bitácora y se
    # libera al terminar el proceso. No mantener transacciones abiertas en SMTP.
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", [BLOQUEO])
        adquirido = cursor.fetchone()[0]
    if not adquirido:
        return {"enviadas": 0, "vencidos_marcados": 0, "motivo": "revision_en_curso"}
    config = None
    try:
        config, _ = ConfigAlertaFlota.objects.get_or_create(tipo=TIPO)
        config.ultima_ejecucion = timezone.now()
        config.ultimo_resultado = {"motivo": "procesando"}
        config.save(update_fields=["ultima_ejecucion", "ultimo_resultado"])
        envio = configuracion_envio(config)
        resultado = {"enviadas": 0, "vencidos_marcados": 0, "motivo": envio["motivo"]}
        if not envio["motivo"]:
            for evento in vencimientos_actuales(config):
                for email in envio["destinatarios"]:
                    resultado["enviadas"] += int(_enviar_evento(evento, email, envio["remitente"]))
            # El estado aún dice "procesando"; aquí interesa el conteo persistido.
            resumen = resumen_avisos_vencimiento()
            resultado["pendientes"] = resumen["pendientes"]
            resultado["motivo"] = "avisos_pendientes" if resumen["pendientes"] else ""
        config.ultimo_resultado = resultado
        config.save(update_fields=["ultimo_resultado"])
        return resultado
    except Exception:
        if config:
            ConfigAlertaFlota.objects.filter(pk=config.pk).update(ultimo_resultado={"motivo": "error_ejecucion"})
        raise
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", [BLOQUEO])
