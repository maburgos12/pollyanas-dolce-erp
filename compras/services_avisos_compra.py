"""Avisos al solicitante cuando Compras registra una compra realizada.

Patrón outbox: los renglones de aviso se crean dentro de la misma transacción
que la compra —así una transacción revertida no deja aviso ni envío— y el envío
se despacha en ``transaction.on_commit``. El renglón es la cola persistente: si
Celery, Resend o Meta fallan, el aviso queda con su estado y motivo y puede
reintentarse desde la pantalla sin duplicar nada.
"""
from __future__ import annotations

import logging

from django.core.mail import EmailMultiAlternatives
from django.db import transaction
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone

from core import whatsapp
from core.contactos import (
    CORREO_INVALIDO, SIN_CORREO, SIN_TELEFONO, TELEFONO_INVALIDO,
    nombre_para_saludo, resolver_correo, resolver_telefono,
)
from core.notificaciones import PUBLIC_BASE_URL

from .models import AvisoCompraDepartamental

logger = logging.getLogger(__name__)

MOTIVOS_SIN_CONTACTO = {SIN_CORREO, SIN_TELEFONO, CORREO_INVALIDO, TELEFONO_INVALIDO}


def url_solicitud(compra) -> str:
    """Enlace al artículo dentro de su solicitud; conserva los permisos actuales."""
    item = compra.item
    ruta = reverse("compras:departamental_detalle", args=[item.solicitud_id])
    return f"{PUBLIC_BASE_URL}{ruta}#item-{item.pk}"


def contexto_mensaje(compra) -> dict:
    item = compra.item
    solicitud = item.solicitud
    return {
        "nombre": nombre_para_saludo(solicitud.solicitante),
        "articulo": item.descripcion,
        "folio": solicitud.folio,
        "fecha": f"{compra.fecha_compra:%d/%m/%Y}",
        "url": url_solicitud(compra),
    }


def crear_avisos(compra) -> list[AvisoCompraDepartamental]:
    """Registra un aviso pendiente por canal. Idempotente por (compra, canal)."""
    solicitante = compra.item.solicitud.solicitante
    avisos = []
    for canal in (AvisoCompraDepartamental.CANAL_CORREO, AvisoCompraDepartamental.CANAL_WHATSAPP):
        aviso, _ = AvisoCompraDepartamental.objects.get_or_create(
            compra=compra, canal=canal, defaults={"destinatario": solicitante}
        )
        avisos.append(aviso)
    return avisos


def programar_avisos(compra) -> list[AvisoCompraDepartamental]:
    """Crea la cola y despacha el envío una vez confirmada la transacción."""
    avisos = crear_avisos(compra)
    transaction.on_commit(lambda: _despachar(compra.pk))
    return avisos


def _despachar(compra_id: int) -> None:
    from .tasks import enviar_avisos_compra_realizada

    try:
        enviar_avisos_compra_realizada.delay(compra_id)
    except Exception:
        # Sin broker el aviso queda PENDIENTE y se reintenta desde la pantalla.
        logger.exception("[compras] No se pudo encolar el aviso de compra %s", compra_id)


def _asunto(contexto: dict) -> str:
    return f"Compra realizada · {contexto['folio']}"


def _enviar_correo(aviso, contexto) -> None:
    correo, motivo = resolver_correo(aviso.destinatario)
    if not correo:
        _marcar(aviso, AvisoCompraDepartamental.ESTADO_SIN_CONTACTO, detalle=motivo, destino="")
        return
    aviso.destino = correo
    texto = render_to_string("compras/emails/compra_realizada.txt", contexto)
    html = render_to_string("compras/emails/compra_realizada.html", contexto)
    mensaje = EmailMultiAlternatives(subject=_asunto(contexto), body=texto, to=[correo])
    mensaje.attach_alternative(html, "text/html")
    try:
        enviados = mensaje.send(fail_silently=False)
    except Exception as exc:
        # El backend de Resend distingue rechazo (RuntimeError con HTTP) de
        # incidencia de red; sin identificador no podemos afirmar el resultado.
        detalle = f"{exc}"[:400]
        estado = (AvisoCompraDepartamental.ESTADO_FALLIDO if "Resend API error" in detalle
                  else AvisoCompraDepartamental.ESTADO_INCIERTO)
        _marcar(aviso, estado, detalle=detalle, destino=correo)
        return
    if not enviados:
        _marcar(aviso, AvisoCompraDepartamental.ESTADO_FALLIDO,
                detalle="El backend de correo no aceptó el mensaje.", destino=correo)
        return
    _marcar(aviso, AvisoCompraDepartamental.ESTADO_ENVIADO, destino=correo,
            referencia=getattr(mensaje, "resend_email_id", "") or "")


def _enviar_whatsapp(aviso, contexto) -> None:
    telefono, motivo = resolver_telefono(aviso.destinatario)
    if not telefono:
        _marcar(aviso, AvisoCompraDepartamental.ESTADO_SIN_CONTACTO, detalle=motivo, destino="")
        return
    resultado = whatsapp.enviar_plantilla(
        telefono=telefono,
        plantilla="",
        parametros=[contexto["nombre"], contexto["articulo"], contexto["folio"], contexto["fecha"]],
    )
    _marcar(aviso, resultado.estado, detalle=resultado.detalle, destino=telefono,
            referencia=resultado.referencia)


def _marcar(aviso, estado: str, *, detalle: str = "", destino: str = "", referencia: str = "") -> None:
    aviso.estado = estado
    aviso.detalle = detalle
    aviso.destino = destino
    if referencia:
        aviso.referencia_externa = referencia
    if estado == AvisoCompraDepartamental.ESTADO_ENVIADO:
        aviso.enviado_en = timezone.now()


@transaction.atomic
def enviar_aviso(aviso, *, actor=None) -> AvisoCompraDepartamental:
    """Ejecuta un canal y guarda su resultado real. No lanza al llamador.

    El renglón se toma con ``skip_locked``: si otro proceso ya está enviando
    este mismo aviso, esta llamada no hace nada en lugar de mandar un duplicado.
    """
    bloqueado = (AvisoCompraDepartamental.objects.select_for_update(skip_locked=True)
                 .filter(pk=aviso.pk).first())
    if bloqueado is None:
        return aviso
    aviso = bloqueado
    if aviso.estado == AvisoCompraDepartamental.ESTADO_ENVIADO:
        return aviso
    contexto = contexto_mensaje(aviso.compra)
    aviso.intentos += 1
    aviso.ultimo_intento_en = timezone.now()
    if actor is not None:
        aviso.ultimo_intento_por = actor
    try:
        if aviso.canal == AvisoCompraDepartamental.CANAL_CORREO:
            _enviar_correo(aviso, contexto)
        else:
            _enviar_whatsapp(aviso, contexto)
    except Exception as exc:  # una falla de canal nunca revienta la pantalla
        logger.exception("[compras] Aviso %s falló de forma inesperada", aviso.pk)
        _marcar(aviso, AvisoCompraDepartamental.ESTADO_FALLIDO, detalle=f"{exc}"[:400],
                destino=aviso.destino)
    aviso.save(update_fields=[
        "estado", "detalle", "destino", "referencia_externa", "intentos",
        "enviado_en", "ultimo_intento_en", "ultimo_intento_por", "actualizado_en",
    ])
    return aviso


def reconciliar_incierto(aviso) -> AvisoCompraDepartamental:
    """Cierra un resultado incierto antes de permitir un reenvío.

    Solo el correo puede consultarse: si Resend devolvió identificador, se
    confirma contra su API. WhatsApp no ofrece consulta por idempotencia, así
    que un incierto sin identificador exige decisión de una persona.
    """
    if aviso.estado != AvisoCompraDepartamental.ESTADO_INCIERTO:
        return aviso
    if aviso.canal != AvisoCompraDepartamental.CANAL_CORREO or not aviso.referencia_externa:
        return aviso
    from config.email_backends import retrieve_resend_email

    try:
        datos = retrieve_resend_email(aviso.referencia_externa)
    except Exception as exc:
        aviso.detalle = f"No se pudo reconciliar con el proveedor: {exc}"[:400]
        aviso.save(update_fields=["detalle", "actualizado_en"])
        return aviso
    if datos.get("id"):
        aviso.estado = AvisoCompraDepartamental.ESTADO_ENVIADO
        aviso.enviado_en = aviso.enviado_en or timezone.now()
        aviso.detalle = "Confirmado con el proveedor tras un resultado incierto."
        aviso.save(update_fields=["estado", "enviado_en", "detalle", "actualizado_en"])
    return aviso


def enviar_avisos_pendientes(compra, *, actor=None) -> list[AvisoCompraDepartamental]:
    avisos = []
    for aviso in AvisoCompraDepartamental.objects.filter(compra=compra).order_by("canal"):
        if aviso.estado == AvisoCompraDepartamental.ESTADO_ENVIADO:
            avisos.append(aviso)
            continue
        avisos.append(enviar_aviso(aviso, actor=actor))
    return avisos
