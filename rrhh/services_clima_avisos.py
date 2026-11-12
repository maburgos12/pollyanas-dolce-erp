"""Invitaciones de clima por WhatsApp, cerradas hasta habilitación explícita."""

import os
from datetime import timedelta

from django.core import signing
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from core import whatsapp
from core.contactos import normalizar_telefono
from core.notificaciones import PUBLIC_BASE_URL

from .models import AvisoClima, Empleado, ParticipanteClima


PLANTILLAS = {
    AvisoClima.INVITACION: "clima_laboral_invitacion_v2",
    AvisoClima.RECORDATORIO: "clima_laboral_recordatorio_v2",
    AvisoClima.CONFIRMACION: "clima_laboral_confirmacion_v1",
}


def _habilitado():
    return os.getenv("CLIMA_META_ENVIOS_HABILITADOS", "").lower() in {"1", "true", "yes", "on"}


def procesar_avisos_clima():
    """Una invitación y un recordatorio máximo por persona y campaña."""
    if not _habilitado():
        return {"estado": "deshabilitado", "procesados": 0}
    procesados = 0
    candidatos = ParticipanteClima.objects.filter(
        envio_whatsapp_autorizado=True,
        respondio=False,
        empleado__activo=True,
        empleado__tipo_personal=Empleado.TIPO_POLLYANA,
        campana__recepcion_habilitada=True,
        campana__fecha_cierre__gte=timezone.localdate(),
    ).values_list("pk", flat=True)
    for pk in candidatos.iterator():
        envio = _preparar_aviso(pk)
        if envio:
            aviso_id, telefono, plantilla, parametros = envio
            if not ParticipanteClima.objects.filter(pk=pk, respondio=False, campana__recepcion_habilitada=True).exists():
                continue
            try:
                resultado = whatsapp.enviar_plantilla(
                    telefono=telefono, plantilla=plantilla, parametros=parametros,
                )
            except Exception:
                # El estado queda INCIERTO; una caída tampoco reenvía automáticamente.
                procesados += 1
                continue
            AvisoClima.objects.filter(pk=aviso_id).update(
                estado=resultado.estado, referencia_externa=resultado.referencia,
            )
            procesados += 1
    for aviso_id in AvisoClima.objects.filter(tipo=AvisoClima.CONFIRMACION, estado="PENDIENTE").values_list("pk", flat=True).iterator():
        procesados += int(enviar_confirmacion_clima(aviso_id))
    return {"estado": "ejecutado", "procesados": procesados}


def enviar_confirmacion_clima(aviso_id):
    """Confirma una respuesta ya guardada, sin incluir respuestas ni liga individual."""
    if not _habilitado():
        return False
    with transaction.atomic():
        aviso = (AvisoClima.objects.select_for_update(skip_locked=True)
                 .select_related("participante__campana", "participante__empleado")
                 .filter(pk=aviso_id, tipo=AvisoClima.CONFIRMACION, estado="PENDIENTE").first())
        if not aviso or not aviso.participante.respondio or not aviso.participante.envio_whatsapp_autorizado:
            return False
        if not aviso.participante.empleado.activo or aviso.participante.empleado.tipo_personal != Empleado.TIPO_POLLYANA:
            return False
        telefono = _telefono_unico(aviso.participante)
        if not telefono:
            return False
        aviso.estado = "INCIERTO"
        aviso.intentado_en = timezone.now()
        aviso.save(update_fields=["estado", "intentado_en"])
    try:
        resultado = whatsapp.enviar_plantilla(
            telefono=telefono, plantilla=PLANTILLAS[AvisoClima.CONFIRMACION], parametros=[],
        )
    except Exception:
        return True  # INCIERTO evita duplicar ante un timeout.
    AvisoClima.objects.filter(pk=aviso_id, estado="INCIERTO").update(
        estado=resultado.estado, referencia_externa=resultado.referencia,
    )
    return True


def _telefono_unico(participante):
    telefono = normalizar_telefono(participante.empleado.telefono)
    if not telefono:
        return None
    for otro in participante.campana.participantes.select_related("empleado").exclude(pk=participante.pk):
        if normalizar_telefono(otro.empleado.telefono) == telefono:
            return None
    return telefono


@transaction.atomic
def _preparar_aviso(pk):
    participante = (ParticipanteClima.objects.select_for_update(skip_locked=True)
                    .select_related("campana", "empleado").filter(pk=pk).first())
    if not participante or not _habilitado() or not participante.envio_whatsapp_autorizado:
        return False
    campana, empleado = participante.campana, participante.empleado
    if (participante.respondio or not empleado.activo or empleado.tipo_personal != Empleado.TIPO_POLLYANA
            or not campana.recepcion_habilitada
            or campana.fecha_cierre < timezone.localdate()):
        return False
    telefono = _telefono_unico(participante)
    if not telefono:
        return False
    invitacion = AvisoClima.objects.filter(participante=participante, tipo=AvisoClima.INVITACION).first()
    if invitacion is None:
        tipo = AvisoClima.INVITACION
    elif (invitacion.estado == whatsapp.RESULTADO_ENVIADO
          and invitacion.intentado_en <= timezone.now() - timedelta(days=3)
          and not AvisoClima.objects.filter(participante=participante, tipo=AvisoClima.RECORDATORIO).exists()):
        tipo = AvisoClima.RECORDATORIO
    else:
        return False
    aviso = AvisoClima.objects.create(participante=participante, tipo=tipo, estado="INCIERTO", intentado_en=timezone.now())
    token = signing.Signer(salt="rrhh.clima").sign(str(participante.pk))
    liga = PUBLIC_BASE_URL + reverse("rrhh:clima_acceso_publico", args=[token])
    partes = participante.nombre.split()
    # shortcut: RRHH guarda dos apellidos antes del nombre en mayúsculas; usar un campo propio si cambia el padrón.
    primer_nombre = partes[2].capitalize() if len(partes) >= 3 and participante.nombre.isupper() else "equipo"
    return (aviso.pk, telefono, PLANTILLAS[tipo], [
        primer_nombre,
        campana.fecha_cierre.strftime("%d/%m/%Y"), liga,
    ])
