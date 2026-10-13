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

from .models import AvisoClima, ParticipanteClima


PLANTILLAS = {
    AvisoClima.INVITACION: "clima_laboral_invitacion_v1",
    AvisoClima.RECORDATORIO: "clima_laboral_recordatorio_v1",
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
    return {"estado": "ejecutado", "procesados": procesados}


@transaction.atomic
def _preparar_aviso(pk):
    participante = (ParticipanteClima.objects.select_for_update(skip_locked=True)
                    .select_related("campana", "empleado").filter(pk=pk).first())
    if not participante or not _habilitado() or not participante.envio_whatsapp_autorizado:
        return False
    campana, empleado = participante.campana, participante.empleado
    if (participante.respondio or not empleado.activo or not campana.recepcion_habilitada
            or campana.fecha_cierre < timezone.localdate()):
        return False
    telefono = normalizar_telefono(empleado.telefono)
    if not telefono:
        return False
    # No dirigir una liga individual a un celular compartido en el padrón de la campaña.
    for otro in campana.participantes.select_related("empleado").exclude(pk=pk):
        if normalizar_telefono(otro.empleado.telefono) == telefono:
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
    return (aviso.pk, telefono, PLANTILLAS[tipo], [
        participante.nombre.split()[0] if participante.nombre.split() else "Colaborador",
        campana.fecha_cierre.strftime("%d/%m/%Y"), liga,
    ])
