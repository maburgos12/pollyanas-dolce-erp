from __future__ import annotations

import hashlib

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.db import connection, transaction
from django.utils import timezone

from core.models import Notificacion
from core.notificaciones import crear_notificaciones
from fallas.models import BitacoraFalla, ReporteFalla
from mantenimiento.services_access import can_access_mantenimiento

from .models import RespuestaHigiene


def _usuarios_mantenimiento():
    return [
        user
        for user in get_user_model().objects.filter(is_active=True).prefetch_related("groups", "module_access")
        if can_access_mantenimiento(user)
    ]


def notificar_falla_mantenimiento(reporte: ReporteFalla, actor) -> None:
    usuarios = _usuarios_mantenimiento()
    crear_notificaciones(
        usuarios,
        titulo=f"Nueva falla en {reporte.sucursal.nombre}",
        mensaje=reporte.titulo,
        url="/mantenimiento/",
        actor=actor,
        objeto_tipo="ReporteFalla",
        objeto_id=reporte.pk,
    )
    emails = sorted({(usuario.email or "").strip() for usuario in usuarios if (usuario.email or "").strip()})
    if emails:
        send_mail(
            subject=f"Nueva falla en {reporte.sucursal.nombre}",
            message=f"{reporte.titulo}\n\n{reporte.descripcion}\n\nAbrir Mantenimiento: /mantenimiento/",
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "") or None,
            recipient_list=emails,
            fail_silently=False,
        )


def notificar_evento_higiene(reporte: ReporteFalla, respuesta: RespuestaHigiene, actor) -> None:
    continuidad = respuesta.continuidad_falla
    if continuidad == RespuestaHigiene.CONTINUIDAD_IGUAL:
        fecha_reporte = timezone.localdate(reporte.fecha_reporte)
        dias = (respuesta.registro.fecha - fecha_reporte).days
        if dias not in {3, 6}:
            return
        titulo = f"Falla sin resolver por {dias} días en {reporte.sucursal.nombre}"
        evento = f"igual-{dias}"
    elif continuidad == RespuestaHigiene.CONTINUIDAD_CAMBIO:
        titulo = f"Falla cambió o empeoró en {reporte.sucursal.nombre}"
        evento = f"cambio-{respuesta.pk}"
    elif continuidad == RespuestaHigiene.CONTINUIDAD_CORRECCION:
        titulo = f"Validar corrección en {reporte.sucursal.nombre}"
        evento = f"correccion-{respuesta.pk}"
    else:
        return

    url = f"/mantenimiento/?open=falla:{reporte.pk}&evento=higiene-{evento}"
    lock_value = f"higiene-evento|{reporte.pk}|{evento}"
    lock_key = int.from_bytes(
        hashlib.blake2b(lock_value.encode("utf-8"), digest_size=8).digest(),
        byteorder="big",
        signed=True,
    )
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [lock_key])
        usuarios = _usuarios_mantenimiento()
        usuarios_notificados = set(
            Notificacion.objects.filter(
                usuario_id__in=[usuario.pk for usuario in usuarios],
                tipo=Notificacion.TIPO_SISTEMA,
                objeto_tipo="ReporteFalla",
                objeto_id=str(reporte.pk),
                url=url,
            ).values_list("usuario_id", flat=True)
        )
        crear_notificaciones(
            [usuario for usuario in usuarios if usuario.pk not in usuarios_notificados],
            titulo=titulo,
            mensaje=f"{reporte.titulo} · {respuesta.observacion}",
            url=url,
            actor=actor,
            objeto_tipo="ReporteFalla",
            objeto_id=reporte.pk,
        )


def crear_reporte_falla(
    *,
    sucursal,
    usuario,
    categoria,
    tipo_objetivo,
    titulo,
    descripcion,
    prioridad,
    activo_relacionado=None,
    area_instalacion="",
    evidencia=None,
    justificacion_sin_foto="",
    comentario_bitacora,
) -> ReporteFalla:
    reporte = ReporteFalla(
        sucursal=sucursal,
        activo_relacionado=activo_relacionado,
        categoria=categoria,
        tipo_objetivo=tipo_objetivo,
        area_instalacion=area_instalacion,
        titulo=titulo,
        descripcion=descripcion,
        prioridad=prioridad,
        foto_evidencia=evidencia,
        justificacion_sin_foto=justificacion_sin_foto,
        reportado_por=usuario,
    )
    reporte.full_clean()
    reporte.save()
    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=usuario,
        estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
        comentario=comentario_bitacora,
    )
    transaction.on_commit(lambda: notificar_falla_mantenimiento(reporte, usuario), robust=True)
    return reporte
