from __future__ import annotations

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.db import transaction

from core.notificaciones import crear_notificaciones
from fallas.models import BitacoraFalla, ReporteFalla
from mantenimiento.services_access import can_access_mantenimiento


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
