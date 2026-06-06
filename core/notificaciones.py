from __future__ import annotations

import logging
import os
from collections.abc import Iterable

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail

from core.access import ROLE_ADMIN, ROLE_DG
from core.models import Notificacion

logger = logging.getLogger(__name__)

# URL base pública para los enlaces de los correos (los links relativos no sirven en email).
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "https://erp.pollyanasdolce.com").rstrip("/")


def _panel_url(pk) -> str:
    return f"/seguimiento/panel/{pk}/"


def _enviar_correo_dg(asunto: str, cuerpo: str) -> None:
    """Envía un correo al Director General. Nunca rompe el flujo si falla el envío."""
    director_email = (getattr(settings, "DIRECTOR_EMAIL", "") or "").strip()
    if not director_email:
        return
    from_email = getattr(settings, "DEFAULT_FROM_EMAIL", "") or None
    try:
        send_mail(asunto, cuerpo, from_email, [director_email], fail_silently=True)
    except Exception:
        logger.exception("Fallo al enviar correo de seguimiento al DG")


def _cuerpo_correo_seguimiento(item, *, encabezado: str, actor=None, extra: str = "") -> str:
    actor_nombre = ""
    if actor:
        actor_nombre = actor.get_full_name() or actor.username
    fecha_limite = item.fecha_limite.strftime("%d/%m/%Y %H:%M") if item.fecha_limite else "Sin fecha"
    lineas = [
        encabezado,
        "",
        f"Acuerdo: {item.titulo}",
        f"Tipo: {item.get_tipo_display()}",
        f"Responsable: {actor_nombre or '—'}",
        f"Área: {item.area or '—'}",
        f"Fecha límite: {fecha_limite}",
    ]
    if extra:
        lineas += ["", extra]
    lineas += ["", f"Abrir el acuerdo: {PUBLIC_BASE_URL}{_panel_url(item.pk)}"]
    return "\n".join(lineas)


def _usuarios_activos(users: Iterable) -> list:
    seen = set()
    activos = []
    for user in users:
        if not user or not getattr(user, "is_active", False) or user.id in seen:
            continue
        seen.add(user.id)
        activos.append(user)
    return activos


def crear_notificacion(
    *,
    usuario,
    titulo: str,
    mensaje: str = "",
    url: str = "",
    tipo: str = Notificacion.TIPO_SISTEMA,
    prioridad: str = Notificacion.PRIORIDAD_NORMAL,
    actor=None,
    objeto_tipo: str = "",
    objeto_id: str | int = "",
):
    if not usuario or not getattr(usuario, "is_active", False):
        return None
    return Notificacion.objects.create(
        usuario=usuario,
        actor=actor if getattr(actor, "is_authenticated", False) else None,
        titulo=titulo[:160],
        mensaje=mensaje,
        url=url,
        tipo=tipo,
        prioridad=prioridad,
        objeto_tipo=objeto_tipo,
        objeto_id=str(objeto_id or ""),
    )


def crear_notificaciones(
    usuarios: Iterable,
    *,
    titulo: str,
    mensaje: str = "",
    url: str = "",
    tipo: str = Notificacion.TIPO_SISTEMA,
    prioridad: str = Notificacion.PRIORIDAD_NORMAL,
    actor=None,
    objeto_tipo: str = "",
    objeto_id: str | int = "",
    excluir=None,
) -> int:
    excluir_id = getattr(excluir, "id", None)
    creadas = 0
    for usuario in _usuarios_activos(usuarios):
        if excluir_id and usuario.id == excluir_id:
            continue
        if crear_notificacion(
            usuario=usuario,
            titulo=titulo,
            mensaje=mensaje,
            url=url,
            tipo=tipo,
            prioridad=prioridad,
            actor=actor,
            objeto_tipo=objeto_tipo,
            objeto_id=objeto_id,
        ):
            creadas += 1
    return creadas


def usuarios_por_grupo(*group_names: str) -> list:
    User = get_user_model()
    return list(
        User.objects.filter(is_active=True, groups__name__in=group_names)
        .distinct()
        .order_by("username")
    )


def usuarios_direccion_general() -> list:
    User = get_user_model()
    return list(
        User.objects.filter(is_active=True)
        .filter(groups__name__in=[ROLE_DG, ROLE_ADMIN])
        .distinct()
        .order_by("username")
    ) + list(User.objects.filter(is_active=True, is_superuser=True).order_by("username"))


def usuarios_jefe_de_empleado(empleado) -> list:
    if not empleado:
        return []
    jefe = getattr(empleado, "jefe_directo", None)
    jefe_usuario = getattr(jefe, "usuario_erp", None)
    if jefe_usuario:
        return [jefe_usuario]

    departamento = (getattr(empleado, "departamento", "") or getattr(empleado, "area", "") or "").upper()
    if "VENTA" in departamento or "REPART" in departamento:
        return usuarios_por_grupo("VENTAS")
    if "PRODU" in departamento or "HORNO" in departamento or "ARMADO" in departamento or "LOGIST" in departamento:
        return usuarios_por_grupo("PRODUCCION")
    return []


def notificar_permiso_solicitado(permiso, *, actor=None) -> int:
    empleados = usuarios_jefe_de_empleado(permiso.empleado)
    origen = permiso.get_origen_solicitud_display()
    return crear_notificaciones(
        empleados,
        titulo=f"Permiso pendiente: {permiso.empleado.nombre}",
        mensaje=f"{permiso.folio} · {permiso.get_tipo_display()} · {origen}",
        url=_url_permiso_por_origen(permiso),
        tipo=Notificacion.TIPO_PERMISO,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.PermisoSalida",
        objeto_id=permiso.id,
        excluir=actor,
    )


def notificar_hora_extra_solicitada(hora_extra, *, actor=None) -> int:
    return crear_notificaciones(
        [hora_extra.jefe_directo],
        titulo=f"Hora extra pendiente: {hora_extra.empleado.nombre}",
        mensaje=f"{hora_extra.fecha:%d/%m/%Y} · {hora_extra.horas} h · requiere autorización de jefe directo",
        url="/rrhh/horas-extra/",
        tipo=Notificacion.TIPO_HORA_EXTRA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.HoraExtra",
        objeto_id=hora_extra.id,
        excluir=actor,
    )


def notificar_prestamo_solicitado(prestamo, *, actor=None) -> int:
    return crear_notificaciones(
        [prestamo.jefe_directo],
        titulo=f"Préstamo pendiente: {prestamo.empleado.nombre}",
        mensaje=f"{prestamo.folio} · ${prestamo.importe} · requiere autorización de jefe directo",
        url=f"/rrhh/prestamos/{prestamo.id}/",
        tipo=Notificacion.TIPO_PRESTAMO,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.Prestamo",
        objeto_id=prestamo.id,
        excluir=actor,
    )


def notificar_prestamo_para_direccion(prestamo, *, actor=None) -> int:
    return crear_notificaciones(
        usuarios_direccion_general(),
        titulo=f"Préstamo para Dirección: {prestamo.empleado.nombre}",
        mensaje=f"{prestamo.folio} autorizado por jefe directo. Falta autorización de Dirección General.",
        url=f"/rrhh/prestamos/{prestamo.id}/",
        tipo=Notificacion.TIPO_PRESTAMO,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.Prestamo",
        objeto_id=prestamo.id,
        excluir=actor,
    )


def notificar_prestamo_aprobado(prestamo, *, actor=None) -> int:
    return crear_notificaciones(
        [prestamo.creado_por, prestamo.jefe_directo],
        titulo=f"Préstamo aprobado: {prestamo.empleado.nombre}",
        mensaje=f"{prestamo.folio} aprobado por Dirección General. Cuotas generadas.",
        url=f"/rrhh/prestamos/{prestamo.id}/",
        tipo=Notificacion.TIPO_PRESTAMO,
        actor=actor,
        objeto_tipo="rrhh.Prestamo",
        objeto_id=prestamo.id,
        excluir=actor,
    )


def notificar_seguimiento_avance(item, *, actor=None, mensaje_extra: str = "", enviar_correo: bool = True) -> int:
    destinatarios = usuarios_direccion_general()
    titulo = item.titulo[:80]
    responsable = ""
    if actor:
        responsable = f"{actor.get_full_name() or actor.username} · "
    creadas = crear_notificaciones(
        destinatarios,
        titulo=f"Avance: {titulo}",
        mensaje=f"{responsable}{item.get_tipo_display()} · {item.area or 'Sin área'}" + (f"\n{mensaje_extra}" if mensaje_extra else ""),
        url=_panel_url(item.pk),
        tipo=Notificacion.TIPO_SEGUIMIENTO,
        prioridad=Notificacion.PRIORIDAD_NORMAL,
        actor=actor,
        objeto_tipo="seguimiento.SeguimientoItem",
        objeto_id=item.pk,
        excluir=actor,
    )
    if enviar_correo:
        _enviar_correo_dg(
            f"[Seguimiento] Avance: {titulo}",
            _cuerpo_correo_seguimiento(item, encabezado="Un colaborador registró un avance.", actor=actor, extra=mensaje_extra),
        )
    return creadas


def notificar_seguimiento_prorroga(item, fecha_solicitada, motivo: str = "", *, actor=None) -> int:
    destinatarios = usuarios_direccion_general()
    titulo = item.titulo[:80]
    actor_nombre = ""
    if actor:
        actor_nombre = f"{actor.get_full_name() or actor.username} · "
    creadas = crear_notificaciones(
        destinatarios,
        titulo=f"Prórroga solicitada: {titulo}",
        mensaje=f"{actor_nombre}Nueva fecha: {fecha_solicitada:%d/%m/%Y}" + (f"\nMotivo: {motivo[:120]}" if motivo else ""),
        url=_panel_url(item.pk),
        tipo=Notificacion.TIPO_SEGUIMIENTO,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="seguimiento.SeguimientoItem",
        objeto_id=item.pk,
        excluir=actor,
    )
    extra = f"Nueva fecha solicitada: {fecha_solicitada:%d/%m/%Y}"
    if motivo:
        extra += f"\nMotivo: {motivo[:300]}"
    _enviar_correo_dg(
        f"[Seguimiento] Prórroga solicitada: {titulo}",
        _cuerpo_correo_seguimiento(item, encabezado="Un colaborador pide más tiempo para un acuerdo.", actor=actor, extra=extra),
    )
    return creadas


def notificar_seguimiento_entrega(item, *, actor=None) -> int:
    destinatarios = usuarios_direccion_general()
    titulo = item.titulo[:80]
    actor_nombre = ""
    if actor:
        actor_nombre = f"{actor.get_full_name() or actor.username} · "
    creadas = crear_notificaciones(
        destinatarios,
        titulo=f"Listo para revisión: {titulo}",
        mensaje=f"{actor_nombre}{item.get_tipo_display()} enviado a revisión.",
        url=_panel_url(item.pk),
        tipo=Notificacion.TIPO_SEGUIMIENTO,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="seguimiento.SeguimientoItem",
        objeto_id=item.pk,
        excluir=actor,
    )
    _enviar_correo_dg(
        f"[Seguimiento] Listo para revisión: {titulo}",
        _cuerpo_correo_seguimiento(item, encabezado="Un colaborador entregó un acuerdo y espera tu revisión.", actor=actor),
    )
    return creadas


def notificar_seguimiento_completado(item, *, actor=None) -> int:
    destinatarios = usuarios_direccion_general()
    titulo = item.titulo[:80]
    actor_nombre = ""
    if actor:
        actor_nombre = f"{actor.get_full_name() or actor.username} · "
    creadas = crear_notificaciones(
        destinatarios,
        titulo=f"Completado: {titulo}",
        mensaje=f"{actor_nombre}{item.get_tipo_display()} marcado como completado.",
        url=_panel_url(item.pk),
        tipo=Notificacion.TIPO_SEGUIMIENTO,
        prioridad=Notificacion.PRIORIDAD_NORMAL,
        actor=actor,
        objeto_tipo="seguimiento.SeguimientoItem",
        objeto_id=item.pk,
        excluir=actor,
    )
    _enviar_correo_dg(
        f"[Seguimiento] Completado: {titulo}",
        _cuerpo_correo_seguimiento(item, encabezado="Un colaborador cerró un acuerdo que no requería tu aprobación.", actor=actor),
    )
    return creadas


def _url_permiso_por_origen(permiso) -> str:
    if permiso.origen_solicitud == permiso.ORIGEN_BONOS_PRODUCCION:
        return "/bonos-produccion/app/?tab=permisos"
    if permiso.origen_solicitud == permiso.ORIGEN_BONOS_VENTAS:
        return "/bonos-ventas/app/?tab=permisos"
    return "/rrhh/permisos/"
