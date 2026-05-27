from __future__ import annotations

from collections.abc import Iterable

from django.contrib.auth import get_user_model

from core.access import ROLE_ADMIN, ROLE_DG
from core.models import Notificacion


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


def _url_permiso_por_origen(permiso) -> str:
    if permiso.origen_solicitud == permiso.ORIGEN_BONOS_PRODUCCION:
        return "/bonos-produccion/app/?tab=permisos"
    if permiso.origen_solicitud == permiso.ORIGEN_BONOS_VENTAS:
        return "/bonos-ventas/app/?tab=permisos"
    return "/rrhh/permisos/"
