from __future__ import annotations

from django.contrib.auth.models import AbstractBaseUser
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.utils import timezone

from core.access import ROLE_DG, group_name_variants
from recetas.utils.normalizacion import normalizar_nombre

from .models import Empleado, PermisoSalida


DIRECCION_DEPARTAMENTOS = {
    Empleado.DEP_ADMINISTRACION,
    Empleado.DEP_VENTAS,
    Empleado.DEP_PRODUCCION,
    Empleado.DEP_RRHH,
    Empleado.DEP_COMPRAS,
    Empleado.DEP_LOGISTICA,
}

DIRECCION_NOMBRES = {
    "YESENIA SOTO INZUNZA",
    "SOTO INZUNZA YESENIA",
    "JOHANA LOPEZ",
    "JOHANA LOPEZ LOPEZ",
    "LOPEZ PALOS JOHANA ADELIN",
    "CAROLINA CAYETANO",
    "CAYETANO VALENZUELA CAROLINA",
    "PAULA",
    "PAULA LUGO",
    "LUGO ESPINOZA PAULA ELIZABETH",
}

DIRECCION_PUESTO_KEYWORDS = (
    "JEFE",
    "ENCARGAD",
    "RESPONSABLE",
    "COORDINADOR",
    "LIDER",
)


def can_authorize_direccion(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return user.groups.filter(name__iexact=ROLE_DG).exists()


def usuario_direccion_general_para_autorizacion():
    User = get_user_model()
    return (
        User.objects.filter(is_active=True, groups__name__in=group_name_variants(ROLE_DG))
        .distinct()
        .order_by("username")
        .first()
        or User.objects.filter(is_active=True, is_superuser=True).order_by("username").first()
    )


def usuario_jefe_directo_permiso(permiso: PermisoSalida):
    empleado = getattr(permiso, "empleado", None)
    jefe = getattr(empleado, "jefe_directo", None)
    return getattr(jefe, "usuario_erp", None)


def _es_mismo_empleado(user: AbstractBaseUser, empleado: Empleado | None) -> bool:
    if not user or not user.is_authenticated or not empleado:
        return False
    return getattr(empleado, "usuario_erp_id", None) == user.id


def can_resolver_permiso_jefe(user: AbstractBaseUser, permiso: PermisoSalida) -> bool:
    if not user or not user.is_authenticated or not permiso:
        return False
    if permiso.estado != PermisoSalida.ESTADO_SOLICITADO:
        return False
    if permiso.estado_jefe != PermisoSalida.ESTADO_JEFE_PENDIENTE:
        return False
    if _es_mismo_empleado(user, permiso.empleado):
        return False
    return getattr(usuario_jefe_directo_permiso(permiso), "id", None) == user.id


def resolver_permiso_jefe(
    permiso: PermisoSalida,
    user: AbstractBaseUser,
    *,
    aprobar: bool,
) -> PermisoSalida:
    if not can_resolver_permiso_jefe(user, permiso):
        raise PermissionDenied("Solo el jefe directo asignado puede resolver este permiso.")

    permiso.autorizado_jefe_por = user
    permiso.fecha_autorizacion_jefe = timezone.now()
    if aprobar:
        permiso.estado_jefe = PermisoSalida.ESTADO_JEFE_PREAUTORIZADO
        update_fields = ["estado_jefe", "autorizado_jefe_por", "fecha_autorizacion_jefe", "actualizado_en"]
        if not permiso.requiere_direccion:
            permiso.estado = PermisoSalida.ESTADO_APROBADO
            permiso.autorizado_por = user
            update_fields += ["estado", "autorizado_por"]
        permiso.save(update_fields=update_fields)
    else:
        permiso.estado_jefe = PermisoSalida.ESTADO_JEFE_RECHAZADO
        permiso.estado = PermisoSalida.ESTADO_RECHAZADO
        permiso.autorizado_por = user
        permiso.save(
            update_fields=[
                "estado_jefe",
                "autorizado_jefe_por",
                "fecha_autorizacion_jefe",
                "estado",
                "autorizado_por",
                "actualizado_en",
            ]
        )
    return permiso


def resolver_permiso_direccion(
    permiso: PermisoSalida,
    user: AbstractBaseUser,
    *,
    aprobar: bool,
) -> PermisoSalida:
    if not can_authorize_direccion(user):
        raise PermissionDenied("Solo Dirección General puede resolver este permiso.")
    if not permiso.requiere_direccion:
        raise PermissionDenied("Este permiso no requiere autorización de Dirección.")
    if _es_mismo_empleado(user, permiso.empleado):
        raise PermissionDenied("No puedes resolver tu propio permiso.")
    if permiso.estado != PermisoSalida.ESTADO_SOLICITADO:
        raise PermissionDenied("Este permiso ya fue resuelto.")

    now = timezone.now()
    permiso.autorizado_direccion_por = user
    permiso.fecha_autorizacion_direccion = now
    if permiso.estado_jefe == PermisoSalida.ESTADO_JEFE_PENDIENTE:
        permiso.estado_jefe = (
            PermisoSalida.ESTADO_JEFE_PREAUTORIZADO if aprobar else PermisoSalida.ESTADO_JEFE_RECHAZADO
        )
        permiso.autorizado_jefe_por = user
        permiso.fecha_autorizacion_jefe = now
    if aprobar:
        permiso.estado_direccion = PermisoSalida.ESTADO_DIRECCION_AUTORIZADO
        permiso.estado = PermisoSalida.ESTADO_APROBADO
    else:
        permiso.estado_direccion = PermisoSalida.ESTADO_DIRECCION_RECHAZADO
        permiso.estado = PermisoSalida.ESTADO_RECHAZADO
    permiso.autorizado_por = user
    permiso.save(
        update_fields=[
            "estado_jefe",
            "autorizado_jefe_por",
            "fecha_autorizacion_jefe",
            "estado_direccion",
            "autorizado_direccion_por",
            "fecha_autorizacion_direccion",
            "estado",
            "autorizado_por",
            "actualizado_en",
        ]
    )
    return permiso


def permiso_requiere_autorizacion_direccion(empleado: Empleado | None) -> bool:
    """
    Solo jefaturas/reportes directos a DG pasan por autorización de Dirección.
    El personal operativo mantiene flujo jefe directo -> RRHH.
    """
    if not empleado:
        return False

    nombre = normalizar_nombre(empleado.nombre or "")
    nombres_direccion = {normalizar_nombre(item) for item in DIRECCION_NOMBRES}
    if nombre in nombres_direccion or any(nombre.startswith(item) for item in nombres_direccion if len(item) > 5):
        return True

    if empleado.jefe_directo_id:
        return False

    departamento = (empleado.departamento or empleado.departamento_origen or "").strip().upper()
    if departamento not in DIRECCION_DEPARTAMENTOS:
        return False

    puesto = f"{empleado.puesto or ''} {empleado.puesto_operativo or ''} {empleado.area or ''}".upper()
    if any(keyword in puesto for keyword in DIRECCION_PUESTO_KEYWORDS):
        return True

    return departamento in {Empleado.DEP_COMPRAS, Empleado.DEP_LOGISTICA} and not empleado.jefe_directo_id
