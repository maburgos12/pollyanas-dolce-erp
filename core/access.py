from django.contrib.auth.models import AbstractBaseUser

ROLE_DG = "DG"
ROLE_ADMIN = "ADMIN"
ROLE_COMPRAS = "COMPRAS"
ROLE_ALMACEN = "ALMACEN"
ROLE_PRODUCCION = "PRODUCCION"
ROLE_VENTAS = "VENTAS"
ROLE_LOGISTICA = "LOGISTICA"
ROLE_RRHH = "RRHH"
ROLE_LECTURA = "LECTURA"

ROLE_ORDER = [
    ROLE_DG,
    ROLE_ADMIN,
    ROLE_COMPRAS,
    ROLE_ALMACEN,
    ROLE_PRODUCCION,
    ROLE_VENTAS,
    ROLE_LOGISTICA,
    ROLE_RRHH,
    ROLE_LECTURA,
]


def _group_names(user: AbstractBaseUser) -> set[str]:
    if not user or not user.is_authenticated:
        return set()
    return set(user.groups.values_list("name", flat=True))


def has_any_role(user: AbstractBaseUser, *roles: str) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return bool(_group_names(user).intersection(set(roles)))


def _is_locked(user: AbstractBaseUser, lock_field: str) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return False
    profile = getattr(user, "userprofile", None)
    if not profile:
        return False
    return bool(getattr(profile, lock_field, False))


def primary_role(user: AbstractBaseUser) -> str:
    groups = _group_names(user)
    for role in ROLE_ORDER:
        if role in groups:
            return role
    return ""


def can_view_compras(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_COMPRAS, ROLE_ALMACEN, ROLE_LECTURA) and not _is_locked(
        user, "lock_compras"
    )


def can_manage_compras(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_COMPRAS) and not _is_locked(user, "lock_compras")


def can_view_inventario(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_ALMACEN, ROLE_COMPRAS, ROLE_LECTURA) and not _is_locked(
        user, "lock_inventario"
    )


def can_manage_inventario(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_ALMACEN) and not _is_locked(user, "lock_inventario")


def can_view_reportes(user: AbstractBaseUser) -> bool:
    return has_any_role(
        user,
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_COMPRAS,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_LOGISTICA,
        ROLE_RRHH,
        ROLE_LECTURA,
    ) and not _is_locked(user, "lock_reportes")


def can_view_maestros(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_COMPRAS, ROLE_ALMACEN, ROLE_LECTURA) and not _is_locked(
        user, "lock_maestros"
    )


def can_view_recetas(user: AbstractBaseUser) -> bool:
    return has_any_role(
        user,
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_COMPRAS,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_LECTURA,
    ) and not _is_locked(user, "lock_recetas")


def can_view_audit(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_auditoria")


def can_manage_users(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def can_view_crm(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS, ROLE_LECTURA) and not _is_locked(user, "lock_crm")


def can_manage_crm(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_VENTAS) and not _is_locked(user, "lock_crm")


def can_view_logistica(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_LOGISTICA, ROLE_LECTURA) and not _is_locked(
        user, "lock_logistica"
    )


def can_manage_logistica(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_LOGISTICA) and not _is_locked(user, "lock_logistica")


def can_view_rrhh(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_RRHH, ROLE_LECTURA) and not _is_locked(user, "lock_rrhh")


def can_manage_rrhh(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_RRHH) and not _is_locked(user, "lock_rrhh")


def can_capture_piso(user: AbstractBaseUser) -> bool:
    return has_any_role(
        user,
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_LOGISTICA,
    ) and not _is_locked(user, "lock_captura_piso")


def is_branch_capture_only(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated or user.is_superuser:
        return False
    profile = getattr(user, "userprofile", None)
    if not profile:
        return False
    return bool(getattr(profile, "modo_captura_sucursal", False))
