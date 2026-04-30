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
ROLE_REPARTIDOR = "repartidor"

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
    cached = getattr(user, "_group_names_cache", None)
    if cached is not None:
        return set(cached)
    prefetched = getattr(user, "_prefetched_objects_cache", {}) or {}
    prefetched_groups = prefetched.get("groups")
    if prefetched_groups is not None:
        names = {str(group.name) for group in prefetched_groups}
        setattr(user, "_group_names_cache", frozenset(names))
        return names
    names = set(user.groups.values_list("name", flat=True))
    setattr(user, "_group_names_cache", frozenset(names))
    return names


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


def is_repartidor_only(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated or user.is_superuser or user.is_staff:
        return False
    groups = _group_names(user)
    if not (ROLE_REPARTIDOR in groups or hasattr(user, "repartidor_logistica")):
        return False
    elevated = {
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_COMPRAS,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_LOGISTICA,
        ROLE_RRHH,
        ROLE_LECTURA,
        "compras_logistica",
        "supervisor_logistica",
        "personal_sucursal",
    }
    return not bool(groups.intersection(elevated))


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


def can_view_product_closure(user: AbstractBaseUser) -> bool:
    return can_view_reportes(user)


def can_build_product_closure(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_PRODUCCION, ROLE_ALMACEN) and not _is_locked(
        user, "lock_reportes"
    )


def can_rebuild_product_closure(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_reportes")


def can_lock_product_closure(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_reportes")


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


def can_view_orquestacion(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_auditoria")


def can_manage_orquestacion(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_auditoria")


def can_view_crm(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS, ROLE_LECTURA) and not _is_locked(user, "lock_crm")


def can_manage_crm(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_VENTAS) and not _is_locked(user, "lock_crm")


def can_view_ventas_eventos(user: AbstractBaseUser) -> bool:
    return has_any_role(
        user,
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_VENTAS,
        ROLE_PRODUCCION,
        ROLE_COMPRAS,
        ROLE_LECTURA,
    ) and not _is_locked(user, "lock_crm")


def can_view_ventas(user: AbstractBaseUser) -> bool:
    return can_view_ventas_eventos(user)


def can_manage_ventas_eventos(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_VENTAS) and not _is_locked(user, "lock_crm")


def can_view_logistica(user: AbstractBaseUser) -> bool:
    if is_repartidor_only(user):
        return False
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_LOGISTICA, ROLE_LECTURA) and not _is_locked(user, "lock_logistica")


def can_manage_logistica(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_LOGISTICA) and not _is_locked(user, "lock_logistica")


def can_view_rrhh(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_RRHH, ROLE_LECTURA) and not _is_locked(user, "lock_rrhh")


def can_manage_rrhh(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_RRHH) and not _is_locked(user, "lock_rrhh")


def can_view_activos(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_inventario")


def can_view_control(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_reportes")


def can_view_sistema(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def can_capture_piso(user: AbstractBaseUser) -> bool:
    if is_repartidor_only(user):
        return False
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


def _get_role_label(user: AbstractBaseUser) -> str:
    if not user or not user.is_authenticated:
        return ""
    if user.is_superuser:
        return "Superusuario"

    role_map = {
        ROLE_DG: "Director General",
        ROLE_ADMIN: "Administrador",
        ROLE_COMPRAS: "Compras",
        ROLE_ALMACEN: "Almacen",
        ROLE_PRODUCCION: "Produccion",
        ROLE_VENTAS: "Ventas",
        ROLE_LOGISTICA: "Logistica",
        ROLE_RRHH: "RRHH",
        ROLE_LECTURA: "Solo lectura",
        ROLE_REPARTIDOR: "Repartidor",
    }
    groups = _group_names(user)
    for role, label in role_map.items():
        if role in groups:
            return label
    return "Usuario"
