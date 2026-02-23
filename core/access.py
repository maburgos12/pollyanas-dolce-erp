from django.contrib.auth.models import AbstractBaseUser

ROLE_DG = "DG"
ROLE_ADMIN = "ADMIN"
ROLE_COMPRAS = "COMPRAS"
ROLE_ALMACEN = "ALMACEN"
ROLE_PRODUCCION = "PRODUCCION"
ROLE_VENTAS = "VENTAS"
ROLE_RRHH = "RRHH"
ROLE_LECTURA = "LECTURA"


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


def can_view_compras(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_COMPRAS, ROLE_ALMACEN, ROLE_LECTURA)


def can_manage_compras(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_COMPRAS)


def can_view_inventario(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_ALMACEN, ROLE_COMPRAS, ROLE_LECTURA)


def can_manage_inventario(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_ALMACEN)


def can_view_reportes(user: AbstractBaseUser) -> bool:
    return has_any_role(
        user,
        ROLE_DG,
        ROLE_ADMIN,
        ROLE_COMPRAS,
        ROLE_ALMACEN,
        ROLE_PRODUCCION,
        ROLE_VENTAS,
        ROLE_RRHH,
        ROLE_LECTURA,
    )


def can_view_maestros(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_COMPRAS, ROLE_ALMACEN, ROLE_LECTURA)


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
    )


def can_view_audit(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def can_view_crm(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS, ROLE_LECTURA)


def can_manage_crm(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_VENTAS)


def can_view_rrhh(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_RRHH, ROLE_LECTURA)


def can_manage_rrhh(user: AbstractBaseUser) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_RRHH)
