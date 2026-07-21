"""Object-level access policy shared by Mantenimiento v2 queries."""

from activos.models import OrdenMantenimiento
from core.access import (
    can_manage_module,
    can_manage_submodule,
    can_view_module,
    can_view_submodule,
    is_admin_or_dg,
)
from fallas.models import ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad


MAINTENANCE_GROUPS = {"dg", "mantenimiento"}


def _maintenance_group_names(user):
    cached = getattr(user, "_maintenance_group_names_cache", None)
    if cached is None:
        cached = frozenset(name.lower() for name in user.groups.values_list("name", flat=True))
        setattr(user, "_maintenance_group_names_cache", cached)
    return cached


def can_access_mantenimiento(user):
    """Single read gate for Mantenimiento UI, API permissions, and query scope."""
    if not user or not user.is_authenticated or not user.is_active:
        return False
    groups = _maintenance_group_names(user)
    return (
        is_admin_or_dg(user)
        or bool(groups & MAINTENANCE_GROUPS)
        or can_manage_module(user, "mantenimiento")
        or can_view_module(user, "activos")
        or can_manage_submodule(user, "mantenimiento", "bandeja")
        or can_view_submodule(user, "mantenimiento", "app")
        or can_view_submodule(user, "mantenimiento", "dashboard")
    )


def authorized_branch_ids(user):
    """Return ``None`` for global scope, otherwise canonical branch PKs."""
    if not can_access_mantenimiento(user):
        return []
    groups = _maintenance_group_names(user)
    explicit = getattr(user, "_module_access_map_cache", None)
    if explicit is None:
        explicit = {row.module: row.access for row in user.module_access.only("module", "access")}
        setattr(user, "_module_access_map_cache", dict(explicit))
    has_global_manage = explicit.get("mantenimiento") == "manage"
    has_global_inbox_manage = explicit.get("mantenimiento.bandeja") == "manage"
    if (
        is_admin_or_dg(user)
        or has_global_manage
        or has_global_inbox_manage
        or groups & MAINTENANCE_GROUPS
    ):
        return None
    profile = getattr(user, "userprofile", None)
    branch_id = getattr(profile, "sucursal_id", None)
    return [branch_id] if branch_id else []


def _authorized_queryset(user, queryset, branch_lookup):
    branch_ids = authorized_branch_ids(user)
    if branch_ids is None:
        return queryset
    return queryset.filter(**{f"{branch_lookup}__in": branch_ids})


def authorized_fallas(user):
    return _authorized_queryset(user, ReporteFalla.objects.all(), "sucursal_id")


def authorized_orders(user):
    return _authorized_queryset(user, OrdenMantenimiento.objects.all(), "activo_ref__sucursal_id")


def authorized_unit_reports(user):
    return _authorized_queryset(user, ReporteUnidad.objects.all(), "unidad__sucursal_id")


def authorized_repairs(user):
    return _authorized_queryset(user, ReparacionUnidad.objects.all(), "unidad__sucursal_id")


def authorized_unit_services(user):
    return _authorized_queryset(user, ServicioRealizadoUnidad.objects.vigentes(), "unidad__sucursal_id")


def can_view_costs(user):
    """Costs remain restricted to current global Mantenimiento managers."""
    return is_admin_or_dg(user) or can_manage_module(user, "mantenimiento")
