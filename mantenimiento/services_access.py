"""Object-level access policy shared by Mantenimiento v2 queries."""

from activos.models import OrdenMantenimiento
from core.access import can_manage_module, is_admin_or_dg
from fallas.models import ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad


def authorized_branch_ids(user):
    """Return ``None`` for global scope, otherwise canonical branch PKs."""
    if is_admin_or_dg(user) or can_manage_module(user, "mantenimiento"):
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
    return _authorized_queryset(user, ServicioRealizadoUnidad.objects.all(), "unidad__sucursal_id")


def can_view_costs(user):
    """Costs remain restricted to current global Mantenimiento managers."""
    return is_admin_or_dg(user) or can_manage_module(user, "mantenimiento")
