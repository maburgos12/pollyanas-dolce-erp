"""Authenticated access to operational evidence stored under ``MEDIA_ROOT``."""

from django.conf import settings
from django.db.models import Q
from django.http import Http404
from django.views.static import serve as static_serve

from activos.models import EvidenciaOrden, OrdenMantenimiento
from core.access import can_view_inventario, can_view_logistica, can_view_submodule
from fallas.models import EvidenciaSeguimientoFalla, ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad
from mantenimiento.services_access import (
    authorized_fallas,
    authorized_orders,
    authorized_repairs,
    authorized_unit_reports,
    authorized_unit_services,
    can_access_mantenimiento,
)


def _maintenance_can_access(user, queryset):
    return can_access_mantenimiento(user) and queryset.exists()


def _can_access_falla_media(user, path):
    reports = ReporteFalla.objects.filter(foto_evidencia=path)
    timeline = EvidenciaSeguimientoFalla.objects.filter(
        archivo=path,
        bitacora__reporte_id__in=ReporteFalla.objects.values("pk"),
    )
    if can_view_submodule(user, "fallas", "mis_reportes"):
        return reports.exists() or timeline.exists()
    return (
        _maintenance_can_access(user, authorized_fallas(user).filter(foto_evidencia=path))
        or _maintenance_can_access(
            user,
            EvidenciaSeguimientoFalla.objects.filter(
                archivo=path,
                bitacora__reporte_id__in=authorized_fallas(user).values("pk"),
            ),
        )
    )


def _can_access_activos_media(user, path):
    orders = OrdenMantenimiento.objects.filter(factura_archivo=path)
    evidences = EvidenciaOrden.objects.filter(archivo=path)
    if can_view_inventario(user):
        return orders.exists() or evidences.exists()
    return (
        _maintenance_can_access(user, authorized_orders(user).filter(factura_archivo=path))
        or _maintenance_can_access(
            user,
            evidences.filter(orden_id__in=authorized_orders(user).values("pk")),
        )
    )


def _can_access_logistica_media(user, path):
    unit_reports = ReporteUnidad.objects.filter(foto=path)
    unit_services = ServicioRealizadoUnidad.objects.filter(archivo_factura=path)
    repairs = ReparacionUnidad.objects.filter(
        Q(archivo_factura=path) | Q(foto_nota=path),
    )
    if can_view_logistica(user):
        return unit_reports.exists() or unit_services.exists() or repairs.exists()
    return (
        _maintenance_can_access(user, authorized_unit_reports(user).filter(foto=path))
        or _maintenance_can_access(
            user,
            authorized_unit_services(user).filter(archivo_factura=path),
        )
        or _maintenance_can_access(
            user,
            authorized_repairs(user).filter(
                Q(archivo_factura=path) | Q(foto_nota=path),
            ),
        )
    )


def _can_access_operational_media(user, path):
    if not user or not user.is_authenticated or not user.is_active:
        return False
    if path.startswith("fallas/"):
        return _can_access_falla_media(user, path)
    if path.startswith("activos/"):
        return _can_access_activos_media(user, path)
    if path.startswith(("logistica/reportes/", "servicios_unidad/", "reparaciones_unidad/")):
        return _can_access_logistica_media(user, path)
    if path.startswith(("compras/departamentales/", "compras/cotizaciones/")):
        from compras.access_departamentales import puede_gestionar_compras_departamentales
        from compras.models import CotizacionCompraDepartamental, ItemCompraDepartamental
        from reportes.models import AreaPresupuestoResponsable

        if puede_gestionar_compras_departamentales(user):
            return True
        item_ids = ItemCompraDepartamental.objects.filter(imagen=path).values("pk")
        if path.startswith("compras/cotizaciones/"):
            item_ids = CotizacionCompraDepartamental.objects.filter(documento=path).values("item_id")
        return AreaPresupuestoResponsable.objects.filter(
            usuario=user,
            puede_capturar=True,
            area__solicitudes_compra_departamentales__items__in=item_ids,
        ).exists()
    return False


def serve_private_maintenance_media(request, path):
    """Serve operational evidence only when the current user can see its parent record."""
    if not settings.DEBUG and not _can_access_operational_media(request.user, path):
        raise Http404
    response = static_serve(request, path, document_root=settings.MEDIA_ROOT)
    response["Cache-Control"] = "private, no-store"
    response["X-Content-Type-Options"] = "nosniff"
    return response
