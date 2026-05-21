from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import (
    BonoVentasEmpleadoViewSet,
    ConfigBonoVentasPeriodoViewSet,
    PermisosVentasEquipoViewSet,
    RegistroDiarioVentasViewSet,
    VentaCategoriaSucursalViewSet,
)
from .views_html import bonos_ventas_dashboard, bonos_ventas_manifest, bonos_ventas_pwa, bonos_ventas_sw

router = DefaultRouter()
router.register("periodos", ConfigBonoVentasPeriodoViewSet, basename="bonoventas-periodo")
router.register("ventas-categoria", VentaCategoriaSucursalViewSet, basename="bonoventas-cat")
router.register("bonos", BonoVentasEmpleadoViewSet, basename="bonoventas-bono")
router.register("registros-diarios", RegistroDiarioVentasViewSet, basename="bonoventas-registro")
router.register("permisos", PermisosVentasEquipoViewSet, basename="bonoventas-permiso")

urlpatterns = [
    path("dashboard/", bonos_ventas_dashboard, name="bonos-ventas-dashboard"),
    path("app/", bonos_ventas_pwa, name="bonos-ventas-app"),
    path("manifest.json", bonos_ventas_manifest, name="bonos-ventas-manifest"),
    path("sw.js", bonos_ventas_sw, name="bonos-ventas-sw"),
    path("", include(router.urls)),
]
