from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import (
    BonoProduccionViewSet,
    ConfigBonoPeriodoViewSet,
    HorasExtraProduccionEquipoViewSet,
    PermisosProduccionEquipoViewSet,
    RegistroDiarioViewSet,
)
from .views_html import bonos_produccion_dashboard, bonos_produccion_manifest, bonos_produccion_pwa, bonos_produccion_sw

router = DefaultRouter()
router.register("periodos", ConfigBonoPeriodoViewSet, basename="bonoproduccion-periodo")
router.register("bonos", BonoProduccionViewSet, basename="bonoproduccion-bono")
router.register("registros-diarios", RegistroDiarioViewSet, basename="bonoproduccion-registro")
router.register("permisos", PermisosProduccionEquipoViewSet, basename="bonoproduccion-permiso")
router.register("horas-extra", HorasExtraProduccionEquipoViewSet, basename="bonoproduccion-hora-extra")

urlpatterns = [
    path("dashboard/", bonos_produccion_dashboard, name="bonos-produccion-dashboard"),
    path("app/", bonos_produccion_pwa, name="bonos-produccion-app"),
    path("manifest.json", bonos_produccion_manifest, name="bonos-produccion-manifest"),
    path("sw.js", bonos_produccion_sw, name="bonos-produccion-sw"),
    path("", include(router.urls)),
]
