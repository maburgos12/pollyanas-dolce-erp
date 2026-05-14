from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import BonoProduccionViewSet, ConfigBonoPeriodoViewSet, RegistroDiarioViewSet
from .views_html import bonos_produccion_pwa

router = DefaultRouter()
router.register("periodos", ConfigBonoPeriodoViewSet, basename="bonoproduccion-periodo")
router.register("bonos", BonoProduccionViewSet, basename="bonoproduccion-bono")
router.register("registros-diarios", RegistroDiarioViewSet, basename="bonoproduccion-registro")

urlpatterns = [
    path("app/", bonos_produccion_pwa, name="bonos-produccion-app"),
    path("", include(router.urls)),
]
