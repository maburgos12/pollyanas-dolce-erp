from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import (
    BonoVentasEmpleadoViewSet,
    ConfigBonoVentasPeriodoViewSet,
    RegistroDiarioVentasViewSet,
    VentaCategoriaSucursalViewSet,
)
from .views_html import bonos_ventas_pwa

router = DefaultRouter()
router.register("periodos", ConfigBonoVentasPeriodoViewSet, basename="bonoventas-periodo")
router.register("ventas-categoria", VentaCategoriaSucursalViewSet, basename="bonoventas-cat")
router.register("bonos", BonoVentasEmpleadoViewSet, basename="bonoventas-bono")
router.register("registros-diarios", RegistroDiarioVentasViewSet, basename="bonoventas-registro")

urlpatterns = [
    path("app/", bonos_ventas_pwa, name="bonos-ventas-app"),
    path("", include(router.urls)),
]
