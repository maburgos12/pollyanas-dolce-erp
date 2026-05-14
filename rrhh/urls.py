from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import api_views, views

app_name = "rrhh"

router = DefaultRouter()
router.register(r"api/asistencias", api_views.AsistenciaViewSet, basename="asistencia")
router.register(r"api/horas-extra", api_views.HoraExtraViewSet, basename="hora-extra")
router.register(r"api/permisos", api_views.PermisoSalidaViewSet, basename="permiso")

urlpatterns = [
    path("", include(router.urls)),
    path("api/me/", api_views.capital_humano_me, name="capital_humano_me"),
    path("", views.empleados, name="home"),
    path("empleados/", views.empleados, name="empleados"),
    path("nomina/", views.nomina, name="nomina"),
    path("nomina/<int:pk>/", views.nomina_detail, name="nomina_detail"),
    path("nomina/<int:pk>/estatus/<str:estatus>/", views.nomina_status, name="nomina_status"),
    path("app/", views.pwa_capital_humano, name="rrhh_pwa"),
    path("app/permisos/", views.pwa_permisos, name="rrhh_pwa_permisos"),
    path("app/horas-extra/", views.pwa_horas_extra, name="rrhh_pwa_he"),
    path("dashboard/", views.dashboard_ch, name="rrhh_dashboard"),
    path("asistencias/", views.asistencias_view, name="rrhh_asistencias"),
    path("importar-checador/", views.importar_checador, name="rrhh_importar"),
    path("horas-extra/", views.horas_extra_list, name="rrhh_he_list"),
    path("permisos/", views.permisos_list, name="rrhh_permisos_list"),
]
