from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import api_views, views, views_prestamos

app_name = "rrhh"

router = DefaultRouter()
router.register(r"api/asistencias", api_views.AsistenciaViewSet, basename="asistencia")
router.register(r"api/horas-extra", api_views.HoraExtraViewSet, basename="hora-extra")
router.register(r"api/permisos", api_views.PermisoSalidaViewSet, basename="permiso")

urlpatterns = [
    path("", include(router.urls)),
    path("api/me/", api_views.capital_humano_me, name="capital_humano_me"),
    path("api/mi-perfil/", api_views.mi_perfil, name="rrhh_mi_perfil"),
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
    path("prestamos/", views_prestamos.prestamos_lista, name="rrhh_prestamos_lista"),
    path("prestamos/nuevo/", views_prestamos.prestamo_nuevo, name="rrhh_prestamo_nuevo"),
    path("prestamos/<int:pk>/", views_prestamos.prestamo_detalle, name="rrhh_prestamo_detalle"),
    path("prestamos/<int:pk>/auth-jefe/", views_prestamos.prestamo_autorizar_jefe, name="rrhh_prestamo_auth_jefe"),
    path("prestamos/<int:pk>/auth-dg/", views_prestamos.prestamo_autorizar_dg, name="rrhh_prestamo_auth_dg"),
    path("prestamos/cuota/<int:cuota_pk>/cobro/", views_prestamos.prestamo_cobro_manual, name="rrhh_prestamo_cobro"),
    path("prestamos/importar-contpaq/", views_prestamos.importar_contpaq, name="rrhh_importar_contpaq"),
    path("prestamos/quincena/", views_prestamos.quincena_cobros, name="rrhh_quincena_cobros"),
]
