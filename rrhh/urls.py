from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import (
    api_receptor,
    api_views,
    views,
    views_asistencia,
    views_documentos,
    views_prestamos,
    views_suspensiones,
    views_vacantes,
)
from .views_html import asignacion_sucursal_view, asignacion_sucursales_api, usuarios_sucursal_view, usuarios_sucursal_update

app_name = "rrhh"

router = DefaultRouter()
router.register(r"api/asistencias", api_views.AsistenciaViewSet, basename="asistencia")
router.register(r"api/horas-extra", api_views.HoraExtraViewSet, basename="hora-extra")
router.register(r"api/permisos", api_views.PermisoSalidaViewSet, basename="permiso")
router.register(r"api/vacaciones", api_views.SolicitudVacacionesViewSet, basename="vacaciones")

urlpatterns = [
    path("", views.empleados, name="home"),
    path("api/me/", api_views.capital_humano_me, name="capital_humano_me"),
    path("api/mi-perfil/", api_views.mi_perfil, name="rrhh_mi_perfil"),
    path("api/asistencia-hik/", api_receptor.receptor_asistencia_hik, name="rrhh_receptor_hik"),
    path("", include(router.urls)),
    path("empleados/", views.empleados, name="empleados"),
    path("asignacion-sucursal/", asignacion_sucursal_view, name="rrhh_asignacion_sucursal"),
    path("api/asignacion-sucursales/", asignacion_sucursales_api, name="rrhh_asignacion_sucursales_api"),
    path("usuarios-sucursal/", usuarios_sucursal_view, name="rrhh_usuarios_sucursal"),
    path("api/usuarios-sucursal/actualizar/", usuarios_sucursal_update, name="rrhh_usuarios_sucursal_update"),
    path("nomina/", views.nomina, name="nomina"),
    path("nomina/<int:pk>/", views.nomina_detail, name="nomina_detail"),
    path("nomina/<int:pk>/estatus/<str:estatus>/", views.nomina_status, name="nomina_status"),
    path("app/", views.pwa_capital_humano, name="rrhh_pwa"),
    path("app/permisos/", views.pwa_permisos, name="rrhh_pwa_permisos"),
    path("app/vacaciones/", views.pwa_vacaciones, name="rrhh_pwa_vacaciones"),
    path("app/horas-extra/", views.pwa_horas_extra, name="rrhh_pwa_he"),
    path("dashboard/", views.dashboard_ch, name="rrhh_dashboard"),
    path("indicadores/", views.indicadores_ch, name="rrhh_indicadores"),
    path("organizacion/", views.organizacion_ch, name="rrhh_organizacion"),
    path("catalogos/", views.catalogos_ch, name="rrhh_catalogos"),
    path("vacantes/", views_vacantes.vacantes_lista, name="rrhh_vacantes"),
    path("vacantes/nueva/", views_vacantes.vacante_nueva, name="rrhh_vacante_nueva"),
    path("vacantes/<int:pk>/", views_vacantes.vacante_detalle, name="rrhh_vacante_detalle"),
    path("vacantes/<int:pk>/accion/", views_vacantes.vacante_accion, name="rrhh_vacante_accion"),
    path("empleados/<int:empleado_pk>/documentos/", views_documentos.empleado_documentos, name="rrhh_empleado_documentos"),
    path("empleados/<int:empleado_pk>/documentos/<int:doc_pk>/eliminar/", views_documentos.empleado_documento_eliminar, name="rrhh_documento_eliminar"),
    path("asistencias/", views.asistencias_view, name="rrhh_asistencias"),
    path("asistencias/monitor/", views_asistencia.monitor_sincronizacion, name="rrhh_monitor_sync"),
    path("reporte-asistencia/", views_asistencia.reporte_asistencia, name="rrhh_reporte_asistencia"),
    path(
        "reporte-asistencia/incidencia/<int:incidencia_id>/editar/",
        views_asistencia.editar_incidencia,
        name="rrhh_incidencia_editar",
    ),
    path("importar-checador/", views.importar_checador, name="rrhh_importar"),
    path("horas-extra/", views.horas_extra_list, name="rrhh_he_list"),
    path("permisos/", views.permisos_list, name="rrhh_permisos_list"),
    path("suspensiones/", views_suspensiones.rrhh_suspensiones, name="rrhh_suspensiones"),
    path("suspensiones/crear/", views_suspensiones.crear_suspension, name="rrhh_suspension_crear"),
    path(
        "suspensiones/<int:suspension_id>/cancelar/",
        views_suspensiones.cancelar_suspension,
        name="rrhh_suspension_cancelar",
    ),
    path("vacaciones/", views.vacaciones_list, name="rrhh_vacaciones_list"),
    path("reglamento-interno/", views.reglamento_interno, name="rrhh_reglamento_interno"),
    path("prestamos/", views_prestamos.prestamos_lista, name="rrhh_prestamos_lista"),
    path("prestamos/nuevo/", views_prestamos.prestamo_nuevo, name="rrhh_prestamo_nuevo"),
    path("prestamos/<int:pk>/", views_prestamos.prestamo_detalle, name="rrhh_prestamo_detalle"),
    path("prestamos/<int:pk>/imprimir/", views_prestamos.prestamo_imprimir, name="rrhh_prestamo_imprimir"),
    path("prestamos/<int:pk>/auth-jefe/", views_prestamos.prestamo_autorizar_jefe, name="rrhh_prestamo_auth_jefe"),
    path("prestamos/<int:pk>/auth-dg/", views_prestamos.prestamo_autorizar_dg, name="rrhh_prestamo_auth_dg"),
    path("prestamos/cuota/<int:cuota_pk>/cobro/", views_prestamos.prestamo_cobro_manual, name="rrhh_prestamo_cobro"),
    path("prestamos/importar-contpaq/", views_prestamos.importar_contpaq, name="rrhh_importar_contpaq"),
    path("prestamos/quincena/", views_prestamos.quincena_cobros, name="rrhh_quincena_cobros"),
]
