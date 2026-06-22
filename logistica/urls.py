from django.urls import path

from . import views
from . import views_pwa

app_name = "logistica"

urlpatterns = [
    path("app/", views_pwa.pwa_app, name="pwa_app"),
    path("sw.js", views_pwa.pwa_sw, name="pwa_sw"),
    path("", views.dashboard, name="home"),
    path("ejecutivo/", views.dashboard_ejecutivo, name="dashboard_ejecutivo"),
    path("tickets/", views.tickets_kanban, name="tickets_kanban"),
    path("tickets/<int:pk>/actualizar/", views.ticket_actualizar, name="ticket_actualizar"),
    path("flota/", views.flota_resumen, name="flota_resumen"),
    path("rutas/", views.rutas, name="rutas"),
    path("rutas/control/", views.control_rutas, name="control_rutas"),
    path("rutas/puntos/", views.puntos_logisticos, name="puntos_logisticos"),
    path("rutas/puntos/<int:pk>/editar/", views.punto_logistico_edit, name="punto_logistico_edit"),
    path("rutas/puntos/<int:pk>/toggle/", views.punto_logistico_toggle, name="punto_logistico_toggle"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("capturas/", views.capturas_pwa, name="capturas_pwa"),
    path("unidades/", views.unidades_list, name="unidades_list"),
    path("unidades/nueva/", views.unidad_create, name="unidad_create"),
    path("unidades/<int:pk>/", views.unidad_detalle, name="unidad_detalle"),
    path("unidades/<int:pk>/documentos/nuevo/", views.unidad_documento_nuevo, name="unidad_documento_nuevo"),
    path("unidades/<int:pk>/servicios/nuevo/", views.unidad_servicio_nuevo, name="unidad_servicio_nuevo"),
    path("unidades/<int:pk>/lavados/nuevo/", views.unidad_lavado_nuevo, name="unidad_lavado_nuevo"),
    path("unidades/<int:pk>/reparaciones/nuevo/", views.unidad_reparacion_nueva, name="unidad_reparacion_nueva"),
    path("unidades/<int:pk>/editar/", views.unidad_edit, name="unidad_edit"),
    path("unidades/<int:pk>/toggle/", views.unidad_toggle, name="unidad_toggle"),
    path("reportes/", views.reportes_lista, name="reportes_lista"),
    path("reportes/nuevo/", views.reporte_crear, name="reporte_crear"),
    path("bitacoras/", views.bitacoras_lista, name="bitacoras_lista"),
    path("rutas/<int:pk>/", views.ruta_detail, name="ruta_detail"),
]
