from django.urls import path

from . import views

app_name = "activos"

urlpatterns = [
    path("", views.dashboard, name="home"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("activos/", views.activos_catalog, name="activos"),
    path("planes/", views.planes, name="planes"),
    path("ordenes/", views.ordenes, name="ordenes"),
    path("seguimiento/", views.seguimiento_compras_view, name="seguimiento-compras"),
    path("reportes/", views.reportes_servicio, name="reportes"),
    path("ordenes/<int:pk>/estatus/<str:estatus>/", views.actualizar_orden_estatus, name="orden_estatus"),
    path("calendario/", views.calendario, name="calendario"),
    path("api/dashboard-ejecutivo/", views.api_dashboard_ejecutivo, name="api-dashboard-ejecutivo"),
    path("api/bandeja-compras/", views.api_bandeja_compras, name="api-bandeja-compras"),
]
