from django.urls import path

from . import views
from . import views_pwa

app_name = "logistica"

urlpatterns = [
    path("app/", views_pwa.pwa_app, name="pwa_app"),
    path("", views.dashboard, name="home"),
    path("ejecutivo/", views.dashboard_ejecutivo, name="dashboard_ejecutivo"),
    path("tickets/", views.tickets_kanban, name="tickets_kanban"),
    path("tickets/<int:pk>/actualizar/", views.ticket_actualizar, name="ticket_actualizar"),
    path("flota/", views.flota_resumen, name="flota_resumen"),
    path("rutas/", views.rutas, name="rutas"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("capturas/", views.capturas_pwa, name="capturas_pwa"),
    path("unidades/", views.unidades_list, name="unidades_list"),
    path("unidades/nueva/", views.unidad_create, name="unidad_create"),
    path("unidades/<int:pk>/editar/", views.unidad_edit, name="unidad_edit"),
    path("unidades/<int:pk>/toggle/", views.unidad_toggle, name="unidad_toggle"),
    path("rutas/<int:pk>/", views.ruta_detail, name="ruta_detail"),
]
