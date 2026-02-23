from django.urls import path

from . import views

app_name = "activos"

urlpatterns = [
    path("", views.dashboard, name="home"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("ordenes/", views.ordenes, name="ordenes"),
    path("ordenes/<int:pk>/estatus/<str:estatus>/", views.actualizar_orden_estatus, name="orden_estatus"),
    path("calendario/", views.calendario, name="calendario"),
]
