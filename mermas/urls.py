from django.urls import path

from . import views

app_name = "mermas"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("nuevo/", views.crear_registro, name="crear"),
    path("productos/buscar/", views.buscar_productos, name="buscar_productos"),
    path("<int:pk>/", views.detalle, name="detalle"),
    path("<int:pk>/enviar/", views.enviar_cedis, name="enviar"),
    path("<int:pk>/recibir/", views.recibir_cedis, name="recibir"),
]
