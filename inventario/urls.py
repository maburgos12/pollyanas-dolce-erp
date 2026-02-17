from django.urls import path

from . import views

app_name = "inventario"

urlpatterns = [
    path("existencias/", views.existencias, name="existencias"),
    path("movimientos/", views.movimientos, name="movimientos"),
    path("ajustes/", views.ajustes, name="ajustes"),
    path("alertas/", views.alertas, name="alertas"),
]
