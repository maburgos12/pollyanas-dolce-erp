from django.urls import path

from . import views

app_name = "compras"

urlpatterns = [
    path("solicitudes/", views.solicitudes, name="solicitudes"),
    path("ordenes/", views.ordenes, name="ordenes"),
    path("recepciones/", views.recepciones, name="recepciones"),
]
