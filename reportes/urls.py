from django.urls import path

from . import views

app_name = "reportes"

urlpatterns = [
    path("costo-receta/", views.costo_receta, name="costo_receta"),
    path("consumo/", views.consumo, name="consumo"),
    path("faltantes/", views.faltantes, name="faltantes"),
]
