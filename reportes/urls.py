from django.urls import path

from . import views

app_name = "reportes"

urlpatterns = [
    path("", views.consumo, name="home"),
    path("costo-receta/", views.costo_receta, name="costo_receta"),
    path("consumo/", views.consumo, name="consumo"),
    path("faltantes/", views.faltantes, name="faltantes"),
    path("bi/", views.bi, name="bi"),
]
