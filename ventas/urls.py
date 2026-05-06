from django.urls import path
from . import views

app_name = "ventas"

urlpatterns = [
    path("", views.PronosticoVentasView, name="home"),
    path("pronostico/", views.PronosticoVentasView, name="pronostico"),
]
