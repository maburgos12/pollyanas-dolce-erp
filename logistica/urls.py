from django.urls import path

from . import views

app_name = "logistica"

urlpatterns = [
    path("", views.rutas, name="rutas"),
    path("rutas/", views.rutas, name="rutas_alt"),
    path("rutas/<int:pk>/", views.ruta_detail, name="ruta_detail"),
]
