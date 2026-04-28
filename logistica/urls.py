from django.urls import path

from . import views
from . import views_pwa

app_name = "logistica"

urlpatterns = [
    path("app/", views_pwa.pwa_app, name="pwa_app"),
    path("", views.dashboard, name="home"),
    path("rutas/", views.rutas, name="rutas"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("rutas/<int:pk>/", views.ruta_detail, name="ruta_detail"),
]
