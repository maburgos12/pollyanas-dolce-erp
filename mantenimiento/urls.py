from django.urls import path

from . import views

app_name = "mantenimiento"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("app/", views.pwa_mantenimiento, name="app"),
    path("bandeja/<str:tipo>/<int:pk>/actualizar/", views.actualizar_item, name="mant-actualizar"),
]
