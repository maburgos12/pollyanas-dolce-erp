from django.urls import include, path

from . import views

app_name = "integraciones"

urlpatterns = [
    path("", views.panel, name="panel"),
    path(
        "horarios-especiales/",
        include(("horarios_especiales.urls", "horarios_especiales"), namespace="horarios_especiales"),
    ),
]
