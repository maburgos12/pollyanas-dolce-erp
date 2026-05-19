"""
URLs limpias del módulo de Proyectos de Inversión.
Montadas en /inversiones/ desde config/urls.py.
Separadas de reportes/urls.py para no mezclar con el módulo de reportes.
"""
from django.urls import path

from reportes import investment_views

app_name = "inversiones"

urlpatterns = [
    path(
        "",
        investment_views.inversiones_portafolio,
        name="portafolio",
    ),
    path(
        "nuevo/",
        investment_views.inversiones_wizard,
        name="wizard",
    ),
    path(
        "<int:project_id>/",
        investment_views.inversiones_detalle,
        name="detalle",
    ),
]
