from django.urls import path

from . import views

app_name = "visitas_sucursal"

urlpatterns = [
    path("", views.lista_visitas, name="lista"),
    path("app/", views.app_visitas_sucursal, name="app"),
    path("nueva/", views.nueva_visita, name="nueva"),
    path("<int:pk>/", views.detalle_visita, name="detalle"),
    path("hallazgos/<int:pk>/convertir-falla/", views.convertir_hallazgo_falla, name="convertir_falla"),
]
