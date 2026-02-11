from django.urls import path
from . import views

urlpatterns = [
    path("recetas/", views.recetas_list, name="recetas_list"),
    path("recetas/<int:pk>/", views.receta_detail, name="receta_detail"),
    path("matching/pendientes/", views.matching_pendientes, name="matching_pendientes"),
    path("matching/aprobar/<int:linea_id>/", views.aprobar_matching, name="aprobar_matching"),
    path("mrp/", views.mrp_form, name="mrp_form"),
]
