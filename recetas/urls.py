from django.urls import path
from . import views

app_name = "recetas"

urlpatterns = [
    path("recetas/", views.recetas_list, name="recetas_list"),
    path("recetas/<int:pk>/", views.receta_detail, name="receta_detail"),
    path("recetas/<int:pk>/actualizar/", views.receta_update, name="receta_update"),
    path("recetas/<int:pk>/presentaciones/nueva/", views.presentacion_create, name="presentacion_create"),
    path("recetas/<int:pk>/presentaciones/<int:presentacion_id>/editar/", views.presentacion_edit, name="presentacion_edit"),
    path("recetas/<int:pk>/presentaciones/<int:presentacion_id>/eliminar/", views.presentacion_delete, name="presentacion_delete"),
    path("recetas/<int:pk>/lineas/nueva/", views.linea_create, name="linea_create"),
    path("recetas/<int:pk>/lineas/<int:linea_id>/editar/", views.linea_edit, name="linea_edit"),
    path("recetas/<int:pk>/lineas/<int:linea_id>/eliminar/", views.linea_delete, name="linea_delete"),
    path("matching/pendientes/", views.matching_pendientes, name="matching_pendientes"),
    path("matching/aprobar/<int:linea_id>/", views.aprobar_matching, name="aprobar_matching"),
    path("mrp/", views.mrp_form, name="mrp_form"),
]
