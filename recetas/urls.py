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
    path("plan-produccion/", views.plan_produccion, name="plan_produccion"),
    path("plan-produccion/nuevo/", views.plan_produccion_create, name="plan_produccion_create"),
    path("plan-produccion/<int:plan_id>/eliminar/", views.plan_produccion_delete, name="plan_produccion_delete"),
    path("plan-produccion/<int:plan_id>/exportar/", views.plan_produccion_export, name="plan_produccion_export"),
    path("plan-produccion/<int:plan_id>/solicitud-imprimir/", views.plan_produccion_solicitud_print, name="plan_produccion_solicitud_print"),
    path("plan-produccion/<int:plan_id>/items/nuevo/", views.plan_produccion_item_create, name="plan_produccion_item_create"),
    path("plan-produccion/<int:plan_id>/items/<int:item_id>/eliminar/", views.plan_produccion_item_delete, name="plan_produccion_item_delete"),
    path("mrp/", views.mrp_form, name="mrp_form"),
]
