from django.urls import path

from . import views

app_name = "compras"

urlpatterns = [
    path("solicitudes/", views.solicitudes, name="solicitudes"),
    path("solicitudes/importar/", views.importar_solicitudes, name="solicitudes_importar"),
    path("solicitudes/imprimir/", views.solicitudes_print, name="solicitudes_print"),
    path("solicitudes/<int:pk>/estatus/<str:estatus>/", views.actualizar_solicitud_estatus, name="solicitud_estatus"),
    path("solicitudes/<int:pk>/crear-orden/", views.crear_orden_desde_solicitud, name="solicitud_crear_orden"),
    path("ordenes/", views.ordenes, name="ordenes"),
    path("ordenes/<int:pk>/estatus/<str:estatus>/", views.actualizar_orden_estatus, name="orden_estatus"),
    path("recepciones/", views.recepciones, name="recepciones"),
    path("recepciones/<int:pk>/estatus/<str:estatus>/", views.actualizar_recepcion_estatus, name="recepcion_estatus"),
]
