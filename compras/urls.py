from django.urls import path

from . import views

app_name = "compras"

urlpatterns = [
    path("solicitudes/", views.solicitudes, name="solicitudes"),
    path("solicitudes/<int:pk>/estatus/<str:estatus>/", views.actualizar_solicitud_estatus, name="solicitud_estatus"),
    path("ordenes/", views.ordenes, name="ordenes"),
    path("ordenes/<int:pk>/estatus/<str:estatus>/", views.actualizar_orden_estatus, name="orden_estatus"),
    path("recepciones/", views.recepciones, name="recepciones"),
    path("recepciones/<int:pk>/estatus/<str:estatus>/", views.actualizar_recepcion_estatus, name="recepcion_estatus"),
]
