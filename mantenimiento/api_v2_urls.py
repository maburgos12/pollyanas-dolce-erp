from django.urls import path

from mantenimiento import api_v2


app_name = "mantenimiento_api_v2"

urlpatterns = [
    path("reportes/<int:pk>/orden/", api_v2.orden_desde_reporte_v2, name="mantenimiento-v2-orden-desde-reporte"),
    path("items/<str:tipo>/<int:pk>/vinculos/", api_v2.vinculos_v2, name="mantenimiento-v2-vinculos"),
    path("vinculos/<int:pk>/", api_v2.retirar_vinculo_v2, name="mantenimiento-v2-retirar-vinculo"),
    path("historial/", api_v2.historial_v2, name="mantenimiento-v2-historial"),
    path("bandeja/", api_v2.bandeja_v2, name="mantenimiento-v2-bandeja"),
    path("items/<str:tipo>/<int:pk>/", api_v2.item_v2, name="mantenimiento-v2-item"),
    path("evidencias/<str:tipo>/<int:pk>/", api_v2.evidencia_v2, name="mantenimiento-v2-evidencia"),
]
