from django.urls import path

from mantenimiento import api_v2


app_name = "mantenimiento_api_v2"

urlpatterns = [
    path("bandeja/", api_v2.bandeja_v2, name="mantenimiento-v2-bandeja"),
    path("items/<str:tipo>/<int:pk>/", api_v2.item_v2, name="mantenimiento-v2-item"),
    path("evidencias/<str:tipo>/<int:pk>/", api_v2.evidencia_v2, name="mantenimiento-v2-evidencia"),
]
