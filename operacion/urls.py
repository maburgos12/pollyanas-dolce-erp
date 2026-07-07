from django.urls import path

from . import views

app_name = "operacion"

urlpatterns = [
    path("", views.app_home, name="app_home"),
    path("sw.js", views.app_sw, name="app_sw"),
    path("bitacoras/", views.bitacoras_home, name="bitacoras_home"),
    path("bitacoras/<str:tipo>/", views.bitacora_captura, name="bitacora_captura"),
]
