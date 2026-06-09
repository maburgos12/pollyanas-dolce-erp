app_name = "syncfy_client"

from django.urls import path

from syncfy_client import views

urlpatterns = [
    path("bancos/", views.bancos_view, name="bancos"),
    path("bancos/sincronizar-credenciales/", views.sincronizar_credenciales_view, name="sincronizar_credenciales"),
    path("bancos/<str:banco>/credential/", views.guardar_credential_view, name="guardar_credential"),
]
