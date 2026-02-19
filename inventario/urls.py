from django.urls import path

from . import views

app_name = "inventario"

urlpatterns = [
    path("existencias/", views.existencias, name="existencias"),
    path("movimientos/", views.movimientos, name="movimientos"),
    path("ajustes/", views.ajustes, name="ajustes"),
    path("alertas/", views.alertas, name="alertas"),
    path("importar/", views.importar_archivos, name="importar_archivos"),
    path("sync-drive-now/", views.sync_drive_now, name="sync_drive_now"),
]
