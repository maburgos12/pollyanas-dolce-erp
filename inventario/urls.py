from django.urls import path

from . import views

app_name = "inventario"

urlpatterns = [
    path("", views.existencias, name="home"),
    path("existencias/", views.existencias, name="existencias"),
    path("movimientos/", views.movimientos, name="movimientos"),
    path("ajustes/", views.ajustes, name="ajustes"),
    path("alertas/", views.alertas, name="alertas"),
    path("aliases/", views.aliases_catalog, name="aliases_catalog"),
    path("importar/", views.importar_archivos, name="importar_archivos"),
    path("carga-almacen/", views.importar_archivos, name="carga_almacen"),
    path("sync-drive-now/", views.sync_drive_now, name="sync_drive_now"),
]
