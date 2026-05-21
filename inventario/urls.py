from django.urls import path

from . import views

app_name = "inventario"

urlpatterns = [
    path("", views.dashboard, name="home"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("existencias/", views.existencias, name="existencias"),
    path("movimientos/", views.movimientos, name="movimientos"),
    path("ajustes/", views.ajustes, name="ajustes"),
    path("alertas/", views.alertas, name="alertas"),
    path("conteo-fisico/", views.conteo_fisico_list, name="conteo_fisico_list"),
    path("conteo-fisico/<int:conteo_id>/", views.conteo_fisico_detail, name="conteo_fisico_detail"),
    path("conteo-fisico/<int:conteo_id>/revision/", views.conteo_fisico_revision, name="conteo_fisico_revision"),
    path("conteo-fisico/<int:conteo_id>/cerrar/", views.conteo_fisico_cerrar, name="conteo_fisico_cerrar"),
    path("conteo-fisico/<int:conteo_id>/exportar/", views.conteo_fisico_export, name="conteo_fisico_export"),
    path("aliases/", views.aliases_catalog, name="aliases_catalog"),
    path("importar/", views.importar_archivos, name="importar_archivos"),
    path("carga-almacen/", views.importar_archivos, name="carga_almacen"),
    path("sync-drive-now/", views.sync_drive_now, name="sync_drive_now"),
    path("captura/", views.captura_diaria, name="captura_diaria"),
    path("auditoria/", views.auditoria_inventario, name="auditoria_inventario"),
]
