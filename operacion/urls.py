from django.urls import path

from . import views

app_name = "operacion"

urlpatterns = [
    path("", views.app_home, name="app_home"),
    path("sucursal/", views.sucursal_tools, name="sucursal_tools"),
    path("sw.js", views.app_sw, name="app_sw"),
    path("api/fallas/activos/", views.fallas_activos_api, name="fallas_activos_api"),
    path("api/fallas/crear/", views.fallas_crear_api, name="fallas_crear_api"),
    path("api/mermas-insumos/catalogo/", views.mermas_insumos_catalogo_api, name="mermas_insumos_catalogo_api"),
    path("api/mermas-insumos/crear/", views.mermas_insumos_crear_api, name="mermas_insumos_crear_api"),
    path(
        "api/mermas-insumos/<int:merma_id>/aprobar/",
        views.mermas_insumos_aprobar_api,
        name="mermas_insumos_aprobar_api",
    ),
    path(
        "api/mermas-insumos/<int:merma_id>/decidir/",
        views.mermas_insumos_decidir_api,
        name="mermas_insumos_decidir_api",
    ),
    path("bitacoras/", views.bitacoras_home, name="bitacoras_home"),
    path("bitacoras/<str:tipo>/", views.bitacora_captura, name="bitacora_captura"),
]
