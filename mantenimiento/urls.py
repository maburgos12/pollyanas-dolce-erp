from django.urls import path

from . import views

app_name = "mantenimiento"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("app/", views.pwa_mantenimiento, name="app"),
    path("sw.js", views.pwa_sw, name="pwa-sw"),
    path("nueva-falla/", views.crear_falla, name="crear-falla"),
    path("servicios/crear/", views.crear_servicio_mantenimiento, name="crear-servicio"),
    path("reportes/unidad/nuevo/", views.crear_reporte_unidad, name="crear-reporte-unidad"),
    path("bandeja/<str:tipo>/<int:pk>/actualizar/", views.actualizar_item, name="mant-actualizar"),
    path("bandeja/<str:tipo>/<int:pk>/cancelar/", views.solicitar_cancelacion, name="mant-cancelar"),
    path("bandeja/falla/<int:pk>/duplicado/", views.marcar_duplicado, name="mant-duplicado"),
    path("cancelaciones/<int:solicitud_id>/resolver/", views.resolver_cancelacion, name="mant-resolver-cancelacion"),
    path("planes/<int:pk>/ejecutar/", views.registrar_ejecucion_plan, name="mant-plan-ejecutar"),
    path("planes/gestionar/", views.gestionar_plan, name="mant-plan-gestionar"),
    path("flota/servicio/", views.registrar_servicio_flota, name="mant-flota-servicio"),
    path("flota/tipos/", views.gestionar_tipo_servicio, name="mant-flota-tipo"),
    path("proveedores/", views.gestionar_proveedor, name="mant-proveedor"),
    path("proveedores/alta/", views.alta_proveedor_seguimiento, name="mant-proveedor-alta"),
    path("proveedores/<int:pk>/eliminar/", views.eliminar_proveedor, name="mant-proveedor-eliminar"),
]
