from django.urls import path

from . import views

app_name = "mantenimiento_api"

urlpatterns = [
    path("me/", views.mi_perfil, name="mant-perfil"),
    path("session-token/", views.session_token, name="mant-session-token"),
    path("sucursales/", views.sucursales, name="mant-sucursales"),
    path("catalogos/", views.catalogos_movil, name="mant-catalogos"),
    path("proveedores/", views.proveedores_servicio, name="mant-proveedores"),
    path("proveedores/importables/", views.proveedores_importables_movil, name="mant-proveedores-importables"),
    path("proveedores/importar/", views.importar_proveedores_movil, name="mant-proveedores-importar"),
    path("proveedores/<int:pk>/", views.proveedor_servicio_detalle, name="mant-proveedor-detalle"),
    path("planes/", views.planes_movil, name="mant-planes"),
    path("planes/<int:pk>/", views.plan_movil_detalle, name="mant-plan-detalle"),
    path("cancelaciones/", views.cancelaciones_movil, name="mant-cancelaciones"),
    path("cancelaciones/<int:solicitud_id>/resolver/", views.resolver_cancelacion_movil, name="mant-cancelacion-resolver"),
    path("resumen/", views.resumen_movil, name="mant-resumen"),
    path("resumen/planes/<int:pk>/ejecutar/", views.ejecutar_plan_movil, name="mant-resumen-plan-ejecutar"),
    path("fallas/", views.crear_falla_movil, name="mant-falla-crear"),
    path("servicios-puntuales/", views.crear_servicio_movil, name="mant-servicio-puntual"),
    path("reportes-unidad/", views.crear_reporte_unidad_movil, name="mant-reporte-unidad-crear"),
    path("bandeja/", views.bandeja, name="mant-bandeja"),
    path("bandeja/<str:tipo>/<int:pk>/actualizar/", views.actualizar_item, name="mant-actualizar"),
    path("activos/", views.ActivoListView.as_view(), name="mant-activos"),
    path("activos/rapido/", views.ActivoQuickCreateView.as_view(), name="mant-activos-rapido"),
    path("unidades/", views.UnidadListView.as_view(), name="mant-unidades"),
    path("tipos-servicio/", views.TipoServicioListView.as_view(), name="mant-tipos"),
    path("ordenes/", views.OrdenMantenimientoListCreateView.as_view(), name="mant-ordenes"),
    path("ordenes/<int:pk>/", views.OrdenMantenimientoDetailView.as_view(), name="mant-orden-detail"),
    path("reparaciones/", views.ReparacionListCreateView.as_view(), name="mant-reparaciones"),
    path("servicios/", views.ServicioListCreateView.as_view(), name="mant-servicios"),
]
