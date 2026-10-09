from django.urls import path

from .views_documentos_financieros import documentos_trabajo
from .views_conciliacion_documental import conciliacion_documental, conciliacion_documental_csv
from .views_vinculos_proveedores import vinculos_proveedores
from . import views
from .views_reporte_orden import orden_desde_reporte
from .views_consolidacion_higiene import consolidacion_higiene

app_name = "mantenimiento"

urlpatterns = [
    path("conciliacion-documental/", conciliacion_documental, name="conciliacion_documental"),
    path("conciliacion-documental/exportar/", conciliacion_documental_csv, name="conciliacion_documental_csv"),
    path("trabajos/<str:tipo>/<int:pk>/documentos/", documentos_trabajo, name="documentos-trabajo"),
    path("proveedores/vinculos-documentales/", vinculos_proveedores, name="vinculos-proveedores"),
    path("reportes/<int:pk>/orden/", orden_desde_reporte, name="orden-desde-reporte"),
    path("", views.dashboard, name="dashboard"),
    path(
        "consolidacion-higiene/",
        consolidacion_higiene,
        name="consolidacion-higiene",
    ),
    path("app/", views.pwa_mantenimiento, name="app"),
    path("sw.js", views.pwa_sw, name="pwa-sw"),
    path("nueva-falla/", views.crear_falla, name="crear-falla"),
    path("servicios/crear/", views.crear_servicio_mantenimiento, name="crear-servicio"),
    path("reportes/unidad/nuevo/", views.crear_reporte_unidad, name="crear-reporte-unidad"),
    path("bandeja/<str:tipo>/<int:pk>/actualizar/", views.actualizar_item, name="mant-actualizar"),
    path("bandeja/<str:tipo>/<int:pk>/cancelar/", views.solicitar_cancelacion, name="mant-cancelar"),
    path("bandeja/<str:tipo>/<int:pk>/duplicado/", views.marcar_duplicado, name="mant-duplicado"),
    path("cancelaciones/<int:solicitud_id>/resolver/", views.resolver_cancelacion, name="mant-resolver-cancelacion"),
    path("planes/<int:pk>/ejecutar/", views.registrar_ejecucion_plan, name="mant-plan-ejecutar"),
    path("planes/gestionar/", views.gestionar_plan, name="mant-plan-gestionar"),
    path("flota/servicio/", views.registrar_servicio_flota, name="mant-flota-servicio"),
    path("flota/tipos/", views.gestionar_tipo_servicio, name="mant-flota-tipo"),
    path("proveedores/", views.gestionar_proveedor, name="mant-proveedor"),
    path("proveedores/alta/", views.alta_proveedor_seguimiento, name="mant-proveedor-alta"),
    path("proveedores/<int:pk>/eliminar/", views.eliminar_proveedor, name="mant-proveedor-eliminar"),
]
