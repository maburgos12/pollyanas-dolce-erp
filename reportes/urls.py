from django.urls import path

from . import views
from . import investment_views
from .views_produccion import ProducidoVsVendidoMermaView

app_name = "reportes"

urlpatterns = [
    path("", views.consumo, name="home"),
    path("produccion-automatica/", views.production_orders, name="production_orders"),
    path("ventas/", views.ventas, name="ventas"),
    path("cierre-operativo/", views.cierre_operativo, name="cierre_operativo"),
    path("cierre-producto/", views.cierre_producto, name="cierre_producto"),
    path("produccion/", ProducidoVsVendidoMermaView.as_view(), name="producido_vs_vendido"),
    path("produccion/data/", ProducidoVsVendidoMermaView.as_view(), name="producido_vs_vendido_data"),
    path("financiero/", views.costo_receta, name="financiero"),
    path("presupuesto-maestro/", views.presupuesto_maestro, name="presupuesto_maestro"),
    path("mermas-devoluciones/", views.mermas_devoluciones, name="mermas_devoluciones"),
    path("auditoria-insumos/", views.auditoria_insumos, name="auditoria_insumos"),
    path("proyeccion-produccion/", views.proyeccion_produccion, name="proyeccion_produccion"),
    path("presupuestos/importar/", views.presupuesto_importar_por_area, name="presupuesto_importar_por_area"),
    path(
        "gastos-operativos/captura-manual/",
        views.gastos_operativos_captura_manual,
        name="gastos_operativos_captura_manual",
    ),
    path(
        "gastos-operativos/manual/<int:gasto_id>/eliminar/",
        views.gastos_operativos_manual_delete,
        name="gastos_operativos_manual_delete",
    ),
    path("gastos-operativos/importar/", views.gastos_operativos_importar, name="gastos_operativos_importar"),
    path("costo-receta/", views.costo_receta, name="costo_receta"),
    path("consumo/", views.consumo, name="consumo"),
    path("faltantes/", views.faltantes, name="faltantes"),
    path("bi/", views.bi, name="bi"),
    path("bi/refresh/", views.bi_force_refresh, name="bi_force_refresh"),
    path("sucursales/", investment_views.proyectos_inversion, name="proyectos_inversion_sucursales"),
    path("proyectos-inversion/", investment_views.proyectos_inversion, name="proyectos_inversion"),
    path(
        "proyectos_inversion/comparativo/",
        investment_views.proyectos_inversion_comparativo,
        name="proyectos_inversion_comparativo",
    ),
    path(
        "calibracion/",
        investment_views.proyectos_inversion_calibracion,
        name="proyectos_inversion_calibracion",
    ),
    path(
        "expansion/simulador/",
        investment_views.proyectos_inversion_expansion_simulador,
        name="proyectos_inversion_expansion_simulador",
    ),
    path(
        "proyectos_inversion/expansion/",
        investment_views.proyectos_inversion_expansion,
        name="proyectos_inversion_expansion",
    ),
    path(
        "proyectos-inversion/<int:project_id>/",
        investment_views.proyecto_inversion_detail,
        name="proyecto_inversion_detail",
    ),
    path(
        "proyectos-inversion/nuevo/bamoa/",
        investment_views.proyecto_bamoa_wizard,
        name="proyecto_bamoa_wizard",
    ),
    path(
        "proyectos-inversion/<int:project_id>/export-viabilidad/",
        investment_views.proyecto_viabilidad_export_excel,
        name="proyecto_viabilidad_export_excel",
    ),
]
