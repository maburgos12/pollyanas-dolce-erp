# Auto-generated: exposes all views so  `from . import views; views.fn` works
from .recetas import (
    recetas_list, recetas_sync_all, recetas_sync_group, recetas_sync_new,
    receta_sync_point,
    costeo_dashboard, costeo_dashboard_snapshot,
    receta_create, receta_detail, receta_update, receta_delete,
    receta_sync_derivados, receta_versiones_export, receta_copy_lineas,
    linea_edit, linea_create, linea_delete, linea_apply_direct_base_replacement,
    presentacion_create, presentacion_edit, presentacion_delete,
    drivers_costeo, drivers_costeo_delete, drivers_costeo_plantilla,
    drivers_costeo_importar,
)
from .matching import (
    matching_pendientes, matching_insumos_search,
    aprobar_matching, aprobar_matching_sugerido, aprobar_matching_sugerido_lote,
    linea_repoint_canonical,
    receta_aprobar_sugeridos, receta_repoint_canonical,
    receta_apply_direct_base_replacements,
)
from .plan import (
    # private helpers imported by api/views
    _build_forecast_backtest_preview, _build_forecast_from_history,
    _filter_forecast_result_by_confianza, _forecast_session_payload,
    _forecast_vs_solicitud_preview, _normalize_periodo_mes,
    _resolve_receta_for_sales, _resolve_solicitud_window,
    _resolve_sucursal_for_sales, _ui_to_model_alcance,
    # public views
    forecast_preview_export, forecast_vs_solicitud_export, forecast_backtest_export,
    pronosticos_descargar_plantilla, pronosticos_importar,
    ventas_historicas_descargar_plantilla, ventas_historicas_importar,
    solicitud_ventas_descargar_plantilla, solicitud_ventas_guardar,
    solicitud_ventas_importar, solicitud_ventas_aplicar_desde_forecast,
    pronostico_estadistico_desde_historial,
    plan_produccion_generar_desde_pronostico,
    plan_produccion, plan_produccion_export, plan_produccion_periodo_export,
    plan_produccion_estado_dashboard_export, plan_produccion_dg_dashboard,
    plan_produccion_create, plan_produccion_delete,
    plan_produccion_item_create, plan_produccion_item_delete,
    plan_produccion_solicitud_print, plan_produccion_solicitud_compras_print,
    plan_produccion_generar_solicitudes, plan_produccion_aplicar_consumo,
    plan_produccion_cerrar,
    dg_operacion_dashboard, dg_operacion_dashboard_export,
    produccion_cedis_weekly_dashboard,
)
from .reabasto import (
    reabasto_cedis, reabasto_cedis_captura,
    reabasto_cedis_politica_guardar, reabasto_cedis_inventario_guardar,
    reabasto_cedis_linea_guardar, reabasto_cedis_cierre_guardar,
    reabasto_cedis_linea_eliminar, reabasto_cedis_estado_guardar,
    reabasto_cedis_importar, reabasto_cedis_consolidado_export,
    reabasto_cedis_generar_plan, reabasto_cedis_generar_compras,
)
from .mrp import mrp_form
