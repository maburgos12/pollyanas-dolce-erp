from django.urls import path
from .views import (
    ComprasSolicitudCreateView,
    InventarioSugerenciasCompraView,
    MRPExplodeView,
    MRPRequerimientosView,
    PresupuestosConsolidadoView,
    RecetaCostoHistoricoView,
    RecetaVersionesView,
)

urlpatterns = [
    path("mrp/explode/", MRPExplodeView.as_view(), name="api_mrp_explode"),
    path("mrp/calcular-requerimientos/", MRPRequerimientosView.as_view(), name="api_mrp_calcular_requerimientos"),
    path("inventario/sugerencias-compra/", InventarioSugerenciasCompraView.as_view(), name="api_inventario_sugerencias_compra"),
    path("compras/solicitud/", ComprasSolicitudCreateView.as_view(), name="api_compras_solicitud"),
    path("presupuestos/consolidado/<str:periodo>/", PresupuestosConsolidadoView.as_view(), name="api_presupuestos_consolidado"),
    path("recetas/<int:receta_id>/versiones/", RecetaVersionesView.as_view(), name="api_receta_versiones"),
    path("recetas/<int:receta_id>/costo-historico/", RecetaCostoHistoricoView.as_view(), name="api_receta_costo_historico"),
]
