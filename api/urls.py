from django.urls import path
from .views import (
    ComprasSolicitudCreateView,
    InventarioSugerenciasCompraView,
    MRPExplodeView,
    MRPRequerimientosView,
    RecetaCostoHistoricoView,
    RecetaVersionesView,
)

urlpatterns = [
    path("mrp/explode/", MRPExplodeView.as_view(), name="api_mrp_explode"),
    path("mrp/calcular-requerimientos/", MRPRequerimientosView.as_view(), name="api_mrp_calcular_requerimientos"),
    path("inventario/sugerencias-compra/", InventarioSugerenciasCompraView.as_view(), name="api_inventario_sugerencias_compra"),
    path("compras/solicitud/", ComprasSolicitudCreateView.as_view(), name="api_compras_solicitud"),
    path("recetas/<int:receta_id>/versiones/", RecetaVersionesView.as_view(), name="api_receta_versiones"),
    path("recetas/<int:receta_id>/costo-historico/", RecetaCostoHistoricoView.as_view(), name="api_receta_costo_historico"),
]
