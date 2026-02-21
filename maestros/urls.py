from django.urls import path
from . import views

app_name = "maestros"

urlpatterns = [
    path('', views.InsumoListView.as_view(), name='home'),
    # Proveedores
    path('proveedores/', views.ProveedorListView.as_view(), name='proveedor_list'),
    path('proveedores/crear/', views.ProveedorCreateView.as_view(), name='proveedor_create'),
    path('proveedores/<int:pk>/editar/', views.ProveedorUpdateView.as_view(), name='proveedor_update'),
    path('proveedores/<int:pk>/eliminar/', views.ProveedorDeleteView.as_view(), name='proveedor_delete'),
    
    # Insumos
    path('insumos/', views.InsumoListView.as_view(), name='insumo_list'),
    path('point-pendientes/', views.point_pending_review, name='point_pending_review'),
    path('insumos/export-point/', views.insumo_point_mapping_csv, name='insumo_point_mapping_csv'),
    path('insumos/crear/', views.InsumoCreateView.as_view(), name='insumo_create'),
    path('insumos/<int:pk>/editar/', views.InsumoUpdateView.as_view(), name='insumo_update'),
    path('insumos/<int:pk>/eliminar/', views.InsumoDeleteView.as_view(), name='insumo_delete'),
]
