from django.urls import path

from . import views

app_name = "fallas"

urlpatterns = [
    path("", views.dashboard_view, name="dashboard"),
    path("app/", views.pwa_app, name="app"),
    path("reportar/", views.pwa_reporte, name="pwa-reporte"),
    path("mis-reportes/", views.pwa_mis_reportes, name="pwa-mis-reportes"),
    path("mis-reportes/<int:pk>/editar/", views.pwa_editar_reporte, name="pwa-editar-reporte"),
    path("mis-reportes/<int:pk>/eliminar/", views.pwa_eliminar_reporte, name="pwa-eliminar-reporte"),
    path("sucursales/", views.SucursalFallaListView.as_view(), name="sucursales"),
    path("categorias/", views.CategoriaFallaListView.as_view(), name="categorias"),
    path("categorias/todas/", views.CategoriaFallaAdminView.as_view(), name="categorias-admin"),
    path("categorias/<int:pk>/", views.CategoriaFallaUpdateView.as_view(), name="categoria-detail"),
    path("activos/", views.ActivoFallaListView.as_view(), name="activos"),
    path("me/", views.perfil_actual, name="perfil-actual"),
    path("usuarios-gestion/", views.usuarios_gestion, name="usuarios-gestion"),
    path("reportes/", views.ReporteFallaListCreateView.as_view(), name="reportes-list"),
    path("reportes/<int:pk>/", views.ReporteFallaDetailView.as_view(), name="reporte-detail"),
    path("reportes/<int:pk>/estatus/", views.cambiar_estatus, name="cambiar-estatus"),
    path("dashboard/stats/", views.dashboard_stats, name="dashboard-stats"),
]
