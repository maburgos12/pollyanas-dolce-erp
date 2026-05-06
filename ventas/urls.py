from django.urls import path
from . import views

app_name = "ventas"

urlpatterns = [
    path("", views.evento_list, name="home"),
    path("pronostico/", views.PronosticoVentasView, name="pronostico"),
    path("pronostico/exportar.xlsx", views.PronosticoExportExcelView, name="pronostico_export_excel"),
    path("eventos/", views.evento_list, name="eventos"),
    path("eventos/nuevo/", views.evento_create, name="evento_create"),
    path("eventos/<int:event_id>/", views.evento_detail, name="evento_detail"),
    path("eventos/<int:event_id>/eliminar/", views.evento_delete, name="evento_delete"),
    path("eventos/<int:event_id>/actualizar/", views.evento_update, name="evento_update"),
    path("eventos/<int:event_id>/ajustes/editor/", views.evento_adjustments_editor, name="evento_adjustments_editor"),
    path("eventos/<int:event_id>/cargar-ajustes/", views.evento_upload_adjustments, name="evento_upload_adjustments"),
    path("eventos/<int:event_id>/forecast/", views.evento_generate_forecast, name="evento_forecast"),
    path("eventos/<int:event_id>/generar-archivos-proyeccion/", views.evento_generate_projection_files, name="evento_generate_projection_files"),
    path("eventos/<int:event_id>/archivos-proyeccion/<int:artifact_id>/descargar/", views.evento_download_projection_artifact, name="evento_download_projection_artifact"),
    path("eventos/<int:event_id>/exportar-semana.xlsx", views.evento_export_week_projection, name="evento_export_week_projection"),
    path("eventos/<int:event_id>/exportar-dia.xlsx", views.evento_export_main_day_projection, name="evento_export_main_day_projection"),
    path("eventos/<int:event_id>/exportar-por-dia.xlsx", views.evento_export_branch_day_projection, name="evento_export_branch_day_projection"),
    path("eventos/<int:event_id>/exportar-dashboard.xlsx", views.evento_export_executive_dashboard, name="evento_export_executive_dashboard"),
    path("eventos/<int:event_id>/submit-aprobacion/", views.evento_submit_approval, name="evento_submit_approval"),
    path("eventos/<int:event_id>/aprobar/", views.evento_approve, name="evento_approve"),
    path("eventos/<int:event_id>/rechazar/", views.evento_reject, name="evento_reject"),
    path("eventos/<int:event_id>/generar-produccion/", views.evento_generate_production, name="evento_generate_production"),
    path("eventos/<int:event_id>/confirmar-produccion/", views.evento_confirm_production, name="evento_confirm_production"),
    path("eventos/<int:event_id>/capacidad/", views.evento_capacity_rule_create, name="evento_capacity_rule_create"),
    path("eventos/<int:event_id>/capacidad/<int:rule_id>/eliminar/", views.evento_capacity_rule_delete, name="evento_capacity_rule_delete"),
    path("eventos/<int:event_id>/generar-insumos/", views.evento_generate_inputs, name="evento_generate_inputs"),
    path("eventos/<int:event_id>/generar-compras/", views.evento_generate_purchases, name="evento_generate_purchases"),
    path("eventos/<int:event_id>/generar-finanzas/", views.evento_generate_financials, name="evento_generate_financials"),
    path("eventos/<int:event_id>/postmortem/", views.evento_postmortem, name="evento_postmortem"),
    path("eventos/<int:event_id>/cerrar/", views.evento_close, name="evento_close"),
]
