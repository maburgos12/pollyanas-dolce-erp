from django.urls import path
from django.views.generic import RedirectView
from . import views

app_name = "ventas"

urlpatterns = [
    path("", views.PronosticoVentasView, name="home"),
    path(
        "eventos/",
        RedirectView.as_view(pattern_name="ventas:pronostico", permanent=False),
        name="eventos",
    ),
    path(
        "tendencias/",
        RedirectView.as_view(pattern_name="ventas:pronostico", permanent=False),
        name="tendencias",
    ),
    path("pronostico/", views.PronosticoVentasView, name="pronostico"),
    path("pronostico/esperando/<str:task_id>/", views.PronosticoEsperandoView, name="pronostico_esperando"),
    path("pronostico/status/<str:task_id>/", views.PronosticoStatusView, name="pronostico_status"),
    path("pronostico/guardar/", views.PronosticoGuardarView, name="pronostico_guardar"),
    path("pronostico/guardados/", views.PronosticoListaView, name="pronostico_guardados"),
    path("pronostico/guardados/<int:pk>/ajustes/", views.PronosticoAjustesView, name="pronostico_ajustes"),
    path("pronostico/guardados/<int:pk>/excel/", views.PronosticoExcelView, name="pronostico_excel"),
    path("pronostico/guardados/<int:pk>/imprimir/", views.PronosticoPrintView, name="pronostico_print"),
    path("pronostico/guardados/<int:pk>/eliminar/", views.PronosticoEliminarView, name="pronostico_eliminar"),
    path("pronostico/guardados/<int:pk>/", views.PronosticoDetalleView, name="pronostico_detalle"),
]
