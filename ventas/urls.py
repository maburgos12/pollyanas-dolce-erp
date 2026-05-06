from django.urls import path
from . import views

app_name = "ventas"

urlpatterns = [
    path("", views.PronosticoVentasView, name="home"),
    path("pronostico/", views.PronosticoVentasView, name="pronostico"),
    path("pronostico/esperando/<str:task_id>/", views.PronosticoEsperandoView, name="pronostico_esperando"),
    path("pronostico/status/<str:task_id>/", views.PronosticoStatusView, name="pronostico_status"),
    path("pronostico/guardar/", views.PronosticoGuardarView, name="pronostico_guardar"),
    path("pronostico/guardados/", views.PronosticoListaView, name="pronostico_guardados"),
    path("pronostico/guardados/<int:pk>/excel/", views.PronosticoExcelView, name="pronostico_excel"),
    path("pronostico/guardados/<int:pk>/eliminar/", views.PronosticoEliminarView, name="pronostico_eliminar"),
    path("pronostico/guardados/<int:pk>/", views.PronosticoDetalleView, name="pronostico_detalle"),
]
