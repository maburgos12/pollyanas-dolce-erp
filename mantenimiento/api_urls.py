from django.urls import path

from . import views

app_name = "mantenimiento_api"

urlpatterns = [
    path("me/", views.mi_perfil, name="mant-perfil"),
    path("bandeja/", views.bandeja, name="mant-bandeja"),
    path("bandeja/<str:tipo>/<int:pk>/actualizar/", views.actualizar_item, name="mant-actualizar"),
    path("activos/", views.ActivoListView.as_view(), name="mant-activos"),
    path("unidades/", views.UnidadListView.as_view(), name="mant-unidades"),
    path("tipos-servicio/", views.TipoServicioListView.as_view(), name="mant-tipos"),
    path("ordenes/", views.OrdenMantenimientoListCreateView.as_view(), name="mant-ordenes"),
    path("reparaciones/", views.ReparacionListCreateView.as_view(), name="mant-reparaciones"),
    path("servicios/", views.ServicioListCreateView.as_view(), name="mant-servicios"),
]
