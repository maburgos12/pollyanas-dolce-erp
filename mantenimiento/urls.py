from django.urls import path

from . import views

urlpatterns = [
    path("me/", views.mi_perfil, name="mant-perfil"),
    path("session-token/", views.session_token, name="mant-session-token"),
    path("activos/", views.ActivoListView.as_view(), name="mant-activos"),
    path("unidades/", views.UnidadListView.as_view(), name="mant-unidades"),
    path("tipos-servicio/", views.TipoServicioListView.as_view(), name="mant-tipos"),
    path("ordenes/", views.OrdenMantenimientoListCreateView.as_view(), name="mant-ordenes"),
    path("ordenes/<int:pk>/", views.OrdenMantenimientoDetailView.as_view(), name="mant-orden-detail"),
    path("reparaciones/", views.ReparacionListCreateView.as_view(), name="mant-reparaciones"),
    path("servicios/", views.ServicioListCreateView.as_view(), name="mant-servicios"),
]
