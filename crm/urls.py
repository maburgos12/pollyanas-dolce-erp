from django.urls import path

from . import views

app_name = "crm"

urlpatterns = [
    path("", views.dashboard, name="home"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("clientes/", views.clientes, name="clientes"),
    path("pedidos/", views.pedidos, name="pedidos"),
    path("pedidos/<int:pedido_id>/", views.pedido_detail, name="pedido_detail"),
]
