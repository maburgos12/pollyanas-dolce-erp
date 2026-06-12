from django.urls import path

from conciliacion import views

app_name = "conciliacion"

urlpatterns = [
    path("bancaria/", views.conciliacion_bancaria_view, name="bancaria"),
    path("bancaria/movimiento/<int:movimiento_id>/", views.movimiento_conciliacion_detalle_view, name="movimiento_detalle"),
]
