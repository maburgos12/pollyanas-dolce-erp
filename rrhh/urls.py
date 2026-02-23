from django.urls import path

from . import views

app_name = "rrhh"

urlpatterns = [
    path("", views.empleados, name="home"),
    path("empleados/", views.empleados, name="empleados"),
    path("nomina/", views.nomina, name="nomina"),
    path("nomina/<int:pk>/", views.nomina_detail, name="nomina_detail"),
    path("nomina/<int:pk>/estatus/<str:estatus>/", views.nomina_status, name="nomina_status"),
]
