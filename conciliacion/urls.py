from django.urls import path

from conciliacion import views

app_name = "conciliacion"

urlpatterns = [
    path("bancaria/", views.conciliacion_bancaria_view, name="bancaria"),
]
