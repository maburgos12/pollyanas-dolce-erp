from django.urls import path

from . import views

app_name = "operacion"

urlpatterns = [
    path("", views.app_home, name="app_home"),
]
