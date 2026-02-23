from django.urls import path

from . import views

app_name = "integraciones"

urlpatterns = [
    path("", views.panel, name="panel"),
]
