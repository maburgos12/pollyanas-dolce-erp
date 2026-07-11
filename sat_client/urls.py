from django.urls import path

from sat_client import views

app_name = "sat_client"

urlpatterns = [
    path("", views.panel_fiscal, name="panel_fiscal"),
]
