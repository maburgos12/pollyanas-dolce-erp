from django.urls import path

from . import views

app_name = "control"

urlpatterns = [
    path("", views.discrepancias, name="home"),
    path("discrepancias/", views.discrepancias, name="discrepancias"),
]
