from django.urls import path

from . import views

app_name = "horarios_especiales"

urlpatterns = [
    path("", views.index, name="index"),
]

