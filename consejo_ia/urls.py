from django.urls import path

from . import views

urlpatterns = [
    path("", views.consejo_ia_home, name="consejo_ia_home"),
]
