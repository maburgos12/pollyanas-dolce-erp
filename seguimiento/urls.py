from django.urls import path

from . import views

app_name = "seguimiento"

urlpatterns = [
    path("", views.mi_seguimiento, name="mi_seguimiento"),
    path("<int:pk>/checklist/<int:check_id>/", views.toggle_checklist, name="toggle_checklist"),
    path("<int:pk>/retroalimentacion/", views.registrar_feedback, name="registrar_feedback"),
    path("<int:pk>/evidencias/", views.subir_evidencia, name="subir_evidencia"),
]
