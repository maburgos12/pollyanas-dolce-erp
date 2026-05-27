from django.urls import path

from . import views
from . import webhooks

app_name = "seguimiento"

urlpatterns = [
    path("", views.mi_seguimiento, name="mi_seguimiento"),
    path("webhooks/agente-dg/", webhooks.agente_dg_webhook, name="webhook_agente_dg"),
    path("minutas/", views.seguimiento_minutas, name="minutas"),
    path("proyectos/", views.seguimiento_proyectos, name="proyectos"),
    path("compromisos/", views.seguimiento_compromisos, name="compromisos"),
    path("<int:pk>/checklist/<int:check_id>/", views.toggle_checklist, name="toggle_checklist"),
    path("<int:pk>/retroalimentacion/", views.registrar_feedback, name="registrar_feedback"),
    path("<int:pk>/evidencias/", views.subir_evidencia, name="subir_evidencia"),
    path("<int:pk>/prorroga/", views.solicitar_prorroga, name="solicitar_prorroga"),
]
