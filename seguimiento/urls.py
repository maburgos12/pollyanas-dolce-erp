from django.urls import path

from . import views
from . import webhooks

app_name = "seguimiento"

urlpatterns = [
    path("", views.mi_seguimiento, name="mi_seguimiento"),
    path("panel/", views.panel_dg, name="panel_dg"),
    path("panel/<int:pk>/", views.detalle_item_dg, name="detalle_dg"),
    path("revision/", views.bandeja_revision, name="bandeja_revision"),
    path("webhooks/agente-dg/", webhooks.agente_dg_webhook, name="webhook_agente_dg"),
    path("minutas/", views.seguimiento_minutas, name="minutas"),
    path("proyectos/", views.seguimiento_proyectos, name="proyectos"),
    path("compromisos/", views.seguimiento_compromisos, name="compromisos"),
    path("<int:pk>/", views.detalle_item, name="detalle"),
    path("<int:pk>/checklist/<int:check_id>/", views.toggle_checklist, name="toggle_checklist"),
    path("<int:pk>/retroalimentacion/", views.registrar_feedback, name="registrar_feedback"),
    path("<int:pk>/evidencias/", views.subir_evidencia, name="subir_evidencia"),
    path("<int:pk>/prorroga/", views.solicitar_prorroga, name="solicitar_prorroga"),
    path("<int:pk>/entregar/", views.entregar_para_revision, name="entregar"),
    path("<int:pk>/completar/", views.completar_directamente, name="completar"),
    path("<int:pk>/resolver/", views.resolver_revision, name="resolver_revision"),
    path("<int:pk>/prorroga/<int:prorroga_id>/resolver/", views.resolver_prorroga, name="resolver_prorroga"),
]
