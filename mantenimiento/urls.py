from django.urls import path

from . import views

app_name = "mantenimiento"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("app/", views.pwa_mantenimiento, name="app"),
    path("nueva-falla/", views.crear_falla, name="crear-falla"),
    path("bandeja/<str:tipo>/<int:pk>/actualizar/", views.actualizar_item, name="mant-actualizar"),
    path("bandeja/<str:tipo>/<int:pk>/cancelar/", views.solicitar_cancelacion, name="mant-cancelar"),
    path("cancelaciones/<int:solicitud_id>/resolver/", views.resolver_cancelacion, name="mant-resolver-cancelacion"),
    path("planes/<int:pk>/ejecutar/", views.registrar_ejecucion_plan, name="mant-plan-ejecutar"),
]
