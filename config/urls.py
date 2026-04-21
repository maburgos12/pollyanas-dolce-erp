from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from core import views as core_views
from orquestacion import chat_views as ai_chat_views
from rentabilidad import views_rentabilidad

admin.site.site_url = "/dashboard/"

urlpatterns = [
    path("admin/", admin.site.urls),
    path("health/", core_views.health_check, name="health"),
    path("favicon.ico", RedirectView.as_view(url="/static/favicon.ico", permanent=False)),
    path("apple-touch-icon.png", RedirectView.as_view(url="/static/apple-touch-icon.png", permanent=False)),
    path(
        "apple-touch-icon-precomposed.png",
        RedirectView.as_view(url="/static/apple-touch-icon-precomposed.png", permanent=False),
    ),
    path("login/", core_views.login_view, name="login"),
    path("logout/", core_views.logout_view, name="logout"),
    path("auditoria/", core_views.audit_log_view, name="audit_log"),
    path("ia-privada/", ai_chat_views.chat_home, name="ai_private_hub"),
    path("ia-privada/api/conversations/", ai_chat_views.conversations_api, name="ai_private_conversations_api"),
    path("ia-privada/api/conversations/new/", ai_chat_views.create_conversation_api, name="ai_private_conversation_create_api"),
    path(
        "ia-privada/api/conversations/<uuid:conversation_id>/",
        ai_chat_views.conversation_detail_api,
        name="ai_private_conversation_detail_api",
    ),
    path(
        "ia-privada/api/conversations/<uuid:conversation_id>/stream/",
        ai_chat_views.stream_message_api,
        name="ai_private_message_stream_api",
    ),
    path("usuarios-accesos/", core_views.users_access_view, name="users_access"),

    path("", core_views.home_redirect, name="home"),
    path("dashboard/", core_views.dashboard, name="dashboard"),
    path("plan-produccion/", RedirectView.as_view(url="/recetas/plan-produccion/", permanent=False)),
    path("maestros/", include(("maestros.urls", "maestros"), namespace="maestros")),
    path("recetas/", include(("recetas.urls", "recetas"), namespace="recetas")),
    path("compras/", include(("compras.urls", "compras"), namespace="compras")),
    path("inventario/", include(("inventario.urls", "inventario"), namespace="inventario")),
    path("activos/", include(("activos.urls", "activos"), namespace="activos")),
    path("control/", include(("control.urls", "control"), namespace="control")),
    path("crm/", include(("crm.urls", "crm"), namespace="crm")),
    path("ventas/", include(("ventas.urls", "ventas"), namespace="ventas")),
    path("rrhh/", include(("rrhh.urls", "rrhh"), namespace="rrhh")),
    path("logistica/", include(("logistica.urls", "logistica"), namespace="logistica")),
    path("reportes/", include(("reportes.urls", "reportes"), namespace="reportes")),
    path("integraciones/", include(("integraciones.urls", "integraciones"), namespace="integraciones")),
    path("orquestacion/", include(("orquestacion.urls", "orquestacion"), namespace="orquestacion")),
    path("rentabilidad/", views_rentabilidad.dashboard_rentabilidad, name="rentabilidad_dashboard"),
    path("rentabilidad/<int:pk>/", views_rentabilidad.detalle_sucursal, name="rentabilidad_detalle"),
    path(
        "rentabilidad/<int:pk>/analizar/",
        views_rentabilidad.analizar_con_ia,
        name="rentabilidad_analizar",
    ),
    path("rentabilidad/analizar-todas/", views_rentabilidad.analizar_todas, name="rentabilidad_analizar_todas"),
    path("api/", include("api.urls")),
    path("api/pos-bridge/", include(("pos_bridge.api.urls", "pos_bridge_api"), namespace="pos_bridge_api")),
]
