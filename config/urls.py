from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from core import views as core_views

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
    path("rrhh/", include(("rrhh.urls", "rrhh"), namespace="rrhh")),
    path("logistica/", include(("logistica.urls", "logistica"), namespace="logistica")),
    path("reportes/", include(("reportes.urls", "reportes"), namespace="reportes")),
    path("integraciones/", include(("integraciones.urls", "integraciones"), namespace="integraciones")),
    path("api/", include("api.urls")),
]
