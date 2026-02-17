from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from core import views as core_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("health/", core_views.health_check, name="health"),
    path("login/", core_views.login_view, name="login"),
    path("logout/", core_views.logout_view, name="logout"),
    path("auditoria/", core_views.audit_log_view, name="audit_log"),

    path("", RedirectView.as_view(url="/login/", permanent=False)),
    path("dashboard/", core_views.dashboard, name="dashboard"),
    path("maestros/", include(("maestros.urls", "maestros"), namespace="maestros")),
    path("recetas/", include(("recetas.urls", "recetas"), namespace="recetas")),
    path("compras/", include(("compras.urls", "compras"), namespace="compras")),
    path("inventario/", include(("inventario.urls", "inventario"), namespace="inventario")),
    path("reportes/", include(("reportes.urls", "reportes"), namespace="reportes")),
    path("api/", include("api.urls")),
]
