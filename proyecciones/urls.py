from django.urls import path

from proyecciones.views.forecast_revision import forecast_quincenal_generar, forecast_quincenal_revision

app_name = "proyecciones"

urlpatterns = [
    path("forecast-quincenal/", forecast_quincenal_revision, name="forecast_quincenal_revision"),
    path("forecast-quincenal/generar/", forecast_quincenal_generar, name="forecast_quincenal_generar"),
]
