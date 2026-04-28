from django.contrib import admin

from .models import ProyeccionProduccion


@admin.register(ProyeccionProduccion)
class ProyeccionProduccionAdmin(admin.ModelAdmin):
    list_display = (
        "periodo",
        "sucursal",
        "receta",
        "venta_proyectada",
        "stock_actual",
        "unidades_proyectadas_ajustadas",
        "factor_merma",
        "factor_devolucion",
        "confianza",
    )
    list_filter = ("periodo", "sucursal", "confianza", "metodo")
    search_fields = ("receta__nombre", "receta__codigo_point", "sucursal__codigo", "sucursal__nombre")
    autocomplete_fields = ("receta", "sucursal")
