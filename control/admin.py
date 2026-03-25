from django.contrib import admin

from .models import MermaPOS, VentaPOS


@admin.register(VentaPOS)
class VentaPOSAdmin(admin.ModelAdmin):
    list_display = ("fecha", "sucursal", "receta", "codigo_point", "cantidad", "tickets", "monto_total", "fuente")
    list_filter = ("fecha", "sucursal", "fuente")
    search_fields = ("codigo_point", "producto_texto", "receta__nombre", "sucursal__nombre")
    autocomplete_fields = ("receta", "sucursal")
    ordering = ("-fecha", "-id")


@admin.register(MermaPOS)
class MermaPOSAdmin(admin.ModelAdmin):
    list_display = ("fecha", "sucursal", "receta", "codigo_point", "cantidad", "responsable_texto", "motivo", "fuente")
    list_filter = ("fecha", "sucursal", "fuente", "responsable_texto")
    search_fields = ("codigo_point", "producto_texto", "motivo", "responsable_texto", "receta__nombre", "sucursal__nombre")
    autocomplete_fields = ("receta", "sucursal")
    ordering = ("-fecha", "-id")

# Register your models here.
