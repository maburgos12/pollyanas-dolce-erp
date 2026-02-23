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
    list_display = ("fecha", "sucursal", "receta", "codigo_point", "cantidad", "motivo", "fuente")
    list_filter = ("fecha", "sucursal", "fuente")
    search_fields = ("codigo_point", "producto_texto", "motivo", "receta__nombre", "sucursal__nombre")
    autocomplete_fields = ("receta", "sucursal")
    ordering = ("-fecha", "-id")

# Register your models here.
