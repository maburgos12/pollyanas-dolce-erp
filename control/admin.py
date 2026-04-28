from django.contrib import admin

from .models import DevolucionSucursalMatriz, MermaMensualSucursal, MermaPOS, VentaPOS


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


@admin.register(MermaMensualSucursal)
class MermaMensualSucursalAdmin(admin.ModelAdmin):
    list_display = (
        "periodo",
        "sucursal",
        "receta",
        "nombre_producto",
        "unidades_merma",
        "costo_merma",
        "unidades_vendidas",
        "pct_merma_sobre_venta",
        "justificacion_principal",
    )
    list_filter = ("periodo", "sucursal", "fuente")
    search_fields = ("nombre_producto", "receta__nombre", "sucursal__codigo", "justificacion_principal")
    autocomplete_fields = ("sucursal", "receta")
    ordering = ("-periodo", "sucursal__codigo", "-costo_merma")


@admin.register(DevolucionSucursalMatriz)
class DevolucionSucursalMatrizAdmin(admin.ModelAdmin):
    list_display = (
        "periodo",
        "sucursal_origen",
        "receta",
        "unidades",
        "costo_estimado",
        "motivo",
        "transfer_line",
    )
    list_filter = ("periodo", "sucursal_origen", "motivo")
    search_fields = ("receta__nombre", "sucursal_origen__codigo", "transfer_line__item_name", "transfer_line__transfer_external_id")
    autocomplete_fields = ("sucursal_origen", "receta")
    ordering = ("-periodo", "-transfer_line__registered_at")
