from django.contrib import admin

from .models import (
    AjusteInventario,
    AlmacenSyncRun,
    ConsumoInsumoMensual,
    ConteoFisicoMensual,
    ExistenciaInsumo,
    InventarioConfig,
    LineaConteoFisico,
    MovimientoInventario,
)


@admin.register(ConsumoInsumoMensual)
class ConsumoInsumoMensualAdmin(admin.ModelAdmin):
    list_display = (
        "periodo",
        "insumo",
        "consumo_teorico",
        "consumo_real",
        "diferencia_pct",
        "diferencia_costo",
        "alerta",
    )
    list_filter = ("periodo", "alerta", "insumo__tipo_item")
    search_fields = ("insumo__nombre", "insumo__codigo_point", "insumo__nombre_point")
    readonly_fields = ("actualizado_en",)


admin.site.register(ExistenciaInsumo)
admin.site.register(MovimientoInventario)
admin.site.register(AjusteInventario)
admin.site.register(AlmacenSyncRun)
admin.site.register(InventarioConfig)


class LineaConteoFisicoInline(admin.TabularInline):
    model = LineaConteoFisico
    extra = 0
    fields = ("nombre", "unidad", "stock_teorico", "stock_contado", "diferencia", "costo_diferencia", "ajuste_aplicado")
    readonly_fields = ("nombre", "unidad", "stock_teorico", "diferencia", "costo_diferencia", "ajuste_aplicado")


@admin.register(ConteoFisicoMensual)
class ConteoFisicoMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "fecha_conteo", "responsable", "estatus", "cerrado_en")
    list_filter = ("estatus", "periodo")
    search_fields = ("responsable__username", "observaciones")
    inlines = [LineaConteoFisicoInline]
