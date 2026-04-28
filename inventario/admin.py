from django.contrib import admin

from .models import (
    AjusteInventario,
    AlmacenSyncRun,
    ConsumoInsumoMensual,
    ExistenciaInsumo,
    InventarioConfig,
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
