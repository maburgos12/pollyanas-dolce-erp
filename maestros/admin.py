from django.contrib import admin
from .models import CostoInsumo, Insumo, InsumoAlias, PointPendingMatch, Proveedor, UnidadMedida

@admin.register(UnidadMedida)
class UnidadMedidaAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "tipo", "factor_to_base")
    search_fields = ("codigo", "nombre")
    list_filter = ("tipo",)

@admin.register(Proveedor)
class ProveedorAdmin(admin.ModelAdmin):
    list_display = ("nombre", "lead_time_dias", "activo")
    search_fields = ("nombre",)
    list_filter = ("activo",)

@admin.register(Insumo)
class InsumoAdmin(admin.ModelAdmin):
    list_display = ("nombre", "tipo_item", "categoria", "codigo", "codigo_point", "unidad_base", "proveedor_principal", "activo")
    search_fields = ("nombre", "categoria", "codigo", "codigo_point", "nombre_point", "nombre_normalizado")
    list_filter = ("activo", "tipo_item", "categoria", "unidad_base")

@admin.register(CostoInsumo)
class CostoInsumoAdmin(admin.ModelAdmin):
    list_display = ("insumo", "proveedor", "fecha", "costo_unitario", "moneda")
    search_fields = ("insumo__nombre", "proveedor__nombre")
    list_filter = ("moneda", "fecha", "proveedor")


@admin.register(InsumoAlias)
class InsumoAliasAdmin(admin.ModelAdmin):
    list_display = ("nombre", "insumo", "nombre_normalizado", "creado_en")
    search_fields = ("nombre", "nombre_normalizado", "insumo__nombre")
    list_filter = ("creado_en",)


@admin.register(PointPendingMatch)
class PointPendingMatchAdmin(admin.ModelAdmin):
    list_display = (
        "tipo",
        "point_codigo",
        "point_nombre",
        "clasificacion_operativa",
        "visible_en_operacion",
        "method",
        "fuzzy_score",
        "actualizado_en",
    )
    search_fields = ("point_codigo", "point_nombre", "fuzzy_sugerencia")
    list_filter = ("tipo", "method", "clasificacion_operativa", "visible_en_operacion")
    actions = ("mark_historical_only", "mark_seasonal_hidden", "mark_operational_visible")

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if request.GET.get("q") or request.GET.get("clasificacion_operativa") or request.GET.get("visible_en_operacion"):
            return qs
        return qs.visible_en_operacion()

    @admin.action(description="Marcar como solo histórico y ocultar de operación")
    def mark_historical_only(self, request, queryset):
        queryset.update(
            clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_HISTORICO,
            visible_en_operacion=False,
        )

    @admin.action(description="Marcar como temporada y ocultar de operación")
    def mark_seasonal_hidden(self, request, queryset):
        queryset.update(
            clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_TEMPORADA,
            visible_en_operacion=False,
        )

    @admin.action(description="Restaurar como operativo actual")
    def mark_operational_visible(self, request, queryset):
        queryset.update(
            clasificacion_operativa=PointPendingMatch.CLASIFICACION_OPERATIVA_ACTIVO,
            visible_en_operacion=True,
        )
