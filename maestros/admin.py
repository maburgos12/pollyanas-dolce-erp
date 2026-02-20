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
    list_display = ("nombre", "categoria", "codigo", "codigo_point", "unidad_base", "proveedor_principal", "activo")
    search_fields = ("nombre", "categoria", "codigo", "codigo_point", "nombre_point", "nombre_normalizado")
    list_filter = ("activo", "categoria", "unidad_base")

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
    list_display = ("tipo", "point_codigo", "point_nombre", "method", "fuzzy_score", "actualizado_en")
    search_fields = ("point_codigo", "point_nombre", "fuzzy_sugerencia")
    list_filter = ("tipo", "method")
