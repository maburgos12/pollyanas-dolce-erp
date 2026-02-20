from django.contrib import admin
from .models import Receta, RecetaCodigoPointAlias, LineaReceta, PlanProduccion, PlanProduccionItem

class LineaRecetaInline(admin.TabularInline):
    model = LineaReceta
    extra = 0
    readonly_fields = ("insumo_texto", "cantidad", "unidad_texto", "costo_linea_excel", "match_score", "match_method", "match_status")
    can_delete = False


class RecetaCodigoPointAliasInline(admin.TabularInline):
    model = RecetaCodigoPointAlias
    extra = 0
    fields = ("codigo_point", "codigo_point_normalizado", "nombre_point", "activo")
    readonly_fields = ("codigo_point_normalizado",)

@admin.register(Receta)
class RecetaAdmin(admin.ModelAdmin):
    list_display = ("nombre", "codigo_point", "sheet_name", "pendientes_matching")
    search_fields = ("nombre", "codigo_point", "sheet_name")
    inlines = [LineaRecetaInline, RecetaCodigoPointAliasInline]

@admin.register(LineaReceta)
class LineaRecetaAdmin(admin.ModelAdmin):
    list_display = ("receta", "insumo_texto", "insumo", "cantidad", "unidad_texto", "match_status", "match_score")
    search_fields = ("receta__nombre", "insumo_texto", "insumo__nombre")
    list_filter = ("match_status", "match_method")
    autocomplete_fields = ("insumo",)


class PlanProduccionItemInline(admin.TabularInline):
    model = PlanProduccionItem
    extra = 0
    autocomplete_fields = ("receta",)


@admin.register(PlanProduccion)
class PlanProduccionAdmin(admin.ModelAdmin):
    list_display = ("nombre", "fecha_produccion", "creado_por", "creado_en")
    search_fields = ("nombre",)
    list_filter = ("fecha_produccion",)
    inlines = [PlanProduccionItemInline]


@admin.register(PlanProduccionItem)
class PlanProduccionItemAdmin(admin.ModelAdmin):
    list_display = ("plan", "receta", "cantidad", "creado_en")
    search_fields = ("plan__nombre", "receta__nombre")
    autocomplete_fields = ("plan", "receta")


@admin.register(RecetaCodigoPointAlias)
class RecetaCodigoPointAliasAdmin(admin.ModelAdmin):
    list_display = ("codigo_point", "codigo_point_normalizado", "receta", "activo", "actualizado_en")
    search_fields = ("codigo_point", "codigo_point_normalizado", "nombre_point", "receta__nombre")
    list_filter = ("activo",)
