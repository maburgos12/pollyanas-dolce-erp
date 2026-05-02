from django.contrib import admin

from .models import BitacoraFalla, CategoriaFalla, ReporteFalla


class BitacoraInline(admin.TabularInline):
    model = BitacoraFalla
    extra = 0
    readonly_fields = ["usuario", "estatus_anterior", "estatus_nuevo", "comentario", "timestamp"]
    can_delete = False


@admin.register(CategoriaFalla)
class CategoriaFallaAdmin(admin.ModelAdmin):
    list_display = ["nombre", "tipo", "activo", "orden"]
    list_editable = ["activo", "orden"]
    list_filter = ["tipo", "activo"]
    search_fields = ["nombre"]


@admin.register(ReporteFalla)
class ReporteFallaAdmin(admin.ModelAdmin):
    list_display = ["id", "sucursal", "titulo", "categoria", "area", "prioridad", "estatus", "reportado_por", "fecha_reporte"]
    list_filter = ["estatus", "prioridad", "area", "sucursal", "categoria"]
    search_fields = ["titulo", "descripcion", "sucursal__nombre"]
    readonly_fields = [
        "fecha_reporte",
        "reportado_por",
        "latitud",
        "longitud",
        "tiempo_respuesta_horas",
        "tiempo_resolucion_horas",
    ]
    inlines = [BitacoraInline]
    date_hierarchy = "fecha_reporte"
