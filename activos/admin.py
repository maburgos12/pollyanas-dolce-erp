from django.contrib import admin

from .models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento


@admin.register(Activo)
class ActivoAdmin(admin.ModelAdmin):
    list_display = (
        "codigo",
        "nombre",
        "categoria",
        "ubicacion",
        "estado",
        "criticidad",
        "activo",
        "actualizado_en",
    )
    list_filter = ("estado", "criticidad", "activo", "categoria")
    search_fields = ("codigo", "nombre", "categoria", "ubicacion")
    ordering = ("nombre",)


@admin.register(PlanMantenimiento)
class PlanMantenimientoAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "nombre",
        "activo_ref",
        "tipo",
        "frecuencia_dias",
        "proxima_ejecucion",
        "estatus",
        "activo",
    )
    list_filter = ("tipo", "estatus", "activo")
    search_fields = ("nombre", "activo_ref__nombre", "activo_ref__codigo")
    autocomplete_fields = ("activo_ref",)


class BitacoraMantenimientoInline(admin.TabularInline):
    model = BitacoraMantenimiento
    extra = 0
    autocomplete_fields = ("usuario",)


@admin.register(OrdenMantenimiento)
class OrdenMantenimientoAdmin(admin.ModelAdmin):
    list_display = (
        "folio",
        "activo_ref",
        "tipo",
        "prioridad",
        "estatus",
        "fecha_programada",
        "fecha_cierre",
        "costo_total_admin",
    )
    list_filter = ("tipo", "prioridad", "estatus")
    search_fields = ("folio", "activo_ref__nombre", "activo_ref__codigo", "responsable")
    autocomplete_fields = ("activo_ref", "plan_ref", "creado_por", "aprobado_por")
    inlines = [BitacoraMantenimientoInline]

    def costo_total_admin(self, obj):
        return obj.costo_total

    costo_total_admin.short_description = "Costo total"


@admin.register(BitacoraMantenimiento)
class BitacoraMantenimientoAdmin(admin.ModelAdmin):
    list_display = ("orden", "fecha", "accion", "usuario", "costo_adicional")
    list_filter = ("accion",)
    search_fields = ("orden__folio", "comentario", "usuario__username")
    autocomplete_fields = ("orden", "usuario")
