from django.contrib import admin

from .models import SeguimientoChecklistItem, SeguimientoComentario, SeguimientoEvidencia, SeguimientoItem


class SeguimientoChecklistInline(admin.TabularInline):
    model = SeguimientoChecklistItem
    extra = 0


@admin.register(SeguimientoItem)
class SeguimientoItemAdmin(admin.ModelAdmin):
    list_display = ("titulo", "tipo", "estatus", "responsable_user", "responsable_empleado", "fecha_limite", "updated_at")
    list_filter = ("tipo", "estatus", "area", "requiere_aprobacion")
    search_fields = ("titulo", "descripcion", "entregable_esperado", "responsable_user__username", "responsable_empleado__nombre")
    autocomplete_fields = ("responsable_user", "responsable_empleado", "creado_por", "aprobado_por")
    inlines = [SeguimientoChecklistInline]


@admin.register(SeguimientoComentario)
class SeguimientoComentarioAdmin(admin.ModelAdmin):
    list_display = ("seguimiento", "usuario", "tipo", "created_at")
    list_filter = ("tipo",)
    search_fields = ("seguimiento__titulo", "usuario__username", "comentario")


@admin.register(SeguimientoEvidencia)
class SeguimientoEvidenciaAdmin(admin.ModelAdmin):
    list_display = ("seguimiento", "usuario", "nombre_original", "estatus", "created_at")
    list_filter = ("estatus",)
    search_fields = ("seguimiento__titulo", "usuario__username", "nombre_original")
