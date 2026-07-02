from django.contrib import admin

from .models import ChecklistVisita, HallazgoVisita, VisitaSucursal


class ChecklistInline(admin.TabularInline):
    model = ChecklistVisita
    extra = 0


class HallazgoInline(admin.TabularInline):
    model = HallazgoVisita
    extra = 0


@admin.register(VisitaSucursal)
class VisitaSucursalAdmin(admin.ModelAdmin):
    list_display = ("sucursal", "fecha_programada", "estatus", "responsable", "auditor")
    list_filter = ("estatus", "tipo", "sucursal")
    search_fields = ("sucursal__nombre", "observaciones")
    inlines = [ChecklistInline, HallazgoInline]


@admin.register(HallazgoVisita)
class HallazgoVisitaAdmin(admin.ModelAdmin):
    list_display = ("visita", "categoria", "prioridad", "estatus", "requiere_falla", "reporte_falla")
    list_filter = ("estatus", "prioridad", "requiere_falla")
    search_fields = ("descripcion", "accion_correctiva", "responsable")
