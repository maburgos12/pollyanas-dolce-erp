from django.contrib import admin

from .models import Empleado, NominaLinea, NominaPeriodo


@admin.register(Empleado)
class EmpleadoAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "area", "puesto", "tipo_contrato", "salario_diario", "activo")
    list_filter = ("activo", "tipo_contrato", "area")
    search_fields = ("codigo", "nombre", "telefono", "email")


class NominaLineaInline(admin.TabularInline):
    model = NominaLinea
    extra = 0


@admin.register(NominaPeriodo)
class NominaPeriodoAdmin(admin.ModelAdmin):
    list_display = ("folio", "tipo_periodo", "fecha_inicio", "fecha_fin", "estatus", "total_neto")
    list_filter = ("tipo_periodo", "estatus")
    search_fields = ("folio",)
    inlines = [NominaLineaInline]


@admin.register(NominaLinea)
class NominaLineaAdmin(admin.ModelAdmin):
    list_display = ("periodo", "empleado", "dias_trabajados", "total_percepciones", "descuentos", "neto_calculado")
    list_filter = ("periodo__tipo_periodo",)
    search_fields = ("periodo__folio", "empleado__nombre", "empleado__codigo")
