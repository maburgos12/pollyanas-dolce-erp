from django.contrib import admin

from .models import Empleado, NominaConceptoLinea, NominaImportacion, NominaLinea, NominaPeriodo


@admin.register(Empleado)
class EmpleadoAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "area", "puesto", "rfc", "curp", "nss", "salario_diario", "activo")
    list_filter = ("activo", "tipo_contrato", "area")
    search_fields = ("codigo", "nombre", "rfc", "curp", "nss", "telefono", "email")


class NominaConceptoLineaInline(admin.TabularInline):
    model = NominaConceptoLinea
    extra = 0


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
    list_display = (
        "periodo",
        "empleado",
        "dias_trabajados",
        "horas_trabajadas",
        "horas_extra",
        "total_percepciones",
        "descuentos",
        "neto_calculado",
    )
    list_filter = ("periodo__tipo_periodo",)
    search_fields = ("periodo__folio", "empleado__nombre", "empleado__codigo")
    inlines = [NominaConceptoLineaInline]


@admin.register(NominaConceptoLinea)
class NominaConceptoLineaAdmin(admin.ModelAdmin):
    list_display = ("linea", "tipo", "codigo_concepto", "nombre", "valor", "importe")
    list_filter = ("tipo", "codigo_concepto")
    search_fields = ("linea__periodo__folio", "linea__empleado__nombre", "nombre", "codigo_concepto")


@admin.register(NominaImportacion)
class NominaImportacionAdmin(admin.ModelAdmin):
    list_display = (
        "archivo_nombre",
        "estatus",
        "periodo",
        "empleados_detectados",
        "total_percepciones",
        "total_deducciones",
        "total_neto",
        "created_at",
    )
    list_filter = ("estatus", "created_at")
    search_fields = ("archivo_nombre", "archivo_hash", "periodo__folio")
