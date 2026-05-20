from django.contrib import admin

from .models import BonoProduccionEmpleado, ConfigBonoArea, ConfigBonoPeriodo, RegistroDiarioProduccion


class RegistroDiarioInline(admin.TabularInline):
    model = RegistroDiarioProduccion
    extra = 0
    fields = [
        "dia",
        "tiene_uniforme",
        "tiene_puntualidad",
        "tiene_asistencia",
        "tiene_produccion",
        "cantidad_embetunados",
        "observacion",
    ]


class ConfigBonoAreaInline(admin.TabularInline):
    model = ConfigBonoArea
    extra = 0
    fields = [
        "area",
        "usa_produccion",
        "pct_produccion",
        "pct_asistencia",
        "pct_puntualidad",
        "pct_uniforme",
        "limite_produccion",
        "limite_asistencia",
        "limite_puntualidad",
        "limite_uniforme",
    ]


@admin.register(ConfigBonoPeriodo)
class ConfigBonoPeriodoAdmin(admin.ModelAdmin):
    list_display = [
        "mes",
        "anio",
        "dias_laborables",
        "monto_hornos",
        "monto_area_produccion",
        "monto_armado",
        "monto_logistica",
        "monto_crucero",
    ]
    list_filter = ["anio", "mes"]
    inlines = [ConfigBonoAreaInline]


@admin.register(ConfigBonoArea)
class ConfigBonoAreaAdmin(admin.ModelAdmin):
    list_display = ["periodo", "area", "usa_produccion", "pct_produccion", "pct_asistencia", "pct_puntualidad", "pct_uniforme"]
    list_filter = ["area", "usa_produccion"]


@admin.register(BonoProduccionEmpleado)
class BonoProduccionEmpleadoAdmin(admin.ModelAdmin):
    list_display = ["empleado", "area", "periodo", "dias_trabajados", "total_a_pagar", "estatus"]
    list_filter = ["area", "estatus", "periodo"]
    search_fields = ["empleado__nombre", "empleado__codigo"]
    inlines = [RegistroDiarioInline]


@admin.register(RegistroDiarioProduccion)
class RegistroDiarioProduccionAdmin(admin.ModelAdmin):
    list_display = ["bono", "dia", "tiene_asistencia", "tiene_uniforme", "tiene_puntualidad", "tiene_produccion", "cantidad_embetunados"]
    list_filter = ["tiene_asistencia", "tiene_uniforme", "tiene_puntualidad", "tiene_produccion"]

# Register your models here.
