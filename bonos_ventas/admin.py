from django.contrib import admin

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas, VentaCategoriaSucursal


class RegistroVentasInline(admin.TabularInline):
    model = RegistroDiarioVentas
    extra = 0
    fields = ["dia", "tiene_uniforme", "tiene_puntualidad", "tiene_asistencia", "puntos_de_vista"]


@admin.register(ConfigBonoVentasPeriodo)
class ConfigBonoVentasPeriodoAdmin(admin.ModelAdmin):
    list_display = [
        "mes",
        "anio",
        "dias_laborables",
        "bono_base",
        "bono_ventas_adicional",
        "umbral_crecimiento_pct",
        "cancela_por_asistencia",
        "cancela_por_puntualidad",
    ]
    list_filter = ["anio", "mes"]


@admin.register(VentaCategoriaSucursal)
class VentaCategoriaSucursalAdmin(admin.ModelAdmin):
    list_display = [
        "sucursal",
        "categoria",
        "periodo",
        "cantidad_actual",
        "cantidad_anterior",
        "pct_crecimiento",
        "activo_bono",
        "monto_bono_categoria",
        "fuente",
    ]
    list_filter = ["sucursal", "categoria", "periodo", "fuente", "activo_bono"]


@admin.register(BonoVentasEmpleado)
class BonoVentasEmpleadoAdmin(admin.ModelAdmin):
    list_display = ["empleado", "sucursal", "periodo", "dias_trabajados", "sub1", "bono_ventas", "total_a_pagar", "estatus"]
    list_filter = ["sucursal", "estatus", "periodo"]
    search_fields = ["empleado__nombre", "empleado__codigo"]
    inlines = [RegistroVentasInline]


@admin.register(RegistroDiarioVentas)
class RegistroDiarioVentasAdmin(admin.ModelAdmin):
    list_display = ["bono", "dia", "tiene_asistencia", "tiene_uniforme", "tiene_puntualidad"]
    list_filter = ["tiene_asistencia", "tiene_uniforme", "tiene_puntualidad"]

# Register your models here.
