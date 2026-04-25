"""
sucursales/admin_rentabilidad.py

Registrar en admin.py:
    from .admin_rentabilidad import SucursalRentabilidadAdmin
    admin.site.register(SucursalRentabilidad, SucursalRentabilidadAdmin)
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from .models_rentabilidad import SucursalRentabilidad, EstadoRentabilidad


ESTADO_ICONS = {
    EstadoRentabilidad.SUBSIDIADA:   "🔴",
    EstadoRentabilidad.EQUILIBRIO:   "🟡",
    EstadoRentabilidad.RECUPERACION: "🔵",
    EstadoRentabilidad.RENTABLE:     "🟢",
    EstadoRentabilidad.ESTRELLA:     "⭐",
    EstadoRentabilidad.SIN_DATOS:    "⚫",
}

class SucursalRentabilidadAdmin(admin.ModelAdmin):
    list_display = [
        "sucursal", "periodo_fmt", "estado_badge",
        "ventas_netas_fmt", "margen_bruto_fmt", "utilidad_fmt",
        "avance_pe_fmt", "payback_fmt", "alerta_nivel",
    ]
    list_filter  = ["estado", "alerta_nivel", "periodo", "calculado_por_agente"]
    search_fields = ["sucursal__nombre"]
    date_hierarchy = "periodo"
    ordering = ["-periodo", "-ventas_brutas"]
    readonly_fields = [
        "estado", "alerta_nivel", "calculado_en", "calculado_por_agente",
        "diagnostico_ia", "recomendaciones_ia",
        # Propiedades calculadas (se muestran en el detail)
        "ventas_netas_display", "margen_bruto_display", "utilidad_display",
        "pe_display", "recuperacion_display",
    ]

    fieldsets = (
        ("Identificación", {
            "fields": ("sucursal", "periodo", "fecha_apertura", "inversion_inicial"),
        }),
        ("Ingresos", {
            "fields": ("ventas_brutas", "descuentos", "devoluciones"),
        }),
        ("Costos variables", {
            "fields": ("costo_materia_prima", "costo_reventa", "empaque", "otros_costos_variables"),
        }),
        ("Gastos fijos", {
            "fields": ("renta", "nomina_directa", "servicios_luz_agua",
                       "mantenimiento", "gastos_admin_prorrateados", "otros_gastos_fijos"),
        }),
        ("Subsidio", {
            "fields": ("subsidio_recibido",),
        }),
        ("Resumen calculado (solo lectura)", {
            "fields": ("ventas_netas_display", "margen_bruto_display",
                       "utilidad_display", "pe_display", "recuperacion_display"),
            "classes": ("collapse",),
        }),
        ("Diagnóstico IA", {
            "fields": ("estado", "alerta_nivel", "diagnostico_ia",
                       "recomendaciones_ia", "calculado_por_agente", "calculado_en"),
        }),
        ("Notas", {
            "fields": ("notas_manuales",),
        }),
    )

    actions = ["recalcular_estado", "lanzar_agente_ia"]

    # ---- Columnas formateadas ---- #
    def periodo_fmt(self, obj):
        return obj.periodo.strftime("%b %Y")
    periodo_fmt.short_description = "Periodo"
    periodo_fmt.admin_order_field = "periodo"

    def estado_badge(self, obj):
        icon = ESTADO_ICONS.get(obj.estado, "")
        return format_html('<b>{} {}</b>', icon, obj.get_estado_display())
    estado_badge.short_description = "Estado"

    def ventas_netas_fmt(self, obj):
        return f"${obj.ventas_netas:,.0f}"
    ventas_netas_fmt.short_description = "Ventas netas"

    def margen_bruto_fmt(self, obj):
        pct = obj.porcentaje_margen_bruto
        color = "#15803D" if pct >= 55 else ("#854D0E" if pct >= 40 else "#B91C1C")
        return format_html('<span style="color:{}">{:.1f}%</span>', color, pct)
    margen_bruto_fmt.short_description = "Margen bruto"

    def utilidad_fmt(self, obj):
        val = obj.utilidad_operativa
        color = "#15803D" if val >= 0 else "#B91C1C"
        return format_html('<span style="color:{}">${:,.0f}</span>', color, val)
    utilidad_fmt.short_description = "Utilidad op."

    def avance_pe_fmt(self, obj):
        pct = obj.porcentaje_avance_pe
        color = "#15803D" if pct >= 100 else ("#854D0E" if pct >= 85 else "#B91C1C")
        return format_html('<span style="color:{}">{:.1f}%</span>', color, pct)
    avance_pe_fmt.short_description = "Avance PE"

    def payback_fmt(self, obj):
        p = obj.payback_meses_estimados
        if p is None:
            return format_html('<span style="color:#B91C1C;">No viable</span>')
        if p == 0:
            return format_html('<span style="color:#15803D;">Completado</span>')
        return f"{p} meses"
    payback_fmt.short_description = "Payback"

    # ---- Campos readonly de display ---- #
    def ventas_netas_display(self, obj): return f"${obj.ventas_netas:,.2f}"
    ventas_netas_display.short_description = "Ventas netas"

    def margen_bruto_display(self, obj):
        return f"${obj.margen_bruto:,.2f}  ({obj.porcentaje_margen_bruto}%)"
    margen_bruto_display.short_description = "Margen bruto"

    def utilidad_display(self, obj):
        return f"${obj.utilidad_operativa:,.2f}  ({obj.porcentaje_utilidad_operativa}%)"
    utilidad_display.short_description = "Utilidad operativa"

    def pe_display(self, obj):
        return f"PE = ${obj.punto_equilibrio_mensual:,.2f}  |  Avance: {obj.porcentaje_avance_pe}%  |  Brecha: ${obj.brecha_pe:,.2f}"
    pe_display.short_description = "Punto de equilibrio"

    def recuperacion_display(self, obj):
        return (f"{obj.porcentaje_recuperacion_inversion}% recuperado  |  "
                f"Pendiente: ${obj.inversion_pendiente:,.2f}  |  "
                f"Payback: {obj.payback_meses_estimados} meses  |  "
                f"ROI: {obj.roi_anualizado}%")
    recuperacion_display.short_description = "Recuperación inversión"

    # ---- Acciones masivas ---- #
    def recalcular_estado(self, request, queryset):
        for obj in queryset:
            obj.calcular_estado()
            obj.save(update_fields=["estado", "alerta_nivel"])
        self.message_user(request, f"{queryset.count()} registros recalculados.")
    recalcular_estado.short_description = "Recalcular estado (sin IA)"

    def lanzar_agente_ia(self, request, queryset):
        from .agente_rentabilidad import analizar_sucursal
        ok, fail = 0, 0
        for obj in queryset:
            try:
                analizar_sucursal(obj, guardar=True)
                ok += 1
            except Exception:
                fail += 1
        self.message_user(request, f"IA: {ok} exitosos, {fail} fallidos.")
    lanzar_agente_ia.short_description = "Analizar con agente IA"
