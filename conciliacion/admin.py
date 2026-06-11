from django.contrib import admin

from conciliacion.models import ConceptoConciliacion, ImportacionBancaria


@admin.register(ConceptoConciliacion)
class ConceptoConciliacionAdmin(admin.ModelAdmin):
    list_display = (
        "codigo",
        "nombre",
        "familia",
        "tipo_movimiento",
        "cfdi_esperado",
        "forma_pago_esperada",
        "permite_conciliacion_automatica",
        "activo",
    )
    list_filter = (
        "familia",
        "tipo_movimiento",
        "cfdi_esperado",
        "afecta_iva",
        "afecta_isr",
        "activo",
    )
    search_fields = ("codigo", "nombre", "descripcion", "cuenta_contable_sugerida")
    readonly_fields = ("creado_en", "actualizado_en")
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "codigo",
                    "nombre",
                    "descripcion",
                    "familia",
                    "tipo_movimiento",
                    "activo",
                    "prioridad",
                )
            },
        ),
        (
            "Evidencia fiscal",
            {
                "fields": (
                    "cfdi_esperado",
                    "forma_pago_esperada",
                    "requiere_rep",
                    "requiere_cfdi_recibido",
                    "requiere_evidencia_externa",
                    "evidencia_requerida",
                )
            },
        ),
        (
            "Reglas contables",
            {
                "fields": (
                    "afecta_iva",
                    "afecta_isr",
                    "afecta_flujo",
                    "cuenta_contable_sugerida",
                    "palabras_clave",
                )
            },
        ),
        (
            "Automatizacion",
            {
                "fields": (
                    "permite_conciliacion_automatica",
                    "tolerancia_monto",
                    "tolerancia_porcentaje",
                    "creado_en",
                    "actualizado_en",
                )
            },
        ),
    )


@admin.register(ImportacionBancaria)
class ImportacionBancariaAdmin(admin.ModelAdmin):
    list_display = (
        "cuenta",
        "fuente",
        "estado",
        "archivo_nombre",
        "total_filas",
        "movimientos_nuevos",
        "movimientos_duplicados",
        "filas_con_error",
        "creado_en",
    )
    list_filter = ("fuente", "estado", "cuenta__banco", "creado_en")
    search_fields = ("archivo_nombre", "archivo_hash", "cuenta__nombre_display", "cuenta__numero_cuenta")
    readonly_fields = ("creado_en", "actualizado_en")
