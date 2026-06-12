from django.contrib import admin

from conciliacion.models import (
    CfdiSucursalResolucion,
    ConceptoConciliacion,
    ContraparteConciliacion,
    CuentaBancariaPropia,
    CuentaContableConciliacion,
    ImportacionBancaria,
    InstrumentoFinancieroConciliacion,
    ReglaClasificacionMovimiento,
    SucursalIdentificadorFiscal,
)


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


@admin.register(CuentaContableConciliacion)
class CuentaContableConciliacionAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "tipo", "naturaleza", "agrupador_sat", "cuenta_contpaqi", "activa")
    list_filter = ("tipo", "naturaleza", "activa")
    search_fields = ("codigo", "nombre", "agrupador_sat", "cuenta_contpaqi")
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(CuentaBancariaPropia)
class CuentaBancariaPropiaAdmin(admin.ModelAdmin):
    list_display = ("alias", "cuenta_bancaria", "empresa_rfc", "ultimos_digitos", "cuenta_contable", "activa")
    list_filter = ("activa", "empresa_rfc", "cuenta_bancaria__banco")
    search_fields = (
        "alias",
        "empresa_rfc",
        "clabe",
        "ultimos_digitos",
        "cuenta_bancaria__nombre_display",
        "cuenta_bancaria__numero_cuenta",
    )
    autocomplete_fields = ("cuenta_bancaria", "cuenta_contable")
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(ContraparteConciliacion)
class ContraparteConciliacionAdmin(admin.ModelAdmin):
    list_display = ("tipo", "nombre", "rfc", "cuenta_contable", "activa")
    list_filter = ("tipo", "activa")
    search_fields = ("nombre", "rfc", "palabras_clave")
    autocomplete_fields = ("cuenta_contable",)
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(InstrumentoFinancieroConciliacion)
class InstrumentoFinancieroConciliacionAdmin(admin.ModelAdmin):
    list_display = ("tipo", "nombre", "institucion", "numero_referencia", "cuenta_contable_pasivo", "activo")
    list_filter = ("tipo", "institucion", "activo")
    search_fields = ("nombre", "institucion", "numero_referencia", "patrones_descripcion")
    autocomplete_fields = (
        "contraparte",
        "cuenta_bancaria_pago",
        "cuenta_contable_pasivo",
        "cuenta_contable_intereses",
    )
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(ReglaClasificacionMovimiento)
class ReglaClasificacionMovimientoAdmin(admin.ModelAdmin):
    list_display = (
        "nombre",
        "concepto",
        "tipo_movimiento",
        "prioridad",
        "contraparte_tipo",
        "instrumento_tipo",
        "requiere_cuenta_propia_destino",
        "confianza_base",
        "activa",
    )
    list_filter = (
        "tipo_movimiento",
        "contraparte_tipo",
        "instrumento_tipo",
        "requiere_cuenta_propia_destino",
        "activa",
    )
    search_fields = ("nombre", "concepto__codigo", "concepto__nombre", "patrones_descripcion")
    autocomplete_fields = ("concepto", "cuenta_debe_sugerida", "cuenta_haber_sugerida")
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(SucursalIdentificadorFiscal)
class SucursalIdentificadorFiscalAdmin(admin.ModelAdmin):
    list_display = ("sucursal", "patron", "tipo", "prioridad", "activo")
    list_filter = ("tipo", "activo", "sucursal")
    search_fields = ("sucursal__codigo", "sucursal__nombre", "patron", "descripcion")
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(CfdiSucursalResolucion)
class CfdiSucursalResolucionAdmin(admin.ModelAdmin):
    list_display = ("cfdi", "sucursal", "fuente", "confianza", "revisado", "actualizado_en")
    list_filter = ("fuente", "sucursal", "revisado", "confianza")
    search_fields = ("cfdi__uuid", "cfdi__rfc_receptor", "cfdi__nombre_receptor", "texto_detectado")
    readonly_fields = ("creado_en", "actualizado_en")
    autocomplete_fields = ("cfdi", "sucursal")


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
