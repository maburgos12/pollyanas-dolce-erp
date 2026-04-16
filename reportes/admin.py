from django.contrib import admin

from reportes.models import (
    Alert,
    AutoControlSettings,
    AutoPurchaseRequestSnapshot,
    CargaGastoOperativoArchivo,
    CategoriaGasto,
    CentroCosto,
    CorteOficialDiario,
    EmpresaResultadoMensual,
    ExpansionPolicyConfig,
    ExpansionZoneScore,
    ForecastCalibrationProfile,
    GastoOperativoMensual,
    InsumoCostoHistoricoMensual,
    OperationsMetricSnapshot,
    PresupuestoImport,
    PresupuestoLineaMensual,
    PresupuestoResumenMensual,
    ProductionExecutionLog,
    ProductionOrder,
    ProductionOrderLine,
    ProyectoInversion,
    ProyectoInversionAlerta,
    ProyectoInversionEscenario,
    ProyectoInversionGasto,
    ProyectoInversionPagoDeuda,
    ProyectoInversionSnapshotMensual,
    ProductoCostoOperativoMensual,
    ProductoPricingDecisionMensual,
    ProductoSucursalContribucionMensual,
    RecetaCostoHistoricoMensual,
    ReglaCostoHistoricoInsumo,
    ReglaAsignacionGasto,
    SupplierLeadTime,
)


@admin.register(CargaGastoOperativoArchivo)
class CargaGastoOperativoArchivoAdmin(admin.ModelAdmin):
    list_display = (
        "uploaded_at",
        "original_filename",
        "source_channel",
        "status",
        "processed_rows",
        "loaded_rows",
        "project_refresh_count",
        "uploaded_by",
    )
    list_filter = ("status", "source_channel", "target_year", "uploaded_at")
    search_fields = ("original_filename", "stored_file_path", "file_hash")


@admin.register(CentroCosto)
class CentroCostoAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "tipo", "sucursal", "activo")
    list_filter = ("tipo", "activo")
    search_fields = ("codigo", "nombre", "sucursal__codigo", "sucursal__nombre")


@admin.register(CategoriaGasto)
class CategoriaGastoAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "capa_objetivo", "bucket", "activo")
    list_filter = ("capa_objetivo", "bucket", "activo")
    search_fields = ("codigo", "nombre")


@admin.register(ReglaAsignacionGasto)
class ReglaAsignacionGastoAdmin(admin.ModelAdmin):
    list_display = ("nombre", "categoria_gasto", "centro_costo", "base_reparto", "activo", "prioridad")
    list_filter = ("base_reparto", "activo", "categoria_gasto")
    search_fields = ("nombre", "categoria_gasto__codigo", "centro_costo__codigo")


@admin.register(GastoOperativoMensual)
class GastoOperativoMensualAdmin(admin.ModelAdmin):
    list_display = (
        "periodo",
        "centro_costo",
        "categoria_gasto",
        "external_key",
        "monto",
        "tipo_dato",
        "fuente",
        "es_estimado",
    )
    list_filter = ("periodo", "tipo_dato", "fuente", "es_estimado", "categoria_gasto", "centro_costo")
    search_fields = ("external_key", "comentario", "archivo_soporte", "centro_costo__codigo", "categoria_gasto__codigo")


@admin.register(ProductoCostoOperativoMensual)
class ProductoCostoOperativoMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "receta", "unidades_base", "asp", "costo_fabricacion_unit")
    list_filter = ("periodo",)
    search_fields = ("receta__nombre", "receta__codigo_point")


@admin.register(ProductoSucursalContribucionMensual)
class ProductoSucursalContribucionMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "sucursal", "receta", "venta_total", "contribucion_total", "margen_contribucion_pct")
    list_filter = ("periodo", "sucursal")
    search_fields = ("receta__nombre", "receta__codigo_point", "sucursal__codigo", "sucursal__nombre")


@admin.register(EmpresaResultadoMensual)
class EmpresaResultadoMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "venta_total", "margen_bruto_total", "contribucion_total", "utilidad_operativa_total")
    list_filter = ("periodo",)


@admin.register(CorteOficialDiario)
class CorteOficialDiarioAdmin(admin.ModelAdmin):
    list_display = ("corte_date", "total_amount", "total_tickets", "avg_ticket", "branch_scope", "source_label")
    list_filter = ("corte_date", "source_label")
    search_fields = ("branch_scope", "source_label", "evidence_path", "notes")


@admin.register(ProductoPricingDecisionMensual)
class ProductoPricingDecisionMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "receta", "accion_sugerida", "asp_actual", "gap_precio", "impacto_estimado")
    list_filter = ("periodo", "accion_sugerida")
    search_fields = ("receta__nombre", "receta__codigo_point")


@admin.register(InsumoCostoHistoricoMensual)
class InsumoCostoHistoricoMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "insumo", "costo_unitario", "metodo", "source_date", "sample_count")
    list_filter = ("periodo", "metodo")
    search_fields = ("insumo__nombre", "insumo__codigo_point", "insumo__codigo")


@admin.register(ReglaCostoHistoricoInsumo)
class ReglaCostoHistoricoInsumoAdmin(admin.ModelAdmin):
    list_display = ("insumo_origen", "metodo", "insumo_referencia", "prioridad", "activo")
    list_filter = ("metodo", "activo")
    search_fields = (
        "insumo_origen__nombre",
        "insumo_origen__codigo_point",
        "insumo_origen__codigo",
        "insumo_referencia__nombre",
        "insumo_referencia__codigo_point",
        "insumo_referencia__codigo",
        "notas",
    )


@admin.register(RecetaCostoHistoricoMensual)
class RecetaCostoHistoricoMensualAdmin(admin.ModelAdmin):
    list_display = ("periodo", "receta", "costo_total", "costo_por_unidad_rendimiento", "coverage_pct")
    list_filter = ("periodo",)
    search_fields = ("receta__nombre", "receta__codigo_point")


@admin.register(PresupuestoImport)
class PresupuestoImportAdmin(admin.ModelAdmin):
    list_display = ("fuente_nombre", "sheet_name", "tipo", "archivo_hash", "created_at")
    list_filter = ("tipo", "sheet_name")
    search_fields = ("fuente_nombre", "titulo", "archivo_ruta", "archivo_hash")


@admin.register(PresupuestoLineaMensual)
class PresupuestoLineaMensualAdmin(admin.ModelAdmin):
    list_display = ("period", "concept", "monthly_budget", "monthly_actual", "monthly_variance", "audit_status", "importacion")
    list_filter = ("period", "audit_status", "importacion__fuente_nombre")
    search_fields = ("concept", "account_code", "external_key", "importacion__fuente_nombre")


@admin.register(PresupuestoResumenMensual)
class PresupuestoResumenMensualAdmin(admin.ModelAdmin):
    list_display = ("period", "tipo", "fuente_nombre", "total_budget", "total_actual", "total_variance", "line_count")
    list_filter = ("period", "tipo", "fuente_nombre")
    search_fields = ("fuente_nombre",)


@admin.register(ProductionExecutionLog)
class ProductionExecutionLogAdmin(admin.ModelAdmin):
    list_display = (
        "fecha",
        "sucursal",
        "receta",
        "recomendado",
        "aprobado",
        "producido_real",
        "vendido_real",
        "merma",
        "desviacion",
        "stock_visible",
        "decision_score",
        "recommendation_version",
        "usuario",
    )
    list_filter = ("fecha", "sucursal", "usuario")
    search_fields = (
        "receta__nombre",
        "receta__codigo_point",
        "sucursal__codigo",
        "sucursal__nombre",
        "usuario__username",
        "comentario",
    )


class ProductionOrderLineInline(admin.TabularInline):
    model = ProductionOrderLine
    extra = 0
    autocomplete_fields = ("receta",)
    fields = (
        "receta",
        "cantidad_recomendada",
        "cantidad_aprobada",
        "cantidad_ejecutada",
        "decision_score",
        "riesgo_merma",
        "motivo",
    )
    readonly_fields = ("created_at", "updated_at")


@admin.register(ProductionOrder)
class ProductionOrderAdmin(admin.ModelAdmin):
    list_display = (
        "fecha",
        "sucursal",
        "status",
        "source",
        "recommendation_version",
        "approved_by",
        "approved_at",
        "released_at",
        "executed_at",
    )
    list_filter = ("fecha", "status", "source", "sucursal")
    search_fields = ("sucursal__codigo", "sucursal__nombre", "recommendation_version")
    inlines = [ProductionOrderLineInline]


@admin.register(ForecastCalibrationProfile)
class ForecastCalibrationProfileAdmin(admin.ModelAdmin):
    list_display = (
        "reference_date",
        "sucursal",
        "familia",
        "weekly_pattern",
        "rotation_band",
        "sample_size",
        "wape_before_pct",
        "wape_after_pct",
        "hit_rate_before_pct",
        "hit_rate_after_pct",
        "buffer_multiplier",
    )
    list_filter = ("reference_date", "weekly_pattern", "rotation_band", "sucursal")
    search_fields = ("familia", "sucursal__codigo", "sucursal__nombre")


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = (
        "fecha",
        "tipo",
        "severidad",
        "entidad",
        "sucursal",
        "impacto_estimado",
        "resuelta",
        "resolved_at",
        "resolved_by",
    )
    list_filter = ("fecha", "tipo", "severidad", "resuelta", "sucursal")
    search_fields = ("entidad", "mensaje", "sucursal__codigo", "sucursal__nombre", "receta__nombre", "insumo__nombre")


@admin.register(OperationsMetricSnapshot)
class OperationsMetricSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        "fecha",
        "adoption_pct",
        "approval_deviation_avg",
        "execution_deviation_avg",
        "merma_total",
        "impacto_economico_estimado",
        "impacto_real",
        "desviacion_impacto",
        "adopcion_real",
        "efectividad_recomendaciones",
        "generated_at",
    )
    list_filter = ("fecha",)


@admin.register(SupplierLeadTime)
class SupplierLeadTimeAdmin(admin.ModelAdmin):
    list_display = ("insumo", "proveedor", "lead_time_dias", "frecuencia_pedido_dias", "lote_minimo", "activo")
    list_filter = ("activo", "proveedor")
    search_fields = ("insumo__nombre", "insumo__codigo", "proveedor__nombre")


@admin.register(AutoControlSettings)
class AutoControlSettingsAdmin(admin.ModelAdmin):
    list_display = (
        "singleton_key",
        "max_variacion_produccion_pct",
        "max_compra_diaria",
        "min_stock_seguridad",
        "enable_auto_purchase",
        "enable_alerts",
        "actualizado_por",
        "actualizado_en",
    )


@admin.register(AutoPurchaseRequestSnapshot)
class AutoPurchaseRequestSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        "fecha",
        "sucursal",
        "insumo",
        "proveedor",
        "cantidad_sugerida",
        "stock_actual",
        "stock_objetivo",
        "purchase_priority_score",
        "fecha_sugerida_compra",
    )
    list_filter = ("fecha", "sucursal", "proveedor")
    search_fields = (
        "insumo__nombre",
        "insumo__codigo",
        "sucursal__codigo",
        "sucursal__nombre",
        "solicitud__folio",
    )


@admin.register(ProyectoInversion)
class ProyectoInversionAdmin(admin.ModelAdmin):
    list_display = ("nombre_proyecto", "tipo_proyecto", "sucursal_relacionada", "estatus", "fecha_inicio", "fecha_apertura", "monto_inversion_planeado", "monto_inversion_real", "capital_inicial_aportado", "discount_rate")
    list_filter = ("tipo_proyecto", "estatus", "recovery_strategy")
    search_fields = ("nombre_proyecto", "sucursal_relacionada__codigo", "sucursal_relacionada__nombre")


@admin.register(ProyectoInversionGasto)
class ProyectoInversionGastoAdmin(admin.ModelAdmin):
    list_display = ("proyecto", "fecha", "categoria", "descripcion", "proveedor_nombre", "monto_total", "financiado")
    list_filter = ("categoria", "financiado", "fecha")
    search_fields = ("proyecto__nombre_proyecto", "descripcion", "proveedor_nombre", "referencia_compra", "referencia_contable")


@admin.register(ProyectoInversionPagoDeuda)
class ProyectoInversionPagoDeudaAdmin(admin.ModelAdmin):
    list_display = ("proyecto", "fecha_pago", "monto_pago", "interes_pagado", "capital_amortizado", "saldo_insoluto")
    list_filter = ("fecha_pago",)
    search_fields = ("proyecto__nombre_proyecto", "referencia", "notas")


@admin.register(ProyectoInversionEscenario)
class ProyectoInversionEscenarioAdmin(admin.ModelAdmin):
    list_display = (
        "proyecto",
        "nombre",
        "tipo_escenario",
        "estatus_simulacion",
        "ventas_promedio_mensuales",
        "margen_bruto_pct",
        "gastos_operativos_mensuales",
        "capturado_por",
    )
    list_filter = ("tipo_escenario", "estatus_simulacion")
    search_fields = ("proyecto__nombre_proyecto", "nombre")


@admin.register(ProyectoInversionSnapshotMensual)
class ProyectoInversionSnapshotMensualAdmin(admin.ModelAdmin):
    list_display = ("proyecto", "periodo", "ventas_mensuales", "flujo_libre", "recuperacion_acumulada", "porcentaje_recuperado", "cash_on_cash", "data_source", "confidence_score", "health_status")
    list_filter = ("periodo", "proyecto", "data_source", "health_status")
    search_fields = ("proyecto__nombre_proyecto",)


@admin.register(ProyectoInversionAlerta)
class ProyectoInversionAlertaAdmin(admin.ModelAdmin):
    list_display = ("proyecto", "codigo", "severidad", "activa", "last_detected_at", "resolved_at")
    list_filter = ("codigo", "severidad", "activa")
    search_fields = ("proyecto__nombre_proyecto", "titulo", "mensaje")


@admin.register(ExpansionPolicyConfig)
class ExpansionPolicyConfigAdmin(admin.ModelAdmin):
    list_display = ("nombre", "activa", "min_free_cashflow_total", "max_debt_to_income_ratio", "max_average_payback_months", "max_projects_in_risk")
    list_filter = ("activa",)
    search_fields = ("nombre", "notes")


@admin.register(ExpansionZoneScore)
class ExpansionZoneScoreAdmin(admin.ModelAdmin):
    list_display = ("ciudad", "zona", "score_estimado", "ventas_promedio", "densidad")
    search_fields = ("ciudad", "zona", "competencia")
