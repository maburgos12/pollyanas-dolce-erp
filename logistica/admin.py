from django.contrib import admin

from .models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    ConfigAlertaFlota,
    DocumentoUnidad,
    EntregaRuta,
    InspeccionDiaria,
    InspeccionVehiculo,
    LavadoUnidad,
    ReparacionUnidad,
    Repartidor,
    ReporteUnidad,
    RutaEntrega,
    ServicioRealizadoUnidad,
    TipoServicioUnidad,
    Unidad,
)


@admin.register(RutaEntrega)
class RutaEntregaAdmin(admin.ModelAdmin):
    list_display = (
        "folio",
        "nombre",
        "fecha_ruta",
        "chofer",
        "unidad",
        "estatus",
        "total_entregas",
        "entregas_completadas",
        "entregas_incidencia",
        "monto_estimado_total",
    )
    list_filter = ("estatus", "fecha_ruta")
    search_fields = ("folio", "nombre", "chofer", "unidad")
    readonly_fields = (
        "folio",
        "created_at",
        "updated_at",
        "total_entregas",
        "entregas_completadas",
        "entregas_incidencia",
        "monto_estimado_total",
    )


@admin.register(EntregaRuta)
class EntregaRutaAdmin(admin.ModelAdmin):
    list_display = (
        "ruta",
        "secuencia",
        "cliente_nombre",
        "pedido",
        "estatus",
        "monto_estimado",
        "ventana_inicio",
        "ventana_fin",
        "entregado_at",
    )
    list_filter = ("estatus", "ruta__fecha_ruta")
    search_fields = ("ruta__folio", "cliente_nombre", "pedido__folio", "direccion", "contacto")
    autocomplete_fields = ("ruta", "pedido")


@admin.register(Unidad)
class UnidadAdmin(admin.ModelAdmin):
    list_display = ("codigo", "descripcion", "sucursal", "placa", "activa", "folio_consecutivo")
    list_filter = ("activa", "sucursal")
    search_fields = ("codigo", "descripcion", "placa", "sucursal__nombre", "sucursal__codigo")


@admin.register(Repartidor)
class RepartidorAdmin(admin.ModelAdmin):
    list_display = (
        "user",
        "telefono",
        "sucursal",
        "unidad_asignada",
        "numero_licencia",
        "licencia_expedicion",
        "licencia_expiracion",
    )
    list_filter = ("sucursal", "unidad_asignada", "licencia_expiracion")
    search_fields = (
        "user__username",
        "user__first_name",
        "user__last_name",
        "telefono",
        "numero_licencia",
        "unidad_asignada__codigo",
    )
    autocomplete_fields = ("user", "unidad_asignada", "sucursal")


@admin.register(ReporteUnidad)
class ReporteUnidadAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "unidad",
        "repartidor",
        "tipo",
        "severidad",
        "estatus",
        "fecha_reporte",
        "ip_reporte",
        "asignado_a",
        "proveedor_servicio",
        "costo_servicio",
        "notificacion_escalada",
    )
    list_filter = ("estatus", "severidad", "tipo", "fecha_reporte", "unidad__sucursal", "notificacion_escalada")
    search_fields = (
        "unidad__codigo",
        "unidad__placa",
        "repartidor__user__username",
        "descripcion",
        "proveedor_servicio",
        "notas_compras",
    )
    readonly_fields = ("fecha_reporte", "actualizado_en")
    autocomplete_fields = ("repartidor", "unidad", "asignado_a")


@admin.register(BitacoraRepartidor)
class BitacoraRepartidorAdmin(admin.ModelAdmin):
    list_display = ("repartidor", "fecha", "km_inicio", "km_fin", "actualizado_en")
    list_filter = ("fecha", "repartidor__sucursal")
    search_fields = ("repartidor__user__username", "repartidor__user__first_name", "repartidor__user__last_name", "novedades")
    readonly_fields = ("creado_en", "actualizado_en")
    autocomplete_fields = ("repartidor",)


@admin.register(BitacoraSalidaLlegada)
class BitacoraSalidaLlegadaAdmin(admin.ModelAdmin):
    list_display = (
        "folio",
        "repartidor",
        "unidad",
        "hora_salida",
        "hora_llegada",
        "km_salida",
        "km_llegada",
        "cerrada",
        "ip_registro",
    )
    list_filter = ("cerrada", "unidad", "fecha")
    search_fields = ("folio", "repartidor__user__username", "unidad__codigo", "ip_registro")
    readonly_fields = ("fecha", "folio", "hora_salida", "hora_llegada", "ip_registro")
    autocomplete_fields = ("repartidor", "unidad")


@admin.register(InspeccionVehiculo)
class InspeccionVehiculoAdmin(admin.ModelAdmin):
    list_display = (
        "fecha",
        "repartidor",
        "unidad",
        "km_entrada",
        "km_salida",
        "nivel_gas_entrada",
        "tiene_golpes",
        "ip_registro",
    )
    list_filter = ("fecha", "unidad__sucursal", "nivel_gas_entrada", "tiene_golpes")
    search_fields = ("repartidor__user__username", "unidad__codigo", "descripcion_golpes", "observaciones", "ip_registro")
    readonly_fields = ("fecha", "ip_registro")
    autocomplete_fields = ("repartidor", "unidad")


@admin.register(InspeccionDiaria)
class InspeccionDiariaAdmin(admin.ModelAdmin):
    list_display = ("unidad", "repartidor", "fecha", "hora", "tiene_fallas", "reporte_generado")
    list_filter = ("fecha", "tiene_fallas", "unidad")
    search_fields = ("unidad__codigo", "repartidor__user__username")
    autocomplete_fields = ("unidad", "repartidor", "reporte_generado")
    readonly_fields = ("fecha", "hora", "ip_registro")


@admin.register(DocumentoUnidad)
class DocumentoUnidadAdmin(admin.ModelAdmin):
    list_display = ("unidad", "tipo", "fecha_vencimiento", "vigente", "registrado_por")
    list_filter = ("tipo", "vigente", "unidad")
    search_fields = ("unidad__codigo", "tipo")
    autocomplete_fields = ("unidad", "registrado_por")
    readonly_fields = ("fecha_registro",)


@admin.register(TipoServicioUnidad)
class TipoServicioUnidadAdmin(admin.ModelAdmin):
    list_display = ("nombre", "tipo_intervalo", "intervalo_km", "intervalo_meses", "activo")
    list_filter = ("tipo_intervalo", "activo", "aplica_todas_unidades")
    search_fields = ("nombre", "notas")


@admin.register(ServicioRealizadoUnidad)
class ServicioRealizadoUnidadAdmin(admin.ModelAdmin):
    list_display = (
        "unidad",
        "tipo_servicio",
        "fecha_servicio",
        "km_al_servicio",
        "costo",
        "proxima_fecha",
        "proximos_km",
    )
    list_filter = ("tipo_servicio", "unidad")
    search_fields = ("unidad__codigo", "tipo_servicio__nombre", "proveedor", "notas")
    autocomplete_fields = ("unidad", "tipo_servicio", "registrado_por")
    readonly_fields = ("fecha_registro", "proxima_fecha", "proximos_km")


@admin.register(LavadoUnidad)
class LavadoUnidadAdmin(admin.ModelAdmin):
    list_display = ("unidad", "fecha", "costo", "registrado_por")
    list_filter = ("unidad",)
    search_fields = ("unidad__codigo", "notas")
    autocomplete_fields = ("unidad", "registrado_por")
    readonly_fields = ("fecha_registro",)


@admin.register(ReparacionUnidad)
class ReparacionUnidadAdmin(admin.ModelAdmin):
    list_display = ("unidad", "fecha_ingreso", "fecha_entrega", "costo_total", "proveedor", "reporte_origen")
    list_filter = ("unidad",)
    search_fields = ("unidad__codigo", "descripcion_falla", "descripcion_reparacion", "proveedor", "notas")
    autocomplete_fields = ("unidad", "reporte_origen", "registrado_por")
    readonly_fields = ("fecha_registro",)


@admin.register(ConfigAlertaFlota)
class ConfigAlertaFlotaAdmin(admin.ModelAdmin):
    list_display = ("tipo", "activa", "dias_anticipacion_1", "dias_anticipacion_2", "dias_anticipacion_3")
    list_filter = ("activa", "tipo")
    filter_horizontal = ("destinatarios",)
