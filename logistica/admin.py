from django.contrib import admin

from .models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    EntregaRuta,
    InspeccionVehiculo,
    Repartidor,
    ReporteUnidad,
    RutaEntrega,
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
    list_display = ("codigo", "descripcion", "sucursal", "placa", "activa")
    list_filter = ("activa", "sucursal")
    search_fields = ("codigo", "descripcion", "placa", "sucursal__nombre", "sucursal__codigo")


@admin.register(Repartidor)
class RepartidorAdmin(admin.ModelAdmin):
    list_display = ("user", "telefono", "sucursal", "unidad_asignada")
    list_filter = ("sucursal", "unidad_asignada")
    search_fields = ("user__username", "user__first_name", "user__last_name", "telefono", "unidad_asignada__codigo")
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
        "fecha",
        "repartidor",
        "unidad",
        "km_salida",
        "km_llegada",
        "nivel_gas_salida",
        "nivel_gas_llegada",
        "ip_registro",
    )
    list_filter = ("fecha", "unidad__sucursal", "nivel_gas_salida", "nivel_gas_llegada")
    search_fields = ("folio", "repartidor__user__username", "unidad__codigo", "ip_registro")
    readonly_fields = ("fecha", "hora_salida", "hora_llegada", "ip_registro")
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
