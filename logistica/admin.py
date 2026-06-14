from django.contrib import admin

from .models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    ConfigAlertaFlota,
    DocumentoUnidad,
    EntregaRuta,
    EventoRuta,
    InspeccionDiaria,
    InspeccionVehiculo,
    LavadoUnidad,
    ParadaRuta,
    ParadaEntregaEvidencia,
    PuntoLogistico,
    ReparacionUnidad,
    Repartidor,
    ReporteUnidad,
    ReporteUnidadReafirmacion,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
    UbicacionRuta,
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
        "repartidor",
        "unidad_operativa",
        "estatus",
        "cumplimiento_porcentaje",
        "total_entregas",
        "entregas_completadas",
        "entregas_incidencia",
        "monto_estimado_total",
    )
    list_filter = ("estatus", "fecha_ruta", "repartidor", "unidad_operativa")
    search_fields = ("folio", "nombre", "chofer", "unidad", "repartidor__user__username", "unidad_operativa__codigo")
    readonly_fields = (
        "folio",
        "estatus",
        "created_at",
        "updated_at",
        "cumplimiento_porcentaje",
        "total_entregas",
        "entregas_completadas",
        "entregas_incidencia",
        "monto_estimado_total",
    )
    autocomplete_fields = ("repartidor", "unidad_operativa", "bitacora_salida")

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj and obj.estatus != RutaEntrega.ESTATUS_PLANEADA:
            fields.extend(
                [
                    "nombre",
                    "fecha_ruta",
                    "chofer",
                    "unidad",
                    "repartidor",
                    "unidad_operativa",
                    "bitacora_salida",
                    "hora_inicio_real",
                    "hora_cierre_real",
                ]
            )
        return tuple(dict.fromkeys(fields))

    def has_delete_permission(self, request, obj=None):
        return False


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

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj and obj.ruta.estatus != RutaEntrega.ESTATUS_PLANEADA:
            fields.extend(
                [
                    "ruta",
                    "secuencia",
                    "pedido",
                    "cliente_nombre",
                    "direccion",
                    "contacto",
                    "telefono",
                    "ventana_inicio",
                    "ventana_fin",
                    "estatus",
                    "monto_estimado",
                    "comentario",
                    "entregado_at",
                ]
            )
        return tuple(dict.fromkeys(fields))

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        obj.ruta.recompute_totals()
        obj.ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])


@admin.register(PuntoLogistico)
class PuntoLogisticoAdmin(admin.ModelAdmin):
    list_display = ("nombre", "tipo", "sucursal", "radio_geocerca_metros", "activo")
    list_filter = ("tipo", "activo", "sucursal")
    search_fields = ("nombre", "sucursal__nombre", "sucursal__codigo", "notas")
    autocomplete_fields = ("sucursal",)


@admin.register(ParadaRuta)
class ParadaRutaAdmin(admin.ModelAdmin):
    list_display = ("ruta", "orden", "punto_nombre_snapshot", "punto", "estado", "hora_estimada", "hora_llegada_real", "distancia_llegada_metros")
    list_filter = ("estado", "ruta__fecha_ruta", "punto__tipo")
    search_fields = ("ruta__folio", "ruta__nombre", "punto__nombre", "notas")
    autocomplete_fields = ("ruta", "punto")
    readonly_fields = (
        "ruta",
        "punto",
        "orden",
        "punto_nombre_snapshot",
        "latitud_geocerca",
        "longitud_geocerca",
        "radio_geocerca_metros",
        "estado",
        "hora_llegada_real",
        "hora_salida_real",
        "distancia_llegada_metros",
        "creado_en",
        "actualizado_en",
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class RutaCargaChecklistLineaInline(admin.TabularInline):
    model = RutaCargaChecklistLinea
    extra = 0
    can_delete = False
    fields = (
        "parada",
        "item_code",
        "item_name",
        "unit",
        "cantidad_enviada_esperada",
        "cantidad_cargada",
        "estatus",
        "motivo_diferencia",
        "validado_por",
        "validado_en",
    )
    readonly_fields = fields

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(RutaCargaChecklist)
class RutaCargaChecklistAdmin(admin.ModelAdmin):
    list_display = ("ruta", "estatus", "sincronizado_en", "confirmado_por", "confirmado_en")
    list_filter = ("estatus", "ruta__fecha_ruta")
    search_fields = ("ruta__folio", "ruta__nombre", "lineas__item_name", "lineas__item_code")
    readonly_fields = (
        "ruta",
        "estatus",
        "point_sync_job",
        "sincronizado_en",
        "confirmado_por",
        "confirmado_en",
        "motivo_override",
        "notas",
        "creado_en",
        "actualizado_en",
    )
    autocomplete_fields = ("ruta", "point_sync_job", "confirmado_por")
    inlines = [RutaCargaChecklistLineaInline]

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(RutaCargaChecklistLinea)
class RutaCargaChecklistLineaAdmin(admin.ModelAdmin):
    list_display = ("checklist", "parada", "item_name", "cantidad_enviada_esperada", "cantidad_cargada", "estatus")
    list_filter = ("estatus", "checklist__ruta__fecha_ruta", "motivo_diferencia")
    search_fields = ("checklist__ruta__folio", "item_name", "item_code", "transfer_external_id", "detail_external_id")
    readonly_fields = [field.name for field in RutaCargaChecklistLinea._meta.fields]
    autocomplete_fields = ("checklist", "parada", "validado_por")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(ParadaEntregaEvidencia)
class ParadaEntregaEvidenciaAdmin(admin.ModelAdmin):
    list_display = ("ruta", "parada", "tipo", "cantidad_entregada", "capturado_por", "capturado_en")
    list_filter = ("tipo", "ruta__fecha_ruta")
    search_fields = ("ruta__folio", "parada__punto_nombre_snapshot", "comentario", "client_event_id")
    readonly_fields = [field.name for field in ParadaEntregaEvidencia._meta.fields]
    autocomplete_fields = ("ruta", "parada", "linea_carga", "capturado_por")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(UbicacionRuta)
class UbicacionRutaAdmin(admin.ModelAdmin):
    list_display = ("ruta", "repartidor", "unidad", "timestamp_servidor", "fuera_de_geocerca", "precision_metros", "bateria_porcentaje")
    list_filter = ("fuera_de_geocerca", "timestamp_servidor", "unidad")
    search_fields = ("ruta__folio", "repartidor__user__username", "unidad__codigo", "ip_registro")
    readonly_fields = (
        "ruta",
        "repartidor",
        "unidad",
        "latitud",
        "longitud",
        "precision_metros",
        "velocidad_kmh",
        "bateria_porcentaje",
        "timestamp_dispositivo",
        "timestamp_servidor",
        "ip_registro",
        "fuera_de_geocerca",
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(EventoRuta)
class EventoRutaAdmin(admin.ModelAdmin):
    list_display = ("ruta", "tipo", "severidad", "parada", "distancia_metros", "creado_en", "creado_por")
    list_filter = ("tipo", "severidad", "creado_en")
    search_fields = ("ruta__folio", "ruta__nombre", "descripcion", "parada__punto__nombre")
    readonly_fields = (
        "ruta",
        "parada",
        "ubicacion",
        "tipo",
        "severidad",
        "descripcion",
        "latitud",
        "longitud",
        "distancia_metros",
        "metadata",
        "creado_por",
        "creado_en",
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Unidad)
class UnidadAdmin(admin.ModelAdmin):
    list_display = ("codigo", "descripcion", "sucursal", "placa", "activa", "folio_consecutivo")
    list_filter = ("activa", "sucursal")
    search_fields = ("codigo", "descripcion", "placa", "sucursal__nombre", "sucursal__codigo")


@admin.register(Repartidor)
class RepartidorAdmin(admin.ModelAdmin):
    list_display = (
        "user",
        "tipo_identidad",
        "empresa_externa",
        "telefono",
        "sucursal",
        "unidad_asignada",
        "numero_licencia",
        "licencia_expedicion",
        "licencia_expiracion",
    )
    list_filter = ("tipo_identidad", "sucursal", "unidad_asignada", "licencia_expiracion")
    search_fields = (
        "user__username",
        "user__first_name",
        "user__last_name",
        "telefono",
        "numero_licencia",
        "empresa_externa",
        "autorizado_por",
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


@admin.register(ReporteUnidadReafirmacion)
class ReporteUnidadReafirmacionAdmin(admin.ModelAdmin):
    list_display = ("reporte", "repartidor", "creado_en", "ip_registro")
    list_filter = ("creado_en", "reporte__unidad", "repartidor__sucursal")
    search_fields = ("reporte__unidad__codigo", "repartidor__user__username", "comentario", "ip_registro")
    readonly_fields = ("creado_en", "ip_registro")
    autocomplete_fields = ("reporte", "repartidor")


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


@admin.register(CargaCombustibleUnidad)
class CargaCombustibleUnidadAdmin(admin.ModelAdmin):
    list_display = ("fecha_registro", "unidad", "repartidor", "bitacora", "litros", "importe_total", "nivel_gas_despues")
    list_filter = ("fecha_registro", "unidad", "repartidor")
    search_fields = ("bitacora__folio", "unidad__codigo", "repartidor__user__username", "repartidor__user__first_name")
    readonly_fields = ("fecha_registro", "ip_registro")
    autocomplete_fields = ("bitacora", "unidad", "repartidor")


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
    list_display = ("unidad", "fecha", "partes_lavadas_display", "costo", "registrado_por", "ip_registro")
    list_filter = ("unidad", "lavado_exterior", "lavado_interior", "lavado_caja_refrigerada", "fecha")
    search_fields = ("unidad__codigo", "notas", "registrado_por__username")
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
