from django.contrib import admin

from .models import EntregaRuta, RutaEntrega


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
