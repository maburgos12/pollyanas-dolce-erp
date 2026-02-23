from django.contrib import admin

from .models import Cliente, PedidoCliente, SeguimientoPedido


@admin.register(Cliente)
class ClienteAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "telefono", "email", "tipo_cliente", "activo")
    list_filter = ("activo", "tipo_cliente")
    search_fields = ("codigo", "nombre", "telefono", "email")


class SeguimientoInline(admin.TabularInline):
    model = SeguimientoPedido
    extra = 0
    readonly_fields = ("fecha_evento", "created_by")


@admin.register(PedidoCliente)
class PedidoClienteAdmin(admin.ModelAdmin):
    list_display = ("folio", "cliente", "estatus", "prioridad", "canal", "fecha_compromiso", "monto_estimado")
    list_filter = ("estatus", "prioridad", "canal")
    search_fields = ("folio", "cliente__nombre", "descripcion", "sucursal")
    inlines = [SeguimientoInline]


@admin.register(SeguimientoPedido)
class SeguimientoPedidoAdmin(admin.ModelAdmin):
    list_display = ("pedido", "fecha_evento", "estatus_anterior", "estatus_nuevo", "created_by")
    list_filter = ("estatus_nuevo",)
    search_fields = ("pedido__folio", "pedido__cliente__nombre", "comentario")
