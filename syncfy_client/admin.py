from django.contrib import admin

from syncfy_client.models import CuentaBancaria, LogSyncfy, MovimientoBancario


@admin.register(CuentaBancaria)
class CuentaBancariaAdmin(admin.ModelAdmin):
    list_display = ("banco", "nombre_display", "numero_cuenta", "activa", "ultima_sync", "saldo_actual")
    list_filter = ("banco", "activa", "creado_en")
    search_fields = ("nombre_display", "numero_cuenta", "id_site_syncfy", "id_credential", "id_account")
    readonly_fields = ("creado_en", "actualizado_en", "ultima_sync", "saldo_actual")


@admin.register(MovimientoBancario)
class MovimientoBancarioAdmin(admin.ModelAdmin):
    list_display = (
        "cuenta",
        "fecha_transaccion",
        "tipo",
        "monto",
        "moneda",
        "conciliado",
        "tipo_conciliacion",
        "descripcion",
    )
    list_filter = ("cuenta__banco", "tipo", "moneda", "conciliado", "tipo_conciliacion", "fecha_transaccion")
    search_fields = ("id_transaction", "descripcion", "cuenta__numero_cuenta", "nota_conciliacion")
    readonly_fields = ("descargado_en", "extra_raw", "conciliado_en")
    date_hierarchy = "fecha_transaccion"


@admin.register(LogSyncfy)
class LogSyncfyAdmin(admin.ModelAdmin):
    list_display = ("nivel", "cuenta", "movimientos_nuevos", "movimientos_total", "duracion_segundos", "creado_en")
    list_filter = ("nivel", "creado_en", "cuenta__banco")
    search_fields = ("mensaje", "cuenta__nombre_display", "cuenta__numero_cuenta")
    readonly_fields = ("creado_en",)
