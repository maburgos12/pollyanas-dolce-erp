from django.contrib import admin

from conciliacion.models import ImportacionBancaria


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
