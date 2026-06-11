from django.contrib import admin

from sat_client.models import CfdiDescargado, CfdiPagoRelacionado, LogDescargaSat, SolicitudDescarga


@admin.register(SolicitudDescarga)
class SolicitudDescargaAdmin(admin.ModelAdmin):
    list_display = (
        "id_solicitud",
        "rfc_solicitante",
        "direccion",
        "fecha_inicial",
        "fecha_final",
        "estado",
        "numero_cfdis",
        "creado_en",
    )
    list_filter = ("direccion", "estado", "tipo_solicitud", "creado_en")
    search_fields = ("id_solicitud", "rfc_solicitante", "codigo_estado")
    readonly_fields = ("creado_en", "actualizado_en")


@admin.register(CfdiDescargado)
class CfdiDescargadoAdmin(admin.ModelAdmin):
    list_display = (
        "uuid",
        "tipo_cfdi",
        "rfc_emisor",
        "rfc_receptor",
        "total",
        "fecha_emision",
        "conciliado",
    )
    list_filter = ("tipo_cfdi", "tipo_comprobante", "moneda", "conciliado", "fecha_emision")
    search_fields = ("uuid", "rfc_emisor", "nombre_emisor", "rfc_receptor", "nombre_receptor")
    readonly_fields = ("descargado_en",)
    date_hierarchy = "fecha_emision"


@admin.register(CfdiPagoRelacionado)
class CfdiPagoRelacionadoAdmin(admin.ModelAdmin):
    list_display = ("cfdi_pago", "uuid_relacionado", "fecha_pago", "monto", "forma_pago", "num_parcialidad")
    list_filter = ("forma_pago", "moneda", "fecha_pago", "cfdi_pago__tipo_cfdi")
    search_fields = ("cfdi_pago__uuid", "uuid_relacionado")
    readonly_fields = ("creado_en",)
    date_hierarchy = "fecha_pago"


@admin.register(LogDescargaSat)
class LogDescargaSatAdmin(admin.ModelAdmin):
    list_display = ("nivel", "solicitud", "cfdis_descargados", "cfdis_nuevos", "duracion_segundos", "creado_en")
    list_filter = ("nivel", "creado_en")
    search_fields = ("mensaje", "solicitud__id_solicitud")
    readonly_fields = ("creado_en",)
