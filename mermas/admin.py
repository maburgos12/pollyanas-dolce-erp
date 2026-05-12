from django.contrib import admin

from .models import MermaEvidencia, MermaProducto, MermaRegistro, PersonalEnviosSucursal


class MermaProductoInline(admin.TabularInline):
    model = MermaProducto
    extra = 0


class MermaEvidenciaInline(admin.TabularInline):
    model = MermaEvidencia
    extra = 0
    readonly_fields = ("creado_en",)


@admin.register(MermaRegistro)
class MermaRegistroAdmin(admin.ModelAdmin):
    list_display = ("folio", "sucursal", "estatus", "ticket_point", "repartidor", "iniciado_en", "alerta_ventas")
    list_filter = ("estatus", "alerta_ventas", "sucursal", "iniciado_en")
    search_fields = ("folio", "ticket_point", "sucursal__nombre", "productos__producto_texto", "productos__receta__nombre")
    readonly_fields = ("folio", "created_at", "updated_at")
    inlines = [MermaProductoInline, MermaEvidenciaInline]


@admin.register(MermaProducto)
class MermaProductoAdmin(admin.ModelAdmin):
    list_display = ("registro", "nombre_producto", "cantidad_enviada", "cantidad_recibida", "conforme")
    search_fields = ("registro__folio", "producto_texto", "receta__nombre")


@admin.register(MermaEvidencia)
class MermaEvidenciaAdmin(admin.ModelAdmin):
    list_display = ("registro", "tipo", "subido_por", "creado_en")
    list_filter = ("tipo", "creado_en")


@admin.register(PersonalEnviosSucursal)
class PersonalEnviosSucursalAdmin(admin.ModelAdmin):
    list_display = ("nombre", "user", "sucursal", "telefono", "activo")
    list_filter = ("activo", "sucursal")
    search_fields = ("user__first_name", "user__last_name", "user__username", "telefono")

    @admin.display(description="Nombre")
    def nombre(self, obj):
        return obj.user.get_full_name() or obj.user.username
