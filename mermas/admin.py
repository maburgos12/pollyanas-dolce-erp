from django.contrib import admin

from .models import (
    MermaEvidencia, MermaInsumo, MermaInsumoEvento, MermaProducto, MermaRegistro,
    OrdenAjustePoint, PersonalEnviosSucursal,
)


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


@admin.register(MermaInsumo)
class MermaInsumoAdmin(admin.ModelAdmin):
    list_display = ("id", "sucursal", "nombre_point", "cantidad_reportada", "unidad_point", "estatus", "jefe_inmediato", "creado_en")
    list_filter = ("estatus", "sucursal", "unidad_point")
    search_fields = ("codigo_point", "nombre_point", "reportado_por__username", "jefe_inmediato__username")
    readonly_fields = tuple(field.name for field in MermaInsumo._meta.fields)

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(MermaInsumoEvento)
class MermaInsumoEventoAdmin(admin.ModelAdmin):
    list_display = ("merma", "estado_anterior", "estado_nuevo", "actor", "creado_en")
    readonly_fields = ("merma", "estado_anterior", "estado_nuevo", "actor", "motivo", "metadata", "creado_en")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(OrdenAjustePoint)
class OrdenAjustePointAdmin(admin.ModelAdmin):
    list_display = ("id", "merma", "sucursal", "codigo_point", "cantidad", "estatus", "intentos", "actualizado_en")
    list_filter = ("estatus", "sucursal")
    readonly_fields = ("merma", "sucursal", "codigo_point", "unidad_point", "cantidad", "idempotency_key", "payload_hash", "existencia_antes", "existencia_despues", "referencia_point", "evidencia_tecnica", "creado_en", "aplicado_en", "actualizado_en")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
