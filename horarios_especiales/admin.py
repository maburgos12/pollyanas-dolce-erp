from django.contrib import admin

from .models import (
    HorarioEspecialBitacora,
    HorarioEspecialDetalle,
    HorarioEspecialIntentoPublicacion,
    SolicitudHorarioEspecial,
    SucursalAlias,
    SucursalPlataformaExterna,
)


class HorarioEspecialDetalleInline(admin.TabularInline):
    model = HorarioEspecialDetalle
    extra = 0
    readonly_fields = ("created_at", "updated_at")


@admin.register(SucursalAlias)
class SucursalAliasAdmin(admin.ModelAdmin):
    list_display = ("alias", "sucursal", "source", "is_active", "updated_at")
    list_filter = ("source", "is_active")
    search_fields = ("alias", "alias_normalizado", "sucursal__codigo", "sucursal__nombre")
    readonly_fields = ("alias_normalizado", "created_at", "updated_at")


@admin.register(SucursalPlataformaExterna)
class SucursalPlataformaExternaAdmin(admin.ModelAdmin):
    list_display = ("sucursal", "platform", "external_location_name", "is_active", "last_published_at")
    list_filter = ("platform", "is_active")
    search_fields = ("sucursal__codigo", "sucursal__nombre", "external_location_id", "external_location_name")
    readonly_fields = ("created_at", "updated_at", "last_validated_at", "last_published_at")


@admin.register(SolicitudHorarioEspecial)
class SolicitudHorarioEspecialAdmin(admin.ModelAdmin):
    list_display = ("request_code", "status", "source_channel", "requested_by", "approved_by", "created_at")
    list_filter = ("status", "source_channel")
    search_fields = ("request_code", "raw_command", "reason")
    readonly_fields = ("request_code", "idempotency_key", "created_at", "updated_at", "approved_at", "executed_at", "cancelled_at")
    inlines = [HorarioEspecialDetalleInline]


@admin.register(HorarioEspecialIntentoPublicacion)
class HorarioEspecialIntentoPublicacionAdmin(admin.ModelAdmin):
    list_display = ("detail", "platform", "status", "attempt_no", "started_at", "finished_at")
    list_filter = ("platform", "status")
    search_fields = ("detail__request__request_code", "detail__sucursal__codigo", "external_operation_id", "error_message")
    readonly_fields = ("started_at", "finished_at", "request_payload_json", "response_payload_json", "error_payload_json")


@admin.register(HorarioEspecialBitacora)
class HorarioEspecialBitacoraAdmin(admin.ModelAdmin):
    list_display = ("request", "action", "actor_user", "actor_role", "created_at")
    list_filter = ("action", "actor_role")
    search_fields = ("request__request_code", "actor_user__username")
    readonly_fields = ("payload_json", "created_at")

