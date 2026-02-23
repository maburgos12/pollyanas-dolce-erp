from django.contrib import admin

from .models import PublicApiAccessLog, PublicApiClient


@admin.register(PublicApiClient)
class PublicApiClientAdmin(admin.ModelAdmin):
    list_display = ("nombre", "clave_prefijo", "activo", "last_used_at", "created_at")
    list_filter = ("activo",)
    search_fields = ("nombre", "clave_prefijo", "descripcion")
    readonly_fields = ("clave_prefijo", "clave_hash", "last_used_at", "created_at", "updated_at")


@admin.register(PublicApiAccessLog)
class PublicApiAccessLogAdmin(admin.ModelAdmin):
    list_display = ("client", "method", "endpoint", "status_code", "created_at")
    list_filter = ("method", "status_code", "created_at")
    search_fields = ("client__nombre", "endpoint")
    autocomplete_fields = ("client",)
