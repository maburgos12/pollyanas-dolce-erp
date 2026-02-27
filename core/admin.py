from django.contrib import admin
from .models import Sucursal, Departamento, UserProfile, AuditLog

@admin.register(Sucursal)
class SucursalAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "activa")
    search_fields = ("codigo", "nombre")
    list_filter = ("activa",)

@admin.register(Departamento)
class DepartamentoAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre")
    search_fields = ("codigo", "nombre")

@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ("user", "departamento", "sucursal", "modo_captura_sucursal", "telefono")
    search_fields = ("user__username", "user__email")
    list_filter = ("departamento", "sucursal", "modo_captura_sucursal")

@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ("timestamp", "user", "action", "model", "object_id")
    list_filter = ("action", "model")
    search_fields = ("model", "object_id", "user__username")
    readonly_fields = ("timestamp", "user", "action", "model", "object_id", "payload")
