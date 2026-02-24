from django.db import models
from django.conf import settings
from django.utils import timezone

class Sucursal(models.Model):
    codigo = models.CharField(max_length=20, unique=True)
    nombre = models.CharField(max_length=120)
    activa = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Sucursal"
        verbose_name_plural = "Sucursales"

    def __str__(self) -> str:
        return f"{self.codigo} - {self.nombre}"

class Departamento(models.Model):
    codigo = models.CharField(max_length=30, unique=True)
    nombre = models.CharField(max_length=120)

    class Meta:
        verbose_name = "Departamento"
        verbose_name_plural = "Departamentos"

    def __str__(self) -> str:
        return f"{self.codigo} - {self.nombre}"

class UserProfile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    departamento = models.ForeignKey(Departamento, null=True, blank=True, on_delete=models.SET_NULL)
    sucursal = models.ForeignKey(Sucursal, null=True, blank=True, on_delete=models.SET_NULL)
    telefono = models.CharField(max_length=30, blank=True, default="")
    lock_maestros = models.BooleanField(default=False)
    lock_recetas = models.BooleanField(default=False)
    lock_compras = models.BooleanField(default=False)
    lock_inventario = models.BooleanField(default=False)
    lock_reportes = models.BooleanField(default=False)
    lock_crm = models.BooleanField(default=False)
    lock_logistica = models.BooleanField(default=False)
    lock_rrhh = models.BooleanField(default=False)
    lock_captura_piso = models.BooleanField(default=False)
    lock_auditoria = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Perfil de usuario"
        verbose_name_plural = "Perfiles de usuario"

    def __str__(self) -> str:
        return f"Perfil: {self.user.username}"

class AuditLog(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    action = models.CharField(max_length=64)  # CREATE/UPDATE/DELETE/APPROVE/IMPORT
    model = models.CharField(max_length=128)
    object_id = models.CharField(max_length=64, blank=True, default="")
    payload = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Bitácora (Audit)"
        verbose_name_plural = "Bitácora (Audit)"
        ordering = ["-timestamp"]

    def __str__(self) -> str:
        return f"{self.timestamp:%Y-%m-%d %H:%M} {self.action} {self.model} {self.object_id}"
