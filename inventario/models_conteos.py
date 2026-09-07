"""Conteos físicos de sucursal: evidencia independiente del inventario operativo."""
from django.conf import settings
from django.db import models
from django.db.models import Q


class ConteoSucursal(models.Model):
    ESTADOS = [(s, s.capitalize()) for s in ('CAPTURA','ENVIADO','RECONTEO','ACEPTADO','CANCELADO')]
    sucursal = models.ForeignKey('core.Sucursal', on_delete=models.PROTECT)
    responsable = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name='conteos_asignados')
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name='conteos_creados')
    fecha = models.DateField()
    titulo = models.CharField(max_length=180)
    estado = models.CharField(max_length=12, choices=ESTADOS, default='CAPTURA')
    version = models.PositiveIntegerField(default=1)
    ronda = models.PositiveIntegerField(default=1)
    observaciones = models.TextField(blank=True, default='')
    referencia = models.JSONField(default=dict, blank=True)
    request_id = models.UUIDField(unique=True)
    payload_hash = models.CharField(max_length=64)
    creado_en = models.DateTimeField(auto_now_add=True)
    iniciado_en = models.DateTimeField(null=True, blank=True)
    enviado_en = models.DateTimeField(null=True, blank=True)
    aceptado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-fecha','-pk']
        indexes = [models.Index(fields=['sucursal','fecha','estado'])]


class LineaConteoSucursal(models.Model):
    conteo = models.ForeignKey(ConteoSucursal, on_delete=models.PROTECT, related_name='lineas')
    producto = models.ForeignKey('pos_bridge.PointProduct', null=True, blank=True, on_delete=models.PROTECT)
    insumo = models.ForeignKey('maestros.Insumo', null=True, blank=True, on_delete=models.PROTECT)
    codigo = models.CharField(max_length=120)
    nombre = models.CharField(max_length=255)
    unidad = models.CharField(max_length=60)
    fuente_unidad = models.CharField(max_length=255)

    @property
    def tipo(self):
        return 'producto' if self.producto_id else 'insumo'

    class Meta:
        ordering = ['nombre','pk']
        constraints = [
            models.CheckConstraint(check=Q(producto__isnull=False, insumo__isnull=True)|Q(producto__isnull=True, insumo__isnull=False), name='cs_linea_fuente_xor'),
            models.UniqueConstraint(fields=['conteo','producto'],name='cs_producto_unico'),
            models.UniqueConstraint(fields=['conteo','insumo'],name='cs_insumo_unico'),
        ]


class LecturaConteoSucursal(models.Model):
    linea = models.ForeignKey(LineaConteoSucursal, on_delete=models.PROTECT, related_name='lecturas')
    ronda = models.PositiveIntegerField()
    cantidad = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    incidencia = models.TextField(blank=True, default='')
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['linea','ronda'],name='cs_lectura_ronda_unica'), models.CheckConstraint(check=Q(cantidad__isnull=True)|Q(cantidad__gte=0),name='cs_cantidad_no_negativa')]


class EventoConteoSucursal(models.Model):
    conteo = models.ForeignKey(ConteoSucursal, on_delete=models.PROTECT, related_name='eventos')
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    action = models.CharField(max_length=40)
    payload = models.JSONField(default=dict)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['pk']

    def save(self, *args, **kwargs):
        if not self._state.adding:
            raise ValueError('Los eventos de conteo son inmutables.')
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise ValueError('Los eventos de conteo no se eliminan.')


class OperacionConteoSucursal(models.Model):
    conteo = models.ForeignKey(ConteoSucursal, on_delete=models.PROTECT, related_name='operaciones')
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    request_id = models.UUIDField()
    fingerprint = models.CharField(max_length=64)
    result = models.JSONField(default=dict)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['conteo','request_id'],name='cs_operacion_unica')]


class AccesoConteoSucursal(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    sucursal = models.ForeignKey('core.Sucursal', on_delete=models.PROTECT)
    activo = models.BooleanField(default=True)
    capturar = models.BooleanField(default=False)
    revisar = models.BooleanField(default=False)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['user','sucursal'],name='cs_acceso_unico')]
