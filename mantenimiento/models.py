from django.conf import settings
from django.db import models
from django.utils import timezone


class ProveedorServicio(models.Model):
    """Talleres, técnicos y empresas de mantenimiento — separado de los proveedores de insumos."""

    nombre = models.CharField(max_length=200)
    contacto = models.CharField(max_length=120, blank=True, default="", verbose_name="Nombre del contacto")
    telefono = models.CharField(max_length=30, blank=True, default="")
    especialidad = models.CharField(max_length=120, blank=True, default="",
                                    help_text="Ej. Refrigeración, Electricidad, Mecánica general")
    notas = models.TextField(blank=True, default="")
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre"]
        verbose_name = "Proveedor de servicio"
        verbose_name_plural = "Proveedores de servicio"

    def __str__(self):
        return self.nombre


class SolicitudCancelacion(models.Model):
    TIPO_FALLA = "falla"
    TIPO_UNIDAD = "unidad"
    TIPO_ORDEN = "orden"
    TIPO_CHOICES = [
        (TIPO_FALLA, "Reporte de falla"),
        (TIPO_UNIDAD, "Reporte de unidad logística"),
        (TIPO_ORDEN, "Orden de mantenimiento"),
    ]

    ESTATUS_PENDIENTE = "pendiente"
    ESTATUS_APROBADA = "aprobada"
    ESTATUS_RECHAZADA = "rechazada"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_APROBADA, "Aprobada y eliminada"),
        (ESTATUS_RECHAZADA, "Rechazada"),
    ]

    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    objeto_id = models.PositiveIntegerField()
    referencia = models.CharField(max_length=200)
    motivo = models.TextField()
    estatus = models.CharField(max_length=12, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    solicitado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name="solicitudes_cancelacion",
    )
    resuelto_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cancelaciones_resueltas",
    )
    notas_resolucion = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)
    resuelto_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Solicitud de cancelación"
        verbose_name_plural = "Solicitudes de cancelación"

    def __str__(self):
        return f"{self.get_tipo_display()} #{self.objeto_id} · {self.estatus}"
