from datetime import timedelta
from decimal import Decimal

from django.conf import settings
from django.db import models
from django.utils import timezone

from maestros.models import Proveedor


class Activo(models.Model):
    ESTADO_OPERATIVO = "OPERATIVO"
    ESTADO_MANTENIMIENTO = "MANTENIMIENTO"
    ESTADO_FUERA_SERVICIO = "FUERA_SERVICIO"
    ESTADO_CHOICES = [
        (ESTADO_OPERATIVO, "Operativo"),
        (ESTADO_MANTENIMIENTO, "En mantenimiento"),
        (ESTADO_FUERA_SERVICIO, "Fuera de servicio"),
    ]

    CRITICIDAD_ALTA = "ALTA"
    CRITICIDAD_MEDIA = "MEDIA"
    CRITICIDAD_BAJA = "BAJA"
    CRITICIDAD_CHOICES = [
        (CRITICIDAD_ALTA, "Alta"),
        (CRITICIDAD_MEDIA, "Media"),
        (CRITICIDAD_BAJA, "Baja"),
    ]

    codigo = models.CharField(max_length=32, unique=True, blank=True)
    nombre = models.CharField(max_length=180)
    categoria = models.CharField(max_length=120, blank=True, default="")
    ubicacion = models.CharField(max_length=160, blank=True, default="")
    proveedor_mantenimiento = models.ForeignKey(
        Proveedor,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="activos_mantenimiento",
    )
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_OPERATIVO)
    criticidad = models.CharField(max_length=10, choices=CRITICIDAD_CHOICES, default=CRITICIDAD_MEDIA)
    fecha_alta = models.DateField(default=timezone.localdate)
    valor_reposicion = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0"))
    vida_util_meses = models.PositiveIntegerField(default=60)
    horas_uso_promedio_mes = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal("0"))
    notas = models.TextField(blank=True, default="")
    activo = models.BooleanField(default=True)
    actualizado_en = models.DateTimeField(auto_now=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nombre", "id"]
        verbose_name = "Activo"
        verbose_name_plural = "Activos"

    def save(self, *args, **kwargs):
        if not self.codigo:
            ymd = timezone.localdate().strftime("%y%m")
            prefix = f"ACT-{ymd}-"
            seq = Activo.objects.filter(codigo__startswith=prefix).count() + 1
            self.codigo = f"{prefix}{seq:03d}"
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.codigo} · {self.nombre}"


class PlanMantenimiento(models.Model):
    TIPO_PREVENTIVO = "PREVENTIVO"
    TIPO_CALIBRACION = "CALIBRACION"
    TIPO_LIMPIEZA = "LIMPIEZA"
    TIPO_INSPECCION = "INSPECCION"
    TIPO_CHOICES = [
        (TIPO_PREVENTIVO, "Preventivo"),
        (TIPO_CALIBRACION, "Calibración"),
        (TIPO_LIMPIEZA, "Limpieza"),
        (TIPO_INSPECCION, "Inspección"),
    ]

    ESTATUS_ACTIVO = "ACTIVO"
    ESTATUS_PAUSADO = "PAUSADO"
    ESTATUS_CHOICES = [
        (ESTATUS_ACTIVO, "Activo"),
        (ESTATUS_PAUSADO, "Pausado"),
    ]

    activo_ref = models.ForeignKey(
        Activo,
        on_delete=models.CASCADE,
        related_name="planes_mantenimiento",
    )
    nombre = models.CharField(max_length=180)
    tipo = models.CharField(max_length=16, choices=TIPO_CHOICES, default=TIPO_PREVENTIVO)
    frecuencia_dias = models.PositiveIntegerField(default=30)
    tolerancia_dias = models.PositiveIntegerField(default=0)
    ultima_ejecucion = models.DateField(null=True, blank=True)
    proxima_ejecucion = models.DateField(null=True, blank=True)
    responsable = models.CharField(max_length=120, blank=True, default="")
    instrucciones = models.TextField(blank=True, default="")
    estatus = models.CharField(max_length=16, choices=ESTATUS_CHOICES, default=ESTATUS_ACTIVO)
    activo = models.BooleanField(default=True)
    actualizado_en = models.DateTimeField(auto_now=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["proxima_ejecucion", "id"]
        verbose_name = "Plan de mantenimiento"
        verbose_name_plural = "Planes de mantenimiento"

    def __str__(self):
        return f"{self.nombre} · {self.activo_ref.nombre}"

    def recompute_next_date(self):
        if self.ultima_ejecucion and self.frecuencia_dias > 0:
            self.proxima_ejecucion = self.ultima_ejecucion + timedelta(days=self.frecuencia_dias)

    def save(self, *args, **kwargs):
        if self.estatus == self.ESTATUS_ACTIVO and not self.proxima_ejecucion:
            if self.ultima_ejecucion and self.frecuencia_dias > 0:
                self.recompute_next_date()
        super().save(*args, **kwargs)


class OrdenMantenimiento(models.Model):
    TIPO_PREVENTIVO = "PREVENTIVO"
    TIPO_CORRECTIVO = "CORRECTIVO"
    TIPO_CHOICES = [
        (TIPO_PREVENTIVO, "Preventivo"),
        (TIPO_CORRECTIVO, "Correctivo"),
    ]

    PRIORIDAD_CRITICA = "CRITICA"
    PRIORIDAD_ALTA = "ALTA"
    PRIORIDAD_MEDIA = "MEDIA"
    PRIORIDAD_BAJA = "BAJA"
    PRIORIDAD_CHOICES = [
        (PRIORIDAD_CRITICA, "Crítica"),
        (PRIORIDAD_ALTA, "Alta"),
        (PRIORIDAD_MEDIA, "Media"),
        (PRIORIDAD_BAJA, "Baja"),
    ]

    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_EN_PROCESO = "EN_PROCESO"
    ESTATUS_CERRADA = "CERRADA"
    ESTATUS_CANCELADA = "CANCELADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_EN_PROCESO, "En proceso"),
        (ESTATUS_CERRADA, "Cerrada"),
        (ESTATUS_CANCELADA, "Cancelada"),
    ]

    folio = models.CharField(max_length=24, unique=True, blank=True)
    activo_ref = models.ForeignKey(Activo, on_delete=models.PROTECT, related_name="ordenes_mantenimiento")
    plan_ref = models.ForeignKey(
        PlanMantenimiento,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ordenes_generadas",
    )
    tipo = models.CharField(max_length=16, choices=TIPO_CHOICES, default=TIPO_PREVENTIVO)
    prioridad = models.CharField(max_length=10, choices=PRIORIDAD_CHOICES, default=PRIORIDAD_MEDIA)
    estatus = models.CharField(max_length=16, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    fecha_programada = models.DateField(default=timezone.localdate)
    fecha_inicio = models.DateField(null=True, blank=True)
    fecha_cierre = models.DateField(null=True, blank=True)
    responsable = models.CharField(max_length=120, blank=True, default="")
    descripcion = models.TextField(blank=True, default="")
    costo_repuestos = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0"))
    costo_mano_obra = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0"))
    costo_otros = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0"))
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ordenes_mantenimiento_creadas",
    )
    aprobado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ordenes_mantenimiento_aprobadas",
    )
    actualizado_en = models.DateTimeField(auto_now=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fecha_programada", "-id"]
        verbose_name = "Orden de mantenimiento"
        verbose_name_plural = "Órdenes de mantenimiento"

    def save(self, *args, **kwargs):
        if not self.folio:
            ymd = timezone.localdate().strftime("%y%m%d")
            prefix = f"OM-{ymd}-"
            seq = OrdenMantenimiento.objects.filter(folio__startswith=prefix).count() + 1
            self.folio = f"{prefix}{seq:03d}"
        super().save(*args, **kwargs)

    @property
    def costo_total(self) -> Decimal:
        return (self.costo_repuestos or Decimal("0")) + (self.costo_mano_obra or Decimal("0")) + (
            self.costo_otros or Decimal("0")
        )

    def __str__(self):
        return self.folio


class BitacoraMantenimiento(models.Model):
    orden = models.ForeignKey(OrdenMantenimiento, on_delete=models.CASCADE, related_name="bitacora")
    fecha = models.DateTimeField(default=timezone.now)
    accion = models.CharField(max_length=80)
    comentario = models.TextField(blank=True, default="")
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )
    costo_adicional = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0"))

    class Meta:
        ordering = ["-fecha", "-id"]
        verbose_name = "Bitácora de mantenimiento"
        verbose_name_plural = "Bitácoras de mantenimiento"

    def __str__(self):
        return f"{self.orden.folio} · {self.accion}"
