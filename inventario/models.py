from django.db import models
from django.utils import timezone

from maestros.models import Insumo


class ExistenciaInsumo(models.Model):
    insumo = models.OneToOneField(Insumo, on_delete=models.CASCADE)
    stock_actual = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    punto_reorden = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    actualizado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Existencia de insumo"
        verbose_name_plural = "Existencias de insumos"
        ordering = ["insumo__nombre"]

    def __str__(self):
        return self.insumo.nombre


class MovimientoInventario(models.Model):
    TIPO_ENTRADA = "ENTRADA"
    TIPO_SALIDA = "SALIDA"
    TIPO_CONSUMO = "CONSUMO"
    TIPO_AJUSTE = "AJUSTE"
    TIPO_CHOICES = [
        (TIPO_ENTRADA, "Entrada"),
        (TIPO_SALIDA, "Salida"),
        (TIPO_CONSUMO, "Consumo"),
        (TIPO_AJUSTE, "Ajuste"),
    ]

    fecha = models.DateTimeField(default=timezone.now)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES)
    insumo = models.ForeignKey(Insumo, on_delete=models.PROTECT)
    cantidad = models.DecimalField(max_digits=18, decimal_places=3)
    referencia = models.CharField(max_length=120, blank=True, default="")

    class Meta:
        ordering = ["-fecha"]

    def __str__(self):
        return f"{self.tipo} {self.insumo.nombre} {self.cantidad}"


class AjusteInventario(models.Model):
    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_APLICADO = "APLICADO"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente aprobación"),
        (STATUS_APLICADO, "Aplicado"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    insumo = models.ForeignKey(Insumo, on_delete=models.PROTECT)
    cantidad_sistema = models.DecimalField(max_digits=18, decimal_places=3)
    cantidad_fisica = models.DecimalField(max_digits=18, decimal_places=3)
    motivo = models.CharField(max_length=255)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]

    def save(self, *args, **kwargs):
        if not self.folio:
            ymd = timezone.localdate().strftime("%y%m%d")
            prefix = f"AJ-{ymd}-"
            today_count = AjusteInventario.objects.filter(folio__startswith=prefix).count() + 1
            self.folio = f"{prefix}{today_count:03d}"
        super().save(*args, **kwargs)

    def __str__(self):
        return self.folio
