from django.db import models
from django.utils import timezone

from maestros.models import Insumo


class ExistenciaInsumo(models.Model):
    insumo = models.OneToOneField(Insumo, on_delete=models.CASCADE)
    stock_actual = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    punto_reorden = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_minimo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_maximo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    inventario_promedio = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    dias_llegada_pedido = models.PositiveIntegerField(default=0)
    consumo_diario_promedio = models.DecimalField(max_digits=18, decimal_places=3, default=0)
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
    source_hash = models.CharField(max_length=64, unique=True, null=True, blank=True)

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


class AlmacenSyncRun(models.Model):
    SOURCE_MANUAL = "MANUAL"
    SOURCE_DRIVE = "DRIVE"
    SOURCE_SCHEDULED = "SCHEDULED"
    SOURCE_CHOICES = [
        (SOURCE_MANUAL, "Manual"),
        (SOURCE_DRIVE, "Google Drive"),
        (SOURCE_SCHEDULED, "Programado"),
    ]

    STATUS_OK = "OK"
    STATUS_ERROR = "ERROR"
    STATUS_CHOICES = [
        (STATUS_OK, "OK"),
        (STATUS_ERROR, "Error"),
    ]

    source = models.CharField(max_length=12, choices=SOURCE_CHOICES)
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default=STATUS_OK)
    triggered_by = models.ForeignKey("auth.User", null=True, blank=True, on_delete=models.SET_NULL)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)

    folder_name = models.CharField(max_length=255, blank=True, default="")
    target_month = models.CharField(max_length=7, blank=True, default="")
    fallback_used = models.BooleanField(default=False)
    downloaded_sources = models.CharField(max_length=255, blank=True, default="")

    rows_stock_read = models.PositiveIntegerField(default=0)
    rows_mov_read = models.PositiveIntegerField(default=0)
    matched = models.PositiveIntegerField(default=0)
    unmatched = models.PositiveIntegerField(default=0)
    insumos_created = models.PositiveIntegerField(default=0)
    existencias_updated = models.PositiveIntegerField(default=0)
    movimientos_created = models.PositiveIntegerField(default=0)
    movimientos_skipped_duplicate = models.PositiveIntegerField(default=0)
    aliases_created = models.PositiveIntegerField(default=0)
    pending_preview = models.JSONField(default=list, blank=True)
    message = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["-started_at"]

    def __str__(self):
        return f"{self.source} {self.status} {self.started_at:%Y-%m-%d %H:%M}"
