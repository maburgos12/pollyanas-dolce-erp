from django.db import models
from django.utils import timezone

from maestros.models import Insumo, Proveedor


class SolicitudCompra(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_EN_REVISION = "EN_REVISION"
    STATUS_APROBADA = "APROBADA"
    STATUS_RECHAZADA = "RECHAZADA"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_EN_REVISION, "En revisión"),
        (STATUS_APROBADA, "Aprobada"),
        (STATUS_RECHAZADA, "Rechazada"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    area = models.CharField(max_length=120)
    solicitante = models.CharField(max_length=120)
    insumo = models.ForeignKey(Insumo, on_delete=models.PROTECT)
    proveedor_sugerido = models.ForeignKey(
        Proveedor,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="solicitudes_sugeridas",
    )
    cantidad = models.DecimalField(max_digits=18, decimal_places=3)
    fecha_requerida = models.DateField(default=timezone.localdate)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_BORRADOR)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]

    def save(self, *args, **kwargs):
        if not self.folio:
            ymd = timezone.localdate().strftime("%y%m%d")
            prefix = f"SOL-{ymd}-"
            today_count = SolicitudCompra.objects.filter(folio__startswith=prefix).count() + 1
            self.folio = f"{prefix}{today_count:03d}"
        super().save(*args, **kwargs)

    def __str__(self):
        return self.folio


class OrdenCompra(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_ENVIADA = "ENVIADA"
    STATUS_CONFIRMADA = "CONFIRMADA"
    STATUS_PARCIAL = "PARCIAL"
    STATUS_CERRADA = "CERRADA"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_ENVIADA, "Enviada"),
        (STATUS_CONFIRMADA, "Confirmada"),
        (STATUS_PARCIAL, "Parcial"),
        (STATUS_CERRADA, "Cerrada"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    solicitud = models.ForeignKey(SolicitudCompra, null=True, blank=True, on_delete=models.SET_NULL)
    referencia = models.CharField(max_length=160, blank=True, default="")
    proveedor = models.ForeignKey(Proveedor, on_delete=models.PROTECT)
    fecha_emision = models.DateField(default=timezone.localdate)
    fecha_entrega_estimada = models.DateField(null=True, blank=True)
    monto_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_BORRADOR)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]

    def save(self, *args, **kwargs):
        if not self.folio:
            ymd = timezone.localdate().strftime("%y%m%d")
            prefix = f"OC-{ymd}-"
            today_count = OrdenCompra.objects.filter(folio__startswith=prefix).count() + 1
            self.folio = f"{prefix}{today_count:03d}"
        super().save(*args, **kwargs)

    def __str__(self):
        return self.folio


class RecepcionCompra(models.Model):
    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_DIFERENCIAS = "DIFERENCIAS"
    STATUS_CERRADA = "CERRADA"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente"),
        (STATUS_DIFERENCIAS, "Con diferencias"),
        (STATUS_CERRADA, "Cerrada"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    orden = models.ForeignKey(OrdenCompra, on_delete=models.PROTECT)
    fecha_recepcion = models.DateField(default=timezone.localdate)
    conformidad_pct = models.DecimalField(max_digits=5, decimal_places=2, default=100)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    observaciones = models.CharField(max_length=255, blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]

    def save(self, *args, **kwargs):
        if not self.folio:
            ymd = timezone.localdate().strftime("%y%m%d")
            prefix = f"REC-{ymd}-"
            today_count = RecepcionCompra.objects.filter(folio__startswith=prefix).count() + 1
            self.folio = f"{prefix}{today_count:03d}"
        super().save(*args, **kwargs)

    def __str__(self):
        return self.folio
