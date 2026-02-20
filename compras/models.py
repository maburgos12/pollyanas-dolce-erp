from django.db import models
from django.utils import timezone
from django.conf import settings
from unidecode import unidecode

from maestros.models import Insumo, Proveedor


def _norm_text(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


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


class PresupuestoCompraPeriodo(models.Model):
    TIPO_MES = "mes"
    TIPO_Q1 = "q1"
    TIPO_Q2 = "q2"
    TIPO_CHOICES = [
        (TIPO_MES, "Mensual"),
        (TIPO_Q1, "1ra quincena"),
        (TIPO_Q2, "2da quincena"),
    ]

    periodo_tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    periodo_mes = models.CharField(max_length=7)  # YYYY-MM
    monto_objetivo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    notas = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="presupuestos_compras_actualizados",
    )

    class Meta:
        ordering = ["-periodo_mes", "periodo_tipo"]
        unique_together = [("periodo_tipo", "periodo_mes")]

    def __str__(self):
        return f"{self.get_periodo_tipo_display()} {self.periodo_mes}"


class PresupuestoCompraProveedor(models.Model):
    presupuesto_periodo = models.ForeignKey(
        PresupuestoCompraPeriodo,
        on_delete=models.CASCADE,
        related_name="objetivos_proveedor",
    )
    proveedor = models.ForeignKey(
        Proveedor,
        on_delete=models.PROTECT,
        related_name="presupuestos_compra",
    )
    monto_objetivo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    notas = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="presupuestos_compras_proveedor_actualizados",
    )

    class Meta:
        ordering = ["-presupuesto_periodo_id", "proveedor_id"]
        unique_together = [("presupuesto_periodo", "proveedor")]

    def __str__(self):
        return f"{self.presupuesto_periodo} · {self.proveedor.nombre}"


class PresupuestoCompraCategoria(models.Model):
    presupuesto_periodo = models.ForeignKey(
        PresupuestoCompraPeriodo,
        on_delete=models.CASCADE,
        related_name="objetivos_categoria",
    )
    categoria = models.CharField(max_length=120)
    categoria_normalizada = models.CharField(max_length=140, db_index=True)
    monto_objetivo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    notas = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="presupuestos_compras_categoria_actualizados",
    )

    class Meta:
        ordering = ["-presupuesto_periodo_id", "categoria"]
        unique_together = [("presupuesto_periodo", "categoria_normalizada")]

    def save(self, *args, **kwargs):
        self.categoria = " ".join((self.categoria or "").strip().split())
        self.categoria_normalizada = _norm_text(self.categoria)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.presupuesto_periodo} · {self.categoria}"
