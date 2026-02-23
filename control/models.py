from decimal import Decimal

from django.db import models
from django.utils import timezone


class VentaPOS(models.Model):
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_pos",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_pos",
    )
    fecha = models.DateField(db_index=True)
    codigo_point = models.CharField(max_length=80, blank=True, default="", db_index=True)
    producto_texto = models.CharField(max_length=250, blank=True, default="")
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0"))
    tickets = models.PositiveIntegerField(default=0)
    monto_total = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    fuente = models.CharField(max_length=40, blank=True, default="IMPORT_POS")
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Venta POS"
        verbose_name_plural = "Ventas POS"
        ordering = ["-fecha", "-id"]
        indexes = [
            models.Index(fields=["fecha", "sucursal"]),
            models.Index(fields=["codigo_point", "fecha"]),
        ]

    def __str__(self) -> str:
        sucursal = self.sucursal.codigo if self.sucursal_id else "GLOBAL"
        receta = self.receta.nombre if self.receta_id else (self.producto_texto or self.codigo_point or "SIN_MAPEO")
        return f"{self.fecha} · {sucursal} · {receta} · {self.cantidad}"


class MermaPOS(models.Model):
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="mermas_pos",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="mermas_pos",
    )
    fecha = models.DateField(db_index=True)
    codigo_point = models.CharField(max_length=80, blank=True, default="", db_index=True)
    producto_texto = models.CharField(max_length=250, blank=True, default="")
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0"))
    motivo = models.CharField(max_length=160, blank=True, default="")
    fuente = models.CharField(max_length=40, blank=True, default="IMPORT_POS_MERMA")
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Merma POS"
        verbose_name_plural = "Mermas POS"
        ordering = ["-fecha", "-id"]
        indexes = [
            models.Index(fields=["fecha", "sucursal"]),
            models.Index(fields=["codigo_point", "fecha"]),
        ]

    def __str__(self) -> str:
        sucursal = self.sucursal.codigo if self.sucursal_id else "GLOBAL"
        receta = self.receta.nombre if self.receta_id else (self.producto_texto or self.codigo_point or "SIN_MAPEO")
        return f"{self.fecha} · {sucursal} · {receta} · {self.cantidad}"

# Create your models here.
