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
    responsable_texto = models.CharField(max_length=160, blank=True, default="")
    fuente = models.CharField(max_length=40, blank=True, default="IMPORT_POS_MERMA")
    source_hash = models.CharField(max_length=64, unique=True, null=True, blank=True, db_index=True)
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


class MermaMensualSucursal(models.Model):
    periodo = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="mermas_mensuales",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="mermas_mensuales",
    )
    nombre_producto = models.CharField(max_length=200)
    unidades_merma = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    costo_merma = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    unidades_vendidas = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    pct_merma_sobre_venta = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0"))
    justificacion_principal = models.CharField(max_length=200, blank=True, default="")
    fuente = models.CharField(max_length=50, default="POINT_BRIDGE_WASTE")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Merma mensual por sucursal"
        verbose_name_plural = "Mermas mensuales por sucursal"
        ordering = ["-periodo", "sucursal__codigo", "-costo_merma", "nombre_producto"]
        unique_together = [("periodo", "sucursal", "receta", "nombre_producto")]
        indexes = [
            models.Index(fields=["periodo", "sucursal"], name="merma_mens_suc_idx"),
            models.Index(fields=["periodo", "receta"], name="merma_mens_receta_idx"),
        ]

    def __str__(self) -> str:
        sucursal = self.sucursal.codigo if self.sucursal_id else self.metadata.get("branch_point", "SIN_SUCURSAL")
        return f"{self.periodo:%Y-%m} · {sucursal} · {self.nombre_producto} · ${self.costo_merma}"


class DevolucionSucursalMatriz(models.Model):
    MOTIVO_VIDA_UTIL = "VIDA_UTIL"
    MOTIVO_EXCEDENTE = "EXCEDENTE"
    MOTIVO_CALIDAD = "CALIDAD"
    MOTIVO_OTRO = "OTRO"
    MOTIVO_CHOICES = [
        (MOTIVO_VIDA_UTIL, "Vencimiento/Vida útil"),
        (MOTIVO_EXCEDENTE, "Excedente de producción"),
        (MOTIVO_CALIDAD, "Problema de calidad"),
        (MOTIVO_OTRO, "Otro"),
    ]

    transfer_line = models.OneToOneField(
        "pos_bridge.PointTransferLine",
        on_delete=models.CASCADE,
        related_name="devolucion_vida_util",
    )
    periodo = models.DateField(db_index=True)
    sucursal_origen = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="devoluciones_a_matriz",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="devoluciones_a_matriz",
    )
    unidades = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    costo_estimado = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    motivo = models.CharField(max_length=20, choices=MOTIVO_CHOICES, default=MOTIVO_VIDA_UTIL)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Devolución sucursal a matriz"
        verbose_name_plural = "Devoluciones sucursal a matriz"
        ordering = ["-periodo", "-transfer_line__registered_at", "receta__nombre"]
        indexes = [
            models.Index(fields=["periodo", "sucursal_origen"], name="devol_matriz_period_suc_idx"),
            models.Index(fields=["periodo", "receta"], name="devol_matriz_period_rec_idx"),
        ]

    def __str__(self) -> str:
        origen = self.sucursal_origen.codigo if self.sucursal_origen_id else self.metadata.get("origin_branch_name", "SIN_ORIGEN")
        receta = self.receta.nombre if self.receta_id else self.metadata.get("item_name", "SIN_RECETA")
        destino = self.metadata.get("destination_branch_name", "DEVOLUCIONES")
        return f"{self.periodo:%Y-%m} · {origen} -> {destino} · {receta} · {self.unidades}"
