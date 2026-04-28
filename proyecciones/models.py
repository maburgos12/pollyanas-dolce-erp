from decimal import Decimal

from django.db import models
from django.utils import timezone


class ProyeccionProduccion(models.Model):
    CONFIANZA_ALTA = "ALTA"
    CONFIANZA_MEDIA = "MEDIA"
    CONFIANZA_BAJA = "BAJA"
    CONFIANZA_CHOICES = [
        (CONFIANZA_ALTA, "Alta"),
        (CONFIANZA_MEDIA, "Media"),
        (CONFIANZA_BAJA, "Baja"),
    ]

    periodo = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.PROTECT,
        related_name="proyecciones_produccion",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="proyecciones_produccion",
    )
    venta_proyectada = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    unidades_proyectadas = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    unidades_proyectadas_ajustadas = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    factor_merma = models.DecimalField(max_digits=7, decimal_places=4, default=Decimal("0"))
    factor_devolucion = models.DecimalField(max_digits=7, decimal_places=4, default=Decimal("0"))
    stock_actual = models.DecimalField(max_digits=10, decimal_places=3, default=Decimal("0"))
    metodo = models.CharField(max_length=50, default="PROMEDIO_MOVIL_7D")
    confianza = models.CharField(max_length=10, choices=CONFIANZA_CHOICES, default=CONFIANZA_BAJA)
    dias_historial = models.PositiveSmallIntegerField(default=0)
    metadata = models.JSONField(default=dict, blank=True)
    generado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Proyección de producción"
        verbose_name_plural = "Proyecciones de producción"
        ordering = ["periodo", "sucursal__codigo", "receta__nombre"]
        unique_together = [("periodo", "receta", "sucursal")]
        indexes = [
            models.Index(fields=["periodo", "sucursal"], name="proy_prod_period_suc_idx"),
            models.Index(fields=["periodo", "receta"], name="proy_prod_period_rec_idx"),
        ]

    def __str__(self) -> str:
        sucursal = self.sucursal.codigo if self.sucursal_id else "TODAS"
        return f"{self.periodo:%Y-%m-%d} · {sucursal} · {self.receta.nombre} · {self.unidades_proyectadas_ajustadas}"
