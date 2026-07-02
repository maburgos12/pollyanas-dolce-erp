from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class BitacoraOperativa(models.Model):
    TIPO_SALIDAS_CFP1 = "SALIDAS_CFP1"
    TIPO_INVENTARIO_CFP1 = "INVENTARIO_CFP1"
    TIPO_PLAGAS = "PLAGAS"
    TIPO_CFP11 = "CFP11"
    TIPO_ROTACION = "ROTACION"
    TIPO_REBANADO = "REBANADO"
    TIPO_CHOICES = [
        (TIPO_SALIDAS_CFP1, "Salidas CFP1 a sucursales"),
        (TIPO_INVENTARIO_CFP1, "Inventario Diario CFP1"),
        (TIPO_PLAGAS, "Registro de control de plagas"),
        (TIPO_CFP11, "Control de Inventario Diario CFP 1.1"),
        (TIPO_ROTACION, "Rotación de producto bitácora"),
        (TIPO_REBANADO, "Producto Rebanado"),
    ]
    ESTATUS_BORRADOR = "BORRADOR"
    ESTATUS_CERRADA = "CERRADA"
    ESTATUS_CHOICES = [(ESTATUS_BORRADOR, "Borrador"), (ESTATUS_CERRADA, "Cerrada")]

    tipo = models.CharField(max_length=32, choices=TIPO_CHOICES)
    fecha = models.DateField(default=timezone.localdate, db_index=True)
    sucursal = models.ForeignKey("core.Sucursal", null=True, blank=True, on_delete=models.SET_NULL)
    estatus = models.CharField(max_length=16, choices=ESTATUS_CHOICES, default=ESTATUS_BORRADOR)
    notas = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)
    cerrado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-fecha", "-id"]

    def cerrar(self):
        self.estatus = self.ESTATUS_CERRADA
        self.cerrado_en = timezone.now()

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} · {self.fecha:%Y-%m-%d}"


class BitacoraOperativaLinea(models.Model):
    bitacora = models.ForeignKey(BitacoraOperativa, on_delete=models.CASCADE, related_name="lineas")
    receta = models.ForeignKey("recetas.Receta", null=True, blank=True, on_delete=models.PROTECT)
    sucursal = models.ForeignKey("core.Sucursal", null=True, blank=True, on_delete=models.SET_NULL)
    datos = models.JSONField(default=dict, blank=True)
    observaciones = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["id"]

    def __str__(self) -> str:
        return str(self.receta or self.bitacora)
