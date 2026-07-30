from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class BitacoraOperativa(models.Model):
    TIPO_SALIDAS_CFP1 = "SALIDAS_CFP1"
    TIPO_INVENTARIO_CFP1 = "INVENTARIO_CFP1"
    TIPO_PLAGAS = "PLAGAS"
    TIPO_HORNOS = "HORNOS"
    TIPO_CFP11 = "CFP11"
    TIPO_ARMADO = "ARMADO"
    TIPO_ROTACION = "ROTACION"
    TIPO_REBANADO = "REBANADO"
    TIPO_CHOICES = [
        (TIPO_SALIDAS_CFP1, "Salidas CFP1 a sucursales"),
        (TIPO_INVENTARIO_CFP1, "Inventario Diario CFP1"),
        (TIPO_PLAGAS, "Registro de control de plagas"),
        (TIPO_HORNOS, "Control producción - Hornos"),
        (TIPO_CFP11, "Control de Inventario Diario CFP 1.1"),
        (TIPO_ARMADO, "Control producción - Armado"),
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
    conteo_guardado_en = models.DateTimeField(null=True, blank=True)
    conteo_guardado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="cortes_ciegos_guardados",
    )

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
    insumo = models.ForeignKey(
        "maestros.Insumo",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="lineas_bitacora_operativa",
    )
    sucursal = models.ForeignKey("core.Sucursal", null=True, blank=True, on_delete=models.SET_NULL)
    datos = models.JSONField(default=dict, blank=True)
    observaciones = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["id"]

    def __str__(self) -> str:
        return str(self.insumo or self.receta or self.bitacora)

    def save(self, *args, **kwargs):
        """Bloquear edición de líneas después de que el corte ciego haya sido sellado."""
        if self.pk and self.bitacora.conteo_guardado_en is not None:
            from django.core.exceptions import ValidationError
            raise ValidationError("No se puede editar el corte después de que ha sido sellado.")
        super().save(*args, **kwargs)


class RegistroHigiene(models.Model):
    TIPO_CLORO_PH = "CLORO_PH"
    TIPO_LIMPIEZA = "LIMPIEZA"
    TIPO_BANOS = "BANOS"
    TIPO_CHOICES = [
        (TIPO_CLORO_PH, "Niveles de cloro y pH"),
        (TIPO_LIMPIEZA, "Programa de limpieza"),
        (TIPO_BANOS, "Limpieza de baños"),
    ]
    ESTATUS_CERRADO = "CERRADO"

    tipo = models.CharField(max_length=16, choices=TIPO_CHOICES, db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        related_name="registros_higiene",
    )
    fecha = models.DateField(default=timezone.localdate, db_index=True)
    clave_instancia = models.CharField(
        max_length=80,
        help_text="Identifica la toma, ronda o programa dentro del día.",
    )
    hora = models.TimeField(null=True, blank=True)
    plantilla_version = models.CharField(max_length=20)
    plantilla_snapshot = models.JSONField(default=dict, blank=True)
    tipo_bano = models.CharField(max_length=16, blank=True, default="")
    uso_bano = models.CharField(max_length=16, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    estatus = models.CharField(max_length=16, default=ESTATUS_CERRADO)
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="registros_higiene_creados",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "-hora", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["sucursal", "fecha", "tipo", "clave_instancia"],
                name="operacion_higiene_instancia_diaria_unica",
            )
        ]
        indexes = [
            models.Index(fields=["sucursal", "fecha", "tipo"]),
        ]
        verbose_name = "Registro diario de higiene"
        verbose_name_plural = "Registros diarios de higiene"

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} · {self.sucursal} · {self.fecha:%Y-%m-%d}"


class RespuestaHigiene(models.Model):
    RESPUESTA_CUMPLE = "CUMPLE"
    RESPUESTA_NO_CUMPLE = "NO_CUMPLE"
    RESPUESTA_NA = "NA"
    RESPUESTA_CHOICES = [
        (RESPUESTA_CUMPLE, "Cumple"),
        (RESPUESTA_NO_CUMPLE, "No cumple"),
        (RESPUESTA_NA, "No aplica"),
    ]

    registro = models.ForeignKey(
        RegistroHigiene,
        on_delete=models.CASCADE,
        related_name="respuestas",
    )
    punto_clave = models.CharField(max_length=100)
    seccion = models.CharField(max_length=80)
    punto_revision = models.CharField(max_length=240)
    orden = models.PositiveSmallIntegerField(default=0)
    respuesta = models.CharField(max_length=16, choices=RESPUESTA_CHOICES, blank=True, default="")
    valor_numerico = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True)
    observacion = models.TextField(blank=True, default="")
    evidencia = models.ImageField(
        upload_to="operacion/higiene/evidencias/%Y/%m/",
        null=True,
        blank=True,
    )
    corregido_en_momento = models.BooleanField(default=False)
    requiere_seguimiento = models.BooleanField(default=False)
    tipo_objetivo = models.CharField(max_length=16, blank=True, default="")
    activo_relacionado = models.ForeignKey(
        "activos.Activo",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="revisiones_higiene",
    )
    area_instalacion = models.CharField(max_length=120, blank=True, default="")
    reporte_falla = models.OneToOneField(
        "fallas.ReporteFalla",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="origen_higiene",
    )

    class Meta:
        ordering = ["orden", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["registro", "punto_clave"],
                name="operacion_higiene_respuesta_punto_unica",
            )
        ]
        verbose_name = "Respuesta de higiene"
        verbose_name_plural = "Respuestas de higiene"

    def __str__(self) -> str:
        return f"{self.registro} · {self.punto_revision}"
