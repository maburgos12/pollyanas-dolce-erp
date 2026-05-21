from decimal import Decimal

from django.conf import settings
from django.db import models
from django.utils import timezone

from maestros.models import Insumo


ALMACEN_CHOICES = [
    ("ALMACEN_1", "Almacén 1 (principal)"),
    ("ALMACEN_CASA_1", "Almacén Casa 1"),
    ("ALMACEN_CASA_2", "Almacén Casa 2"),
    ("CUARTO_FRIO", "Cuarto Frío"),
    ("VELAS", "Almacén de Velas"),
    ("LIMPIEZA", "Almacén de Limpieza"),
    ("OTRO", "Otro"),
]
ALMACEN_LABELS = dict(ALMACEN_CHOICES)


class ExistenciaInsumo(models.Model):
    insumo = models.OneToOneField(Insumo, on_delete=models.CASCADE)
    almacen = models.CharField(
        max_length=20, choices=ALMACEN_CHOICES, default="ALMACEN_1",
        verbose_name="Almacén / Ubicación", db_index=True,
    )
    stock_actual = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    punto_reorden = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_minimo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_maximo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    inventario_promedio = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    dias_llegada_pedido = models.PositiveIntegerField(default=0)
    consumo_diario_promedio = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    trazabilidad_stock = models.JSONField(default=dict, blank=True)
    actualizado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Existencia de insumo"
        verbose_name_plural = "Existencias de insumos"
        ordering = ["almacen", "insumo__nombre"]

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
    almacen = models.CharField(max_length=20, choices=ALMACEN_CHOICES, default="ALMACEN_1", blank=True)
    notas = models.CharField(max_length=255, blank=True, default="", verbose_name="Notas / destino")
    registrado_por = models.CharField(max_length=120, blank=True, default="", verbose_name="Registrado por")
    source_hash = models.CharField(max_length=64, unique=True, null=True, blank=True)

    class Meta:
        ordering = ["-fecha"]

    def __str__(self):
        return f"{self.tipo} {self.insumo.nombre} {self.cantidad}"


class AjusteInventario(models.Model):
    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_APLICADO = "APLICADO"
    STATUS_RECHAZADO = "RECHAZADO"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente aprobación"),
        (STATUS_APLICADO, "Aplicado"),
        (STATUS_RECHAZADO, "Rechazado"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    insumo = models.ForeignKey(Insumo, on_delete=models.PROTECT)
    cantidad_sistema = models.DecimalField(max_digits=18, decimal_places=3)
    cantidad_fisica = models.DecimalField(max_digits=18, decimal_places=3)
    motivo = models.CharField(max_length=255)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    solicitado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ajustes_inventario_solicitados",
    )
    aprobado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ajustes_inventario_aprobados",
    )
    aprobado_en = models.DateTimeField(null=True, blank=True)
    aplicado_en = models.DateTimeField(null=True, blank=True)
    comentario_revision = models.CharField(max_length=255, blank=True, default="")
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


class InventarioConfig(models.Model):
    reorder_max_diff_pct = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("10.00"))
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Configuración de inventario"
        verbose_name_plural = "Configuración de inventario"

    def save(self, *args, **kwargs):
        # Singleton: solo se usa una fila global para el módulo.
        self.pk = 1
        super().save(*args, **kwargs)

    @classmethod
    def get_solo(cls, default_pct: Decimal | None = None) -> "InventarioConfig":
        defaults = {}
        if default_pct is not None:
            defaults["reorder_max_diff_pct"] = default_pct
        obj, _ = cls.objects.get_or_create(pk=1, defaults=defaults)
        return obj

    def __str__(self):
        return f"Umbral manual PR: {self.reorder_max_diff_pct}%"


class ConsumoInsumoMensual(models.Model):
    ALERTA_OK = "OK"
    ALERTA_MERMA = "MERMA"
    ALERTA_FALTANTE = "FALTANTE"
    ALERTA_SIN_DATOS = "SIN_DATOS"
    ALERTA_CHOICES = [
        (ALERTA_OK, "Dentro de rango"),
        (ALERTA_MERMA, "Merma excesiva"),
        (ALERTA_FALTANTE, "Consumo mayor al teórico"),
        (ALERTA_SIN_DATOS, "Datos insuficientes"),
    ]

    periodo = models.DateField(db_index=True)
    insumo = models.ForeignKey(Insumo, on_delete=models.PROTECT, related_name="consumos_mensuales")
    unidad = models.CharField(max_length=50, blank=True, default="")

    consumo_teorico = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    costo_teorico = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    entradas_periodo = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    stock_inicial = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    stock_final = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    consumo_real = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    costo_real = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    diferencia_unidades = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    diferencia_pct = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    diferencia_costo = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    alerta = models.CharField(max_length=20, choices=ALERTA_CHOICES, default=ALERTA_SIN_DATOS, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Consumo mensual de insumo"
        verbose_name_plural = "Consumos mensuales de insumos"
        ordering = ["periodo", "insumo__nombre"]
        unique_together = [("periodo", "insumo")]
        indexes = [
            models.Index(fields=["periodo", "alerta"]),
            models.Index(fields=["insumo", "periodo"]),
        ]

    def __str__(self):
        return f"{self.periodo:%Y-%m} · {self.insumo.nombre}"


class ConteoFisicoMensual(models.Model):
    ESTATUS_BORRADOR = "BORRADOR"
    ESTATUS_REVISION = "REVISION"
    ESTATUS_CERRADO = "CERRADO"
    ESTATUS_CHOICES = [
        (ESTATUS_BORRADOR, "En captura"),
        (ESTATUS_REVISION, "En revisión"),
        (ESTATUS_CERRADO, "Cerrado"),
    ]

    periodo = models.DateField()
    fecha_conteo = models.DateField()
    responsable = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_BORRADOR)
    observaciones = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    cerrado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "Conteo físico mensual"
        verbose_name_plural = "Conteos físicos mensuales"
        ordering = ["-periodo"]
        unique_together = ["periodo"]

    def __str__(self):
        return f"Conteo físico {self.periodo:%Y-%m}"


class LineaConteoFisico(models.Model):
    conteo = models.ForeignKey(ConteoFisicoMensual, on_delete=models.CASCADE, related_name="lineas")
    insumo = models.ForeignKey("maestros.Insumo", null=True, blank=True, on_delete=models.PROTECT)
    producto = models.ForeignKey("recetas.Receta", null=True, blank=True, on_delete=models.PROTECT)
    nombre = models.CharField(max_length=200)
    unidad = models.CharField(max_length=50)
    stock_teorico = models.DecimalField(max_digits=12, decimal_places=3)
    stock_contado = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    diferencia = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    costo_unitario = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    costo_diferencia = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    ajuste_aplicado = models.BooleanField(default=False)
    movimiento_inventario = models.ForeignKey(
        "inventario.MovimientoInventario",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    movimiento_producto_cedis = models.ForeignKey(
        "recetas.MovimientoProductoCedis",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    observacion_linea = models.TextField(blank=True)

    class Meta:
        verbose_name = "Línea de conteo físico"
        verbose_name_plural = "Líneas de conteo físico"
        ordering = ["conteo", "nombre"]
        constraints = [
            models.CheckConstraint(
                check=(
                    models.Q(insumo__isnull=False, producto__isnull=True)
                    | models.Q(insumo__isnull=True, producto__isnull=False)
                ),
                name="conteo_linea_insumo_o_producto",
            ),
            models.UniqueConstraint(
                fields=["conteo", "insumo"],
                condition=models.Q(insumo__isnull=False),
                name="uniq_conteo_insumo",
            ),
            models.UniqueConstraint(
                fields=["conteo", "producto"],
                condition=models.Q(producto__isnull=False),
                name="uniq_conteo_producto",
            ),
        ]
        indexes = [
            models.Index(fields=["conteo", "insumo"]),
            models.Index(fields=["conteo", "producto"]),
        ]

    def __str__(self):
        return f"{self.conteo.periodo:%Y-%m} · {self.nombre}"
