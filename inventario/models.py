import re
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils import timezone
from unidecode import unidecode

from maestros.models import Insumo


UBICACION_CFP_1_1 = "CFP_1_1"
UBICACION_ARMADO = "ARMADO"
UBICACION_CFP_1 = "CFP_1"
UBICACION_ALMACEN = "ALMACEN_1"
UBICACION_CEDIS = "CUARTO_FRIO"


ALMACEN_CHOICES = [
    (UBICACION_ALMACEN, "Almacén 1 (principal)"),
    (UBICACION_CFP_1_1, "CFP 1.1"),
    (UBICACION_ARMADO, "Armado"),
    (UBICACION_CFP_1, "CFP 1"),
    ("ALMACEN_CASA_1", "Almacén Casa 1"),
    ("ALMACEN_CASA_2", "Almacén Casa 2"),
    (UBICACION_CEDIS, "Cuarto Frío"),
    ("VELAS", "Almacén de Velas"),
    ("LIMPIEZA", "Almacén de Limpieza"),
    ("OTRO", "Otro"),
]
ALMACEN_LABELS = dict(ALMACEN_CHOICES)


class ExistenciaInsumo(models.Model):
    insumo = models.ForeignKey(Insumo, on_delete=models.CASCADE, related_name="existencias")
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
        constraints = [
            models.UniqueConstraint(
                fields=["insumo", "almacen"],
                name="uniq_existencia_insumo_almacen",
            ),
        ]

    def __str__(self):
        return self.insumo.nombre


def normalizar_codigo_lote(codigo_point):
    codigo = unidecode(str(codigo_point or "")).upper().strip()
    return re.sub(r"[^A-Z0-9]+", "-", codigo).strip("-")


def construir_codigo_lote(prefijo, identidad, fecha, origen_id):
    sufijo = f"-{fecha}-{origen_id}"
    espacio_identidad = 120 - len(prefijo) - len(sufijo) - 1
    if len(identidad) > espacio_identidad:
        digest = sha256(identidad.encode("ascii")).hexdigest()[:12].upper()
        identidad = f"{identidad[:espacio_identidad - 13]}-{digest}"
    return f"{prefijo}-{identidad}{sufijo}"


class LoteProduccion(models.Model):
    DISPONIBLE = "DISPONIBLE"
    AGOTADO = "AGOTADO"
    RETENIDO = "RETENIDO"
    CANCELADO = "CANCELADO"
    ESTADO_CHOICES = [
        (DISPONIBLE, "Disponible"),
        (AGOTADO, "Agotado"),
        (RETENIDO, "Retenido"),
        (CANCELADO, "Cancelado"),
    ]

    codigo = models.CharField(max_length=120, unique=True, editable=False)
    insumo = models.ForeignKey(
        Insumo,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="lotes_produccion",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.PROTECT,
        related_name="lotes_produccion",
    )
    cantidad_inicial = models.DecimalField(max_digits=18, decimal_places=3)
    unidad = models.ForeignKey("maestros.UnidadMedida", on_delete=models.PROTECT)
    producido_en = models.DateTimeField()
    linea_origen = models.OneToOneField(
        "operacion.BitacoraOperativaLinea",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="lote_generado",
    )
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES, default=DISPONIBLE)
    es_apertura = models.BooleanField(default=False)
    observaciones = models.CharField(max_length=255, blank=True, default="")

    def clean(self):
        errors = {}
        if self.pk:
            persisted = type(self).objects.filter(pk=self.pk).values(
                "codigo",
                "insumo_id",
                "receta_id",
                "cantidad_inicial",
                "unidad_id",
                "producido_en",
                "linea_origen_id",
                "creado_por_id",
                "es_apertura",
            ).first()
            if persisted:
                immutable_fields = {
                    "codigo": "codigo",
                    "insumo": "insumo_id",
                    "receta": "receta_id",
                    "cantidad_inicial": "cantidad_inicial",
                    "unidad": "unidad_id",
                    "producido_en": "producido_en",
                    "linea_origen": "linea_origen_id",
                    "creado_por": "creado_por_id",
                    "es_apertura": "es_apertura",
                }
                for field, attribute in immutable_fields.items():
                    if getattr(self, attribute) != persisted[attribute]:
                        errors[field] = "Este dato historico del lote no puede modificarse."
            if errors:
                raise ValidationError(errors)
            return

        try:
            cantidad = Decimal(str(self.cantidad_inicial))
        except (ArithmeticError, TypeError, ValueError):
            cantidad = None
        if cantidad is None or not cantidad.is_finite() or cantidad <= 0:
            errors["cantidad_inicial"] = "La cantidad inicial debe ser mayor que cero."
        if self.producido_en and timezone.is_naive(self.producido_en):
            errors["producido_en"] = "La fecha de produccion debe incluir zona horaria."

        if self.es_apertura:
            if self.linea_origen_id:
                errors["linea_origen"] = "Un lote de apertura no debe inventar una linea historica."
            if not self.observaciones.strip():
                errors["observaciones"] = "Un lote de apertura requiere una observacion."
        elif not self.linea_origen_id:
            errors["linea_origen"] = "Un lote ordinario requiere una linea de bitacora de origen."
        elif self.receta_id and self.linea_origen.receta_id != self.receta_id:
            errors["linea_origen"] = "La linea de origen debe corresponder a la receta del lote."

        if self.receta_id:
            from recetas.models import Receta

            if self.receta.tipo == Receta.TIPO_PREPARACION and not self.insumo_id:
                errors["insumo"] = "Una preparacion interna requiere un insumo canonico."
            if not self.insumo_id and self.receta.tipo != Receta.TIPO_PRODUCTO_FINAL:
                errors["receta"] = "Un lote sin insumo requiere una receta de producto final."
            if (
                self.receta.tipo == Receta.TIPO_PREPARACION
                and self.insumo_id
                and self.insumo.unidad_base_id
                and self.unidad_id != self.insumo.unidad_base_id
            ):
                errors["unidad"] = "La unidad debe coincidir con la unidad base del insumo."
            if (
                self.receta.tipo == Receta.TIPO_PRODUCTO_FINAL
                and self.receta.rendimiento_unidad_id
                and self.unidad_id != self.receta.rendimiento_unidad_id
            ):
                errors["unidad"] = "La unidad debe coincidir con la unidad de rendimiento de la receta."

        if self.insumo_id:
            codigo_point = self.insumo.codigo_point
        elif self.receta_id:
            codigo_point = self.receta.codigo_point
        else:
            codigo_point = ""
        if not normalizar_codigo_lote(codigo_point):
            errors["codigo"] = "El lote requiere una identidad canonica de Point."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        codigo_existente = ""
        if self.pk:
            codigo_existente = type(self).objects.filter(pk=self.pk).values_list("codigo", flat=True).first() or ""
        self.full_clean(exclude=["codigo"] if not self.codigo else None)
        point_code = self.insumo.codigo_point if self.insumo_id else self.receta.codigo_point
        identity = normalizar_codigo_lote(point_code)
        date_code = timezone.localtime(self.producido_en).strftime("%Y%m%d")

        if codigo_existente:
            super().save(*args, **kwargs)
            return

        if self.es_apertura and not self.pk:
            with transaction.atomic():
                self.codigo = f"INI-PENDIENTE-{uuid4().hex.upper()}"
                super().save(*args, **kwargs)
                self.codigo = construir_codigo_lote("INI", identity, date_code, self.pk)
                type(self).objects.filter(pk=self.pk).update(codigo=self.codigo)
            return

        source_id = self.pk if self.es_apertura else self.linea_origen_id
        self.codigo = construir_codigo_lote(
            "INI" if self.es_apertura else "LOT",
            identity,
            date_code,
            source_id,
        )
        super().save(*args, **kwargs)

    def __str__(self):
        return self.codigo


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
    lote = models.ForeignKey(
        LoteProduccion,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="movimientos",
    )
    linea_bitacora = models.ForeignKey(
        "operacion.BitacoraOperativaLinea",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="movimientos_inventario",
    )
    registrado_por_usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    trazabilidad = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-fecha"]

    def clean(self):
        if not self.lote_id:
            return

        errors = {}
        if not self.lote.insumo_id:
            errors["lote"] = "Un lote de producto final no pertenece al ledger de insumos."
        elif self.insumo_id != self.lote.insumo_id:
            errors["insumo"] = "El movimiento debe usar el mismo insumo que el lote."

        if self.linea_bitacora_id != self.lote.linea_origen_id:
            errors["linea_bitacora"] = "La linea del movimiento debe coincidir con el origen del lote."
        elif self.linea_bitacora_id and self.linea_bitacora.receta_id != self.lote.receta_id:
            errors["linea_bitacora"] = "La linea del movimiento debe corresponder a la receta del lote."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

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
