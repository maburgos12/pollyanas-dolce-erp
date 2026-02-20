from django.db import models
from django.utils import timezone
from maestros.models import Insumo, UnidadMedida
from django.conf import settings
from unidecode import unidecode
from decimal import Decimal


def normalizar_codigo_point(texto: str) -> str:
    if not texto:
        return ""
    raw = unidecode(str(texto)).lower().strip()
    return "".join(ch for ch in raw if ch.isalnum())

class Receta(models.Model):
    TIPO_PREPARACION = "PREPARACION"
    TIPO_PRODUCTO_FINAL = "PRODUCTO_FINAL"
    TIPO_CHOICES = [
        (TIPO_PREPARACION, "Preparación base (pan, betún, crema, relleno)"),
        (TIPO_PRODUCTO_FINAL, "Producto final de venta"),
    ]

    nombre = models.CharField(max_length=250)
    codigo_point = models.CharField(max_length=80, blank=True, default="", db_index=True)
    nombre_normalizado = models.CharField(max_length=260, db_index=True)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_PREPARACION, db_index=True)
    usa_presentaciones = models.BooleanField(default=False)
    sheet_name = models.CharField(max_length=120, blank=True, default="")
    rendimiento_cantidad = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    rendimiento_unidad = models.ForeignKey(
        UnidadMedida,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="recetas_rendimiento",
    )
    hash_contenido = models.CharField(max_length=64, unique=True)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Receta"
        verbose_name_plural = "Recetas"
        ordering = ["nombre"]

    def save(self, *args, **kwargs):
        self.nombre_normalizado = " ".join(unidecode((self.nombre or "")).lower().strip().split())
        super().save(*args, **kwargs)

    @property
    def costo_total_estimado(self):
        return sum((l.costo_total_estimado or 0) for l in self.lineas.all())

    @property
    def costo_total_estimado_decimal(self) -> Decimal:
        total = Decimal("0")
        for linea in self.lineas.all():
            total += Decimal(str(linea.costo_total_estimado or 0))
        return total

    @property
    def rendimiento_kg(self) -> Decimal | None:
        if not self.rendimiento_cantidad or self.rendimiento_cantidad <= 0 or not self.rendimiento_unidad:
            return None

        code = (self.rendimiento_unidad.codigo or "").strip().lower()
        qty = Decimal(self.rendimiento_cantidad)
        if code == "kg":
            return qty
        if code in {"g", "gr"}:
            return qty / Decimal("1000")
        return None

    @property
    def costo_por_kg_estimado(self) -> Decimal | None:
        rendimiento_kg = self.rendimiento_kg
        if not rendimiento_kg or rendimiento_kg <= 0:
            return None
        return self.costo_total_estimado_decimal / rendimiento_kg

    @property
    def costo_por_unidad_rendimiento(self) -> Decimal | None:
        if not self.rendimiento_cantidad or self.rendimiento_cantidad <= 0:
            return None
        return self.costo_total_estimado_decimal / Decimal(str(self.rendimiento_cantidad))

    @property
    def pendientes_matching(self):
        return self.lineas.filter(match_status=LineaReceta.STATUS_NEEDS_REVIEW).count()

    def __str__(self) -> str:
        return self.nombre


class RecetaCodigoPointAlias(models.Model):
    receta = models.ForeignKey(Receta, related_name="codigos_point_aliases", on_delete=models.CASCADE)
    codigo_point = models.CharField(max_length=80)
    codigo_point_normalizado = models.CharField(max_length=90, unique=True, db_index=True)
    nombre_point = models.CharField(max_length=250, blank=True, default="")
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Alias código Point de receta"
        verbose_name_plural = "Aliases código Point de recetas"
        ordering = ["codigo_point"]

    def save(self, *args, **kwargs):
        self.codigo_point_normalizado = normalizar_codigo_point(self.codigo_point)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.codigo_point} -> {self.receta.nombre}"

class LineaReceta(models.Model):
    STATUS_AUTO = "AUTO_APPROVED"
    STATUS_NEEDS_REVIEW = "NEEDS_REVIEW"
    STATUS_REJECTED = "REJECTED"
    STATUS_CHOICES = [
        (STATUS_AUTO, "Auto"),
        (STATUS_NEEDS_REVIEW, "Needs review"),
        (STATUS_REJECTED, "Rejected"),
    ]

    MATCH_EXACT = "EXACT"
    MATCH_CONTAINS = "CONTAINS"
    MATCH_FUZZY = "FUZZY"
    MATCH_NONE = "NO_MATCH"
    MATCH_SUBSECTION = "SUBSECCION"

    TIPO_NORMAL = "NORMAL"
    TIPO_SUBSECCION = "SUBSECCION"
    TIPO_CHOICES = [
        (TIPO_NORMAL, "Componente principal"),
        (TIPO_SUBSECCION, "Subsección de componente"),
    ]

    receta = models.ForeignKey(Receta, related_name="lineas", on_delete=models.CASCADE)
    posicion = models.PositiveIntegerField(default=0)
    tipo_linea = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_NORMAL)
    etapa = models.CharField(max_length=120, blank=True, default="")

    insumo = models.ForeignKey(Insumo, null=True, blank=True, on_delete=models.SET_NULL)
    insumo_texto = models.CharField(max_length=250)
    cantidad = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    unidad_texto = models.CharField(max_length=40, blank=True, default="")
    unidad = models.ForeignKey(UnidadMedida, null=True, blank=True, on_delete=models.SET_NULL)

    costo_linea_excel = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    costo_unitario_snapshot = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    match_score = models.FloatField(default=0)
    match_method = models.CharField(max_length=20, default=MATCH_NONE)
    match_status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_REJECTED)
    aprobado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    aprobado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "Línea de receta"
        verbose_name_plural = "Líneas de receta"
        ordering = ["receta", "posicion"]

    @property
    def costo_total_estimado(self):
        # Modo operativo:
        # - Si la linea esta ligada a un insumo, el costo SIEMPRE sale de cantidad * costo_unitario_snapshot.
        # - Si falta cantidad o costo snapshot, no se calcula costo.
        # - El costo fijo legado (Excel) solo aplica para lineas sin insumo ligado.
        if self.insumo_id:
            if (
                self.cantidad is None
                or self.costo_unitario_snapshot is None
                or self.cantidad <= 0
                or self.costo_unitario_snapshot <= 0
            ):
                return None
            return float(self.cantidad) * float(self.costo_unitario_snapshot)
        if self.costo_linea_excel is not None:
            return float(self.costo_linea_excel)
        return None

    def __str__(self) -> str:
        return f"{self.receta.nombre}: {self.insumo_texto}"


class RecetaPresentacion(models.Model):
    receta = models.ForeignKey(Receta, related_name="presentaciones", on_delete=models.CASCADE)
    nombre = models.CharField(max_length=80)  # Mini, Chico, Mediano, etc.
    peso_por_unidad_kg = models.DecimalField(max_digits=18, decimal_places=6)
    unidades_por_batch = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    unidades_por_pastel = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Presentación de receta"
        verbose_name_plural = "Presentaciones de receta"
        ordering = ["receta", "nombre"]
        unique_together = [("receta", "nombre")]

    @property
    def costo_por_unidad_estimado(self) -> Decimal | None:
        costo_kg = self.receta.costo_por_kg_estimado
        if costo_kg is None:
            return None
        if not self.peso_por_unidad_kg or self.peso_por_unidad_kg <= 0:
            return None
        return costo_kg * Decimal(self.peso_por_unidad_kg)

    @property
    def costo_por_pastel_estimado(self) -> Decimal | None:
        costo_unidad = self.costo_por_unidad_estimado
        if costo_unidad is None:
            return None
        if not self.unidades_por_pastel or self.unidades_por_pastel <= 0:
            return None
        return costo_unidad * Decimal(self.unidades_por_pastel)

    def __str__(self) -> str:
        return f"{self.receta.nombre} - {self.nombre}"


class PlanProduccion(models.Model):
    nombre = models.CharField(max_length=140)
    fecha_produccion = models.DateField(default=timezone.localdate)
    notas = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="planes_produccion_creados",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Plan de producción"
        verbose_name_plural = "Planes de producción"
        ordering = ["-fecha_produccion", "-id"]

    @property
    def costo_total_estimado(self) -> Decimal:
        total = Decimal("0")
        for item in self.items.select_related("receta").all():
            total += item.costo_total_estimado
        return total

    def __str__(self) -> str:
        return f"{self.nombre} ({self.fecha_produccion})"


class PlanProduccionItem(models.Model):
    plan = models.ForeignKey(PlanProduccion, related_name="items", on_delete=models.CASCADE)
    receta = models.ForeignKey(Receta, related_name="plan_items", on_delete=models.PROTECT)
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=1)
    notas = models.CharField(max_length=160, blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Renglón de plan de producción"
        verbose_name_plural = "Renglones de plan de producción"
        ordering = ["id"]

    @property
    def costo_total_estimado(self) -> Decimal:
        receta_total = self.receta.costo_total_estimado_decimal
        return receta_total * Decimal(str(self.cantidad or 0))

    def __str__(self) -> str:
        return f"{self.plan.nombre}: {self.receta.nombre} x {self.cantidad}"
