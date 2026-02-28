from django.db import models, IntegrityError, transaction
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
        (TIPO_PREPARACION, "Insumo interno (batida/mezcla, no venta directa)"),
        (TIPO_PRODUCTO_FINAL, "Producto final de venta (armado)"),
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


class CostoDriver(models.Model):
    SCOPE_PRODUCTO = "PRODUCTO"
    SCOPE_FAMILIA = "FAMILIA"
    SCOPE_LOTE = "LOTE"
    SCOPE_GLOBAL = "GLOBAL"
    SCOPE_CHOICES = [
        (SCOPE_PRODUCTO, "Producto"),
        (SCOPE_FAMILIA, "Familia"),
        (SCOPE_LOTE, "Lote"),
        (SCOPE_GLOBAL, "Global"),
    ]

    nombre = models.CharField(max_length=120)
    scope = models.CharField(max_length=20, choices=SCOPE_CHOICES, default=SCOPE_PRODUCTO, db_index=True)
    receta = models.ForeignKey(
        Receta,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="drivers_costeo",
    )
    familia = models.CharField(max_length=120, blank=True, default="")
    familia_normalizada = models.CharField(max_length=140, blank=True, default="", db_index=True)
    lote_desde = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    lote_hasta = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    mo_pct = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    indirecto_pct = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    mo_fijo = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    indirecto_fijo = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    prioridad = models.PositiveIntegerField(default=100)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Driver de costeo"
        verbose_name_plural = "Drivers de costeo"
        ordering = ["scope", "prioridad", "id"]

    def save(self, *args, **kwargs):
        self.familia_normalizada = " ".join(unidecode((self.familia or "")).lower().strip().split())
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        target = self.receta.nombre if self.receta_id else (self.familia or "Global")
        return f"{self.scope} · {target} ({self.mo_pct}%/{self.indirecto_pct}%)"


class RecetaCostoVersion(models.Model):
    receta = models.ForeignKey(Receta, related_name="versiones_costo", on_delete=models.CASCADE)
    version_num = models.PositiveIntegerField()
    hash_snapshot = models.CharField(max_length=64, db_index=True)
    lote_referencia = models.DecimalField(max_digits=18, decimal_places=6, default=1)

    driver_scope = models.CharField(max_length=20, blank=True, default="")
    driver_nombre = models.CharField(max_length=120, blank=True, default="")
    mo_pct = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    indirecto_pct = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    mo_fijo = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    indirecto_fijo = models.DecimalField(max_digits=18, decimal_places=6, default=0)

    costo_mp = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_mo = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_indirecto = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_total = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    rendimiento_cantidad = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    rendimiento_unidad = models.CharField(max_length=20, blank=True, default="")
    costo_por_unidad_rendimiento = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    fuente = models.CharField(max_length=40, blank=True, default="AUTO")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Versión de costo de receta"
        verbose_name_plural = "Versiones de costo de recetas"
        ordering = ["receta", "-version_num"]
        unique_together = [("receta", "version_num"), ("receta", "hash_snapshot")]

    def __str__(self) -> str:
        return f"{self.receta.nombre} v{self.version_num} · ${self.costo_total}"


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


class VentaHistorica(models.Model):
    receta = models.ForeignKey(Receta, related_name="ventas_historicas", on_delete=models.CASCADE)
    sucursal = models.ForeignKey("core.Sucursal", null=True, blank=True, on_delete=models.SET_NULL)
    fecha = models.DateField(db_index=True)
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    tickets = models.PositiveIntegerField(default=0)
    monto_total = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    fuente = models.CharField(max_length=40, blank=True, default="IMPORT_VENTAS")
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Venta histórica"
        verbose_name_plural = "Ventas históricas"
        ordering = ["-fecha", "receta__nombre"]

    def __str__(self) -> str:
        suc = self.sucursal.codigo if self.sucursal_id else "GLOBAL"
        return f"{self.fecha} · {suc} · {self.receta.nombre} · {self.cantidad}"


class SolicitudVenta(models.Model):
    ALCANCE_MES = "MES"
    ALCANCE_SEMANA = "SEMANA"
    ALCANCE_FIN_SEMANA = "FIN_SEMANA"
    ALCANCE_CHOICES = [
        (ALCANCE_MES, "Mes"),
        (ALCANCE_SEMANA, "Semana"),
        (ALCANCE_FIN_SEMANA, "Fin de semana"),
    ]

    receta = models.ForeignKey(Receta, related_name="solicitudes_venta", on_delete=models.CASCADE)
    sucursal = models.ForeignKey("core.Sucursal", null=True, blank=True, on_delete=models.SET_NULL)
    alcance = models.CharField(max_length=20, choices=ALCANCE_CHOICES, default=ALCANCE_MES, db_index=True)
    periodo = models.CharField(max_length=7, blank=True, default="", db_index=True)  # YYYY-MM
    fecha_inicio = models.DateField(db_index=True)
    fecha_fin = models.DateField(db_index=True)
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    fuente = models.CharField(max_length=40, blank=True, default="UI_SOL_VENTAS")
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Solicitud de venta"
        verbose_name_plural = "Solicitudes de venta"
        ordering = ["-fecha_inicio", "receta__nombre"]
        unique_together = [("receta", "sucursal", "alcance", "fecha_inicio", "fecha_fin")]

    def __str__(self) -> str:
        suc = self.sucursal.codigo if self.sucursal_id else "GLOBAL"
        return f"{self.alcance} {self.fecha_inicio}..{self.fecha_fin} · {suc} · {self.receta.nombre} · {self.cantidad}"


class PronosticoVenta(models.Model):
    receta = models.ForeignKey(Receta, related_name="pronosticos", on_delete=models.CASCADE)
    periodo = models.CharField(max_length=7, db_index=True)  # YYYY-MM
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    fuente = models.CharField(max_length=40, blank=True, default="MANUAL")
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Pronóstico de venta"
        verbose_name_plural = "Pronósticos de venta"
        ordering = ["-periodo", "receta__nombre"]
        unique_together = [("receta", "periodo")]

    def __str__(self) -> str:
        return f"{self.periodo} · {self.receta.nombre} · {self.cantidad}"


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


class PoliticaStockSucursalProducto(models.Model):
    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.CASCADE, related_name="politicas_stock_producto")
    receta = models.ForeignKey(Receta, on_delete=models.CASCADE, related_name="politicas_stock_sucursal")
    stock_minimo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_objetivo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_maximo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    dias_cobertura = models.PositiveIntegerField(default=1)
    stock_seguridad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    lote_minimo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    multiplo_empaque = models.DecimalField(max_digits=18, decimal_places=3, default=1)
    activa = models.BooleanField(default=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Política stock sucursal-producto"
        verbose_name_plural = "Políticas stock sucursal-producto"
        ordering = ["sucursal__codigo", "receta__nombre"]
        unique_together = [("sucursal", "receta")]

    def __str__(self) -> str:
        return f"{self.sucursal.codigo} · {self.receta.nombre}"


class InventarioCedisProducto(models.Model):
    receta = models.OneToOneField(Receta, on_delete=models.CASCADE, related_name="inventario_cedis")
    stock_actual = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_reservado = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Inventario CEDIS de producto"
        verbose_name_plural = "Inventario CEDIS de productos"
        ordering = ["receta__nombre"]

    @property
    def disponible(self) -> Decimal:
        return max(Decimal("0"), Decimal(str(self.stock_actual or 0)) - Decimal(str(self.stock_reservado or 0)))

    def __str__(self) -> str:
        return f"CEDIS · {self.receta.nombre}"


class SolicitudReabastoCedis(models.Model):
    ESTADO_BORRADOR = "BORRADOR"
    ESTADO_ENVIADA = "ENVIADA"
    ESTADO_ATENDIDA = "ATENDIDA"
    ESTADO_CANCELADA = "CANCELADA"
    ESTADO_CHOICES = [
        (ESTADO_BORRADOR, "Borrador"),
        (ESTADO_ENVIADA, "Enviada"),
        (ESTADO_ATENDIDA, "Atendida"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    folio = models.CharField(max_length=24, unique=True, blank=True)
    fecha_operacion = models.DateField(default=timezone.localdate, db_index=True)
    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="solicitudes_reabasto")
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_BORRADOR, db_index=True)
    notas = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="solicitudes_reabasto_creadas",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Solicitud sucursal a CEDIS"
        verbose_name_plural = "Solicitudes sucursal a CEDIS"
        ordering = ["-fecha_operacion", "-id"]
        unique_together = [("fecha_operacion", "sucursal")]

    def _next_folio(self) -> str:
        ymd = timezone.localdate().strftime("%y%m%d")
        prefix = f"SRC-{ymd}-"
        today_count = SolicitudReabastoCedis.objects.filter(folio__startswith=prefix).count() + 1
        return f"{prefix}{today_count:03d}"

    def save(self, *args, **kwargs):
        if self.folio:
            return super().save(*args, **kwargs)
        last_exc = None
        for _ in range(10):
            self.folio = self._next_folio()
            try:
                with transaction.atomic():
                    return super().save(*args, **kwargs)
            except IntegrityError as exc:
                last_exc = exc
                self.folio = ""
                continue
        if last_exc:
            raise last_exc
        raise IntegrityError("No fue posible generar folio único de solicitud sucursal->CEDIS.")

    def __str__(self) -> str:
        return f"{self.folio} · {self.sucursal.codigo}"


class SolicitudReabastoCedisLinea(models.Model):
    solicitud = models.ForeignKey(SolicitudReabastoCedis, on_delete=models.CASCADE, related_name="lineas")
    receta = models.ForeignKey(Receta, on_delete=models.PROTECT, related_name="solicitudes_reabasto_lineas")
    stock_reportado = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    en_transito = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    consumo_proyectado = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    sugerido = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    solicitado = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    justificacion = models.CharField(max_length=255, blank=True, default="")
    observaciones = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Línea solicitud reabasto CEDIS"
        verbose_name_plural = "Líneas solicitud reabasto CEDIS"
        ordering = ["id"]
        unique_together = [("solicitud", "receta")]

    @property
    def delta_vs_sugerido(self) -> Decimal:
        return Decimal(str(self.solicitado or 0)) - Decimal(str(self.sugerido or 0))

    def __str__(self) -> str:
        return f"{self.solicitud.folio} · {self.receta.nombre}"
