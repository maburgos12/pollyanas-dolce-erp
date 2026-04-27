from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models
from django.utils import timezone


class CentroCosto(models.Model):
    TIPO_PRODUCCION = "PRODUCCION"
    TIPO_SUCURSAL = "SUCURSAL_VENTA"
    TIPO_CORPORATIVO = "CORPORATIVO"
    TIPO_LOGISTICA = "LOGISTICA"
    TIPO_CEDIS = "CEDIS"
    TIPO_CHOICES = [
        (TIPO_PRODUCCION, "Producción"),
        (TIPO_SUCURSAL, "Sucursal venta"),
        (TIPO_CORPORATIVO, "Corporativo"),
        (TIPO_LOGISTICA, "Logística"),
        (TIPO_CEDIS, "CEDIS"),
    ]

    codigo = models.CharField(max_length=40, unique=True, db_index=True)
    nombre = models.CharField(max_length=160)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="centros_costo",
    )
    activo = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["tipo", "codigo"]
        verbose_name = "Centro de costo"
        verbose_name_plural = "Centros de costo"

    def __str__(self) -> str:
        return f"{self.codigo} - {self.nombre}"


class CategoriaGasto(models.Model):
    CAPA_FABRICACION = "FABRICACION"
    CAPA_SUCURSAL = "SUCURSAL"
    CAPA_EMPRESA = "EMPRESA"
    CAPA_CHOICES = [
        (CAPA_FABRICACION, "Fabricación"),
        (CAPA_SUCURSAL, "Sucursal"),
        (CAPA_EMPRESA, "Empresa"),
    ]

    BUCKET_MANO_OBRA = "MANO_OBRA_PROD"
    BUCKET_INDIRECTO = "INDIRECTO_PROD"
    BUCKET_EMPAQUE = "EMPAQUE_PROD"
    BUCKET_COMERCIAL = "COMERCIAL_SUCURSAL"
    BUCKET_CORPORATIVO = "CORPORATIVO"
    BUCKET_LOGISTICA = "LOGISTICA"
    BUCKET_OTRO = "OTRO"
    BUCKET_CHOICES = [
        (BUCKET_MANO_OBRA, "Mano de obra producción"),
        (BUCKET_INDIRECTO, "Indirecto producción"),
        (BUCKET_EMPAQUE, "Empaque producción"),
        (BUCKET_COMERCIAL, "Comercial sucursal"),
        (BUCKET_CORPORATIVO, "Corporativo"),
        (BUCKET_LOGISTICA, "Logística"),
        (BUCKET_OTRO, "Otro"),
    ]

    codigo = models.CharField(max_length=50, unique=True, db_index=True)
    nombre = models.CharField(max_length=160)
    capa_objetivo = models.CharField(max_length=20, choices=CAPA_CHOICES, db_index=True)
    bucket = models.CharField(max_length=30, choices=BUCKET_CHOICES, default=BUCKET_OTRO, db_index=True)
    impacta_costo_producto = models.BooleanField(default=False)
    impacta_contribucion_sucursal = models.BooleanField(default=False)
    impacta_utilidad_empresa = models.BooleanField(default=True)
    activo = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["codigo"]
        verbose_name = "Categoría de gasto"
        verbose_name_plural = "Categorías de gasto"

    def __str__(self) -> str:
        return f"{self.codigo} - {self.nombre}"


class ReglaAsignacionGasto(models.Model):
    BASE_NONE = "NONE"
    BASE_VENTAS = "VENTAS"
    BASE_UNIDADES = "UNIDADES"
    BASE_COSTO_MP = "COSTO_MP"
    BASE_CHOICES = [
        (BASE_NONE, "Sin reparto"),
        (BASE_VENTAS, "Por ventas"),
        (BASE_UNIDADES, "Por unidades"),
        (BASE_COSTO_MP, "Por costo MP"),
    ]

    nombre = models.CharField(max_length=160)
    categoria_gasto = models.ForeignKey(
        CategoriaGasto,
        on_delete=models.CASCADE,
        related_name="reglas_asignacion",
    )
    centro_costo = models.ForeignKey(
        CentroCosto,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="reglas_asignacion",
    )
    base_reparto = models.CharField(max_length=20, choices=BASE_CHOICES, default=BASE_NONE)
    activo = models.BooleanField(default=True)
    prioridad = models.PositiveIntegerField(default=100)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["prioridad", "id"]
        verbose_name = "Regla de asignación de gasto"
        verbose_name_plural = "Reglas de asignación de gasto"

    def __str__(self) -> str:
        return self.nombre


class GastoOperativoMensual(models.Model):
    TIPO_DATO_REAL = "REAL"
    TIPO_DATO_PRESUPUESTO = "PRESUPUESTO"
    TIPO_DATO_CHOICES = [
        (TIPO_DATO_REAL, "Real"),
        (TIPO_DATO_PRESUPUESTO, "Presupuesto"),
    ]

    FUENTE_MANUAL = "MANUAL"
    FUENTE_IMPORTADA = "IMPORTADA"
    FUENTE_CHOICES = [
        (FUENTE_MANUAL, "Manual"),
        (FUENTE_IMPORTADA, "Importada"),
    ]

    periodo = models.DateField(db_index=True)
    centro_costo = models.ForeignKey(
        CentroCosto,
        on_delete=models.PROTECT,
        related_name="gastos_mensuales",
    )
    categoria_gasto = models.ForeignKey(
        CategoriaGasto,
        on_delete=models.PROTECT,
        related_name="gastos_mensuales",
    )
    external_key = models.CharField(max_length=120, null=True, blank=True, unique=True, db_index=True)
    monto = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    tipo_dato = models.CharField(
        max_length=20,
        choices=TIPO_DATO_CHOICES,
        default=TIPO_DATO_REAL,
        db_index=True,
    )
    fuente = models.CharField(max_length=20, choices=FUENTE_CHOICES, default=FUENTE_MANUAL)
    comentario = models.TextField(blank=True, default="")
    archivo_soporte = models.CharField(max_length=300, blank=True, default="")
    es_estimado = models.BooleanField(default=False)
    capturado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="gastos_operativos_capturados",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "centro_costo__codigo", "categoria_gasto__codigo", "id"]
        verbose_name = "Gasto operativo mensual"
        verbose_name_plural = "Gastos operativos mensuales"
        indexes = [
            models.Index(fields=["periodo", "centro_costo"], name="rgasto_periodo_centro_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.centro_costo.codigo} · {self.categoria_gasto.codigo}"


class CargaGastoOperativoArchivo(models.Model):
    STATUS_PENDING = "PENDING"
    STATUS_PROCESSING = "PROCESSING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_ERROR = "ERROR"
    STATUS_DUPLICATE = "DUPLICATE"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_PROCESSING, "Procesando"),
        (STATUS_SUCCESS, "Exitosa"),
        (STATUS_ERROR, "Error"),
        (STATUS_DUPLICATE, "Duplicada"),
    ]

    SOURCE_WEB = "WEB"
    SOURCE_DROPBOX = "DROPBOX"
    SOURCE_COMMAND = "COMMAND"
    SOURCE_CHOICES = [
        (SOURCE_WEB, "Web"),
        (SOURCE_DROPBOX, "Carpeta monitoreada"),
        (SOURCE_COMMAND, "Comando"),
    ]

    original_filename = models.CharField(max_length=255, db_index=True)
    stored_file_path = models.CharField(max_length=500, unique=True)
    file_hash = models.CharField(max_length=64, db_index=True)
    file_size_bytes = models.PositiveBigIntegerField(default=0)
    source_channel = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_WEB, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    target_year = models.PositiveSmallIntegerField(default=2026)
    uploaded_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="cargas_gasto_operativo",
    )
    uploaded_at = models.DateTimeField(default=timezone.now, db_index=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    processed_rows = models.PositiveIntegerField(default=0)
    loaded_rows = models.PositiveIntegerField(default=0)
    created_rows = models.PositiveIntegerField(default=0)
    updated_rows = models.PositiveIntegerField(default=0)
    skipped_rows = models.PositiveIntegerField(default=0)
    project_refresh_count = models.PositiveIntegerField(default=0)
    affected_branches = models.JSONField(default=list, blank=True)
    covered_periods = models.JSONField(default=list, blank=True)
    error_log = models.JSONField(default=list, blank=True)
    summary = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-uploaded_at", "-id"]
        verbose_name = "Carga de gasto operativo"
        verbose_name_plural = "Cargas de gasto operativo"
        indexes = [
            models.Index(fields=["status", "uploaded_at"], name="rgopexload_status_uploaded_idx"),
            models.Index(fields=["file_hash", "status"], name="rgopexload_hash_status_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.original_filename} [{self.status}]"


class ProductoCostoOperativoMensual(models.Model):
    periodo = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.CASCADE,
        related_name="costos_operativos_mensuales",
    )
    unidades_base = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    venta_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    asp = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_mp_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    mano_obra_prod_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    indirecto_prod_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    empaque_prod_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_fabricacion_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "receta__nombre"]
        verbose_name = "Costo operativo mensual por producto"
        verbose_name_plural = "Costos operativos mensuales por producto"
        unique_together = [("periodo", "receta")]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.receta.nombre}"


class ProductoSucursalContribucionMensual(models.Model):
    periodo = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.CASCADE,
        related_name="contribuciones_mensuales",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.CASCADE,
        related_name="contribuciones_producto_mensuales",
    )
    unidades_vendidas = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    venta_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    asp = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_producto_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_producto_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    gasto_comercial_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    gasto_comercial_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    contribucion_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    contribucion_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    margen_contribucion_pct = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "sucursal__codigo", "receta__nombre"]
        verbose_name = "Contribución mensual por producto y sucursal"
        verbose_name_plural = "Contribuciones mensuales por producto y sucursal"
        unique_together = [("periodo", "receta", "sucursal")]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.sucursal.codigo} · {self.receta.nombre}"


class EmpresaResultadoMensual(models.Model):
    periodo = models.DateField(unique=True, db_index=True)
    venta_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    costo_materia_prima_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    costo_reventa_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    mano_obra_prod_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    indirecto_prod_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    empaque_prod_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    costo_fabricacion_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    margen_bruto_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    gasto_comercial_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    contribucion_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    gasto_corporativo_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    utilidad_operativa_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo"]
        verbose_name = "Resultado mensual empresa"
        verbose_name_plural = "Resultados mensuales empresa"

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m}"


class ProductBusinessRule(models.Model):
    CLASSIFICATION_REVENTA = "REVENTA"
    CLASSIFICATION_ACCESORIO = "ACCESORIO"
    CLASSIFICATION_SERVICIO = "SERVICIO"
    CLASSIFICATION_FABRICADO = "FABRICADO"
    CLASSIFICATION_CHOICES = [
        (CLASSIFICATION_REVENTA, "Reventa"),
        (CLASSIFICATION_ACCESORIO, "Accesorio"),
        (CLASSIFICATION_SERVICIO, "Servicio"),
        (CLASSIFICATION_FABRICADO, "Fabricado"),
    ]

    product_name = models.CharField(max_length=255, unique=True, db_index=True)
    normalized_name = models.CharField(max_length=255, unique=True, db_index=True, editable=False)
    classification = models.CharField(max_length=20, choices=CLASSIFICATION_CHOICES, db_index=True)
    is_fixed = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["product_name"]
        verbose_name = "Regla de negocio por producto"
        verbose_name_plural = "Reglas de negocio por producto"

    @staticmethod
    def normalize_product_name(product_name: str) -> str:
        return (product_name or "").strip().upper()

    def save(self, *args, **kwargs):
        self.normalized_name = self.normalize_product_name(self.product_name)
        update_fields = kwargs.get("update_fields")
        if update_fields is not None:
            updated = set(update_fields)
            updated.add("normalized_name")
            kwargs["update_fields"] = list(updated)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        fixed = "fija" if self.is_fixed else "contextual"
        return f"{self.product_name} · {self.classification} · {fixed}"


class CorteOficialDiario(models.Model):
    corte_date = models.DateField(unique=True, db_index=True)
    total_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_tickets = models.PositiveIntegerField(default=0)
    avg_ticket = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    contado_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    credito_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    discounts_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    new_customers = models.PositiveIntegerField(default=0)
    branch_scope = models.CharField(max_length=120, blank=True, default="Todas las sucursales")
    source_label = models.CharField(max_length=120, blank=True, default="Corte oficial diario")
    evidence_path = models.CharField(max_length=500, blank=True, default="")
    notes = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-corte_date"]
        verbose_name = "Corte oficial diario"
        verbose_name_plural = "Cortes oficiales diarios"

    def __str__(self) -> str:
        return f"{self.corte_date:%Y-%m-%d} · {self.total_amount}"


class ProductoPricingDecisionMensual(models.Model):
    ACCION_DEFENDER = "DEFENDER"
    ACCION_PROMOVER = "PROMOVER"
    ACCION_CORREGIR_COSTO = "CORREGIR_COSTO"
    ACCION_SUBIR_PRECIO = "SUBIR_PRECIO"
    ACCION_GANCHO = "GANCHO"
    ACCION_REFORMULAR = "REFORMULAR"
    ACCION_CHOICES = [
        (ACCION_DEFENDER, "Defender"),
        (ACCION_PROMOVER, "Promover"),
        (ACCION_CORREGIR_COSTO, "Corregir costo"),
        (ACCION_SUBIR_PRECIO, "Subir precio"),
        (ACCION_GANCHO, "Gancho"),
        (ACCION_REFORMULAR, "Reformular"),
    ]

    periodo = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.CASCADE,
        related_name="pricing_decisions_mensuales",
    )
    asp_actual = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_fabricacion_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    contribucion_unit = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    margen_bruto_pct = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    margen_contribucion_pct = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    precio_objetivo_bruto = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    precio_objetivo_contribucion = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    gap_precio = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    impacto_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    accion_sugerida = models.CharField(max_length=20, choices=ACCION_CHOICES, default=ACCION_DEFENDER, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "accion_sugerida", "-impacto_estimado", "receta__nombre"]
        verbose_name = "Decisión mensual de pricing"
        verbose_name_plural = "Decisiones mensuales de pricing"
        unique_together = [("periodo", "receta")]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.receta.nombre} · {self.accion_sugerida}"


class InsumoCostoHistoricoMensual(models.Model):
    METODO_PROMEDIO_MENSUAL = "MONTHLY_WEIGHTED"
    METODO_ARRASTRE = "ROLLED_FORWARD"
    METODO_POINT_EXISTENCIA = "POINT_EXISTENCIA"
    METODO_EQUIVALENCIA = "ALIAS_RULE"
    METODO_SIGUIENTE = "NEXT_KNOWN"
    METODO_CHOICES = [
        (METODO_PROMEDIO_MENSUAL, "Promedio ponderado mensual"),
        (METODO_ARRASTRE, "Arrastre último costo"),
        (METODO_POINT_EXISTENCIA, "Costo Point existencia"),
        (METODO_EQUIVALENCIA, "Equivalencia histórica"),
        (METODO_SIGUIENTE, "Primer costo posterior"),
    ]

    periodo = models.DateField(db_index=True)
    insumo = models.ForeignKey(
        "maestros.Insumo",
        on_delete=models.CASCADE,
        related_name="costos_historicos_mensuales",
    )
    costo_unitario = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metodo = models.CharField(max_length=30, choices=METODO_CHOICES, db_index=True)
    source_date = models.DateField(null=True, blank=True)
    sample_count = models.PositiveIntegerField(default=0)
    weighted_quantity = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "insumo__nombre"]
        verbose_name = "Costo histórico mensual de insumo"
        verbose_name_plural = "Costos históricos mensuales de insumos"
        unique_together = [("periodo", "insumo")]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.insumo.nombre}"


class ReglaCostoHistoricoInsumo(models.Model):
    METODO_EQUIVALENCIA = "ALIAS_INSUMO"
    METODO_SIGUIENTE = "NEXT_KNOWN_COST"
    METODO_CHOICES = [
        (METODO_EQUIVALENCIA, "Equivalencia con otro insumo"),
        (METODO_SIGUIENTE, "Usar primer costo posterior"),
    ]

    insumo_origen = models.ForeignKey(
        "maestros.Insumo",
        on_delete=models.CASCADE,
        related_name="reglas_costo_historico_origen",
    )
    metodo = models.CharField(max_length=30, choices=METODO_CHOICES, db_index=True)
    insumo_referencia = models.ForeignKey(
        "maestros.Insumo",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="reglas_costo_historico_referencia",
    )
    prioridad = models.PositiveIntegerField(default=100)
    activo = models.BooleanField(default=True)
    notas = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["prioridad", "id"]
        verbose_name = "Regla de costo histórico por insumo"
        verbose_name_plural = "Reglas de costo histórico por insumo"
        unique_together = [("insumo_origen", "metodo", "insumo_referencia")]

    def __str__(self) -> str:
        referencia = self.insumo_referencia.nombre if self.insumo_referencia_id else "mismo insumo"
        return f"{self.insumo_origen.nombre} · {self.metodo} · {referencia}"


class RecetaCostoHistoricoMensual(models.Model):
    periodo = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.CASCADE,
        related_name="costos_historicos_mensuales",
    )
    costo_total = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    costo_por_unidad_rendimiento = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    lineas_costeadas = models.PositiveIntegerField(default=0)
    lineas_totales = models.PositiveIntegerField(default=0)
    coverage_pct = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "receta__nombre"]
        verbose_name = "Costo histórico mensual de receta"
        verbose_name_plural = "Costos históricos mensuales de recetas"
        unique_together = [("periodo", "receta")]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.receta.nombre}"


class ProductoReventaCosto(models.Model):
    FUENTE_POINT_ALMACEN = "POINT_ALMACEN"
    FUENTE_POINT_HISTORIAL = "POINT_PRODUCT_HISTORY"
    FUENTE_MANUAL = "MANUAL"
    FUENTE_CHOICES = [
        (FUENTE_POINT_ALMACEN, "Point ALMACEN"),
        (FUENTE_POINT_HISTORIAL, "Historial producto Point"),
        (FUENTE_MANUAL, "Manual"),
    ]

    producto_point = models.ForeignKey(
        "pos_bridge.PointProduct",
        on_delete=models.CASCADE,
        related_name="costos_reventa",
    )
    costo_unitario = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    fecha_vigencia = models.DateField(db_index=True)
    fuente = models.CharField(max_length=40, choices=FUENTE_CHOICES, default=FUENTE_POINT_ALMACEN, db_index=True)
    proveedor_nombre = models.CharField(max_length=255, blank=True, default="")
    unidad = models.CharField(max_length=40, blank=True, default="")
    cantidad_snapshot = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    source_hash = models.CharField(max_length=64, unique=True, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_vigencia", "producto_point__name"]
        verbose_name = "Costo de producto de reventa"
        verbose_name_plural = "Costos de productos de reventa"
        indexes = [
            models.Index(fields=["producto_point", "fecha_vigencia"]),
        ]

    def __str__(self) -> str:
        return f"{self.producto_point.name} · {self.fecha_vigencia:%Y-%m-%d} · {self.costo_unitario}"


class ProductoReventaCostoHistoricoMensual(models.Model):
    METODO_PROMEDIO_MENSUAL = "MONTHLY_WEIGHTED"
    METODO_ARRASTRE = "ROLLED_FORWARD"
    METODO_POINT_ALMACEN = "POINT_ALMACEN"
    METODO_CHOICES = [
        (METODO_PROMEDIO_MENSUAL, "Promedio ponderado mensual"),
        (METODO_ARRASTRE, "Arrastre último costo"),
        (METODO_POINT_ALMACEN, "Costo Point ALMACEN"),
    ]

    periodo = models.DateField(db_index=True)
    producto_point = models.ForeignKey(
        "pos_bridge.PointProduct",
        on_delete=models.CASCADE,
        related_name="costos_reventa_historicos_mensuales",
    )
    costo_promedio = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metodo = models.CharField(max_length=30, choices=METODO_CHOICES, default=METODO_PROMEDIO_MENSUAL, db_index=True)
    source_date = models.DateField(null=True, blank=True)
    sample_count = models.PositiveIntegerField(default=0)
    weighted_quantity = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "producto_point__name"]
        verbose_name = "Costo histórico mensual de producto de reventa"
        verbose_name_plural = "Costos históricos mensuales de productos de reventa"
        unique_together = [("periodo", "producto_point")]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.producto_point.name}"


class PresupuestoImport(models.Model):
    TIPO_GENERAL = "GENERAL"
    TIPO_DETALLE = "DETALLE"
    TIPO_CHOICES = [
        (TIPO_GENERAL, "General consolidado"),
        (TIPO_DETALLE, "Detalle confiable"),
    ]

    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_GENERAL, db_index=True)
    fuente_nombre = models.CharField(max_length=220, db_index=True)
    archivo_ruta = models.CharField(max_length=500)
    archivo_hash = models.CharField(max_length=64, db_index=True)
    sheet_name = models.CharField(max_length=120, default="GENERAL")
    titulo = models.CharField(max_length=220, blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at", "fuente_nombre"]
        verbose_name = "Importación de presupuesto"
        verbose_name_plural = "Importaciones de presupuesto"
        unique_together = [("tipo", "fuente_nombre", "sheet_name")]

    def __str__(self) -> str:
        return f"{self.fuente_nombre} · {self.sheet_name}"


class PresupuestoLineaMensual(models.Model):
    AUDIT_PENDING = "PENDING"
    AUDIT_OK = "OK"
    AUDIT_DEVIATION = "DESVIACION"
    AUDIT_BAD_FORMULA = "MALA_FORMULA"
    AUDIT_MISSING_DETAIL = "SIN_SOPORTE_DETALLE"
    AUDIT_EXCLUDED_TOTAL = "EXCLUIDO_TOTALIZADOR"
    AUDIT_EXCLUDED_EXTRA = "EXCLUIDO_EXTRAORDINARIO"
    AUDIT_EXCLUDED_DUPLICATE = "EXCLUIDO_DUPLICADO"
    AUDIT_CHOICES = [
        (AUDIT_PENDING, "Pendiente"),
        (AUDIT_OK, "OK"),
        (AUDIT_DEVIATION, "Desviación"),
        (AUDIT_BAD_FORMULA, "Mala fórmula"),
        (AUDIT_MISSING_DETAIL, "Sin soporte detalle"),
        (AUDIT_EXCLUDED_TOTAL, "Excluido totalizador"),
        (AUDIT_EXCLUDED_EXTRA, "Excluido extraordinario"),
        (AUDIT_EXCLUDED_DUPLICATE, "Excluido duplicado"),
    ]

    importacion = models.ForeignKey(
        PresupuestoImport,
        on_delete=models.CASCADE,
        related_name="lineas_mensuales",
    )
    external_key = models.CharField(max_length=180, unique=True, db_index=True)
    period = models.DateField(db_index=True)
    account_code = models.CharField(max_length=80, blank=True, default="")
    concept = models.CharField(max_length=220, db_index=True)
    annual_budget = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    annual_actual = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    annual_variance = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    monthly_budget = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    monthly_actual = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    monthly_variance = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    row_index = models.PositiveIntegerField(default=0)
    audit_status = models.CharField(max_length=30, choices=AUDIT_CHOICES, default=AUDIT_PENDING, db_index=True)
    audit_source = models.CharField(max_length=60, blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-period", "concept", "row_index"]
        verbose_name = "Línea mensual de presupuesto"
        verbose_name_plural = "Líneas mensuales de presupuesto"
        indexes = [
            models.Index(fields=["period", "concept"], name="rpresu_period_concept_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.period:%Y-%m} · {self.concept}"


class PresupuestoResumenMensual(models.Model):
    TIPO_GLOBAL = "GLOBAL"
    TIPO_FUENTE = "FUENTE"
    TIPO_CHOICES = [
        (TIPO_GLOBAL, "Global"),
        (TIPO_FUENTE, "Por fuente"),
    ]

    period = models.DateField(db_index=True)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    fuente_nombre = models.CharField(max_length=220, blank=True, default="", db_index=True)
    total_budget = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_actual = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_variance = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    line_count = models.PositiveIntegerField(default=0)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-period", "tipo", "fuente_nombre"]
        verbose_name = "Resumen mensual de presupuesto"
        verbose_name_plural = "Resumenes mensuales de presupuesto"
        unique_together = [("period", "tipo", "fuente_nombre")]

    def __str__(self) -> str:
        label = self.fuente_nombre or "GLOBAL"
        return f"{self.period:%Y-%m} · {label}"


class AreaPresupuesto(models.Model):
    nombre = models.CharField(max_length=100, unique=True)
    codigo = models.SlugField(max_length=50, unique=True)
    orden = models.PositiveSmallIntegerField(default=0)
    activa = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["orden", "nombre"]
        verbose_name = "Área de presupuesto"
        verbose_name_plural = "Áreas de presupuesto"

    def __str__(self) -> str:
        return self.nombre


class RubroPresupuesto(models.Model):
    TIPO_INGRESO = "INGRESO"
    TIPO_EGRESO = "EGRESO"
    TIPO_COSTO = "COSTO"
    TIPO_CAPEX = "CAPEX"
    TIPO_CHOICES = [
        (TIPO_INGRESO, "Ingreso"),
        (TIPO_EGRESO, "Egreso"),
        (TIPO_COSTO, "Costo"),
        (TIPO_CAPEX, "Capex"),
    ]

    area = models.ForeignKey(
        AreaPresupuesto,
        on_delete=models.PROTECT,
        related_name="rubros",
    )
    concepto = models.CharField(max_length=200)
    codigo_cuenta = models.CharField(max_length=50, blank=True, default="")
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="rubros_presupuesto",
    )
    activo = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["area__orden", "area__nombre", "concepto", "sucursal__codigo"]
        verbose_name = "Rubro de presupuesto"
        verbose_name_plural = "Rubros de presupuesto"
        indexes = [
            models.Index(fields=["area", "tipo"], name="rubro_presu_area_tipo_idx"),
            models.Index(fields=["codigo_cuenta"], name="rubro_presu_cuenta_idx"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["area", "concepto", "codigo_cuenta", "sucursal"],
                name="uniq_rubro_presu_area_concept_suc",
            )
        ]

    def __str__(self) -> str:
        branch = f" · {self.sucursal.codigo}" if self.sucursal_id else ""
        return f"{self.area.codigo} · {self.concepto}{branch}"


class LineaPresupuestoMensual(models.Model):
    VERSION_ORIGINAL = "ORIGINAL"
    VERSION_REVISADO = "REVISADO"
    VERSION_CHOICES = [
        (VERSION_ORIGINAL, "Original"),
        (VERSION_REVISADO, "Revisado"),
    ]

    rubro = models.ForeignKey(
        RubroPresupuesto,
        on_delete=models.PROTECT,
        related_name="lineas_mensuales",
    )
    periodo = models.DateField(db_index=True)
    version = models.CharField(max_length=20, choices=VERSION_CHOICES, default=VERSION_ORIGINAL, db_index=True)
    monto_presupuesto = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    monto_real = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    fuente_real = models.CharField(max_length=100, blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "rubro__area__orden", "rubro__concepto"]
        verbose_name = "Línea de presupuesto mensual"
        verbose_name_plural = "Líneas de presupuesto mensual"
        unique_together = [("rubro", "periodo", "version")]
        indexes = [
            models.Index(fields=["periodo", "version"], name="linea_presu_period_ver_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.rubro.concepto} · {self.version}"


class FactVentaDiaria(models.Model):
    SOURCE_AUTHORITATIVE = "AUTHORITATIVE"
    SOURCE_V2 = "V2_FACT"
    SOURCE_LEGACY = "LEGACY"
    SOURCE_CHOICES = [
        (SOURCE_AUTHORITATIVE, "Venta autoritativa"),
        (SOURCE_V2, "Fact Point v2"),
        (SOURCE_LEGACY, "Point legacy"),
    ]

    fecha = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="facts_venta_diaria",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="facts_venta_diaria",
    )
    point_product = models.ForeignKey(
        "pos_bridge.PointProduct",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="facts_venta_diaria",
    )
    producto_clave = models.CharField(max_length=160, db_index=True)
    producto_nombre = models.CharField(max_length=255, blank=True, default="")
    categoria = models.CharField(max_length=200, blank=True, default="")
    cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    tickets = models.PositiveIntegerField(default=0)
    venta_bruta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    descuento = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    venta_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    venta_neta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    costo_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    margen = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    source_kind = models.CharField(max_length=20, choices=SOURCE_CHOICES, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "sucursal__codigo", "producto_nombre", "id"]
        verbose_name = "Fact venta diaria"
        verbose_name_plural = "Facts venta diaria"
        unique_together = [("fecha", "sucursal", "producto_clave", "source_kind")]
        indexes = [
            models.Index(fields=["fecha", "sucursal"], name="rfact_sale_day_branch_idx"),
            models.Index(fields=["fecha", "receta"], name="rfact_sale_day_recipe_idx"),
            models.Index(fields=["source_kind", "fecha"], name="rfact_sale_src_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.producto_nombre or self.producto_clave}"


class FactInventarioDiario(models.Model):
    fecha = models.DateField(db_index=True)
    insumo = models.ForeignKey(
        "maestros.Insumo",
        on_delete=models.CASCADE,
        related_name="facts_inventario_diario",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="facts_inventario_diario",
    )
    stock_inicial = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    entradas = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    salidas = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_final = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    costo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "insumo__nombre", "id"]
        verbose_name = "Fact inventario diario"
        verbose_name_plural = "Facts inventario diario"
        unique_together = [("fecha", "insumo", "sucursal")]
        indexes = [
            models.Index(fields=["fecha", "insumo"], name="rfact_inv_day_insumo_idx"),
            models.Index(fields=["fecha", "sucursal"], name="rfact_inv_day_branch_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.insumo.nombre}"


class FactProduccionDiaria(models.Model):
    fecha = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="facts_produccion_diaria",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="facts_produccion_diaria",
    )
    producido = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    vendido = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    merma = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    transferido = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "sucursal__codigo", "receta__nombre", "id"]
        verbose_name = "Fact producción diaria"
        verbose_name_plural = "Facts producción diaria"
        unique_together = [("fecha", "sucursal", "receta")]
        indexes = [
            models.Index(fields=["fecha", "sucursal"], name="rfact_prod_day_branch_idx"),
            models.Index(fields=["fecha", "receta"], name="rfact_prod_day_recipe_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.receta or 'Sin receta'}"


class SnapshotLedgerInventarioMensual(models.Model):
    month_start = models.DateField(unique=True, db_index=True)
    month_end = models.DateField()
    is_partial = models.BooleanField(default=False)
    opening_job_id = models.PositiveIntegerField(null=True, blank=True)
    closing_job_id = models.PositiveIntegerField(null=True, blank=True)
    opening_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    production_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    sold_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    waste_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    theoretical_closing = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    actual_closing = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    variance_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-month_start"]
        verbose_name = "Snapshot mensual ledger inventario"
        verbose_name_plural = "Snapshots mensuales ledger inventario"

    def __str__(self) -> str:
        return f"{self.month_start:%Y-%m}"


class SnapshotFlujoCentralMensual(models.Model):
    month_start = models.DateField(unique=True, db_index=True)
    month_end = models.DateField()
    is_partial = models.BooleanField(default=False)
    central_source = models.CharField(max_length=30, blank=True, default="")
    production_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    transfer_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    sold_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    waste_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    supply_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    net_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    actual_inventory_closing = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    inventory_variance_units = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-month_start"]
        verbose_name = "Snapshot mensual flujo central"
        verbose_name_plural = "Snapshots mensuales flujo central"

    def __str__(self) -> str:
        return f"{self.month_start:%Y-%m}"


class StockMensualSucursal(models.Model):
    periodo = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        related_name="stock_mensual_producto",
    )
    producto = models.ForeignKey(
        "pos_bridge.PointProduct",
        on_delete=models.PROTECT,
        related_name="stock_mensual_sucursal",
    )
    stock_apertura = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_cierre = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    fuente_apertura = models.DateTimeField()
    fuente_cierre = models.DateTimeField()
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "sucursal__codigo", "producto__name"]
        verbose_name = "Stock mensual por sucursal"
        verbose_name_plural = "Stocks mensuales por sucursal"
        unique_together = [("periodo", "sucursal", "producto")]
        indexes = [
            models.Index(fields=["periodo", "sucursal"], name="rstock_mes_branch_idx"),
            models.Index(fields=["periodo", "producto"], name="rstock_mes_product_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.periodo:%Y-%m} · {self.sucursal.codigo} · {self.producto.name}"


class ForecastInput(models.Model):
    fecha = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.CASCADE,
        related_name="forecast_inputs",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="forecast_inputs",
    )
    ventas_historicas = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    estacionalidad = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    tendencia = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    moving_avg_7 = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    moving_avg_28 = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    stddev_28 = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    crecimiento_28 = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "receta__nombre"]
        verbose_name = "Input de forecast"
        verbose_name_plural = "Inputs de forecast"
        unique_together = [("fecha", "receta", "sucursal")]
        indexes = [
            models.Index(fields=["fecha", "receta"], name="rfcst_day_recipe_idx"),
            models.Index(fields=["fecha", "sucursal"], name="rfcst_day_branch_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.receta.nombre}"


class ForecastCalibrationProfile(models.Model):
    PATTERN_MON_WED = "MON_WED"
    PATTERN_THU_SUN = "THU_SUN"
    PATTERN_CHOICES = [
        (PATTERN_MON_WED, "Lunes-Miércoles"),
        (PATTERN_THU_SUN, "Jueves-Domingo"),
    ]

    ROTATION_HIGH = "HIGH"
    ROTATION_LOW = "LOW"
    ROTATION_CHOICES = [
        (ROTATION_HIGH, "Alta rotación"),
        (ROTATION_LOW, "Baja rotación"),
    ]

    reference_date = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="forecast_calibration_profiles",
    )
    familia = models.CharField(max_length=120, blank=True, default="", db_index=True)
    weekly_pattern = models.CharField(max_length=16, choices=PATTERN_CHOICES, db_index=True)
    rotation_band = models.CharField(max_length=16, choices=ROTATION_CHOICES, db_index=True)
    sample_size = models.PositiveIntegerField(default=0)
    wape_before_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    wape_after_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    bias_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    hit_rate_before_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    hit_rate_after_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    volatility_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    recent_weight = models.DecimalField(max_digits=6, decimal_places=4, default=0)
    mid_weight = models.DecimalField(max_digits=6, decimal_places=4, default=0)
    older_weight = models.DecimalField(max_digits=6, decimal_places=4, default=0)
    bias_adjustment = models.DecimalField(max_digits=6, decimal_places=4, default=1)
    buffer_multiplier = models.DecimalField(max_digits=6, decimal_places=4, default=1)
    execution_gap_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-reference_date", "sucursal__codigo", "familia", "weekly_pattern", "rotation_band"]
        verbose_name = "Perfil de calibración de forecast"
        verbose_name_plural = "Perfiles de calibración de forecast"
        unique_together = [("reference_date", "sucursal", "familia", "weekly_pattern", "rotation_band")]
        indexes = [
            models.Index(
                fields=["reference_date", "sucursal", "weekly_pattern", "rotation_band"],
                name="rfcstcal_ref_branch_idx",
            ),
            models.Index(
                fields=["reference_date", "familia", "weekly_pattern", "rotation_band"],
                name="rfcstcal_ref_family_idx",
            ),
        ]

    def __str__(self) -> str:
        branch_label = self.sucursal.codigo if self.sucursal_id else "GLOBAL"
        family_label = self.familia or "SIN_FAMILIA"
        return f"{self.reference_date} · {branch_label} · {family_label}"


class AnalyticRefreshWindow(models.Model):
    DATASET_SALES = "FACT_VENTAS"
    DATASET_INVENTORY = "FACT_INVENTARIO"
    DATASET_PRODUCTION = "FACT_PRODUCCION"
    DATASET_FORECAST = "FORECAST_INPUTS"
    DATASET_SNAPSHOT_LEDGER = "SNAPSHOT_LEDGER"
    DATASET_SNAPSHOT_FLOW = "SNAPSHOT_FLOW"
    DATASET_CHOICES = [
        (DATASET_SALES, "Fact ventas"),
        (DATASET_INVENTORY, "Fact inventario"),
        (DATASET_PRODUCTION, "Fact producción"),
        (DATASET_FORECAST, "Forecast inputs"),
        (DATASET_SNAPSHOT_LEDGER, "Snapshot ledger"),
        (DATASET_SNAPSHOT_FLOW, "Snapshot flujo central"),
    ]

    STATUS_PENDING = "PENDING"
    STATUS_PROCESSING = "PROCESSING"
    STATUS_DONE = "DONE"
    STATUS_ERROR = "ERROR"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pendiente"),
        (STATUS_PROCESSING, "Procesando"),
        (STATUS_DONE, "Atendido"),
        (STATUS_ERROR, "Con error"),
    ]

    dataset = models.CharField(max_length=40, choices=DATASET_CHOICES, db_index=True)
    date_from = models.DateField(db_index=True)
    date_to = models.DateField(db_index=True)
    reason = models.CharField(max_length=160, blank=True, default="")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    last_error = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["status", "date_from", "dataset", "id"]
        verbose_name = "Ventana de refresh analítico"
        verbose_name_plural = "Ventanas de refresh analítico"
        indexes = [
            models.Index(fields=["dataset", "status", "date_from"], name="ran_ref_dataset_stat_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.dataset} {self.date_from}→{self.date_to} [{self.status}]"


class AnalyticAuditLog(models.Model):
    STATUS_OK = "OK"
    STATUS_WARNING = "WARNING"
    STATUS_ERROR = "ERROR"
    STATUS_CHOICES = [
        (STATUS_OK, "OK"),
        (STATUS_WARNING, "Warning"),
        (STATUS_ERROR, "Error"),
    ]

    audit_type = models.CharField(max_length=60, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_OK, db_index=True)
    date_from = models.DateField(null=True, blank=True, db_index=True)
    date_to = models.DateField(null=True, blank=True, db_index=True)
    discrepancy_count = models.PositiveIntegerField(default=0)
    message = models.TextField(blank=True, default="")
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-created_at", "-id"]
        verbose_name = "Auditoría analítica"
        verbose_name_plural = "Auditorías analíticas"
        indexes = [
            models.Index(fields=["audit_type", "created_at"], name="ran_audit_type_created_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.audit_type} · {self.status}"


class DashboardFullSnapshot(models.Model):
    months_window = models.PositiveSmallIntegerField(unique=True, db_index=True)
    payload = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    generated_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["months_window"]
        verbose_name = "Snapshot dashboard ejecutivo"
        verbose_name_plural = "Snapshots dashboard ejecutivo"

    def __str__(self) -> str:
        return f"Dashboard {self.months_window}m"


class ProductionExecutionLog(models.Model):
    fecha = models.DateField(db_index=True)
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.PROTECT,
        related_name="production_execution_logs",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        related_name="production_execution_logs",
    )
    recomendado = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    aprobado = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    producido_real = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    vendido_real = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    merma = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    desviacion = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_visible = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    decision_score = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    recommendation_version = models.CharField(max_length=40, blank=True, default="v1")
    comentario = models.TextField(blank=True, default="")
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="production_execution_logs",
    )
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "sucursal__codigo", "receta__nombre", "-id"]
        verbose_name = "Bitácora de ejecución de producción"
        verbose_name_plural = "Bitácoras de ejecución de producción"
        unique_together = [("fecha", "receta", "sucursal")]
        indexes = [
            models.Index(fields=["fecha", "sucursal"], name="rprodexec_day_branch_idx"),
            models.Index(fields=["fecha", "receta"], name="rprodexec_day_recipe_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.sucursal.codigo} · {self.receta.nombre}"


class ProductionOrder(models.Model):
    STATUS_DRAFT = "DRAFT"
    STATUS_PROPOSED = "PROPOSED"
    STATUS_APPROVED = "APPROVED"
    STATUS_RELEASED = "RELEASED"
    STATUS_EXECUTED = "EXECUTED"
    STATUS_CANCELLED = "CANCELLED"
    STATUS_CHOICES = [
        (STATUS_DRAFT, "Borrador"),
        (STATUS_PROPOSED, "Propuesta"),
        (STATUS_APPROVED, "Aprobada"),
        (STATUS_RELEASED, "Liberada"),
        (STATUS_EXECUTED, "Ejecutada"),
        (STATUS_CANCELLED, "Cancelada"),
    ]

    SOURCE_MANUAL = "MANUAL"
    SOURCE_AUTO = "AUTO"
    SOURCE_CHOICES = [
        (SOURCE_MANUAL, "Manual"),
        (SOURCE_AUTO, "Automática"),
    ]

    fecha = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        related_name="production_orders",
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PROPOSED, db_index=True)
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_AUTO, db_index=True)
    recommendation_version = models.CharField(max_length=40, blank=True, default="v1", db_index=True)
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="approved_production_orders",
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="created_production_orders",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    released_at = models.DateTimeField(null=True, blank=True)
    executed_at = models.DateTimeField(null=True, blank=True)
    cancelled_at = models.DateTimeField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "sucursal__codigo", "-id"]
        verbose_name = "Orden de producción"
        verbose_name_plural = "Órdenes de producción"
        unique_together = [("fecha", "sucursal")]
        indexes = [
            models.Index(fields=["fecha", "status"], name="rprodord_day_status_idx"),
            models.Index(fields=["fecha", "sucursal"], name="rprodord_day_branch_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.sucursal.codigo} · {self.status}"


class ProductionOrderLine(models.Model):
    RISK_LOW = "BAJO"
    RISK_MEDIUM = "MEDIO"
    RISK_HIGH = "ALTO"
    RISK_CHOICES = [
        (RISK_LOW, "Bajo"),
        (RISK_MEDIUM, "Medio"),
        (RISK_HIGH, "Alto"),
    ]

    order = models.ForeignKey(
        "reportes.ProductionOrder",
        on_delete=models.CASCADE,
        related_name="lines",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        on_delete=models.PROTECT,
        related_name="production_order_lines",
    )
    cantidad_recomendada = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    cantidad_aprobada = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    cantidad_ejecutada = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    decision_score = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    riesgo_merma = models.CharField(max_length=16, choices=RISK_CHOICES, default=RISK_LOW)
    motivo = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-decision_score", "receta__nombre", "-id"]
        verbose_name = "Línea de orden de producción"
        verbose_name_plural = "Líneas de orden de producción"
        unique_together = [("order", "receta")]
        indexes = [
            models.Index(fields=["order", "decision_score"], name="rprodline_order_score_idx"),
            models.Index(fields=["receta", "created_at"], name="rprodline_recipe_created_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.order.fecha} · {self.order.sucursal.codigo} · {self.receta.nombre}"


class SupplierLeadTime(models.Model):
    insumo = models.ForeignKey(
        "maestros.Insumo",
        on_delete=models.CASCADE,
        related_name="supplier_lead_times",
    )
    proveedor = models.ForeignKey(
        "maestros.Proveedor",
        on_delete=models.CASCADE,
        related_name="supplier_lead_times",
    )
    lead_time_dias = models.PositiveIntegerField(default=0)
    frecuencia_pedido_dias = models.PositiveIntegerField(default=7)
    lote_minimo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    activo = models.BooleanField(default=True, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["insumo__nombre", "proveedor__nombre", "-activo", "id"]
        verbose_name = "Lead time por proveedor"
        verbose_name_plural = "Lead times por proveedor"
        unique_together = [("insumo", "proveedor")]
        indexes = [
            models.Index(fields=["insumo", "activo"], name="rsupplt_insumo_active_idx"),
            models.Index(fields=["proveedor", "activo"], name="rsupplt_prov_active_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.insumo.nombre} · {self.proveedor.nombre}"


class AutoControlSettings(models.Model):
    singleton_key = models.PositiveSmallIntegerField(default=1, unique=True, editable=False)
    max_variacion_produccion_pct = models.DecimalField(max_digits=8, decimal_places=2, default=30)
    max_compra_diaria = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    min_stock_seguridad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    enable_auto_purchase = models.BooleanField(default=True)
    enable_alerts = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="auto_control_settings_updates",
    )
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Control global de automatización"
        verbose_name_plural = "Control global de automatización"

    def __str__(self) -> str:
        return "Control global de automatización"

    @classmethod
    def get_solo(cls) -> "AutoControlSettings":
        obj, _ = cls.objects.get_or_create(pk=1, defaults={"singleton_key": 1})
        return obj


class AutoPurchaseRequestSnapshot(models.Model):
    solicitud = models.OneToOneField(
        "compras.SolicitudCompra",
        on_delete=models.CASCADE,
        related_name="auto_purchase_snapshot",
    )
    production_order = models.ForeignKey(
        "reportes.ProductionOrder",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="purchase_snapshots",
    )
    fecha = models.DateField(db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="auto_purchase_snapshots",
    )
    insumo = models.ForeignKey(
        "maestros.Insumo",
        on_delete=models.CASCADE,
        related_name="auto_purchase_snapshots",
    )
    proveedor = models.ForeignKey(
        "maestros.Proveedor",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="auto_purchase_snapshots",
    )
    fecha_sugerida_compra = models.DateField(null=True, blank=True, db_index=True)
    stock_actual = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    stock_objetivo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    faltante_inmediato = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    faltante_objetivo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    cantidad_sugerida = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    purchase_priority_score = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    lead_time_dias = models.PositiveIntegerField(default=0)
    frecuencia_pedido_dias = models.PositiveIntegerField(default=0)
    lote_minimo = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["fecha", "-purchase_priority_score", "sucursal__codigo", "insumo__nombre"]
        verbose_name = "Snapshot de compra automática"
        verbose_name_plural = "Snapshots de compra automática"
        indexes = [
            models.Index(fields=["fecha", "sucursal"], name="rautopurch_day_branch_idx"),
            models.Index(fields=["fecha", "purchase_priority_score"], name="rautopurch_day_score_idx"),
            models.Index(fields=["fecha_sugerida_compra", "fecha"], name="rautopurch_suggest_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.insumo.nombre}"


class Alert(models.Model):
    TYPE_MERMA = "MERMA"
    TYPE_DESVIACION = "DESVIACION"
    TYPE_OPORTUNIDAD = "OPORTUNIDAD"
    TYPE_STOCK = "STOCK"
    TYPE_CHOICES = [
        (TYPE_MERMA, "Riesgo de merma"),
        (TYPE_DESVIACION, "Desviación de ejecución"),
        (TYPE_OPORTUNIDAD, "Oportunidad de venta"),
        (TYPE_STOCK, "Stock crítico"),
    ]

    SEVERITY_LOW = "LOW"
    SEVERITY_MEDIUM = "MEDIUM"
    SEVERITY_HIGH = "HIGH"
    SEVERITY_CHOICES = [
        (SEVERITY_LOW, "Baja"),
        (SEVERITY_MEDIUM, "Media"),
        (SEVERITY_HIGH, "Alta"),
    ]

    alert_key = models.CharField(max_length=180, unique=True, db_index=True)
    tipo = models.CharField(max_length=20, choices=TYPE_CHOICES, db_index=True)
    severidad = models.CharField(max_length=20, choices=SEVERITY_CHOICES, db_index=True)
    entidad = models.CharField(max_length=200, db_index=True)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="report_alerts",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="report_alerts",
    )
    insumo = models.ForeignKey(
        "maestros.Insumo",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="report_alerts",
    )
    mensaje = models.TextField(blank=True, default="")
    impacto_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    fecha = models.DateField(db_index=True)
    resuelta = models.BooleanField(default=False, db_index=True)
    resolved_at = models.DateTimeField(null=True, blank=True)
    resolved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="resolved_report_alerts",
    )
    resolution_note = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["resuelta", "-impacto_estimado", "-created_at", "-id"]
        verbose_name = "Alerta operativa"
        verbose_name_plural = "Alertas operativas"
        indexes = [
            models.Index(fields=["fecha", "resuelta", "severidad"], name="ralert_day_state_sev_idx"),
            models.Index(fields=["tipo", "fecha"], name="ralert_type_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.tipo} · {self.entidad}"


class OperationsMetricSnapshot(models.Model):
    fecha = models.DateField(unique=True, db_index=True)
    adoption_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    approval_deviation_avg = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    execution_deviation_avg = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    merma_total = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    impacto_economico_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    impacto_real = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    desviacion_impacto = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    adopcion_real = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    efectividad_recomendaciones = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    payload = models.JSONField(default=dict, blank=True)
    generated_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha"]
        verbose_name = "Snapshot de métricas operativas"
        verbose_name_plural = "Snapshots de métricas operativas"

    def __str__(self) -> str:
        return f"{self.fecha} · adopción {self.adoption_pct}%"


class ProyectoInversion(models.Model):
    TIPO_APERTURA_SUCURSAL = "APERTURA_SUCURSAL"
    TIPO_REMODELACION = "REMODELACION"
    TIPO_EXPANSION = "EXPANSION"
    TIPO_REUBICACION = "REUBICACION"
    TIPO_CHOICES = [
        (TIPO_APERTURA_SUCURSAL, "Apertura sucursal"),
        (TIPO_REMODELACION, "Remodelación"),
        (TIPO_EXPANSION, "Expansión"),
        (TIPO_REUBICACION, "Reubicación"),
    ]

    ESTATUS_PLANEACION = "PLANEACION"
    ESTATUS_EJECUCION = "EJECUCION"
    ESTATUS_ACTIVO = "ACTIVO"
    ESTATUS_EN_RECUPERACION = "EN_RECUPERACION"
    ESTATUS_CERRADO = "CERRADO"
    ESTATUS_CANCELADO = "CANCELADO"
    ESTATUS_CHOICES = [
        (ESTATUS_PLANEACION, "Planeación"),
        (ESTATUS_EJECUCION, "Ejecución"),
        (ESTATUS_ACTIVO, "Activo"),
        (ESTATUS_EN_RECUPERACION, "En recuperación"),
        (ESTATUS_CERRADO, "Cerrado"),
        (ESTATUS_CANCELADO, "Cancelado"),
    ]

    RECOVERY_FULL_NET_CASHFLOW = "FULL_NET_CASHFLOW"
    RECOVERY_PERCENTAGE_OF_PROFIT = "PERCENTAGE_OF_PROFIT"
    RECOVERY_PROFIT_AFTER_DEBT_SERVICE = "PROFIT_AFTER_DEBT_SERVICE"
    RECOVERY_CHOICES = [
        (RECOVERY_FULL_NET_CASHFLOW, "100% del flujo neto"),
        (RECOVERY_PERCENTAGE_OF_PROFIT, "Porcentaje de utilidad"),
        (RECOVERY_PROFIT_AFTER_DEBT_SERVICE, "Utilidad después de deuda"),
    ]

    nombre_proyecto = models.CharField(max_length=180, db_index=True)
    tipo_proyecto = models.CharField(max_length=30, choices=TIPO_CHOICES, default=TIPO_APERTURA_SUCURSAL, db_index=True)
    sucursal_relacionada = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="proyectos_inversion",
    )
    fecha_inicio = models.DateField(db_index=True)
    fecha_apertura = models.DateField(null=True, blank=True, db_index=True)
    responsable = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="proyectos_inversion_responsable",
    )
    estatus = models.CharField(max_length=30, choices=ESTATUS_CHOICES, default=ESTATUS_PLANEACION, db_index=True)
    monto_inversion_planeado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    monto_inversion_real = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    capital_inicial_aportado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    deuda_asociada = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    tasa_interes_anual = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    plazo_deuda_meses = models.PositiveIntegerField(default=0)
    pago_mensual_deuda_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    discount_rate = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    roi_objetivo = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    payback_objetivo_meses = models.PositiveIntegerField(default=0)
    porcentaje_utilidad_destinado_a_recuperacion = models.DecimalField(max_digits=8, decimal_places=4, default=1)
    recovery_strategy = models.CharField(
        max_length=40,
        choices=RECOVERY_CHOICES,
        default=RECOVERY_FULL_NET_CASHFLOW,
        db_index=True,
    )
    recovery_percentage = models.DecimalField(max_digits=8, decimal_places=4, default=1)
    cierre_por_recuperacion_total = models.BooleanField(default=True)
    cierre_por_liquidacion_deuda = models.BooleanField(default=False)
    cierre_por_roi_minimo = models.BooleanField(default=False)
    roi_minimo_cierre = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    cierre_manual_habilitado = models.BooleanField(default=True)
    auto_cierre_habilitado = models.BooleanField(default=True)
    observaciones = models.TextField(blank=True, default="")
    fecha_cierre = models.DateField(null=True, blank=True)
    kpis_cierre = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_inicio", "-id"]
        verbose_name = "Proyecto de inversión"
        verbose_name_plural = "Proyectos de inversión"
        indexes = [
            models.Index(fields=["estatus", "fecha_inicio"], name="rpinv_status_start_idx"),
            models.Index(fields=["sucursal_relacionada", "estatus"], name="rpinv_branch_status_idx"),
        ]

    def __str__(self) -> str:
        return self.nombre_proyecto


class ProyectoInversionGasto(models.Model):
    CATEGORIA_OBRA_CIVIL = "OBRA_CIVIL"
    CATEGORIA_EQUIPAMIENTO = "EQUIPAMIENTO"
    CATEGORIA_MOBILIARIO = "MOBILIARIO"
    CATEGORIA_TRAMITES = "TRAMITES"
    CATEGORIA_MARKETING_APERTURA = "MARKETING_APERTURA"
    CATEGORIA_INVENTARIO_INICIAL = "INVENTARIO_INICIAL"
    CATEGORIA_TECNOLOGIA = "TECNOLOGIA"
    CATEGORIA_OTROS = "OTROS"
    CATEGORIA_CHOICES = [
        (CATEGORIA_OBRA_CIVIL, "Obra civil"),
        (CATEGORIA_EQUIPAMIENTO, "Equipamiento"),
        (CATEGORIA_MOBILIARIO, "Mobiliario"),
        (CATEGORIA_TRAMITES, "Trámites"),
        (CATEGORIA_MARKETING_APERTURA, "Marketing apertura"),
        (CATEGORIA_INVENTARIO_INICIAL, "Inventario inicial"),
        (CATEGORIA_TECNOLOGIA, "Tecnología"),
        (CATEGORIA_OTROS, "Otros"),
    ]

    proyecto = models.ForeignKey(
        ProyectoInversion,
        on_delete=models.CASCADE,
        related_name="gastos_inversion",
    )
    fecha = models.DateField(db_index=True)
    categoria = models.CharField(max_length=30, choices=CATEGORIA_CHOICES, db_index=True)
    subcategoria = models.CharField(max_length=120, blank=True, default="")
    descripcion = models.CharField(max_length=255)
    proveedor = models.ForeignKey(
        "maestros.Proveedor",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="gastos_proyecto_inversion",
    )
    proveedor_nombre = models.CharField(max_length=160, blank=True, default="")
    monto = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    iva = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    monto_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    metodo_pago = models.CharField(max_length=80, blank=True, default="")
    financiado = models.BooleanField(default=False)
    referencia_compra = models.CharField(max_length=120, blank=True, default="")
    referencia_contable = models.CharField(max_length=120, blank=True, default="")
    orden_compra = models.ForeignKey(
        "compras.OrdenCompra",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="gastos_capex_proyecto",
    )
    recepcion_compra = models.ForeignKey(
        "compras.RecepcionCompra",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="gastos_capex_proyecto",
    )
    evidencia_url = models.CharField(max_length=500, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    capturado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="gastos_proyecto_inversion_capturados",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "-id"]
        verbose_name = "Gasto de inversión"
        verbose_name_plural = "Gastos de inversión"
        indexes = [
            models.Index(fields=["proyecto", "fecha"], name="rpinvexp_project_date_idx"),
            models.Index(fields=["categoria", "fecha"], name="rpinvexp_cat_date_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.proyecto.nombre_proyecto} · {self.fecha} · {self.descripcion}"

    def save(self, *args, **kwargs):
        if not self.monto_total:
            self.monto_total = (self.monto or 0) + (self.iva or 0)
        if self.proveedor_id and not self.proveedor_nombre:
            self.proveedor_nombre = self.proveedor.nombre
        super().save(*args, **kwargs)


class ProyectoInversionPagoDeuda(models.Model):
    proyecto = models.ForeignKey(
        ProyectoInversion,
        on_delete=models.CASCADE,
        related_name="pagos_deuda",
    )
    fecha_pago = models.DateField(db_index=True)
    monto_pago = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    interes_pagado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    capital_amortizado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    saldo_insoluto = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    referencia = models.CharField(max_length=120, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    capturado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="pagos_deuda_proyecto_capturados",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["fecha_pago", "id"]
        verbose_name = "Pago de deuda del proyecto"
        verbose_name_plural = "Pagos de deuda del proyecto"
        indexes = [
            models.Index(fields=["proyecto", "fecha_pago"], name="rpinvdebt_project_date_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.proyecto.nombre_proyecto} · {self.fecha_pago} · {self.monto_pago}"


class ProyectoInversionEscenario(models.Model):
    TIPO_CONSERVADOR = "CONSERVADOR"
    TIPO_BASE = "BASE"
    TIPO_OPTIMISTA = "OPTIMISTA"
    TIPO_PERSONALIZADO = "PERSONALIZADO"
    TIPO_CHOICES = [
        (TIPO_CONSERVADOR, "Conservador"),
        (TIPO_BASE, "Base"),
        (TIPO_OPTIMISTA, "Optimista"),
        (TIPO_PERSONALIZADO, "Personalizado"),
    ]

    ESTATUS_EN_REVISION = "EN_REVISION"
    ESTATUS_CANDIDATO = "CANDIDATO"
    ESTATUS_DESCARTADO = "DESCARTADO"
    ESTATUS_APROBADO_PRELIMINAR = "APROBADO_PRELIMINAR"
    ESTATUS_CHOICES = [
        (ESTATUS_EN_REVISION, "En revision"),
        (ESTATUS_CANDIDATO, "Candidato"),
        (ESTATUS_DESCARTADO, "Descartado"),
        (ESTATUS_APROBADO_PRELIMINAR, "Aprobado preliminar"),
    ]

    proyecto = models.ForeignKey(
        ProyectoInversion,
        on_delete=models.CASCADE,
        related_name="escenarios",
    )
    nombre = models.CharField(max_length=120)
    tipo_escenario = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_BASE, db_index=True)
    ventas_promedio_mensuales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    crecimiento_mensual_pct = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    margen_bruto_pct = models.DecimalField(max_digits=8, decimal_places=4, default=0)
    gastos_operativos_mensuales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    recovery_strategy_override = models.CharField(
        max_length=40,
        choices=ProyectoInversion.RECOVERY_CHOICES,
        blank=True,
        default="",
    )
    recovery_percentage_override = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    horizonte_meses = models.PositiveIntegerField(default=24)
    simulacion_hash = models.CharField(max_length=64, blank=True, default="", db_index=True)
    estatus_simulacion = models.CharField(
        max_length=30,
        choices=ESTATUS_CHOICES,
        default=ESTATUS_EN_REVISION,
        db_index=True,
    )
    capturado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="proyectos_inversion_escenarios_capturados",
    )
    notas = models.TextField(blank=True, default="")
    resultados = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["proyecto_id", "tipo_escenario", "nombre"]
        verbose_name = "Escenario del proyecto"
        verbose_name_plural = "Escenarios del proyecto"
        unique_together = [("proyecto", "nombre")]

    def __str__(self) -> str:
        return f"{self.proyecto.nombre_proyecto} · {self.nombre}"


class ProyectoInversionSnapshotMensual(models.Model):
    DATA_SOURCE_FACT = "FACT"
    DATA_SOURCE_FALLBACK = "FALLBACK"
    DATA_SOURCE_ESTIMATED = "ESTIMATED"
    DATA_SOURCE_CHOICES = [
        (DATA_SOURCE_FACT, "Fact"),
        (DATA_SOURCE_FALLBACK, "Fallback"),
        (DATA_SOURCE_ESTIMATED, "Estimado"),
    ]

    HEALTH_GREEN = "GREEN"
    HEALTH_YELLOW = "YELLOW"
    HEALTH_RED = "RED"
    HEALTH_STATUS_CHOICES = [
        (HEALTH_GREEN, "Verde"),
        (HEALTH_YELLOW, "Amarillo"),
        (HEALTH_RED, "Rojo"),
    ]

    proyecto = models.ForeignKey(
        ProyectoInversion,
        on_delete=models.CASCADE,
        related_name="snapshots_mensuales",
    )
    periodo = models.DateField(db_index=True)
    periodo_fin = models.DateField()
    es_parcial = models.BooleanField(default=False)
    ventas_mensuales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    costo_venta_mensual = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    utilidad_bruta = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    gastos_operativos = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    nomina = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    renta = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    servicios = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    marketing = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    otros_gastos = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    utilidad_operativa = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    flujo_operativo = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    servicio_deuda = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    interes_pagado = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    capital_amortizado = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    saldo_insoluto = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    flujo_libre = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    flujo_para_recuperacion = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    flujo_neto = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    monto_recuperacion_mes = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    recuperacion_acumulada = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    saldo_pendiente = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    porcentaje_recuperado = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    cash_on_cash = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    roi_mensual = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    roi_acumulado = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    roi_anualizado = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    payback_real_meses = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    payback_proyectado_meses = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    payback_forecast_meses = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    mes_estimado_recuperacion = models.DateField(null=True, blank=True)
    fecha_estimada_recuperacion_forecast = models.DateField(null=True, blank=True)
    confidence_score = models.PositiveSmallIntegerField(default=40)
    data_source = models.CharField(
        max_length=20,
        choices=DATA_SOURCE_CHOICES,
        default=DATA_SOURCE_ESTIMATED,
        db_index=True,
    )
    health_score = models.PositiveSmallIntegerField(default=0)
    health_status = models.CharField(
        max_length=10,
        choices=HEALTH_STATUS_CHOICES,
        default=HEALTH_RED,
    )
    van = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    tir = models.DecimalField(max_digits=8, decimal_places=4, null=True, blank=True)
    fuentes = models.JSONField(default=dict, blank=True)
    calculado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-periodo", "proyecto__nombre_proyecto"]
        verbose_name = "Snapshot mensual del proyecto"
        verbose_name_plural = "Snapshots mensuales del proyecto"
        unique_together = [("proyecto", "periodo")]
        indexes = [
            models.Index(fields=["periodo", "proyecto"], name="rpinvsnap_period_project_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.proyecto.nombre_proyecto} · {self.periodo:%Y-%m}"


class ProyectoInversionAlerta(models.Model):
    CODIGO_PAYBACK_RISK = "PAYBACK_RISK"
    CODIGO_LOW_ROI = "LOW_ROI"
    CODIGO_NEGATIVE_FREE_CASHFLOW = "NEGATIVE_FREE_CASHFLOW"
    CODIGO_CAPEX_OVERRUN = "CAPEX_OVERRUN"
    CODIGO_CHOICES = [
        (CODIGO_PAYBACK_RISK, "Payback en riesgo"),
        (CODIGO_LOW_ROI, "ROI bajo"),
        (CODIGO_NEGATIVE_FREE_CASHFLOW, "Flujo libre negativo"),
        (CODIGO_CAPEX_OVERRUN, "Sobreinversión"),
    ]

    SEVERITY_INFO = "INFO"
    SEVERITY_WARNING = "WARNING"
    SEVERITY_CRITICAL = "CRITICAL"
    SEVERITY_CHOICES = [
        (SEVERITY_INFO, "Info"),
        (SEVERITY_WARNING, "Warning"),
        (SEVERITY_CRITICAL, "Critical"),
    ]

    proyecto = models.ForeignKey(
        ProyectoInversion,
        on_delete=models.CASCADE,
        related_name="alertas",
    )
    snapshot = models.ForeignKey(
        ProyectoInversionSnapshotMensual,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="alertas",
    )
    codigo = models.CharField(max_length=40, choices=CODIGO_CHOICES, db_index=True)
    severidad = models.CharField(max_length=10, choices=SEVERITY_CHOICES, default=SEVERITY_WARNING, db_index=True)
    titulo = models.CharField(max_length=140)
    mensaje = models.TextField()
    activa = models.BooleanField(default=True, db_index=True)
    payload = models.JSONField(default=dict, blank=True)
    first_detected_at = models.DateTimeField(default=timezone.now)
    last_detected_at = models.DateTimeField(default=timezone.now)
    resolved_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-activa", "-last_detected_at", "-id"]
        verbose_name = "Alerta de proyecto de inversión"
        verbose_name_plural = "Alertas de proyectos de inversión"
        indexes = [
            models.Index(fields=["proyecto", "activa"], name="rpinvalert_proj_active_idx"),
            models.Index(fields=["codigo", "activa"], name="rpinvalert_code_active_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.proyecto.nombre_proyecto} · {self.codigo} · {self.severidad}"


class ExpansionPolicyConfig(models.Model):
    nombre = models.CharField(max_length=120, default="Política expansión activa")
    activa = models.BooleanField(default=True, db_index=True)
    min_free_cashflow_total = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    max_debt_to_income_ratio = models.DecimalField(max_digits=8, decimal_places=4, default=Decimal("0.40"))
    max_average_payback_months = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("24"))
    max_projects_in_risk = models.PositiveIntegerField(default=2)
    notes = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-activa", "-actualizado_en", "-id"]
        verbose_name = "Política global de expansión"
        verbose_name_plural = "Políticas globales de expansión"

    def __str__(self) -> str:
        return self.nombre


class ExpansionZoneScore(models.Model):
    ciudad = models.CharField(max_length=120, db_index=True)
    zona = models.CharField(max_length=120, db_index=True)
    score_estimado = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    ventas_promedio = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    densidad = models.DecimalField(max_digits=10, decimal_places=4, default=0)
    competencia = models.CharField(max_length=180, blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-score_estimado", "ciudad", "zona"]
        verbose_name = "Score de zona de expansión"
        verbose_name_plural = "Scores de zonas de expansión"
        unique_together = [("ciudad", "zona")]

    def __str__(self) -> str:
        return f"{self.ciudad} · {self.zona}"
