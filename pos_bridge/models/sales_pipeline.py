from __future__ import annotations

from django.db import models
from django.utils import timezone


class PointSalesExtractionTask(models.Model):
    STATUS_PENDING = "PENDING"
    STATUS_RUNNING = "RUNNING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"
    STATUS_SKIPPED = "SKIPPED"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
        (STATUS_SKIPPED, "Skipped"),
    ]

    SOURCE_MODE_OFFICIAL = "OFFICIAL_CATEGORY_REPORT"
    SOURCE_MODE_CHOICES = [
        (SOURCE_MODE_OFFICIAL, "Official category report"),
    ]

    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        on_delete=models.CASCADE,
        related_name="sales_extraction_tasks",
    )
    branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_extraction_tasks",
    )
    source_mode = models.CharField(max_length=40, choices=SOURCE_MODE_CHOICES, default=SOURCE_MODE_OFFICIAL)
    source_endpoint = models.CharField(max_length=160, default="/Report/PrintReportes?idreporte=3")
    credito_scope = models.CharField(max_length=20, default="null")
    sale_date = models.DateField(db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    worker_name = models.CharField(max_length=120, blank=True, default="")
    claimed_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    extracted_at = models.DateTimeField(null=True, blank=True)
    source_file = models.CharField(max_length=300, blank=True, default="")
    source_hash = models.CharField(max_length=64, blank=True, default="", db_index=True)
    row_count = models.PositiveIntegerField(default=0)
    timings_ms = models.JSONField(default=dict, blank=True)
    summary_json = models.JSONField(default=dict, blank=True)
    observations = models.JSONField(default=dict, blank=True)
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_sales_extraction_tasks"
        ordering = ["sale_date", "branch__name", "id"]
        unique_together = [("sync_job", "branch", "sale_date", "credito_scope", "source_mode")]
        indexes = [
            models.Index(fields=["sync_job", "status"], name="pbs_task_job_status_idx"),
            models.Index(fields=["status", "sale_date"], name="pbs_task_status_day_idx"),
            models.Index(fields=["branch", "sale_date"], name="pbs_task_branch_day_idx"),
        ]

    def mark_running(self, *, worker_name: str) -> None:
        now = timezone.now()
        self.status = self.STATUS_RUNNING
        self.worker_name = worker_name
        self.claimed_at = now
        self.started_at = now

    def __str__(self) -> str:
        branch = self.branch.name if self.branch_id else "SIN_SUCURSAL"
        return f"{self.sale_date} · {branch} · {self.status}"


class PointSalesRawStaging(models.Model):
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        on_delete=models.CASCADE,
        related_name="sales_raw_rows",
    )
    task = models.ForeignKey(
        "pos_bridge.PointSalesExtractionTask",
        on_delete=models.CASCADE,
        related_name="raw_rows",
    )
    row_number = models.PositiveIntegerField(default=0)
    source_mode = models.CharField(max_length=40, default=PointSalesExtractionTask.SOURCE_MODE_OFFICIAL)
    source_endpoint = models.CharField(max_length=160, default="/Report/PrintReportes?idreporte=3")
    source_file = models.CharField(max_length=300)
    source_hash = models.CharField(max_length=64, db_index=True)
    fecha_extraccion = models.DateTimeField(default=timezone.now)
    credito_scope = models.CharField(max_length=20, default="null")
    sucursal_raw = models.CharField(max_length=200, blank=True, default="")
    fecha_raw = models.CharField(max_length=40, blank=True, default="")
    categoria_raw = models.CharField(max_length=200, blank=True, default="")
    codigo_raw = models.CharField(max_length=120, blank=True, default="")
    producto_raw = models.CharField(max_length=255, blank=True, default="")
    total_cantidad_raw = models.CharField(max_length=80, blank=True, default="")
    total_descuento_raw = models.CharField(max_length=80, blank=True, default="")
    total_venta_raw = models.CharField(max_length=80, blank=True, default="")
    total_impuestos_raw = models.CharField(max_length=80, blank=True, default="")
    total_venta_neta_raw = models.CharField(max_length=80, blank=True, default="")
    payload_original_json = models.JSONField(default=dict, blank=True)
    row_hash = models.CharField(max_length=64, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_sales_raw_staging"
        ordering = ["task_id", "row_number", "id"]
        unique_together = [("task", "row_number"), ("task", "row_hash")]
        indexes = [
            models.Index(fields=["sync_job", "source_hash"], name="pbs_raw_job_hash_idx"),
            models.Index(fields=["task", "categoria_raw"], name="pbs_raw_task_cat_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.task_id} · {self.row_number} · {self.producto_raw}"


class PointSalesNormalized(models.Model):
    MATCH_EXACT_CODE = "EXACT_CODE"
    MATCH_ALIAS = "ALIAS"
    MATCH_NAME = "NAME"
    MATCH_NON_RECIPE = "NON_RECIPE"
    MATCH_SIN_CATALOGO = "SIN_MATCH_CATALOGO"
    MATCH_CHOICES = [
        (MATCH_EXACT_CODE, "Exact code"),
        (MATCH_ALIAS, "Alias"),
        (MATCH_NAME, "Name"),
        (MATCH_NON_RECIPE, "Non recipe"),
        (MATCH_SIN_CATALOGO, "Without catalog match"),
    ]

    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        on_delete=models.CASCADE,
        related_name="sales_normalized_rows",
    )
    task = models.ForeignKey(
        "pos_bridge.PointSalesExtractionTask",
        on_delete=models.CASCADE,
        related_name="normalized_rows",
    )
    raw_row = models.OneToOneField(
        "pos_bridge.PointSalesRawStaging",
        on_delete=models.CASCADE,
        related_name="normalized_row",
    )
    branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_normalized_rows",
    )
    sucursal_nombre = models.CharField(max_length=200, db_index=True)
    fecha = models.DateField(db_index=True)
    categoria = models.CharField(max_length=200, db_index=True)
    producto_nombre_historico = models.CharField(max_length=255, blank=True, default="", db_index=True)
    point_product = models.ForeignKey(
        "pos_bridge.PointProduct",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_normalized_rows",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_sales_normalized_rows",
    )
    match_catalogo_status = models.CharField(max_length=30, choices=MATCH_CHOICES, default=MATCH_SIN_CATALOGO)
    total_cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    total_descuento = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_venta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_impuestos = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_venta_neta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    source_hash = models.CharField(max_length=64, db_index=True)
    source_file = models.CharField(max_length=300)
    credito_scope = models.CharField(max_length=20, default="null")
    extracted_at = models.DateTimeField(default=timezone.now)
    normalized_at = models.DateTimeField(default=timezone.now)
    payload_normalized_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_sales_normalized"
        ordering = ["fecha", "sucursal_nombre", "categoria", "producto_nombre_historico", "id"]
        indexes = [
            models.Index(fields=["fecha", "sucursal_nombre"], name="pbs_norm_day_branch_idx"),
            models.Index(fields=["match_catalogo_status", "fecha"], name="pbs_norm_match_day_idx"),
            models.Index(fields=["receta", "fecha"], name="pbs_norm_recipe_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.fecha} · {self.sucursal_nombre} · {self.categoria} · {self.producto_nombre_historico}"


class PointSalesDailyCategoryFact(models.Model):
    GRANULARITY_CATEGORY = "CATEGORY"
    GRANULARITY_CHOICES = [
        (GRANULARITY_CATEGORY, "Category"),
    ]

    branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_daily_category_facts",
    )
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_daily_category_facts",
    )
    sale_date = models.DateField(db_index=True)
    sucursal_nombre = models.CharField(max_length=200, db_index=True)
    categoria = models.CharField(max_length=200, db_index=True)
    source_granularity = models.CharField(max_length=20, choices=GRANULARITY_CHOICES, default=GRANULARITY_CATEGORY)
    total_cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    total_descuento = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_venta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_impuestos = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_venta_neta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    detail_row_count = models.PositiveIntegerField(default=0)
    last_source_hash = models.CharField(max_length=64, blank=True, default="")
    first_extracted_at = models.DateTimeField(null=True, blank=True)
    last_extracted_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_sales_daily_category_fact"
        ordering = ["sale_date", "sucursal_nombre", "categoria", "id"]
        unique_together = [("branch", "sale_date", "categoria", "source_granularity")]
        indexes = [
            models.Index(fields=["sale_date", "sucursal_nombre"], name="pbs_fact_cat_day_branch_idx"),
            models.Index(fields=["sale_date", "categoria"], name="pbs_fact_cat_day_cat_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sale_date} · {self.sucursal_nombre} · {self.categoria}"


class PointSalesDailyProductFact(models.Model):
    GRANULARITY_PRODUCT = "PRODUCT"
    GRANULARITY_CHOICES = [
        (GRANULARITY_PRODUCT, "Product"),
    ]

    branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_daily_product_facts",
    )
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_daily_product_facts",
    )
    sale_date = models.DateField(db_index=True)
    sucursal_nombre = models.CharField(max_length=200, db_index=True)
    categoria = models.CharField(max_length=200, db_index=True)
    producto_nombre_historico = models.CharField(max_length=255, db_index=True)
    point_product = models.ForeignKey(
        "pos_bridge.PointProduct",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_daily_product_facts",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_sales_daily_product_facts",
    )
    match_catalogo_status = models.CharField(
        max_length=30,
        choices=PointSalesNormalized.MATCH_CHOICES,
        default=PointSalesNormalized.MATCH_SIN_CATALOGO,
    )
    source_granularity = models.CharField(max_length=20, choices=GRANULARITY_CHOICES, default=GRANULARITY_PRODUCT)
    total_cantidad = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    total_descuento = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_venta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_impuestos = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_venta_neta = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    source_hash = models.CharField(max_length=64, blank=True, default="", db_index=True)
    source_file = models.CharField(max_length=300, blank=True, default="")
    extracted_at = models.DateTimeField(null=True, blank=True)
    normalized_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_sales_daily_product_fact"
        ordering = ["sale_date", "sucursal_nombre", "categoria", "producto_nombre_historico", "id"]
        unique_together = [("branch", "sale_date", "categoria", "producto_nombre_historico", "source_granularity")]
        indexes = [
            models.Index(fields=["sale_date", "sucursal_nombre"], name="pbs_fact_prod_day_branch_idx"),
            models.Index(fields=["receta", "sale_date"], name="pbs_fact_prod_recipe_day_idx"),
            models.Index(fields=["match_catalogo_status", "sale_date"], name="pbs_fact_prod_match_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sale_date} · {self.sucursal_nombre} · {self.producto_nombre_historico}"


class PointSalesQualityAlert(models.Model):
    SEVERITY_INFO = "INFO"
    SEVERITY_WARNING = "WARNING"
    SEVERITY_CRITICAL = "CRITICAL"
    SEVERITY_CHOICES = [
        (SEVERITY_INFO, "Info"),
        (SEVERITY_WARNING, "Warning"),
        (SEVERITY_CRITICAL, "Critical"),
    ]

    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_quality_alerts",
    )
    task = models.ForeignKey(
        "pos_bridge.PointSalesExtractionTask",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="quality_alerts",
    )
    branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="sales_quality_alerts",
    )
    alert_type = models.CharField(max_length=80, db_index=True)
    severity = models.CharField(max_length=20, choices=SEVERITY_CHOICES, default=SEVERITY_WARNING, db_index=True)
    sucursal = models.CharField(max_length=200, blank=True, default="")
    fecha = models.DateField(null=True, blank=True, db_index=True)
    detalle = models.TextField()
    payload_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_sales_quality_alerts"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["alert_type", "severity"], name="pbs_alert_type_sev_idx"),
            models.Index(fields=["fecha", "sucursal"], name="pbs_alert_day_branch_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.severity} · {self.alert_type} · {self.sucursal or 'GLOBAL'}"
