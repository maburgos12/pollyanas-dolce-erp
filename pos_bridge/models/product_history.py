from __future__ import annotations

from django.db import models


class PointProductHistoryImport(models.Model):
    file_hash = models.CharField(max_length=64, unique=True, db_index=True)
    source_filename = models.CharField(max_length=255)
    report_path = models.CharField(max_length=500)
    report_title = models.CharField(max_length=300, blank=True, default="")
    product_name = models.CharField(max_length=255, db_index=True)
    branch_name = models.CharField(max_length=200, blank=True, default="")
    report_date = models.DateField(null=True, blank=True, db_index=True)
    point_branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="product_history_imports",
    )
    point_product = models.ForeignKey(
        "pos_bridge.PointProduct",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="history_imports",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_history_imports",
    )
    row_count = models.PositiveIntegerField(default=0)
    latest_movement_at = models.DateTimeField(null=True, blank=True, db_index=True)
    latest_unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    raw_metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_product_history_imports"
        ordering = ["-created_at", "-id"]
        verbose_name = "Point product history import"
        verbose_name_plural = "Point product history imports"

    def __str__(self) -> str:
        return f"{self.product_name} · {self.report_date or 'sin fecha'}"


class PointProductHistoryRow(models.Model):
    import_record = models.ForeignKey(
        PointProductHistoryImport,
        on_delete=models.CASCADE,
        related_name="rows",
    )
    row_number = models.PositiveIntegerField()
    movement_at = models.DateTimeField(db_index=True)
    movement_type = models.CharField(max_length=160, blank=True, default="")
    previous_existence = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    new_existence = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    total_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    cancelled = models.BooleanField(default=False)
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_product_history_rows"
        ordering = ["-movement_at", "-row_number", "-id"]
        verbose_name = "Point product history row"
        verbose_name_plural = "Point product history rows"
        unique_together = [("import_record", "row_number")]
        indexes = [
            models.Index(fields=["import_record", "movement_at"], name="pbphr_import_move_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.import_record_id} · {self.movement_at} · {self.unit_cost}"


class PointProductCostReconciliation(models.Model):
    STATUS_MATCH = "MATCH"
    STATUS_DELTA = "DELTA"
    STATUS_MISSING_RECIPE = "MISSING_RECIPE"
    STATUS_MISSING_POINT_COST = "MISSING_POINT_COST"
    STATUS_MISSING_ERP_COST = "MISSING_ERP_COST"
    STATUS_CHOICES = [
        (STATUS_MATCH, "Coincide"),
        (STATUS_DELTA, "Con diferencia"),
        (STATUS_MISSING_RECIPE, "Sin receta ERP"),
        (STATUS_MISSING_POINT_COST, "Sin costo Point"),
        (STATUS_MISSING_ERP_COST, "Sin costo ERP"),
    ]

    import_record = models.OneToOneField(
        PointProductHistoryImport,
        on_delete=models.CASCADE,
        related_name="reconciliation",
    )
    point_branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="product_cost_reconciliations",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_cost_reconciliations",
    )
    point_unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    erp_unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    variance_amount = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    variance_pct = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default=STATUS_DELTA, db_index=True)
    notes = models.TextField(blank=True, default="")
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_product_cost_reconciliations"
        ordering = ["-created_at", "-id"]
        verbose_name = "Point product cost reconciliation"
        verbose_name_plural = "Point product cost reconciliations"

    def __str__(self) -> str:
        return f"{self.import_record.product_name} · {self.status}"
