from __future__ import annotations

from django.db import models


class PointHistoricalInventoryClosing(models.Model):
    """Point stock proven for an exact operational closing date.

    This is deliberately separate from live inventory snapshots and from any
    future physical-count model.  Only VERIFIED batches are eligible for
    monthly reporting.
    """

    STATUS_DRAFT = "DRAFT"
    STATUS_VERIFIED = "VERIFIED"
    STATUS_REJECTED = "REJECTED"
    STATUS_CHOICES = [
        (STATUS_DRAFT, "Borrador"),
        (STATUS_VERIFIED, "Verificado"),
        (STATUS_REJECTED, "Rechazado"),
    ]

    SOURCE_STOCK_HISTORY = "POINT_STOCK_HISTORY"
    SOURCE_OFFICIAL_REPORT = "POINT_OFFICIAL_CLOSING_REPORT"
    SOURCE_CHOICES = [
        (SOURCE_STOCK_HISTORY, "Historial de existencias Point"),
        (SOURCE_OFFICIAL_REPORT, "Reporte oficial de cierre Point"),
    ]

    operational_date = models.DateField(db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_DRAFT, db_index=True)
    source = models.CharField(max_length=40, choices=SOURCE_CHOICES)
    source_fingerprint = models.CharField(max_length=64)
    expected_branch_ids = models.JSONField(default=list)
    expected_product_ids = models.JSONField(default=list)
    metadata = models.JSONField(default=dict, blank=True)
    retrieved_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_historical_inventory_closings"
        ordering = ["-operational_date", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["operational_date", "source_fingerprint"],
                name="pb_hist_close_date_fingerprint_uq",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.operational_date} · {self.get_status_display()}"


class PointHistoricalInventoryClosingLine(models.Model):
    closing = models.ForeignKey(
        PointHistoricalInventoryClosing,
        on_delete=models.CASCADE,
        related_name="lines",
    )
    branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        on_delete=models.PROTECT,
        related_name="historical_inventory_closing_lines",
    )
    product = models.ForeignKey(
        "pos_bridge.PointProduct",
        on_delete=models.PROTECT,
        related_name="historical_inventory_closing_lines",
    )
    stock = models.DecimalField(max_digits=18, decimal_places=3)
    evidence = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_historical_inventory_closing_lines"
        ordering = ["closing_id", "branch_id", "product_id", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["closing", "branch", "product"],
                name="pb_hist_close_branch_product_uq",
            ),
        ]
        indexes = [
            models.Index(fields=["closing", "product"], name="pb_hist_close_product_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.closing.operational_date} · {self.branch} · {self.product}"
