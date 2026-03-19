from __future__ import annotations

from django.db import models


class PointDailySale(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="daily_sales")
    product = models.ForeignKey("pos_bridge.PointProduct", on_delete=models.PROTECT, related_name="daily_sales")
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_daily_sales",
    )
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="daily_sales",
    )
    sale_date = models.DateField(db_index=True)
    quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    tickets = models.PositiveIntegerField(default=0)
    gross_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    tax_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    net_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    source_endpoint = models.CharField(max_length=160, blank=True, default="/Report/VentasCategorias")
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_daily_sales"
        ordering = ["-sale_date", "branch__name", "product__name", "id"]
        verbose_name = "Point daily sale"
        verbose_name_plural = "Point daily sales"
        unique_together = [("sale_date", "branch", "product")]
        indexes = [
            models.Index(fields=["sale_date", "branch"], name="pbs_day_branch_idx"),
            models.Index(fields=["sale_date", "product"], name="pbs_day_product_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sale_date} · {self.branch} · {self.product} · {self.quantity}"
