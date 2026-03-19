from __future__ import annotations

from django.db import models
from django.utils import timezone


class PointInventorySnapshot(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="snapshots")
    product = models.ForeignKey("pos_bridge.PointProduct", on_delete=models.PROTECT, related_name="snapshots")
    stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    min_stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    max_stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    captured_at = models.DateTimeField(default=timezone.now, db_index=True)
    sync_job = models.ForeignKey("pos_bridge.PointSyncJob", on_delete=models.PROTECT, related_name="snapshots")
    raw_payload = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "pos_bridge_inventory_snapshots"
        ordering = ["-captured_at", "-id"]
        verbose_name = "Point inventory snapshot"
        verbose_name_plural = "Point inventory snapshots"
        indexes = [
            models.Index(fields=["branch", "captured_at"]),
            models.Index(fields=["product", "captured_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.branch} / {self.product} / {self.captured_at:%Y-%m-%d %H:%M}"
