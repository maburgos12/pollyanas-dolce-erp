from __future__ import annotations

from django.db import models
from django.utils import timezone


class PointInventoryPayload(models.Model):
    digest = models.CharField(max_length=64, unique=True)
    payload = models.JSONField()

    class Meta:
        db_table = "pos_bridge_inventory_payloads"


class PointInventoryDailyState(models.Model):
    business_day = models.DateField()
    branch_id = models.BigIntegerField()
    product_id = models.BigIntegerField()
    stock = models.DecimalField(max_digits=18, decimal_places=3)
    min_stock = models.DecimalField(max_digits=18, decimal_places=3)
    max_stock = models.DecimalField(max_digits=18, decimal_places=3)

    class Meta:
        db_table = "pos_bridge_inventory_daily_states"
        constraints = [
            models.UniqueConstraint(
                fields=["business_day", "branch_id", "product_id", "stock", "min_stock", "max_stock"],
                name="pb_inventory_daily_state_key",
            ),
        ]


class PointInventoryCutStorage(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="+")
    product = models.ForeignKey("pos_bridge.PointProduct", on_delete=models.PROTECT, related_name="+")
    sync_job = models.ForeignKey("pos_bridge.PointSyncJob", on_delete=models.PROTECT, related_name="+")
    captured_at = models.DateTimeField(db_index=True)
    stock = models.DecimalField(max_digits=18, decimal_places=3, null=True)
    min_stock = models.DecimalField(max_digits=18, decimal_places=3, null=True)
    max_stock = models.DecimalField(max_digits=18, decimal_places=3, null=True)
    raw_payload = models.JSONField(default=dict, blank=True)
    shared_payload = models.ForeignKey(
        PointInventoryPayload, null=True, editable=False, db_index=False,
        on_delete=models.PROTECT, related_name="+",
    )
    state = models.ForeignKey(PointInventoryDailyState, null=True, db_index=False, on_delete=models.PROTECT)

    class Meta:
        db_table = "pos_bridge_inventory_cuts"
        indexes = [
            models.Index(fields=["branch", "captured_at"], name="pos_bridge__branch__6901f5_idx"),
            models.Index(fields=["product", "captured_at"], name="pos_bridge__product_51036d_idx"),
            models.Index(fields=["branch", "product", "-captured_at", "-id"], name="pb_inv_latest_idx"),
        ]


class PointInventorySnapshot(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="snapshots")
    product = models.ForeignKey("pos_bridge.PointProduct", on_delete=models.PROTECT, related_name="snapshots")
    stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    min_stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    max_stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    captured_at = models.DateTimeField(default=timezone.now, db_index=True)
    sync_job = models.ForeignKey("pos_bridge.PointSyncJob", on_delete=models.PROTECT, related_name="snapshots")
    raw_payload = models.JSONField(default=dict, blank=True)
    shared_payload = models.ForeignKey(
        PointInventoryPayload, null=True, editable=False, db_index=False,
        on_delete=models.PROTECT, related_name="+",
    )

    @property
    def effective_raw_payload(self):
        return self.shared_payload.payload if self.shared_payload_id is not None else self.raw_payload

    class Meta:
        managed = False
        db_table = "pos_bridge_inventory_snapshots"
        ordering = ["-captured_at", "-id"]
        verbose_name = "Point inventory snapshot"
        verbose_name_plural = "Point inventory snapshots"

    def __str__(self) -> str:
        return f"{self.branch} / {self.product} / {self.captured_at:%Y-%m-%d %H:%M}"
