from django.db import models
from django.utils import timezone


class PointInsumoInventorySnapshot(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="insumo_snapshots")
    insumo = models.ForeignKey("maestros.Insumo", on_delete=models.PROTECT, related_name="point_inventory_snapshots")
    point_code = models.CharField(max_length=80, db_index=True)
    point_name = models.CharField(max_length=250, blank=True, default="")
    point_quantity = models.DecimalField(max_digits=18, decimal_places=6)
    point_unit = models.CharField(max_length=20)
    quantity_base = models.DecimalField(max_digits=18, decimal_places=6)
    captured_at = models.DateTimeField(default=timezone.now, db_index=True)
    sync_job = models.ForeignKey("pos_bridge.PointSyncJob", on_delete=models.PROTECT, related_name="insumo_snapshots")
    raw_payload = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "pos_bridge_insumo_inventory_snapshots"
        ordering = ["-captured_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["sync_job", "branch", "insumo"],
                name="uniq_point_insumo_snapshot_cycle",
            )
        ]
        indexes = [
            models.Index(
                fields=["branch", "insumo", "-captured_at", "-id"],
                name="pb_insumo_latest_idx",
            )
        ]
