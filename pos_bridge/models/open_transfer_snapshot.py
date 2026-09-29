from __future__ import annotations

from django.core.exceptions import ValidationError
from django.db import models


class _ImmutableSnapshotModel(models.Model):
    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if self.pk and type(self).objects.filter(pk=self.pk).exists():
            raise ValidationError("La evidencia histórica de cierre es inmutable.")
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise ValidationError("La evidencia histórica de cierre es inmutable.")


class PointOpenTransferSnapshot(_ImmutableSnapshotModel):
    sync_job = models.OneToOneField(
        "pos_bridge.PointSyncJob",
        on_delete=models.PROTECT,
        related_name="open_transfer_snapshot",
    )
    operational_date = models.DateField(db_index=True)
    captured_at = models.DateTimeField()
    row_count = models.PositiveIntegerField()
    sha256 = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_open_transfer_snapshots"
        ordering = ["-operational_date", "-captured_at", "-id"]


class PointOpenTransferSnapshotMember(_ImmutableSnapshotModel):
    snapshot = models.ForeignKey(
        PointOpenTransferSnapshot,
        on_delete=models.PROTECT,
        related_name="members",
    )
    source_line_id = models.PositiveBigIntegerField(null=True, blank=True)
    source_hash = models.CharField(max_length=64)
    payload_sha256 = models.CharField(max_length=64)
    transfer_external_id = models.CharField(max_length=40)
    detail_external_id = models.CharField(max_length=40)
    origin_branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        on_delete=models.PROTECT,
        related_name="open_transfer_snapshot_origin_members",
    )
    destination_branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        on_delete=models.PROTECT,
        related_name="open_transfer_snapshot_destination_members",
    )
    origin_branch_external_id = models.CharField(max_length=80)
    origin_branch_name = models.CharField(max_length=200)
    destination_branch_external_id = models.CharField(max_length=80)
    destination_branch_name = models.CharField(max_length=200)
    registered_at = models.DateTimeField()
    sent_at = models.DateTimeField(null=True, blank=True)
    received_at = models.DateTimeField(null=True, blank=True)
    requested_by = models.CharField(max_length=160, blank=True, default="")
    sent_by = models.CharField(max_length=160, blank=True, default="")
    received_by = models.CharField(max_length=160, blank=True, default="")
    item_name = models.CharField(max_length=250)
    item_code = models.CharField(max_length=80, blank=True, default="")
    unit = models.CharField(max_length=40, blank=True, default="")
    requested_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    sent_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    received_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    is_insumo = models.BooleanField(default=False)
    is_received = models.BooleanField(default=False)
    is_cancelled = models.BooleanField(default=False)
    is_finalized = models.BooleanField(default=False)
    is_open = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_open_transfer_snapshot_members"
        ordering = ["snapshot_id", "transfer_external_id", "detail_external_id", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["snapshot", "source_hash"],
                name="pb_open_snapshot_source_uq",
            )
        ]
        indexes = [
            models.Index(
                fields=["snapshot", "transfer_external_id", "detail_external_id"],
                name="pb_open_snapshot_ref_idx",
            )
        ]
