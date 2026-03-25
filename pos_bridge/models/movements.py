from __future__ import annotations

from django.db import models


class PointWasteLine(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="waste_lines")
    erp_branch = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_waste_lines",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_waste_lines",
    )
    insumo = models.ForeignKey(
        "maestros.Insumo",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_waste_lines",
    )
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="waste_lines",
    )
    movement_external_id = models.CharField(max_length=40, db_index=True)
    source_hash = models.CharField(max_length=64, unique=True, db_index=True)
    movement_at = models.DateTimeField(db_index=True)
    responsible = models.CharField(max_length=160, blank=True, default="")
    item_name = models.CharField(max_length=250)
    item_code = models.CharField(max_length=80, blank=True, default="")
    quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    unit = models.CharField(max_length=40, blank=True, default="")
    unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    total_cost = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    justification = models.TextField(blank=True, default="")
    source_endpoint = models.CharField(max_length=160, blank=True, default="/Mermas/get_mermas")
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_waste_lines"
        ordering = ["-movement_at", "branch__name", "item_name", "id"]
        verbose_name = "Point waste line"
        verbose_name_plural = "Point waste lines"
        indexes = [
            models.Index(fields=["movement_at", "branch"], name="pbw_date_branch_idx"),
            models.Index(fields=["movement_external_id"], name="pbw_move_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.movement_at:%Y-%m-%d} · {self.branch} · {self.item_name} · {self.quantity}"


class PointProductionLine(models.Model):
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT, related_name="production_lines")
    erp_branch = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_production_lines",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_production_lines",
    )
    insumo = models.ForeignKey(
        "maestros.Insumo",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_production_lines",
    )
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="production_lines",
    )
    production_external_id = models.CharField(max_length=40, db_index=True)
    detail_external_id = models.CharField(max_length=40, db_index=True)
    source_hash = models.CharField(max_length=64, unique=True, db_index=True)
    production_date = models.DateField(db_index=True)
    responsible = models.CharField(max_length=160, blank=True, default="")
    item_name = models.CharField(max_length=250)
    item_code = models.CharField(max_length=80, blank=True, default="")
    unit = models.CharField(max_length=40, blank=True, default="")
    unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    requested_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    produced_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    is_insumo = models.BooleanField(default=False)
    source_endpoint = models.CharField(max_length=160, blank=True, default="/Produccion/getProduccionGeneral")
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_production_lines"
        ordering = ["-production_date", "branch__name", "item_name", "id"]
        verbose_name = "Point production line"
        verbose_name_plural = "Point production lines"
        indexes = [
            models.Index(fields=["production_date", "branch"], name="pbp_date_branch_idx"),
            models.Index(fields=["production_external_id"], name="pbp_prod_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.production_date} · {self.branch} · {self.item_name} · {self.produced_quantity}"


class PointTransferLine(models.Model):
    origin_branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        on_delete=models.PROTECT,
        related_name="outgoing_transfer_lines",
    )
    destination_branch = models.ForeignKey(
        "pos_bridge.PointBranch",
        on_delete=models.PROTECT,
        related_name="incoming_transfer_lines",
    )
    erp_origin_branch = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_transfer_origin_lines",
    )
    erp_destination_branch = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_transfer_destination_lines",
    )
    receta = models.ForeignKey(
        "recetas.Receta",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_transfer_lines",
    )
    insumo = models.ForeignKey(
        "maestros.Insumo",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_transfer_lines",
    )
    sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="transfer_lines",
    )
    transfer_external_id = models.CharField(max_length=40, db_index=True)
    detail_external_id = models.CharField(max_length=40, db_index=True)
    source_hash = models.CharField(max_length=64, unique=True, db_index=True)
    registered_at = models.DateTimeField(db_index=True)
    sent_at = models.DateTimeField(null=True, blank=True, db_index=True)
    received_at = models.DateTimeField(null=True, blank=True, db_index=True)
    requested_by = models.CharField(max_length=160, blank=True, default="")
    sent_by = models.CharField(max_length=160, blank=True, default="")
    received_by = models.CharField(max_length=160, blank=True, default="")
    item_name = models.CharField(max_length=250)
    item_code = models.CharField(max_length=80, blank=True, default="")
    unit = models.CharField(max_length=40, blank=True, default="")
    unit_cost = models.DecimalField(max_digits=18, decimal_places=6, default=0)
    requested_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    sent_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    received_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    is_insumo = models.BooleanField(default=False)
    is_received = models.BooleanField(default=False)
    is_cancelled = models.BooleanField(default=False)
    is_finalized = models.BooleanField(default=False)
    source_endpoint = models.CharField(max_length=160, blank=True, default="/Transfer/GetTransfer")
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_transfer_lines"
        ordering = ["-received_at", "-registered_at", "destination_branch__name", "item_name", "id"]
        verbose_name = "Point transfer line"
        verbose_name_plural = "Point transfer lines"
        indexes = [
            models.Index(fields=["received_at", "destination_branch"], name="pbt_recv_dest_idx"),
            models.Index(fields=["transfer_external_id"], name="pbt_transfer_idx"),
        ]

    def __str__(self) -> str:
        return (
            f"{self.transfer_external_id} · {self.origin_branch.name} -> {self.destination_branch.name} · "
            f"{self.item_name} · {self.received_quantity}"
        )
