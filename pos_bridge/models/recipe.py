from __future__ import annotations

from django.db import models
from unidecode import unidecode

from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta

from .sync_job import PointSyncJob


def _normalize_name(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


class PointRecipeExtractionRun(models.Model):
    sync_job = models.ForeignKey(
        PointSyncJob,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="recipe_runs",
    )
    workspace = models.CharField(max_length=160, blank=True, default="")
    branch_hint = models.CharField(max_length=160, blank=True, default="")
    root_codes = models.JSONField(default=list, blank=True)
    max_depth = models.PositiveIntegerField(default=3)
    summary = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_recipe_runs"
        ordering = ["-created_at", "-id"]
        verbose_name = "Point recipe extraction run"
        verbose_name_plural = "Point recipe extraction runs"

    def __str__(self) -> str:
        return f"Recipe run #{self.id}"


class PointRecipeNode(models.Model):
    SOURCE_PRODUCT = "PRODUCT"
    SOURCE_INSUMO = "INSUMO"
    SOURCE_CHOICES = [
        (SOURCE_PRODUCT, "Producto final"),
        (SOURCE_INSUMO, "Insumo/preparación"),
    ]

    KIND_FINAL_PRODUCT = "FINAL_PRODUCT"
    KIND_PREPARED_INPUT = "PREPARED_INPUT"
    KIND_CHOICES = [
        (KIND_FINAL_PRODUCT, "Producto final"),
        (KIND_PREPARED_INPUT, "Insumo preparado"),
    ]

    YIELD_WEIGHT = "YIELD_WEIGHT"
    YIELD_VOLUME = "YIELD_VOLUME"
    YIELD_UNIT = "YIELD_UNIT"
    YIELD_UNKNOWN = "YIELD_UNKNOWN"
    YIELD_MODE_CHOICES = [
        (YIELD_WEIGHT, "Por peso"),
        (YIELD_VOLUME, "Por volumen"),
        (YIELD_UNIT, "Por unidad"),
        (YIELD_UNKNOWN, "Sin rendimiento explícito"),
    ]

    run = models.ForeignKey(PointRecipeExtractionRun, on_delete=models.CASCADE, related_name="nodes")
    identity_key = models.CharField(max_length=220)
    source_type = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_PRODUCT)
    node_kind = models.CharField(max_length=30, choices=KIND_CHOICES, default=KIND_FINAL_PRODUCT)
    point_pk = models.CharField(max_length=80, blank=True, default="")
    point_code = models.CharField(max_length=80, blank=True, default="", db_index=True)
    point_name = models.CharField(max_length=255)
    normalized_name = models.CharField(max_length=270, db_index=True, blank=True, default="")
    family = models.CharField(max_length=120, blank=True, default="")
    category = models.CharField(max_length=120, blank=True, default="")
    has_recipe_flag = models.BooleanField(default=False)
    depth = models.PositiveIntegerField(default=0)
    yield_mode = models.CharField(max_length=20, choices=YIELD_MODE_CHOICES, default=YIELD_UNKNOWN)
    yield_quantity = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    yield_unit_text = models.CharField(max_length=40, blank=True, default="")
    yield_unit = models.ForeignKey(
        UnidadMedida,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_recipe_nodes",
    )
    erp_recipe = models.ForeignKey(
        Receta,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_recipe_nodes",
    )
    erp_insumo = models.ForeignKey(
        Insumo,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_recipe_nodes",
    )
    raw_detail = models.JSONField(default=dict, blank=True)
    raw_bom = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_recipe_nodes"
        ordering = ["depth", "point_name", "id"]
        unique_together = [("run", "identity_key")]
        verbose_name = "Point recipe node"
        verbose_name_plural = "Point recipe nodes"

    def save(self, *args, **kwargs):
        self.normalized_name = _normalize_name(self.point_name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.identity_key} - {self.point_name}"


class PointRecipeNodeLine(models.Model):
    COMPONENT_PREPARED_INPUT = "PREPARED_INPUT"
    COMPONENT_DIRECT_INPUT = "DIRECT_INPUT"
    COMPONENT_PACKAGING = "PACKAGING"
    COMPONENT_UNRESOLVED = "UNRESOLVED"
    COMPONENT_CHOICES = [
        (COMPONENT_PREPARED_INPUT, "Insumo preparado"),
        (COMPONENT_DIRECT_INPUT, "Insumo directo"),
        (COMPONENT_PACKAGING, "Empaque"),
        (COMPONENT_UNRESOLVED, "Pendiente"),
    ]

    node = models.ForeignKey(PointRecipeNode, on_delete=models.CASCADE, related_name="lines")
    child_node = models.ForeignKey(
        PointRecipeNode,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="incoming_lines",
    )
    position = models.PositiveIntegerField(default=0)
    point_code = models.CharField(max_length=80, blank=True, default="", db_index=True)
    point_name = models.CharField(max_length=255)
    normalized_name = models.CharField(max_length=270, db_index=True, blank=True, default="")
    quantity = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    unit_text = models.CharField(max_length=40, blank=True, default="")
    unit = models.ForeignKey(
        UnidadMedida,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_recipe_node_lines",
    )
    classification = models.CharField(max_length=20, choices=COMPONENT_CHOICES, default=COMPONENT_UNRESOLVED)
    erp_insumo = models.ForeignKey(
        Insumo,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_recipe_node_lines",
    )
    erp_recipe = models.ForeignKey(
        Receta,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_recipe_node_lines",
    )
    match_method = models.CharField(max_length=32, blank=True, default="")
    match_score = models.FloatField(default=0.0)
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_recipe_node_lines"
        ordering = ["node", "position", "id"]
        verbose_name = "Point recipe node line"
        verbose_name_plural = "Point recipe node lines"

    def save(self, *args, **kwargs):
        self.normalized_name = _normalize_name(self.point_name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.node_id}:{self.position} {self.point_name}"
