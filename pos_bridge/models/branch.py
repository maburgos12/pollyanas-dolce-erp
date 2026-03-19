from __future__ import annotations

from django.db import models
from unidecode import unidecode


def _normalize_name(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


class PointBranch(models.Model):
    STATUS_ACTIVE = "ACTIVE"
    STATUS_INACTIVE = "INACTIVE"
    STATUS_UNKNOWN = "UNKNOWN"
    STATUS_CHOICES = [
        (STATUS_ACTIVE, "Active"),
        (STATUS_INACTIVE, "Inactive"),
        (STATUS_UNKNOWN, "Unknown"),
    ]

    external_id = models.CharField(max_length=80, unique=True, db_index=True)
    name = models.CharField(max_length=200)
    normalized_name = models.CharField(max_length=220, db_index=True, blank=True, default="")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_ACTIVE)
    erp_branch = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_bridge_branches",
    )
    metadata = models.JSONField(default=dict, blank=True)
    last_seen_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_branches"
        ordering = ["name", "id"]
        verbose_name = "Point branch"
        verbose_name_plural = "Point branches"

    def save(self, *args, **kwargs):
        self.normalized_name = _normalize_name(self.name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.external_id} - {self.name}"
