from __future__ import annotations

from django.db import models
from unidecode import unidecode


def _normalize_name(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


class PointProduct(models.Model):
    external_id = models.CharField(max_length=120, unique=True, db_index=True)
    sku = models.CharField(max_length=120, blank=True, default="", db_index=True)
    name = models.CharField(max_length=255)
    normalized_name = models.CharField(max_length=270, db_index=True, blank=True, default="")
    category = models.CharField(max_length=120, blank=True, default="")
    active = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_products"
        ordering = ["name", "id"]
        verbose_name = "Point product"
        verbose_name_plural = "Point products"

    def save(self, *args, **kwargs):
        self.normalized_name = _normalize_name(self.name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.external_id} - {self.name}"
