from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass

from django.conf import settings
from django.db import models
from django.utils import timezone


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


@dataclass
class GeneratedApiKey:
    key: str
    prefix: str


class PublicApiClient(models.Model):
    nombre = models.CharField(max_length=120)
    clave_prefijo = models.CharField(max_length=12, unique=True, db_index=True)
    clave_hash = models.CharField(max_length=64)
    descripcion = models.CharField(max_length=255, blank=True, default="")
    activo = models.BooleanField(default=True)
    last_used_at = models.DateTimeField(null=True, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="integraciones_public_api_clients",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre", "id"]
        verbose_name = "Cliente API pública"
        verbose_name_plural = "Clientes API pública"

    def __str__(self) -> str:
        return f"{self.nombre} ({self.clave_prefijo})"

    @classmethod
    def generate_key(cls) -> GeneratedApiKey:
        raw = f"pk_{secrets.token_urlsafe(30)}"
        return GeneratedApiKey(key=raw, prefix=raw[:12])

    @classmethod
    def create_with_generated_key(cls, *, nombre: str, descripcion: str = "", created_by=None):
        generated = cls.generate_key()
        obj = cls.objects.create(
            nombre=nombre,
            descripcion=descripcion,
            clave_prefijo=generated.prefix,
            clave_hash=_hash_key(generated.key),
            created_by=created_by,
        )
        return obj, generated.key

    def validate(self, raw_key: str) -> bool:
        return self.clave_hash == _hash_key(raw_key)

    def mark_used(self):
        self.last_used_at = timezone.now()
        self.save(update_fields=["last_used_at", "updated_at"])


class PublicApiAccessLog(models.Model):
    client = models.ForeignKey(PublicApiClient, on_delete=models.CASCADE, related_name="access_logs")
    endpoint = models.CharField(max_length=220)
    method = models.CharField(max_length=8)
    status_code = models.PositiveIntegerField(default=200)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        verbose_name = "Log API pública"
        verbose_name_plural = "Logs API pública"

    def __str__(self) -> str:
        return f"{self.client.nombre} {self.method} {self.endpoint} {self.status_code}"
