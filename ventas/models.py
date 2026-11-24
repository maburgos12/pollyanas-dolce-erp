from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone

from core.models import Sucursal
from recetas.models import Receta


class VentaAutoritativaPoint(models.Model):
    branch = models.ForeignKey(
        Sucursal,
        on_delete=models.PROTECT,
        related_name="ventas_autoritativas_point",
    )
    product = models.ForeignKey(
        Receta,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_autoritativas_point",
    )
    sale_date = models.DateField(db_index=True)
    product_code = models.CharField(max_length=80, blank=True, default="", db_index=True)
    point_name = models.CharField(max_length=250, blank=True, default="")
    category = models.CharField(max_length=120, blank=True, default="")
    quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    gross_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    tax_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    net_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    source_file = models.CharField(max_length=300, blank=True, default="")
    source_sheet = models.CharField(max_length=120, blank=True, default="")
    raw_payload = models.JSONField(default=dict, blank=True)
    imported_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "ventas_autoritativas_point"
        ordering = ["-sale_date", "branch__codigo", "point_name"]
        unique_together = [("branch", "sale_date", "product_code")]
        indexes = [
            models.Index(fields=["sale_date", "branch"], name="vap_day_branch_idx"),
            models.Index(fields=["product", "sale_date"], name="vap_product_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sale_date} · {self.branch.codigo} · {self.point_name or self.product_code}"


class PronosticoGuardado(models.Model):
    nombre = models.CharField(max_length=200)
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    sucursales = models.ManyToManyField("core.Sucursal", blank=True)
    resultado_json = models.JSONField(default=dict)
    total_piezas = models.IntegerField(default=0)
    total_ingreso = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]

    def __str__(self) -> str:
        return self.nombre


class ContextoComercialQuerySet(models.QuerySet):
    def _immutable(self, *args, **kwargs):
        from django.core.exceptions import ValidationError
        raise ValidationError('El contexto comercial es inmutable; registre una nueva referencia documental.')

    update = delete = bulk_update = bulk_create = _immutable


class ContextoComercial(models.Model):
    """Declaración comercial informativa: nunca modifica ventas ni asistencia."""
    TIPOS = [('afectacion_parcial', 'Afectación comercial parcial'), ('cierre_empresa', 'Cierre comercial de empresa')]
    clave = models.SlugField(max_length=100, unique=True)
    fecha = models.DateField(db_index=True)
    tipo = models.CharField(max_length=30, choices=TIPOS)
    titulo = models.CharField(max_length=200)
    descripcion = models.TextField()
    evidencia = models.JSONField()
    autor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(auto_now_add=True)
    objects = ContextoComercialQuerySet.as_manager()

    class Meta:
        ordering = ['fecha', 'clave']

    def clean(self):
        from django.core.exceptions import ValidationError, PermissionDenied
        from rrhh.services_permisos import can_authorize_direccion
        if not isinstance(self.evidencia, list) or not self.evidencia or any(not isinstance(item, str) or not item.strip() for item in self.evidencia):
            raise ValidationError({'evidencia': 'Indique al menos una referencia documental no vacía.'})
        if not self.autor_id or not self.autor.is_active or not can_authorize_direccion(self.autor):
            raise PermissionDenied('Se requiere Dirección General activa.')

    def save(self, *args, **kwargs):
        from django.core.exceptions import ValidationError
        if not self._state.adding:
            raise ValidationError('El contexto comercial es inmutable.')
        self.full_clean()
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return ContextoComercialQuerySet._immutable(self)
