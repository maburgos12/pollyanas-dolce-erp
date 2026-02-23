from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models
from django.db.models import Count, Q, Sum
from django.utils import timezone

from crm.models import PedidoCliente


class RutaEntrega(models.Model):
    ESTATUS_PLANEADA = "PLANEADA"
    ESTATUS_EN_RUTA = "EN_RUTA"
    ESTATUS_COMPLETADA = "COMPLETADA"
    ESTATUS_CANCELADA = "CANCELADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PLANEADA, "Planeada"),
        (ESTATUS_EN_RUTA, "En ruta"),
        (ESTATUS_COMPLETADA, "Completada"),
        (ESTATUS_CANCELADA, "Cancelada"),
    ]

    folio = models.CharField(max_length=40, unique=True, blank=True)
    nombre = models.CharField(max_length=160)
    fecha_ruta = models.DateField(default=timezone.localdate)
    chofer = models.CharField(max_length=120, blank=True, default="")
    unidad = models.CharField(max_length=120, blank=True, default="")
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PLANEADA)
    km_estimado = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal("0"))
    notas = models.TextField(blank=True, default="")

    total_entregas = models.PositiveIntegerField(default=0)
    entregas_completadas = models.PositiveIntegerField(default=0)
    entregas_incidencia = models.PositiveIntegerField(default=0)
    monto_estimado_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="logistica_rutas_creadas",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_ruta", "-id"]

    def __str__(self) -> str:
        return self.folio or self.nombre

    def _generate_folio(self) -> str:
        now = timezone.localtime()
        prefix = now.strftime("RUT-%Y%m")
        last = (
            RutaEntrega.objects.filter(folio__startswith=prefix)
            .order_by("-folio")
            .values_list("folio", flat=True)
            .first()
        )
        if not last or "-" not in last:
            seq = 1
        else:
            try:
                seq = int(last.split("-")[-1]) + 1
            except ValueError:
                seq = 1
        return f"{prefix}-{seq:04d}"

    def save(self, *args, **kwargs):
        if not self.folio:
            self.folio = self._generate_folio()
        super().save(*args, **kwargs)

    def recompute_totals(self):
        agg = self.entregas.aggregate(
            total=Count("id"),
            completadas=Count("id", filter=Q(estatus=EntregaRuta.ESTATUS_ENTREGADA)),
            incidencia=Count("id", filter=Q(estatus=EntregaRuta.ESTATUS_INCIDENCIA)),
            monto=Sum("monto_estimado"),
        )
        self.total_entregas = int(agg.get("total") or 0)
        self.entregas_completadas = int(agg.get("completadas") or 0)
        self.entregas_incidencia = int(agg.get("incidencia") or 0)
        self.monto_estimado_total = agg.get("monto") or Decimal("0")


class EntregaRuta(models.Model):
    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_EN_CAMINO = "EN_CAMINO"
    ESTATUS_ENTREGADA = "ENTREGADA"
    ESTATUS_INCIDENCIA = "INCIDENCIA"
    ESTATUS_CANCELADA = "CANCELADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_EN_CAMINO, "En camino"),
        (ESTATUS_ENTREGADA, "Entregada"),
        (ESTATUS_INCIDENCIA, "Incidencia"),
        (ESTATUS_CANCELADA, "Cancelada"),
    ]

    ruta = models.ForeignKey(RutaEntrega, on_delete=models.CASCADE, related_name="entregas")
    secuencia = models.PositiveIntegerField(default=1)
    pedido = models.ForeignKey(
        PedidoCliente,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="entregas_logistica",
    )
    cliente_nombre = models.CharField(max_length=180, blank=True, default="")
    direccion = models.CharField(max_length=250, blank=True, default="")
    contacto = models.CharField(max_length=120, blank=True, default="")
    telefono = models.CharField(max_length=40, blank=True, default="")
    ventana_inicio = models.DateTimeField(null=True, blank=True)
    ventana_fin = models.DateTimeField(null=True, blank=True)
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    monto_estimado = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    comentario = models.TextField(blank=True, default="")
    evidencia_url = models.URLField(blank=True, default="")
    entregado_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["secuencia", "id"]

    def __str__(self) -> str:
        return f"{self.ruta.folio} · {self.cliente_nombre or self.id}"

    def save(self, *args, **kwargs):
        if self.pedido and not self.cliente_nombre:
            self.cliente_nombre = self.pedido.cliente.nombre
        if self.estatus == self.ESTATUS_ENTREGADA and not self.entregado_at:
            self.entregado_at = timezone.now()
        if self.estatus != self.ESTATUS_ENTREGADA and self.entregado_at:
            self.entregado_at = None
        super().save(*args, **kwargs)
