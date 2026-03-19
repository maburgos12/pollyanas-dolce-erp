from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone

from core.models import Sucursal
from recetas.models import Receta
from recetas.utils.normalizacion import normalizar_nombre


class Cliente(models.Model):
    codigo = models.CharField(max_length=40, unique=True, blank=True)
    nombre = models.CharField(max_length=180)
    nombre_normalizado = models.CharField(max_length=180, db_index=True, editable=False)
    telefono = models.CharField(max_length=40, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    tipo_cliente = models.CharField(max_length=40, blank=True, default="")
    sucursal_referencia = models.CharField(max_length=120, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    activo = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre"]

    def __str__(self) -> str:
        return self.nombre

    def _generate_codigo(self) -> str:
        base = "CLI"
        last_id = Cliente.objects.order_by("-id").values_list("id", flat=True).first() or 0
        return f"{base}-{last_id + 1:05d}"

    def save(self, *args, **kwargs):
        self.nombre_normalizado = normalizar_nombre(self.nombre or "")
        if not self.codigo:
            self.codigo = self._generate_codigo()
        super().save(*args, **kwargs)


class PedidoCliente(models.Model):
    ESTATUS_NUEVO = "NUEVO"
    ESTATUS_COTIZADO = "COTIZADO"
    ESTATUS_CONFIRMADO = "CONFIRMADO"
    ESTATUS_EN_PRODUCCION = "EN_PRODUCCION"
    ESTATUS_LISTO = "LISTO"
    ESTATUS_ENTREGADO = "ENTREGADO"
    ESTATUS_CANCELADO = "CANCELADO"
    ESTATUS_CHOICES = [
        (ESTATUS_NUEVO, "Nuevo"),
        (ESTATUS_COTIZADO, "Cotizado"),
        (ESTATUS_CONFIRMADO, "Confirmado"),
        (ESTATUS_EN_PRODUCCION, "En producción"),
        (ESTATUS_LISTO, "Listo"),
        (ESTATUS_ENTREGADO, "Entregado"),
        (ESTATUS_CANCELADO, "Cancelado"),
    ]

    PRIORIDAD_BAJA = "BAJA"
    PRIORIDAD_MEDIA = "MEDIA"
    PRIORIDAD_ALTA = "ALTA"
    PRIORIDAD_URGENTE = "URGENTE"
    PRIORIDAD_CHOICES = [
        (PRIORIDAD_BAJA, "Baja"),
        (PRIORIDAD_MEDIA, "Media"),
        (PRIORIDAD_ALTA, "Alta"),
        (PRIORIDAD_URGENTE, "Urgente"),
    ]

    CANAL_MOSTRADOR = "MOSTRADOR"
    CANAL_WHATSAPP = "WHATSAPP"
    CANAL_TELEFONO = "TELEFONO"
    CANAL_WEB = "WEB"
    CANAL_OTRO = "OTRO"
    CANAL_CHOICES = [
        (CANAL_MOSTRADOR, "Mostrador"),
        (CANAL_WHATSAPP, "WhatsApp"),
        (CANAL_TELEFONO, "Teléfono"),
        (CANAL_WEB, "Web"),
        (CANAL_OTRO, "Otro"),
    ]

    folio = models.CharField(max_length=40, unique=True, blank=True)
    cliente = models.ForeignKey(Cliente, on_delete=models.PROTECT, related_name="pedidos")
    descripcion = models.CharField(max_length=250)
    fecha_compromiso = models.DateField(null=True, blank=True)
    sucursal = models.CharField(max_length=120, blank=True, default="")
    sucursal_ref = models.ForeignKey(
        Sucursal,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="crm_pedidos",
    )
    pickup_reservation = models.OneToOneField(
        "crm.PickupReservation",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="pedido",
    )
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_NUEVO)
    prioridad = models.CharField(max_length=20, choices=PRIORIDAD_CHOICES, default=PRIORIDAD_MEDIA)
    canal = models.CharField(max_length=20, choices=CANAL_CHOICES, default=CANAL_MOSTRADOR)
    monto_estimado = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="crm_pedidos_creados",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at", "-id"]

    def __str__(self) -> str:
        return self.folio or f"Pedido {self.id}"

    def _generate_folio(self) -> str:
        now = timezone.localtime()
        prefix = now.strftime("PED-%Y%m")
        last = (
            PedidoCliente.objects.filter(folio__startswith=prefix)
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
        if self.sucursal_ref_id and not (self.sucursal or "").strip():
            self.sucursal = self.sucursal_ref.nombre
        super().save(*args, **kwargs)


class PickupReservation(models.Model):
    STATUS_ACTIVE = "ACTIVE"
    STATUS_CONFIRMED = "CONFIRMED"
    STATUS_RELEASED = "RELEASED"
    STATUS_EXPIRED = "EXPIRED"
    STATUS_CANCELED = "CANCELED"
    STATUS_REJECTED = "REJECTED"
    STATUS_CHOICES = [
        (STATUS_ACTIVE, "Active"),
        (STATUS_CONFIRMED, "Confirmed"),
        (STATUS_RELEASED, "Released"),
        (STATUS_EXPIRED, "Expired"),
        (STATUS_CANCELED, "Canceled"),
        (STATUS_REJECTED, "Rejected"),
    ]

    SOURCE_WEB = "WEB"
    SOURCE_CHOICES = [
        (SOURCE_WEB, "Web"),
    ]

    token = models.CharField(max_length=64, unique=True, db_index=True)
    receta = models.ForeignKey(Receta, on_delete=models.PROTECT, related_name="pickup_reservations")
    sucursal = models.ForeignKey(Sucursal, on_delete=models.PROTECT, related_name="pickup_reservations")
    quantity = models.DecimalField(max_digits=18, decimal_places=3, default=1)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_ACTIVE, db_index=True)
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_WEB)
    source_client_prefix = models.CharField(max_length=12, blank=True, default="")
    external_reference = models.CharField(max_length=120, blank=True, default="", db_index=True)
    client_name = models.CharField(max_length=180, blank=True, default="")
    snapshot_stock_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    reserved_qty_at_creation = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    buffer_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    available_to_promise = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    snapshot_captured_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True, db_index=True)
    released_at = models.DateTimeField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["status", "expires_at"], name="crm_pickup_status_exp_idx"),
            models.Index(fields=["receta", "sucursal", "status"], name="crm_pickup_recipe_branch_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.token} · {self.receta.nombre} · {self.sucursal.nombre} · {self.status}"


class SeguimientoPedido(models.Model):
    pedido = models.ForeignKey(PedidoCliente, on_delete=models.CASCADE, related_name="seguimientos")
    fecha_evento = models.DateTimeField(default=timezone.now)
    estatus_anterior = models.CharField(max_length=20, blank=True, default="")
    estatus_nuevo = models.CharField(max_length=20, choices=PedidoCliente.ESTATUS_CHOICES, blank=True, default="")
    comentario = models.TextField(blank=True, default="")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="crm_seguimientos_creados",
    )

    class Meta:
        ordering = ["-fecha_evento", "-id"]

    def __str__(self) -> str:
        return f"{self.pedido.folio} · {self.estatus_nuevo or 'nota'}"
