from __future__ import annotations

from django.conf import settings
from django.core.exceptions import ValidationError
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


class DireccionCliente(models.Model):
    cliente = models.ForeignKey(Cliente, on_delete=models.CASCADE, related_name="direcciones")
    alias = models.CharField(max_length=80, blank=True, default="")
    direccion = models.CharField(max_length=300)
    direccion_normalizada = models.CharField(max_length=300, editable=False)
    referencias = models.CharField(max_length=300, blank=True, default="")
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    place_id = models.CharField(max_length=255, blank=True, default="")
    es_predeterminada = models.BooleanField(default=False)
    activa = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-es_predeterminada", "-updated_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["cliente", "direccion_normalizada"],
                name="crm_direccion_cliente_normalizada_unica",
            ),
            models.CheckConstraint(
                check=(
                    models.Q(latitud__isnull=True, longitud__isnull=True)
                    | models.Q(latitud__isnull=False, longitud__isnull=False)
                ),
                name="crm_direccion_gps_par_completo",
            ),
        ]

    def __str__(self) -> str:
        return self.alias or self.direccion

    @staticmethod
    def normalizar_direccion(value: str) -> str:
        return normalizar_nombre((value or "").strip())

    def save(self, *args, **kwargs):
        self.direccion = (self.direccion or "").strip()
        self.direccion_normalizada = self.normalizar_direccion(self.direccion)
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
    direccion_entrega = models.ForeignKey(
        DireccionCliente,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="pedidos",
    )
    external_source = models.CharField(max_length=40, blank=True, default="", db_index=True)
    external_id = models.CharField(max_length=120, blank=True, default="", db_index=True)
    payload_snapshot = models.JSONField(default=dict, blank=True, editable=False)
    tracking_token = models.UUIDField(
        null=True,
        blank=True,
        unique=True,
        editable=False,
    )
    public_api_client = models.ForeignKey(
        "integraciones.PublicApiClient",
        null=True,
        blank=True,
        editable=False,
        on_delete=models.PROTECT,
        related_name="pedidos_omnichannel",
    )
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
        constraints = [
            models.UniqueConstraint(
                fields=["external_source", "external_id"],
                condition=~models.Q(external_source="") & ~models.Q(external_id=""),
                name="crm_pedido_origen_externo_unico",
            ),
        ]

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
        allow_snapshot_backfill = kwargs.pop("_allow_payload_snapshot_backfill", False)
        if not self._state.adding:
            persisted_snapshot = (
                PedidoCliente.objects.filter(pk=self.pk)
                .values_list("payload_snapshot", flat=True)
                .first()
            )
            snapshot_changed = persisted_snapshot != self.payload_snapshot
            valid_backfill = (
                allow_snapshot_backfill
                and not persisted_snapshot
                and bool(self.payload_snapshot)
            )
            if snapshot_changed and not valid_backfill:
                raise ValidationError(
                    {"payload_snapshot": "El snapshot omnicanal es inmutable."},
                )

        if not self.folio:
            self.folio = self._generate_folio()

        sync_sucursal = False
        if self.sucursal_ref_id:
            canonical_name = self.sucursal_ref.nombre
            current_sucursal = self.sucursal or ""

            if self._state.adding:
                if not current_sucursal.strip():
                    self.sucursal = canonical_name
                    sync_sucursal = True
            else:
                previous = (
                    PedidoCliente.objects.filter(pk=self.pk)
                    .values("sucursal", "sucursal_ref__nombre")
                    .first()
                )
                previous_canonical = ""
                if previous:
                    previous_canonical = previous.get("sucursal_ref__nombre") or ""

                if not current_sucursal.strip() or current_sucursal == previous_canonical:
                    self.sucursal = canonical_name
                    sync_sucursal = True

        update_fields = kwargs.get("update_fields")
        if sync_sucursal and update_fields is not None:
            kwargs["update_fields"] = set(update_fields) | {"sucursal"}

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
