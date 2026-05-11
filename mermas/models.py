from decimal import Decimal

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils import timezone


class MermaRegistro(models.Model):
    ESTATUS_CAPTURA = "CAPTURA"
    ESTATUS_ENVIADO_CEDIS = "ENVIADO_CEDIS"
    ESTATUS_RECIBIDO_OK = "RECIBIDO_OK"
    ESTATUS_RECIBIDO_DIFERENCIA = "RECIBIDO_DIFERENCIA"
    ESTATUS_CANCELADO = "CANCELADO"
    ESTATUS_CHOICES = [
        (ESTATUS_CAPTURA, "En captura"),
        (ESTATUS_ENVIADO_CEDIS, "Enviado a CEDIS"),
        (ESTATUS_RECIBIDO_OK, "Recibido sin diferencia"),
        (ESTATUS_RECIBIDO_DIFERENCIA, "Recibido con diferencia"),
        (ESTATUS_CANCELADO, "Cancelado"),
    ]

    folio = models.CharField(max_length=40, unique=True, blank=True)
    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="mermas")
    ticket_point = models.CharField(max_length=80, blank=True, default="")
    estatus = models.CharField(max_length=24, choices=ESTATUS_CHOICES, default=ESTATUS_CAPTURA, db_index=True)
    iniciado_en = models.DateTimeField(default=timezone.now, db_index=True)
    registrado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="mermas_registradas",
    )
    repartidor = models.ForeignKey(
        "logistica.Repartidor",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="mermas_transportadas",
    )
    enviado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="mermas_enviadas",
    )
    enviado_en = models.DateTimeField(null=True, blank=True, db_index=True)
    recibido_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="mermas_recibidas",
    )
    recibido_en = models.DateTimeField(null=True, blank=True, db_index=True)
    repartidor_confirmado = models.BooleanField(default=False)
    nota_sucursal = models.TextField(blank=True, default="")
    nota_recepcion = models.TextField(blank=True, default="")
    alerta_ventas = models.BooleanField(default=False, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-iniciado_en", "-id"]
        verbose_name = "Registro de merma"
        verbose_name_plural = "Registros de merma"

    def __str__(self) -> str:
        return f"{self.folio or 'Merma'} - {self.sucursal.nombre}"

    @property
    def total_productos(self) -> int:
        return self.productos.count()

    @property
    def tiene_diferencia(self) -> bool:
        return self.productos.filter(conforme=False).exists()

    def _generate_folio(self) -> str:
        today = timezone.localdate()
        prefix = today.strftime("MER-%Y%m%d")
        last = (
            MermaRegistro.objects.filter(folio__startswith=prefix)
            .order_by("-folio")
            .values_list("folio", flat=True)
            .first()
        )
        if not last:
            seq = 1
        else:
            try:
                seq = int(last.rsplit("-", 1)[1]) + 1
            except (IndexError, ValueError):
                seq = 1
        return f"{prefix}-{seq:04d}"

    def save(self, *args, **kwargs):
        if not self.folio:
            self.folio = self._generate_folio()
        super().save(*args, **kwargs)

    def marcar_enviado(self, repartidor, user):
        if not repartidor:
            raise ValidationError("Selecciona el repartidor que traslada la merma.")
        self.repartidor = repartidor
        self.estatus = self.ESTATUS_ENVIADO_CEDIS
        self.enviado_por = user
        self.enviado_en = timezone.now()
        self.save(update_fields=["repartidor", "estatus", "enviado_por", "enviado_en", "updated_at"])

    @transaction.atomic
    def marcar_recibido(self, *, user, repartidor_confirmado, nota):
        tiene_diferencia = self.productos.filter(conforme=False).exists()
        self.repartidor_confirmado = repartidor_confirmado
        self.nota_recepcion = nota or ""
        self.recibido_por = user
        self.recibido_en = timezone.now()
        self.alerta_ventas = tiene_diferencia
        self.estatus = self.ESTATUS_RECIBIDO_DIFERENCIA if tiene_diferencia else self.ESTATUS_RECIBIDO_OK
        self.save(
            update_fields=[
                "repartidor_confirmado",
                "nota_recepcion",
                "recibido_por",
                "recibido_en",
                "alerta_ventas",
                "estatus",
                "updated_at",
            ]
        )


class MermaProducto(models.Model):
    registro = models.ForeignKey(MermaRegistro, on_delete=models.CASCADE, related_name="productos")
    receta = models.ForeignKey("recetas.Receta", on_delete=models.PROTECT, null=True, blank=True)
    producto_texto = models.CharField(max_length=220, blank=True, default="")
    cantidad_enviada = models.DecimalField(max_digits=12, decimal_places=3)
    cantidad_recibida = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    conforme = models.BooleanField(null=True, blank=True)
    nota_recepcion = models.CharField(max_length=240, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]
        verbose_name = "Producto de merma"
        verbose_name_plural = "Productos de merma"

    def __str__(self) -> str:
        return self.nombre_producto

    @property
    def nombre_producto(self) -> str:
        if self.receta_id:
            return self.receta.nombre
        return self.producto_texto

    def clean(self):
        if not self.receta_id and not self.producto_texto.strip():
            raise ValidationError("Selecciona un producto o escribe el nombre del producto.")
        if self.cantidad_enviada <= Decimal("0"):
            raise ValidationError("La cantidad enviada debe ser mayor a cero.")


class MermaEvidencia(models.Model):
    TIPO_TICKET = "TICKET"
    TIPO_PRODUCTO_SUCURSAL = "PRODUCTO_SUCURSAL"
    TIPO_PRODUCTO_CEDIS = "PRODUCTO_CEDIS"
    TIPO_CHOICES = [
        (TIPO_TICKET, "Ticket Point"),
        (TIPO_PRODUCTO_SUCURSAL, "Producto en sucursal"),
        (TIPO_PRODUCTO_CEDIS, "Producto recibido CEDIS"),
    ]

    registro = models.ForeignKey(MermaRegistro, on_delete=models.CASCADE, related_name="evidencias")
    tipo = models.CharField(max_length=24, choices=TIPO_CHOICES, db_index=True)
    archivo = models.ImageField(upload_to="mermas/evidencias/%Y/%m/")
    subido_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="mermas_evidencias",
    )
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        verbose_name = "Evidencia de merma"
        verbose_name_plural = "Evidencias de merma"

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} - {self.registro.folio}"
