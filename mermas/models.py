from decimal import Decimal
from hashlib import sha256

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
    repartidor_entrega = models.ForeignKey(
        "logistica.Repartidor",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="mermas_entregadas_cedis",
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
    def marcar_recibido(self, *, user, repartidor_confirmado, repartidor_entrega, nota):
        tiene_diferencia = self.productos.filter(conforme=False).exists()
        self.repartidor_confirmado = repartidor_confirmado
        self.repartidor_entrega = self.repartidor if repartidor_confirmado else repartidor_entrega
        self.nota_recepcion = nota or ""
        self.recibido_por = user
        self.recibido_en = timezone.now()
        self.alerta_ventas = tiene_diferencia
        self.estatus = self.ESTATUS_RECIBIDO_DIFERENCIA if tiene_diferencia else self.ESTATUS_RECIBIDO_OK
        self.save(
            update_fields=[
                "repartidor_confirmado",
                "repartidor_entrega",
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


class PersonalEnviosSucursal(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="personal_envios_sucursales",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="personal_envios_sucursales",
    )
    telefono = models.CharField(max_length=30, blank=True, default="")
    activo = models.BooleanField(default=True, db_index=True)
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["user__first_name", "user__last_name", "user__username"]
        verbose_name = "Personal Envíos a Sucursales"
        verbose_name_plural = "Personal Envíos a Sucursales"

    def __str__(self) -> str:
        return self.user.get_full_name() or self.user.username


class MermaInsumo(models.Model):
    ESTATUS_BORRADOR = "BORRADOR"
    ESTATUS_ENVIADA = "ENVIADA"
    ESTATUS_SIN_RESPONSABLE = "SIN_RESPONSABLE"
    ESTATUS_EN_ACLARACION = "EN_ACLARACION"
    ESTATUS_RECHAZADA = "RECHAZADA"
    ESTATUS_APROBADA = "APROBADA"
    ESTATUS_EJECUTANDO = "EJECUTANDO"
    ESTATUS_APLICADA = "APLICADA"
    ESTATUS_REQUIERE_REVISION = "REQUIERE_REVISION"
    ESTATUS_INTERVENCION_TECNICA = "INTERVENCION_TECNICA"
    ESTATUS_CHOICES = [
        (ESTATUS_BORRADOR, "Borrador"),
        (ESTATUS_ENVIADA, "Enviada"),
        (ESTATUS_SIN_RESPONSABLE, "Sin responsable"),
        (ESTATUS_EN_ACLARACION, "En aclaración"),
        (ESTATUS_RECHAZADA, "Rechazada"),
        (ESTATUS_APROBADA, "Aprobada"),
        (ESTATUS_EJECUTANDO, "Ejecutando"),
        (ESTATUS_APLICADA, "Aplicada"),
        (ESTATUS_REQUIERE_REVISION, "Requiere revisión"),
        (ESTATUS_INTERVENCION_TECNICA, "Intervención técnica"),
    ]

    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="mermas_insumos")
    reportado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="mermas_insumos_reportadas",
    )
    reportante_empleado = models.ForeignKey(
        "rrhh.Empleado",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="mermas_insumos_reportadas",
    )
    jefe_empleado = models.ForeignKey(
        "rrhh.Empleado",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="mermas_insumos_por_aprobar",
    )
    jefe_inmediato = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="mermas_insumos_asignadas",
    )
    insumo = models.ForeignKey(
        "maestros.Insumo",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="mermas_sucursal",
    )
    codigo_point = models.CharField(max_length=80, db_index=True)
    nombre_point = models.CharField(max_length=250)
    unidad_point = models.CharField(max_length=40)
    cantidad_reportada = models.DecimalField(max_digits=18, decimal_places=3)
    cantidad_aprobada = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)
    motivo = models.CharField(max_length=80)
    comentario = models.TextField()
    foto_evidencia = models.ImageField(upload_to="mermas/insumos/%Y/%m/", null=True, blank=True)
    justificacion_sin_foto = models.TextField(blank=True, default="")
    estatus = models.CharField(max_length=32, choices=ESTATUS_CHOICES, default=ESTATUS_BORRADOR, db_index=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        indexes = [
            models.Index(fields=["sucursal", "estatus"], name="merma_ins_suc_est_idx"),
            models.Index(fields=["jefe_inmediato", "estatus"], name="merma_ins_jefe_est_idx"),
        ]

    def clean(self):
        errors = {}
        if self.cantidad_reportada is None or not self.cantidad_reportada.is_finite() or self.cantidad_reportada <= 0:
            errors["cantidad_reportada"] = "La cantidad reportada debe ser mayor a cero."
        if not self.foto_evidencia and not self.justificacion_sin_foto.strip():
            errors["justificacion_sin_foto"] = "Escribe una justificación cuando no adjuntes fotografía."
        if self.cantidad_aprobada is not None:
            if not self.cantidad_aprobada.is_finite() or self.cantidad_aprobada <= 0:
                errors["cantidad_aprobada"] = "La cantidad aprobada debe ser mayor a cero."
            elif self.cantidad_aprobada > self.cantidad_reportada:
                errors["cantidad_aprobada"] = "El jefe no puede aumentar la cantidad reportada."
        if errors:
            raise ValidationError(errors)

    @transaction.atomic
    def aprobar(self, *, jefe, cantidad, motivo=""):
        locked = type(self).objects.select_for_update().get(pk=self.pk)
        if locked.estatus != self.ESTATUS_ENVIADA:
            raise ValidationError("La merma no está disponible para aprobación.")
        if not locked.jefe_inmediato_id or locked.jefe_inmediato_id != getattr(jefe, "id", None):
            raise ValidationError("Solo el jefe inmediato asignado puede aprobar esta merma.")
        cantidad = Decimal(str(cantidad))
        if not cantidad.is_finite() or cantidad <= 0:
            raise ValidationError("La cantidad aprobada debe ser un número positivo válido.")
        if cantidad > locked.cantidad_reportada:
            raise ValidationError("El jefe no puede aumentar la cantidad reportada.")
        if cantidad < locked.cantidad_reportada and not (motivo or "").strip():
            raise ValidationError("Es obligatorio indicar el motivo al aprobar una cantidad menor.")
        estado_anterior = locked.estatus
        locked.cantidad_aprobada = cantidad
        locked.estatus = self.ESTATUS_APROBADA
        locked.full_clean()
        locked.save(update_fields=["cantidad_aprobada", "estatus", "actualizado_en"])
        MermaInsumoEvento.objects.create(
            merma=locked,
            estado_anterior=estado_anterior,
            estado_nuevo=locked.estatus,
            actor=jefe,
            motivo=(motivo or "").strip(),
            metadata={"cantidad_reportada": str(locked.cantidad_reportada), "cantidad_aprobada": str(cantidad)},
        )
        self.cantidad_aprobada = locked.cantidad_aprobada
        self.estatus = locked.estatus
        return locked


class MermaInsumoEvento(models.Model):
    merma = models.ForeignKey(MermaInsumo, on_delete=models.PROTECT, related_name="eventos")
    estado_anterior = models.CharField(max_length=32, blank=True, default="")
    estado_nuevo = models.CharField(max_length=32)
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True, blank=True)
    motivo = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["creado_en", "id"]


class OrdenAjustePoint(models.Model):
    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_SIMULADA = "SIMULADA"
    ESTATUS_EJECUTANDO = "EJECUTANDO"
    ESTATUS_APLICADA = "APLICADA"
    ESTATUS_REQUIERE_REVISION = "REQUIERE_REVISION"
    ESTATUS_INTERVENCION_TECNICA = "INTERVENCION_TECNICA"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_SIMULADA, "Simulada"),
        (ESTATUS_EJECUTANDO, "Ejecutando"),
        (ESTATUS_APLICADA, "Aplicada"),
        (ESTATUS_REQUIERE_REVISION, "Requiere revisión"),
        (ESTATUS_INTERVENCION_TECNICA, "Intervención técnica"),
    ]
    IMMUTABLE_FIELDS = (
        "merma_id", "sucursal_id", "codigo_point", "unidad_point", "cantidad", "idempotency_key", "payload_hash"
    )

    merma = models.OneToOneField(MermaInsumo, on_delete=models.PROTECT, related_name="orden_point")
    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="ordenes_ajuste_point")
    codigo_point = models.CharField(max_length=80)
    unidad_point = models.CharField(max_length=40)
    cantidad = models.DecimalField(max_digits=18, decimal_places=3)
    idempotency_key = models.CharField(max_length=64, unique=True)
    payload_hash = models.CharField(max_length=64)
    estatus = models.CharField(max_length=32, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE, db_index=True)
    intentos = models.PositiveIntegerField(default=0)
    ultimo_error = models.TextField(blank=True, default="")
    existencia_antes = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)
    existencia_despues = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)
    referencia_point = models.CharField(max_length=120, blank=True, default="")
    evidencia_tecnica = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    aplicado_en = models.DateTimeField(null=True, blank=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    @classmethod
    def crear_desde_merma(cls, merma):
        if merma.estatus != MermaInsumo.ESTATUS_APROBADA or merma.cantidad_aprobada is None:
            raise ValidationError("La merma debe estar aprobada antes de crear la orden Point.")
        cantidad_aprobada = Decimal(merma.cantidad_aprobada).quantize(Decimal("0.001"))
        raw_key = f"merma-insumo:{merma.pk}:{merma.sucursal_id}:{merma.codigo_point}:{merma.unidad_point}:{cantidad_aprobada}"
        key = sha256(raw_key.encode("utf-8")).hexdigest()
        payload_hash = sha256(f"{raw_key}:-{cantidad_aprobada}".encode("utf-8")).hexdigest()
        return cls.objects.create(
            merma=merma,
            sucursal=merma.sucursal,
            codigo_point=merma.codigo_point,
            unidad_point=merma.unidad_point,
            cantidad=-cantidad_aprobada,
            idempotency_key=key,
            payload_hash=payload_hash,
        )

    def clean(self):
        if self.cantidad is None or not self.cantidad.is_finite() or self.cantidad >= 0:
            raise ValidationError({"cantidad": "La orden Point debe contener una cantidad negativa."})

    def save(self, *args, **kwargs):
        if self.pk:
            original = type(self).objects.filter(pk=self.pk).values(*self.IMMUTABLE_FIELDS).first()
            if original and any(original[field] != getattr(self, field) for field in self.IMMUTABLE_FIELDS):
                raise ValidationError("El payload de la orden Point es inmutable.")
        super().save(*args, **kwargs)
