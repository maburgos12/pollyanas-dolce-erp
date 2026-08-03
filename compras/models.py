from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import models, IntegrityError, transaction
from django.utils import timezone
from django.conf import settings
from unidecode import unidecode

from maestros.models import Insumo, Proveedor


def _norm_text(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


class SolicitudCompra(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_EN_REVISION = "EN_REVISION"
    STATUS_APROBADA = "APROBADA"
    STATUS_RECHAZADA = "RECHAZADA"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_EN_REVISION, "En revisión"),
        (STATUS_APROBADA, "Aprobada"),
        (STATUS_RECHAZADA, "Rechazada"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    area = models.CharField(max_length=120)
    solicitante = models.CharField(max_length=120)
    insumo = models.ForeignKey(Insumo, on_delete=models.PROTECT)
    proveedor_sugerido = models.ForeignKey(
        Proveedor,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="solicitudes_sugeridas",
    )
    cantidad = models.DecimalField(max_digits=18, decimal_places=3)
    fecha_requerida = models.DateField(default=timezone.localdate)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_BORRADOR)
    fuera_de_catalogo = models.BooleanField(default=False)
    cotizaciones_requeridas = models.PositiveSmallIntegerField(default=0)
    cotizaciones_recibidas = models.PositiveSmallIntegerField(default=0)
    justificacion_excepcion = models.CharField(max_length=255, blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]
        indexes = [
            models.Index(fields=["fecha_requerida"], name="compras_sol_fecha_req_idx"),
            models.Index(fields=["estatus"], name="compras_sol_estatus_idx"),
            models.Index(fields=["area"], name="compras_sol_area_idx"),
            models.Index(fields=["insumo", "fecha_requerida"], name="compras_sol_ins_fecha_idx"),
        ]

    def _next_folio(self) -> str:
        ymd = timezone.localdate().strftime("%y%m%d")
        prefix = f"SOL-{ymd}-"
        today_count = SolicitudCompra.objects.filter(folio__startswith=prefix).count() + 1
        return f"{prefix}{today_count:03d}"

    def save(self, *args, **kwargs):
        if self.folio:
            return super().save(*args, **kwargs)
        last_exc = None
        for _ in range(10):
            self.folio = self._next_folio()
            try:
                with transaction.atomic():
                    return super().save(*args, **kwargs)
            except IntegrityError as exc:
                # Colisión por concurrencia: recalcular siguiente folio y reintentar.
                last_exc = exc
                self.folio = ""
                continue
        if last_exc:
            raise last_exc
        raise IntegrityError("No fue posible generar folio único de solicitud de compra.")

    def __str__(self):
        return self.folio

class OrdenCompra(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_ENVIADA = "ENVIADA"
    STATUS_CONFIRMADA = "CONFIRMADA"
    STATUS_PARCIAL = "PARCIAL"
    STATUS_CERRADA = "CERRADA"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_ENVIADA, "Enviada"),
        (STATUS_CONFIRMADA, "Confirmada"),
        (STATUS_PARCIAL, "Parcial"),
        (STATUS_CERRADA, "Cerrada"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    solicitud = models.ForeignKey(SolicitudCompra, null=True, blank=True, on_delete=models.SET_NULL)
    referencia = models.CharField(max_length=160, blank=True, default="")
    proveedor = models.ForeignKey(Proveedor, on_delete=models.PROTECT)
    fecha_emision = models.DateField(default=timezone.localdate)
    fecha_entrega_estimada = models.DateField(null=True, blank=True)
    monto_estimado = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_BORRADOR)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]
        indexes = [
            models.Index(fields=["solicitud", "estatus"], name="compras_oc_sol_est_idx"),
            models.Index(fields=["fecha_emision", "estatus"], name="compras_oc_fecha_est_idx"),
        ]

    def _next_folio(self) -> str:
        ymd = timezone.localdate().strftime("%y%m%d")
        prefix = f"OC-{ymd}-"
        today_count = OrdenCompra.objects.filter(folio__startswith=prefix).count() + 1
        return f"{prefix}{today_count:03d}"

    def save(self, *args, **kwargs):
        if self.folio:
            return super().save(*args, **kwargs)
        last_exc = None
        for _ in range(10):
            self.folio = self._next_folio()
            try:
                with transaction.atomic():
                    return super().save(*args, **kwargs)
            except IntegrityError as exc:
                last_exc = exc
                self.folio = ""
                continue
        if last_exc:
            raise last_exc
        raise IntegrityError("No fue posible generar folio único de orden de compra.")

    def __str__(self):
        return self.folio


class RecepcionCompra(models.Model):
    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_DIFERENCIAS = "DIFERENCIAS"
    STATUS_CERRADA = "CERRADA"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente"),
        (STATUS_DIFERENCIAS, "Con diferencias"),
        (STATUS_CERRADA, "Cerrada"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True)
    orden = models.ForeignKey(OrdenCompra, on_delete=models.PROTECT)
    fecha_recepcion = models.DateField(default=timezone.localdate)
    conformidad_pct = models.DecimalField(max_digits=5, decimal_places=2, default=100)
    estatus = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    observaciones = models.CharField(max_length=255, blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]
        indexes = [
            models.Index(fields=["orden", "estatus"], name="compras_rec_ord_est_idx"),
        ]

    def _next_folio(self) -> str:
        ymd = timezone.localdate().strftime("%y%m%d")
        prefix = f"REC-{ymd}-"
        today_count = RecepcionCompra.objects.filter(folio__startswith=prefix).count() + 1
        return f"{prefix}{today_count:03d}"

    def save(self, *args, **kwargs):
        if self.folio:
            return super().save(*args, **kwargs)
        last_exc = None
        for _ in range(10):
            self.folio = self._next_folio()
            try:
                with transaction.atomic():
                    return super().save(*args, **kwargs)
            except IntegrityError as exc:
                last_exc = exc
                self.folio = ""
                continue
        if last_exc:
            raise last_exc
        raise IntegrityError("No fue posible generar folio único de recepción.")

    def __str__(self):
        return self.folio


class SolicitudCompraDepartamental(models.Model):
    TIPO_MENSUAL = "MENSUAL"
    TIPO_EXTRAORDINARIA = "EXTRAORDINARIA"
    TIPO_EMERGENCIA = "EMERGENCIA"
    TIPO_CHOICES = [
        (TIPO_MENSUAL, "Mensual"),
        (TIPO_EXTRAORDINARIA, "Extraordinaria"),
        (TIPO_EMERGENCIA, "Emergencia"),
    ]

    ESTADO_BORRADOR = "BORRADOR"
    ESTADO_ENVIADA = "ENVIADA"
    ESTADO_EN_ATENCION = "EN_ATENCION"
    ESTADO_PARCIAL = "PARCIAL"
    ESTADO_COMPLETADA = "COMPLETADA"
    ESTADO_CANCELADA = "CANCELADA"
    ESTADO_CHOICES = [
        (ESTADO_BORRADOR, "Borrador"),
        (ESTADO_ENVIADA, "Enviada a Compras"),
        (ESTADO_EN_ATENCION, "En atención"),
        (ESTADO_PARCIAL, "Parcialmente atendida"),
        (ESTADO_COMPLETADA, "Completada"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    folio = models.CharField(max_length=24, unique=True, blank=True)
    area = models.ForeignKey(
        "reportes.AreaPresupuesto", on_delete=models.PROTECT, related_name="solicitudes_compra_departamentales"
    )
    solicitante = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="solicitudes_compra_departamentales"
    )
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_MENSUAL)
    periodo = models.DateField(help_text="Primer día del mes planeado")
    motivo = models.TextField(blank=True, default="")
    justificacion_extraordinaria = models.TextField(blank=True, default="")
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_BORRADOR, db_index=True)
    comprador_asignado = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="solicitudes_departamentales_asignadas",
    )
    enviada_en = models.DateTimeField(null=True, blank=True)
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en"]
        permissions = [
            ("decidir_exceso_compra_departamental", "Puede decidir excesos de compras departamentales"),
        ]
        indexes = [
            models.Index(fields=["area", "periodo", "estado"], name="comp_dept_area_per_est_idx"),
        ]

    def clean(self):
        super().clean()
        if self.tipo in {self.TIPO_EXTRAORDINARIA, self.TIPO_EMERGENCIA} and not self.justificacion_extraordinaria.strip():
            raise ValidationError({"justificacion_extraordinaria": "Explica por qué la compra está fuera del ciclo mensual."})
        if self.periodo and self.periodo.day != 1:
            raise ValidationError({"periodo": "El periodo debe ser el primer día del mes."})

    def save(self, *args, **kwargs):
        if not self.folio:
            prefix = f"SCD-{timezone.localdate():%y%m}-"
            self.folio = f"{prefix}{SolicitudCompraDepartamental.objects.filter(folio__startswith=prefix).count() + 1:04d}"
        return super().save(*args, **kwargs)

    def __str__(self):
        return self.folio

    def actualizar_estado_desde_items(self):
        estados = list(self.items.values_list("estado", flat=True))
        if not estados or self.estado == self.ESTADO_CANCELADA:
            return self.estado
        terminales = {
            ItemCompraDepartamental.ESTADO_RECIBIDO_CONFORME,
            ItemCompraDepartamental.ESTADO_RECHAZADO,
            ItemCompraDepartamental.ESTADO_CANCELADO,
        }
        terminados = sum(estado in terminales for estado in estados)
        if terminados == len(estados):
            nuevo = self.ESTADO_COMPLETADA
        elif terminados:
            nuevo = self.ESTADO_PARCIAL
        elif any(estado != ItemCompraDepartamental.ESTADO_POR_REVISAR for estado in estados):
            nuevo = self.ESTADO_EN_ATENCION
        else:
            nuevo = self.estado
        if nuevo != self.estado:
            self.estado = nuevo
            self.save(update_fields=["estado", "actualizado_en"])
        return nuevo


class ItemCompraDepartamental(models.Model):
    ESTADO_POR_REVISAR = "POR_REVISAR"
    ESTADO_POR_COTIZAR = "POR_COTIZAR"
    ESTADO_COTIZANDO = "COTIZANDO"
    ESTADO_ESPERANDO_AREA = "ESPERANDO_AREA"
    ESTADO_ESPERANDO_DG = "ESPERANDO_DG"
    ESTADO_POSPUESTO = "POSPUESTO"
    ESTADO_FINANCIAMIENTO = "FINANCIAMIENTO"
    ESTADO_AUTORIZADO = "AUTORIZADO"
    ESTADO_ORDENADO = "ORDENADO"
    ESTADO_RECIBIDO_PARCIAL = "RECIBIDO_PARCIAL"
    ESTADO_PENDIENTE_CONFIRMACION = "PENDIENTE_CONFIRMACION"
    ESTADO_RECIBIDO_CONFORME = "RECIBIDO_CONFORME"
    ESTADO_RECHAZADO = "RECHAZADO"
    ESTADO_CANCELADO = "CANCELADO"
    ESTADO_CHOICES = [
        (ESTADO_POR_REVISAR, "Por revisar"),
        (ESTADO_POR_COTIZAR, "Por cotizar"),
        (ESTADO_COTIZANDO, "Cotizando"),
        (ESTADO_ESPERANDO_AREA, "Esperando información del área"),
        (ESTADO_ESPERANDO_DG, "Esperando autorización de Dirección General"),
        (ESTADO_POSPUESTO, "Pospuesto"),
        (ESTADO_FINANCIAMIENTO, "Evaluando financiamiento"),
        (ESTADO_AUTORIZADO, "Autorizado"),
        (ESTADO_ORDENADO, "Ordenado"),
        (ESTADO_RECIBIDO_PARCIAL, "Recibido parcialmente"),
        (ESTADO_PENDIENTE_CONFIRMACION, "Comprado, pendiente de confirmación"),
        (ESTADO_RECIBIDO_CONFORME, "Recibido conforme"),
        (ESTADO_RECHAZADO, "Rechazado"),
        (ESTADO_CANCELADO, "Cancelado"),
    ]
    RESPONSABLE_AREA = "AREA"
    RESPONSABLE_COMPRAS = "COMPRAS"
    RESPONSABLE_DG = "DG"
    RESPONSABLE_NADIE = "NADIE"
    RESPONSABLE_CHOICES = [
        (RESPONSABLE_AREA, "Área solicitante"),
        (RESPONSABLE_COMPRAS, "Compras"),
        (RESPONSABLE_DG, "Dirección General"),
        (RESPONSABLE_NADIE, "Sin acción pendiente"),
    ]
    PRIORIDAD_NORMAL = "NORMAL"
    PRIORIDAD_ALTA = "ALTA"
    PRIORIDAD_URGENTE = "URGENTE"

    solicitud = models.ForeignKey(SolicitudCompraDepartamental, on_delete=models.CASCADE, related_name="items")
    descripcion = models.CharField(max_length=250)
    categoria = models.CharField(max_length=100, blank=True, default="")
    cantidad = models.DecimalField(max_digits=12, decimal_places=3, default=1)
    unidad = models.CharField(max_length=30, default="pieza")
    fecha_requerida = models.DateField(null=True, blank=True)
    prioridad = models.CharField(
        max_length=12,
        choices=[(PRIORIDAD_NORMAL, "Normal"), (PRIORIDAD_ALTA, "Alta"), (PRIORIDAD_URGENTE, "Urgente")],
        default=PRIORIDAD_NORMAL,
    )
    consecuencia_posponer = models.TextField(blank=True, default="")
    costo_unitario_estimado = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    monto_gastado = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=0,
        help_text="Importe reconocido por la fuente financiera; no se llena al solicitar, cotizar ni ordenar.",
    )
    imagen = models.ImageField(upload_to="compras/departamentales/%Y/%m/", null=True, blank=True)
    rubro = models.ForeignKey(
        "reportes.RubroPresupuesto", null=True, blank=True, on_delete=models.PROTECT, related_name="items_compra_departamental"
    )
    estado = models.CharField(max_length=30, choices=ESTADO_CHOICES, default=ESTADO_POR_REVISAR, db_index=True)
    siguiente_responsable = models.CharField(
        max_length=12, choices=RESPONSABLE_CHOICES, default=RESPONSABLE_COMPRAS
    )
    fecha_compromiso = models.DateField(null=True, blank=True)
    comentario_reciente = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["id"]
        indexes = [models.Index(fields=["estado", "siguiente_responsable"], name="comp_dept_item_flow_idx")]

    @property
    def subtotal_estimado(self):
        if self.costo_unitario_estimado is None:
            return None
        return self.cantidad * self.costo_unitario_estimado

    def clean(self):
        super().clean()
        if self.rubro_id and self.solicitud_id and self.rubro.area_id != self.solicitud.area_id:
            raise ValidationError({"rubro": "El rubro debe pertenecer al área solicitante."})

    def __str__(self):
        return f"{self.solicitud.folio} · {self.descripcion}"


class CotizacionCompraDepartamental(models.Model):
    item = models.ForeignKey(ItemCompraDepartamental, on_delete=models.CASCADE, related_name="cotizaciones")
    proveedor = models.ForeignKey(Proveedor, on_delete=models.PROTECT, related_name="cotizaciones_departamentales")
    documento = models.FileField(upload_to="compras/cotizaciones/%Y/%m/", null=True, blank=True)
    vigencia = models.DateField(null=True, blank=True)
    cantidad_ofertada = models.DecimalField(max_digits=12, decimal_places=3)
    costo_unitario = models.DecimalField(max_digits=14, decimal_places=2)
    descuento = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    impuestos = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    envio = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    instalacion = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    otros_cargos = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    tiempo_entrega_dias = models.PositiveIntegerField(null=True, blank=True)
    garantia_observaciones = models.TextField(blank=True, default="")
    seleccionada = models.BooleanField(default=False)
    creado_en = models.DateTimeField(default=timezone.now)

    @property
    def subtotal(self):
        return self.cantidad_ofertada * self.costo_unitario

    @property
    def total_adquisicion(self):
        return self.subtotal - self.descuento + self.impuestos + self.envio + self.instalacion + self.otros_cargos

    @property
    def costo_efectivo_unitario(self):
        return self.total_adquisicion / self.cantidad_ofertada if self.cantidad_ofertada else Decimal("0")


class CompromisoCompraDepartamental(models.Model):
    item = models.OneToOneField(ItemCompraDepartamental, on_delete=models.CASCADE, related_name="compromiso")
    cotizacion = models.ForeignKey(CotizacionCompraDepartamental, on_delete=models.PROTECT)
    monto = models.DecimalField(max_digits=14, decimal_places=2)
    activo = models.BooleanField(default=True, db_index=True)
    creado_en = models.DateTimeField(default=timezone.now)
    formalizado_en = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Se llena al generar la orden; antes de eso el monto es una reserva presupuestal.",
    )
    liberado_en = models.DateTimeField(null=True, blank=True)


class OrdenCompraDepartamental(models.Model):
    folio = models.CharField(max_length=24, unique=True, blank=True)
    proveedor = models.ForeignKey(Proveedor, on_delete=models.PROTECT, related_name="ordenes_departamentales")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    fecha_emision = models.DateField(default=timezone.localdate)
    fecha_entrega_estimada = models.DateField(null=True, blank=True)
    estado = models.CharField(
        max_length=20,
        choices=[("BORRADOR", "Borrador"), ("ENVIADA", "Enviada"), ("PARCIAL", "Parcial"), ("CERRADA", "Cerrada")],
        default="BORRADOR",
    )
    creado_en = models.DateTimeField(default=timezone.now)

    def save(self, *args, **kwargs):
        if not self.folio:
            prefix = f"OCD-{timezone.localdate():%y%m%d}-"
            self.folio = f"{prefix}{OrdenCompraDepartamental.objects.filter(folio__startswith=prefix).count() + 1:03d}"
        return super().save(*args, **kwargs)


class LineaOrdenCompraDepartamental(models.Model):
    orden = models.ForeignKey(OrdenCompraDepartamental, on_delete=models.CASCADE, related_name="lineas")
    item = models.OneToOneField(ItemCompraDepartamental, on_delete=models.PROTECT, related_name="linea_orden")
    cotizacion = models.ForeignKey(CotizacionCompraDepartamental, on_delete=models.PROTECT)
    cantidad = models.DecimalField(max_digits=12, decimal_places=3)
    costo_unitario = models.DecimalField(max_digits=14, decimal_places=2)
    total = models.DecimalField(max_digits=14, decimal_places=2)


class RecepcionItemDepartamental(models.Model):
    linea_orden = models.ForeignKey(LineaOrdenCompraDepartamental, on_delete=models.PROTECT, related_name="recepciones")
    cantidad_recibida = models.DecimalField(max_digits=12, decimal_places=3)
    observaciones = models.TextField(blank=True, default="")
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    recibido_en = models.DateTimeField(default=timezone.now)

    def save(self, *args, **kwargs):
        result = super().save(*args, **kwargs)
        total = self.linea_orden.recepciones.aggregate(models.Sum("cantidad_recibida"))["cantidad_recibida__sum"] or 0
        item = self.linea_orden.item
        if total < self.linea_orden.cantidad:
            item.estado = ItemCompraDepartamental.ESTADO_RECIBIDO_PARCIAL
            item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
        else:
            item.estado = ItemCompraDepartamental.ESTADO_PENDIENTE_CONFIRMACION
            item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_AREA
        item.save(update_fields=["estado", "siguiente_responsable", "actualizado_en"])
        item.solicitud.actualizar_estado_desde_items()
        return result


class EventoCompraDepartamental(models.Model):
    solicitud = models.ForeignKey(SolicitudCompraDepartamental, on_delete=models.CASCADE, related_name="eventos")
    item = models.ForeignKey(ItemCompraDepartamental, null=True, blank=True, on_delete=models.CASCADE, related_name="eventos")
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    tipo = models.CharField(max_length=60)
    detalle = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]


class PresupuestoCompraPeriodo(models.Model):
    TIPO_MES = "mes"
    TIPO_Q1 = "q1"
    TIPO_Q2 = "q2"
    TIPO_CHOICES = [
        (TIPO_MES, "Mensual"),
        (TIPO_Q1, "1ra quincena"),
        (TIPO_Q2, "2da quincena"),
    ]

    periodo_tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    periodo_mes = models.CharField(max_length=7)  # YYYY-MM
    monto_objetivo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    notas = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="presupuestos_compras_actualizados",
    )

    class Meta:
        ordering = ["-periodo_mes", "periodo_tipo"]
        unique_together = [("periodo_tipo", "periodo_mes")]

    def __str__(self):
        return f"{self.get_periodo_tipo_display()} {self.periodo_mes}"


class PresupuestoCompraProveedor(models.Model):
    presupuesto_periodo = models.ForeignKey(
        PresupuestoCompraPeriodo,
        on_delete=models.CASCADE,
        related_name="objetivos_proveedor",
    )
    proveedor = models.ForeignKey(
        Proveedor,
        on_delete=models.PROTECT,
        related_name="presupuestos_compra",
    )
    monto_objetivo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    notas = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="presupuestos_compras_proveedor_actualizados",
    )

    class Meta:
        ordering = ["-presupuesto_periodo_id", "proveedor_id"]
        unique_together = [("presupuesto_periodo", "proveedor")]

    def __str__(self):
        return f"{self.presupuesto_periodo} · {self.proveedor.nombre}"


class PresupuestoCompraCategoria(models.Model):
    presupuesto_periodo = models.ForeignKey(
        PresupuestoCompraPeriodo,
        on_delete=models.CASCADE,
        related_name="objetivos_categoria",
    )
    categoria = models.CharField(max_length=120)
    categoria_normalizada = models.CharField(max_length=140, db_index=True)
    monto_objetivo = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    notas = models.CharField(max_length=255, blank=True, default="")
    actualizado_en = models.DateTimeField(auto_now=True)
    actualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="presupuestos_compras_categoria_actualizados",
    )

    class Meta:
        ordering = ["-presupuesto_periodo_id", "categoria"]
        unique_together = [("presupuesto_periodo", "categoria_normalizada")]

    def save(self, *args, **kwargs):
        self.categoria = " ".join((self.categoria or "").strip().split())
        self.categoria_normalizada = _norm_text(self.categoria)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.presupuesto_periodo} · {self.categoria}"
