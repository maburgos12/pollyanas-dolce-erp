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
    ESTADO_COMPRADO = "COMPRADO"
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
        (ESTADO_COMPRADO, "Comprado, pendiente de entrega"),
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

    @property
    def intento_vigente(self):
        intentos = getattr(self, "intentos_compra_prefetched", None)
        if intentos is not None:
            return next((intento for intento in intentos if intento.estado == "VIGENTE"), None)
        return self.intentos_compra.filter(estado="VIGENTE").select_related(
            "cotizacion__proveedor", "linea_orden", "compra"
        ).first()

    def clean(self):
        super().clean()
        if self.rubro_id and self.solicitud_id and self.rubro.area_id != self.solicitud.area_id:
            raise ValidationError({"rubro": "El rubro debe pertenecer al área solicitante."})

    def __str__(self):
        return f"{self.solicitud.folio} · {self.descripcion}"


class CotizacionCompraDepartamental(models.Model):
    version = models.PositiveIntegerField(default=1)
    PLATAFORMAS = [("", "Compra directa"), ("AMAZON", "Amazon"), ("MERCADO_LIBRE", "Mercado Libre"), ("OTRA", "Otra tienda en línea")]
    plataforma = models.CharField(max_length=20, choices=PLATAFORMAS, blank=True, default="")
    enlace_producto = models.URLField(max_length=2000, blank=True, default="")
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


class HistorialCotizacionDepartamental(models.Model):
    cotizacion = models.ForeignKey(CotizacionCompraDepartamental, on_delete=models.PROTECT, related_name="historial")
    antes = models.JSONField()
    despues = models.JSONField()
    motivo = models.TextField()
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en", "-pk"]


def _normalizar_update_fields(args, kwargs):
    """Conserva un generador de campos para validar y luego guardar la misma edición."""
    if kwargs.get("update_fields") is not None:
        kwargs["update_fields"] = tuple(kwargs["update_fields"])
    elif len(args) > 3 and args[3] is not None:
        args = (*args[:3], tuple(args[3]), *args[4:])
    return args, kwargs


def _ids_relacion_efectivos(instancia, campos, args, kwargs):
    """Usar valores persistidos para relaciones omitidas de update_fields."""
    update_fields = kwargs.get("update_fields", args[3] if len(args) > 3 else None)
    valores = {campo: getattr(instancia, f"{campo}_id") for campo in campos}
    if not instancia._state.adding and update_fields is not None:
        incluidos = set(update_fields)
        omitidos = [
            campo for campo in campos
            if campo not in incluidos and f"{campo}_id" not in incluidos
        ]
        if omitidos:
            db = kwargs.get("using") or instancia._state.db or "default"
            guardados = type(instancia)._base_manager.using(db).values(
                *(f"{campo}_id" for campo in omitidos)
            ).get(pk=instancia.pk)
            valores.update({campo: guardados[f"{campo}_id"] for campo in omitidos})
    return valores


def _validar_vinculos_compra(item_id, cotizacion_id, intento_id=None, *, using="default"):
    if not item_id or not cotizacion_id:
        return
    item_cotizado = CotizacionCompraDepartamental.objects.using(using).filter(
        pk=cotizacion_id
    ).values_list("item_id", flat=True).first()
    if item_cotizado is not None and item_cotizado != item_id:
        raise ValidationError({"cotizacion": "La cotización debe corresponder al artículo."})
    if intento_id is not None:
        intento = IntentoCompraDepartamental.objects.using(using).filter(
            pk=intento_id
        ).values("item_id", "cotizacion_id").first()
        if intento is not None and (intento["item_id"] != item_id or intento["cotizacion_id"] != cotizacion_id):
            raise ValidationError("El artículo y la cotización deben corresponder al intento de compra.")


class IntentoCompraQuerySet(models.QuerySet):
    CAMPOS_REEMBOLSO = frozenset({
        "reembolso_solicitado", "reembolso_cargos_adicionales", "reembolso_solicitado_en",
        "evidencia_solicitud_reembolso",
    })

    def update(self, **kwargs):
        if self.CAMPOS_REEMBOLSO.intersection(kwargs):
            raise ValidationError("Use save() o el servicio para modificar la solicitud de reembolso.")
        return super().update(**kwargs)

    def bulk_update(self, objs, fields, *args, **kwargs):
        fields = tuple(fields)
        if self.CAMPOS_REEMBOLSO.intersection(fields):
            raise ValidationError("Use save() o el servicio para modificar la solicitud de reembolso.")
        return super().bulk_update(objs, fields, *args, **kwargs)


class IntentoCompraDepartamental(models.Model):
    ESTADO_VIGENTE = "VIGENTE"
    ESTADO_CANCELADO_SIN_PAGO = "CANCELADO_SIN_PAGO"
    ESTADO_REEMBOLSO_SOLICITADO = "REEMBOLSO_SOLICITADO"
    ESTADO_REEMBOLSADO = "REEMBOLSADO"
    ESTADO_ENTREGADO = "ENTREGADO"
    ESTADO_CHOICES = [
        (ESTADO_VIGENTE, "Vigente"),
        (ESTADO_CANCELADO_SIN_PAGO, "Cancelado sin pago"),
        (ESTADO_REEMBOLSO_SOLICITADO, "Reembolso solicitado"),
        (ESTADO_REEMBOLSADO, "Reembolsado"),
        (ESTADO_ENTREGADO, "Entregado"),
    ]
    MOTIVO_PROVEEDOR_CANCELO = "PROVEEDOR_CANCELO"
    MOTIVO_NO_ENTREGO = "NO_ENTREGO"
    MOTIVO_OTRO = "OTRO"
    MOTIVO_CHOICES = [
        (MOTIVO_PROVEEDOR_CANCELO, "Proveedor canceló"),
        (MOTIVO_NO_ENTREGO, "Proveedor no entregó"),
        (MOTIVO_OTRO, "Otro"),
    ]

    item = models.ForeignKey(ItemCompraDepartamental, on_delete=models.PROTECT, related_name="intentos_compra")
    cotizacion = models.ForeignKey(
        CotizacionCompraDepartamental, on_delete=models.PROTECT, related_name="intentos_compra"
    )
    numero = models.PositiveIntegerField(editable=False)
    version = models.PositiveIntegerField(default=1)
    estado = models.CharField(max_length=30, choices=ESTADO_CHOICES, default=ESTADO_VIGENTE, db_index=True)
    motivo_cancelacion = models.CharField(max_length=30, choices=MOTIVO_CHOICES, blank=True, default="")
    detalle_cancelacion = models.TextField(blank=True, default="")
    cancelado_en = models.DateTimeField(null=True, blank=True)
    cancelado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.PROTECT,
        related_name="intentos_compra_cancelados",
    )
    reembolso_solicitado = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    reembolso_cargos_adicionales = models.DecimalField(
        max_digits=14, decimal_places=2, default=Decimal("0.00"),
    )
    reembolso_solicitado_en = models.DateField(null=True, blank=True)
    evidencia_solicitud_reembolso = models.FileField(
        upload_to="compras/departamentales/reembolsos/solicitudes/%Y/%m/", null=True, blank=True
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)
    objects = IntentoCompraQuerySet.as_manager()

    class Meta:
        ordering = ["numero", "pk"]
        constraints = [
            models.UniqueConstraint(
                fields=["item"], condition=models.Q(estado="VIGENTE"),
                name="comp_dept_un_intento_vigente",
            ),
            models.UniqueConstraint(fields=["item", "numero"], name="comp_dept_intento_numero_unico"),
            models.CheckConstraint(check=models.Q(numero__gt=0), name="comp_dept_intento_numero_positivo"),
            models.CheckConstraint(
                check=models.Q(reembolso_solicitado__isnull=True) | models.Q(reembolso_solicitado__gt=0),
                name="comp_dept_solicitud_reembolso_positiva",
            ),
            models.CheckConstraint(
                check=models.Q(reembolso_cargos_adicionales__gte=0),
                name="comp_dept_reembolso_cargos_no_negativos",
            ),
            models.CheckConstraint(
                check=models.Q(reembolso_solicitado__isnull=False) | models.Q(reembolso_cargos_adicionales=0),
                name="comp_dept_reembolso_cargos_requieren_total",
            ),
            models.CheckConstraint(
                check=(
                    models.Q(reembolso_solicitado__isnull=True)
                    | models.Q(reembolso_cargos_adicionales__lte=models.F("reembolso_solicitado"))
                ),
                name="comp_dept_reembolso_cargos_hasta_total",
            ),
        ]

    def clean(self):
        super().clean()
        if self.item_id and self.cotizacion_id and self.cotizacion.item_id != self.item_id:
            raise ValidationError({"cotizacion": "La cotización debe corresponder al artículo del intento."})

    def save(self, *args, **kwargs):
        if kwargs.get("update_fields") is not None:
            kwargs["update_fields"] = tuple(kwargs["update_fields"])
        elif len(args) > 3 and args[3] is not None:
            args = (*args[:3], tuple(args[3]), *args[4:])
        db = kwargs.get("using") or self._state.db or "default"
        relaciones = _ids_relacion_efectivos(self, ("item", "cotizacion"), args, kwargs)
        _validar_vinculos_compra(
            relaciones["item"], relaciones["cotizacion"],
            using=db,
        )
        update_fields = kwargs.get("update_fields", args[3] if len(args) > 3 else None)

        def validar_reembolso(guardado=None):
            solicitado = (
                self.reembolso_solicitado
                if guardado is None or update_fields is None or "reembolso_solicitado" in update_fields
                else guardado.reembolso_solicitado
            )
            cargos = (
                self.reembolso_cargos_adicionales
                if guardado is None or update_fields is None or "reembolso_cargos_adicionales" in update_fields
                else guardado.reembolso_cargos_adicionales
            )
            solicitado = self._meta.get_field("reembolso_solicitado").to_python(solicitado)
            cargos = self._meta.get_field("reembolso_cargos_adicionales").to_python(cargos)
            if cargos is None or cargos < 0:
                raise ValidationError({
                    "reembolso_cargos_adicionales": "Los cargos adicionales no pueden ser negativos."
                })
            if solicitado is None and cargos:
                raise ValidationError({
                    "reembolso_cargos_adicionales": "Los cargos adicionales requieren una solicitud de reembolso."
                })
            if solicitado is not None and cargos > solicitado:
                raise ValidationError({
                    "reembolso_cargos_adicionales": "Los cargos adicionales no pueden superar el total solicitado."
                })
            return solicitado

        if self._state.adding and not self.numero:
            validar_reembolso()
            with transaction.atomic(using=db):
                ItemCompraDepartamental.objects.using(db).select_for_update().get(pk=self.item_id)
                ultimo = type(self).objects.using(db).filter(item_id=self.item_id).aggregate(models.Max("numero"))["numero__max"]
                self.numero = (ultimo or 0) + 1
                return super().save(*args, **kwargs)
        if not self._state.adding:
            with transaction.atomic(using=db):
                guardado = type(self).objects.using(db).select_for_update().get(pk=self.pk)
                solicitado = validar_reembolso(guardado)
                recibido = self.reembolsos.using(db).aggregate(total=models.Sum("importe"))["total"] or Decimal("0")
                if recibido and (solicitado is None or solicitado < recibido):
                    raise ValidationError({"reembolso_solicitado": "La solicitud no puede ser menor que lo reembolsado."})
                return super().save(*args, **kwargs)
        validar_reembolso()
        return super().save(*args, **kwargs)

    @property
    def total_reembolsado(self):
        return self.reembolsos.aggregate(total=models.Sum("importe"))["total"] or Decimal("0")

    @property
    def saldo_reembolso(self):
        return max((self.reembolso_solicitado or Decimal("0")) - self.total_reembolsado, Decimal("0"))


class ReembolsoCompraQuerySet(models.QuerySet):
    def bulk_create(self, objs, *args, **kwargs):
        raise ValidationError("Los reembolsos deben registrarse individualmente.")

    def bulk_update(self, objs, fields, *args, **kwargs):
        raise ValidationError("Los reembolsos registrados no pueden modificarse.")

    def update(self, **kwargs):
        raise ValidationError("Los reembolsos registrados no pueden modificarse.")

    def delete(self):
        raise ValidationError("Los reembolsos registrados no pueden eliminarse.")


class ReembolsoCompraDepartamental(models.Model):
    intento = models.ForeignKey(IntentoCompraDepartamental, on_delete=models.PROTECT, related_name="reembolsos")
    importe = models.DecimalField(max_digits=14, decimal_places=2)
    fecha = models.DateField()
    referencia = models.CharField(max_length=160, blank=True, default="")
    comprobante = models.FileField(
        upload_to="compras/departamentales/reembolsos/recibidos/%Y/%m/", null=True, blank=True
    )
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)
    objects = ReembolsoCompraQuerySet.as_manager()

    class Meta:
        ordering = ["creado_en", "pk"]
        constraints = [
            models.CheckConstraint(check=models.Q(importe__gt=0), name="comp_dept_reembolso_positivo"),
        ]

    def save(self, *args, **kwargs):
        if not self._state.adding:
            raise ValidationError("Los reembolsos registrados no pueden modificarse.")
        self.importe = self._meta.get_field("importe").to_python(self.importe)
        if self.importe is None or self.importe <= 0:
            raise ValidationError({"importe": "El reembolso debe ser mayor que cero."})
        db = kwargs.get("using") or self._state.db or "default"
        with transaction.atomic(using=db):
            intento = IntentoCompraDepartamental.objects.using(db).select_for_update().get(pk=self.intento_id)
            if self.pk is not None and type(self).objects.using(db).filter(pk=self.pk).exists():
                raise ValidationError("Un reembolso registrado no puede reemplazarse.")
            solicitado = intento.reembolso_solicitado
            if solicitado is None:
                raise ValidationError("El intento no tiene un reembolso solicitado.")
            recibido = type(self).objects.using(db).filter(intento_id=self.intento_id).exclude(
                pk=self.pk
            ).aggregate(total=models.Sum("importe"))["total"] or Decimal("0")
            if recibido + self.importe > solicitado:
                raise ValidationError({"importe": "El reembolso supera el saldo solicitado."})
            if args:
                args = (True, *args[1:])
            else:
                kwargs["force_insert"] = True
            return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise ValidationError("Los reembolsos registrados no pueden eliminarse.")


class CompraRealizadaDepartamental(models.Model):
    version = models.PositiveIntegerField(default=1)
    item = models.ForeignKey(ItemCompraDepartamental, on_delete=models.PROTECT, related_name="compras_realizadas")
    intento = models.OneToOneField(IntentoCompraDepartamental, on_delete=models.PROTECT, related_name="compra")
    cotizacion = models.ForeignKey(CotizacionCompraDepartamental, on_delete=models.PROTECT)
    fecha_compra = models.DateField()
    importe_final = models.DecimalField(max_digits=14, decimal_places=2)
    numero_pedido = models.CharField(max_length=160, blank=True, default="")
    comprobante = models.FileField(upload_to="compras/cotizaciones/compras/%Y/%m/", blank=True)
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)

    def clean(self):
        super().clean()
        if self.fecha_compra and self.fecha_compra > timezone.localdate():
            raise ValidationError({"fecha_compra": "La fecha de compra no puede ser futura."})
        if self.importe_final is not None and self.importe_final <= 0:
            raise ValidationError({"importe_final": "El importe final debe ser mayor que cero."})
        if self.cotizacion_id and self.item_id and self.cotizacion.item_id != self.item_id:
            raise ValidationError("La cotización debe corresponder al artículo comprado.")

    def save(self, *args, **kwargs):
        args, kwargs = _normalizar_update_fields(args, kwargs)
        relaciones = _ids_relacion_efectivos(self, ("item", "cotizacion", "intento"), args, kwargs)
        _validar_vinculos_compra(
            relaciones["item"], relaciones["cotizacion"], relaciones["intento"],
            using=kwargs.get("using") or self._state.db or "default",
        )
        return super().save(*args, **kwargs)


class HistorialCompraDepartamental(models.Model):
    """Corrección auditable de una compra ya registrada.

    Registrar la compra cierra la edición de la cotización, así que este es el
    único camino para arreglar un importe mal capturado sin borrar evidencia.
    Corregir no reabre la autorización: la compra ya se pagó.
    """

    compra = models.ForeignKey(
        "CompraRealizadaDepartamental", on_delete=models.PROTECT, related_name="historial"
    )
    antes = models.JSONField()
    despues = models.JSONField()
    motivo = models.TextField()
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en", "-pk"]


class CompromisoCompraDepartamental(models.Model):
    item = models.ForeignKey(ItemCompraDepartamental, on_delete=models.CASCADE, related_name="compromisos")
    intento = models.OneToOneField(
        IntentoCompraDepartamental, null=True, blank=True, on_delete=models.PROTECT, related_name="compromiso"
    )
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

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["item"], condition=models.Q(activo=True, intento__isnull=True),
                name="comp_dept_una_reserva_preorden_activa",
            ),
        ]

    def save(self, *args, **kwargs):
        args, kwargs = _normalizar_update_fields(args, kwargs)
        relaciones = _ids_relacion_efectivos(self, ("item", "cotizacion", "intento"), args, kwargs)
        _validar_vinculos_compra(
            relaciones["item"], relaciones["cotizacion"], relaciones["intento"],
            using=kwargs.get("using") or self._state.db or "default",
        )
        return super().save(*args, **kwargs)


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
    item = models.ForeignKey(ItemCompraDepartamental, on_delete=models.PROTECT, related_name="lineas_orden")
    intento = models.OneToOneField(IntentoCompraDepartamental, on_delete=models.PROTECT, related_name="linea_orden")
    cotizacion = models.ForeignKey(CotizacionCompraDepartamental, on_delete=models.PROTECT)
    cantidad = models.DecimalField(max_digits=12, decimal_places=3)
    costo_unitario = models.DecimalField(max_digits=14, decimal_places=2)
    total = models.DecimalField(max_digits=14, decimal_places=2)

    def save(self, *args, **kwargs):
        args, kwargs = _normalizar_update_fields(args, kwargs)
        relaciones = _ids_relacion_efectivos(self, ("item", "cotizacion", "intento"), args, kwargs)
        _validar_vinculos_compra(
            relaciones["item"], relaciones["cotizacion"], relaciones["intento"],
            using=kwargs.get("using") or self._state.db or "default",
        )
        return super().save(*args, **kwargs)


class RecepcionItemDepartamental(models.Model):
    linea_orden = models.ForeignKey(LineaOrdenCompraDepartamental, on_delete=models.PROTECT, related_name="recepciones")
    cantidad_recibida = models.DecimalField(max_digits=12, decimal_places=3)
    observaciones = models.TextField(blank=True, default="")
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    recibido_en = models.DateTimeField(default=timezone.now)

    def save(self, *args, **kwargs):
        args, kwargs = _normalizar_update_fields(args, kwargs)
        update_fields = kwargs.get("update_fields", args[3] if len(args) > 3 else None)
        db = kwargs.get("using") or self._state.db or "default"
        with transaction.atomic(using=db):
            linea = LineaOrdenCompraDepartamental.objects.using(db).select_related("intento").get(
                pk=self.linea_orden_id
            )
            item = ItemCompraDepartamental.objects.using(db).select_for_update().get(pk=linea.item_id)
            intento = IntentoCompraDepartamental.objects.using(db).select_for_update().get(pk=linea.intento_id)
            linea = LineaOrdenCompraDepartamental.objects.using(db).select_for_update().get(pk=linea.pk)
            anterior = None
            if not self._state.adding:
                anterior = type(self).objects.using(db).select_for_update().get(pk=self.pk)
                if anterior.linea_orden_id != linea.pk:
                    raise ValidationError("La recepción no puede cambiar de orden.")
            cantidad_cambio = anterior is None or (
                (update_fields is None or "cantidad_recibida" in update_fields)
                and anterior.cantidad_recibida != self.cantidad_recibida
            )
            if not cantidad_cambio:
                return super().save(*args, **kwargs)
            if self.cantidad_recibida <= 0:
                raise ValidationError("La cantidad recibida debe ser mayor que cero.")
            vigente = intento.estado == IntentoCompraDepartamental.ESTADO_VIGENTE
            entregado_pendiente = (
                anterior is not None
                and intento.estado == IntentoCompraDepartamental.ESTADO_ENTREGADO
                and item.estado == ItemCompraDepartamental.ESTADO_PENDIENTE_CONFIRMACION
                and not item.intentos_compra.using(db).filter(estado="VIGENTE").exists()
                and not item.intentos_compra.using(db).filter(
                    estado=IntentoCompraDepartamental.ESTADO_ENTREGADO,
                    numero__gt=intento.numero,
                ).exists()
            )
            if not (vigente or entregado_pendiente):
                raise ValidationError("La orden no corresponde al intento vigente del artículo.")
            if vigente and item.estado not in (
                ItemCompraDepartamental.ESTADO_ORDENADO,
                ItemCompraDepartamental.ESTADO_COMPRADO,
                ItemCompraDepartamental.ESTADO_RECIBIDO_PARCIAL,
            ):
                raise ValidationError("El artículo ya no admite modificar la cantidad recibida.")
            result = super().save(*args, **kwargs)
            total = linea.recepciones.using(db).aggregate(models.Sum("cantidad_recibida"))["cantidad_recibida__sum"] or 0
            if total < linea.cantidad:
                intento.estado = IntentoCompraDepartamental.ESTADO_VIGENTE
                item.estado = ItemCompraDepartamental.ESTADO_RECIBIDO_PARCIAL
                item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
            else:
                intento.estado = IntentoCompraDepartamental.ESTADO_ENTREGADO
                item.estado = ItemCompraDepartamental.ESTADO_PENDIENTE_CONFIRMACION
                item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_AREA
            # Toda variación de cantidad invalida formularios abiertos; editar
            # observaciones conserva la versión y no vuelve a procesar la entrega.
            intento.version += 1
            intento.save(update_fields=["estado", "version", "actualizado_en"])
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


class AvisoCompraDepartamental(models.Model):
    """Cola persistente de avisos al solicitante cuando Compras registra la compra.

    Un renglón por compra y canal (`unique_together`) es la protección contra
    duplicados: doble clic, reintento manual y ejecución concurrente escriben
    sobre el mismo renglón en lugar de crear otro envío.
    """

    CANAL_CORREO = "CORREO"
    CANAL_WHATSAPP = "WHATSAPP"
    CANAL_CHOICES = [
        (CANAL_CORREO, "Correo"),
        (CANAL_WHATSAPP, "WhatsApp"),
    ]

    ESTADO_PENDIENTE = "PENDIENTE"
    ESTADO_ENVIADO = "ENVIADO"
    ESTADO_FALLIDO = "FALLIDO"
    ESTADO_SIN_CONTACTO = "SIN_CONTACTO"
    ESTADO_INCIERTO = "INCIERTO"
    ESTADO_SIN_CANAL = "SIN_CANAL"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_ENVIADO, "Enviado"),
        (ESTADO_FALLIDO, "Fallido"),
        (ESTADO_SIN_CONTACTO, "Sin contacto"),
        (ESTADO_INCIERTO, "Resultado incierto"),
        (ESTADO_SIN_CANAL, "Canal no disponible"),
    ]

    compra = models.ForeignKey(
        CompraRealizadaDepartamental, on_delete=models.CASCADE, related_name="avisos"
    )
    canal = models.CharField(max_length=20, choices=CANAL_CHOICES)
    destinatario = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="avisos_compra_departamental"
    )
    destino = models.CharField(
        max_length=160, blank=True, default="", help_text="Correo o teléfono realmente usado."
    )
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE, db_index=True)
    detalle = models.TextField(blank=True, default="", help_text="Motivo del último resultado.")
    referencia_externa = models.CharField(
        max_length=160, blank=True, default="", help_text="Id del mensaje devuelto por el proveedor."
    )
    intentos = models.PositiveIntegerField(default=0)
    enviado_en = models.DateTimeField(null=True, blank=True)
    ultimo_intento_en = models.DateTimeField(null=True, blank=True)
    ultimo_intento_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="avisos_compra_departamental_reintentados",
    )
    creado_en = models.DateTimeField(default=timezone.now)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["canal"]
        constraints = [
            models.UniqueConstraint(fields=["compra", "canal"], name="aviso_compra_canal_unico"),
        ]

    def __str__(self) -> str:
        return f"{self.get_canal_display()} · {self.get_estado_display()}"

    @property
    def puede_reintentarse(self) -> bool:
        """Un envío aceptado nunca se reenvía; el incierto exige reconciliar primero."""
        return self.estado in {self.ESTADO_PENDIENTE, self.ESTADO_FALLIDO, self.ESTADO_SIN_CONTACTO,
                               self.ESTADO_SIN_CANAL, self.ESTADO_INCIERTO}


class DestinoCompraDocumental(models.Model):
    """Relación tipada múltiple; no acredita recepción ni adquisición física."""
    tipo = models.CharField(max_length=6, choices=[('ACTIVO','Equipo'),('ORDEN','Trabajo')])
    item = models.ForeignKey(ItemCompraDepartamental, null=True, blank=True, on_delete=models.SET_NULL, related_name='destinos_documentales')
    activo = models.ForeignKey('activos.Activo', null=True, blank=True, on_delete=models.SET_NULL, related_name='compras_documentales')
    orden = models.ForeignKey('activos.OrdenMantenimiento', null=True, blank=True, on_delete=models.SET_NULL, related_name='compras_documentales')
    item_original_id = models.PositiveBigIntegerField(editable=False)
    destino_original_id = models.PositiveBigIntegerField(editable=False)
    autor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name='destinos_compra_confirmados')
    autor_original_id = models.PositiveBigIntegerField(editable=False)
    motivo = models.TextField()
    evidencia = models.TextField()
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-creado_en','-pk']
        constraints = [
            models.UniqueConstraint(fields=['item_original_id','tipo','destino_original_id'],name='comp_destino_par_original_unico'),
            models.CheckConstraint(check=(models.Q(tipo='ACTIVO',orden__isnull=True) | models.Q(tipo='ORDEN',activo__isnull=True)),name='comp_destino_tipo_valido'),
            models.CheckConstraint(check=~models.Q(motivo='') & ~models.Q(evidencia=''),name='comp_destino_documentado'),
            models.CheckConstraint(check=models.Q(item__isnull=True) | models.Q(item_id=models.F('item_original_id')),name='comp_destino_item_original'),
            models.CheckConstraint(check=models.Q(autor__isnull=True) | models.Q(autor_id=models.F('autor_original_id')),name='comp_destino_autor_original'),
            models.CheckConstraint(check=(models.Q(activo__isnull=True) | models.Q(activo_id=models.F('destino_original_id'))) & (models.Q(orden__isnull=True) | models.Q(orden_id=models.F('destino_original_id'))),name='comp_destino_fuente_original'),
        ]

    def clean(self):
        super().clean()
        if self.tipo not in ('ACTIVO','ORDEN') or (self.tipo=='ACTIVO' and self.orden_id) or (self.tipo=='ORDEN' and self.activo_id):
            raise ValidationError('El destino debe corresponder al tipo seleccionado.')
        destino_id = self.activo_id if self.tipo=='ACTIVO' else self.orden_id
        if (self.item_id and self.item_id != self.item_original_id) or (destino_id and destino_id != self.destino_original_id) or (self.autor_id and self.autor_id != self.autor_original_id):
            raise ValidationError('Los identificadores originales deben corresponder a las fuentes.')
        if not str(self.motivo or '').strip() or not str(self.evidencia or '').strip():
            raise ValidationError('Indica motivo y evidencia documental.')


class ProcedenciaAdquisicion(models.Model):
    """Confirmación documental de una unidad física, sin efectos operativos."""

    recepcion = models.ForeignKey(
        RecepcionItemDepartamental,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="procedencias_adquisicion",
    )
    linea = models.ForeignKey(
        LineaOrdenCompraDepartamental,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="procedencias_adquisicion",
    )
    intento = models.ForeignKey(
        IntentoCompraDepartamental,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="procedencias_adquisicion",
    )
    item = models.ForeignKey(
        ItemCompraDepartamental,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="procedencias_adquisicion",
    )
    activo = models.ForeignKey(
        "activos.Activo",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="procedencias_adquisicion",
    )
    recepcion_original_id = models.PositiveBigIntegerField(editable=False)
    linea_original_id = models.PositiveBigIntegerField(editable=False)
    intento_original_id = models.PositiveBigIntegerField(editable=False)
    item_original_id = models.PositiveBigIntegerField(editable=False)
    activo_original_id = models.PositiveBigIntegerField(editable=False)
    version_confirmada = models.PositiveIntegerField(editable=False)
    referencia_unidad = models.CharField(max_length=200)
    unidad_normalizada = models.CharField(max_length=200, editable=False)
    autor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="procedencias_adquisicion_confirmadas",
    )
    autor_original_id = models.PositiveBigIntegerField(editable=False)
    motivo = models.TextField()
    evidencia = models.TextField()
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-pk"]
        constraints = [
            models.UniqueConstraint(
                fields=["recepcion_original_id", "unidad_normalizada"],
                name="comp_proc_unidad_origen_unico",
            ),
            models.UniqueConstraint(
                fields=["recepcion_original_id", "activo_original_id"],
                name="comp_proc_equipo_origen_unico",
            ),
            models.CheckConstraint(
                check=models.Q(version_confirmada__gt=0),
                name="comp_proc_version_positiva",
            ),
            models.CheckConstraint(
                check=~models.Q(unidad_normalizada="")
                & ~models.Q(referencia_unidad="")
                & ~models.Q(motivo="")
                & ~models.Q(evidencia=""),
                name="comp_proc_documentada",
            ),
            *[
                models.CheckConstraint(
                    check=models.Q(**{f"{fuente}__isnull": True})
                    | models.Q(**{f"{fuente}_id": models.F(f"{fuente}_original_id")}),
                    name=f"comp_proc_{fuente}_original",
                )
                for fuente in (
                    "recepcion",
                    "linea",
                    "intento",
                    "item",
                    "activo",
                    "autor",
                )
            ],
        ]

    def save(self, *args, **kwargs):
        db = kwargs.get("using") or self._state.db or "default"
        if not self._state.adding or (
            self.pk is not None
            and type(self).objects.using(db).filter(pk=self.pk).exists()
        ):
            raise ValidationError("La confirmación documental es inmutable.")
        # INSERT siempre: un objeto nuevo con PK explícita no sustituye una confirmación.
        if args:
            args = (True, *args[1:])
        else:
            kwargs["force_insert"] = True
        return super().save(*args, **kwargs)

    @property
    def revision_pendiente(self):
        return (
            not all(
                (
                    self.recepcion_id,
                    self.linea_id,
                    self.intento_id,
                    self.item_id,
                    self.activo_id,
                )
            )
            or self.intento.version != self.version_confirmada
            or self.recepcion.linea_orden_id != self.linea_id
            or self.linea.intento_id != self.intento_id
            or self.linea.item_id != self.item_id
            or self.intento.item_id != self.item_id
        )
