from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.db.models import Count, Q, Sum
from django.utils import timezone

from core.models import Sucursal
from crm.models import PedidoCliente
from rrhh.services_identidad import nombre_operativo_usuario


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
    repartidor = models.ForeignKey(
        "Repartidor",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="rutas_asignadas",
    )
    acompanante = models.ForeignKey(
        "Repartidor",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="rutas_acompanadas",
    )
    acompanante_manual = models.CharField(max_length=120, blank=True, default="")
    unidad_operativa = models.ForeignKey(
        "Unidad",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="rutas_asignadas",
    )
    bitacora_salida = models.ForeignKey(
        "BitacoraSalidaLlegada",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="rutas_operativas",
    )
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PLANEADA)
    km_estimado = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal("0"))
    notas = models.TextField(blank=True, default="")
    hora_inicio_real = models.DateTimeField(null=True, blank=True)
    hora_cierre_real = models.DateTimeField(null=True, blank=True)
    cumplimiento_porcentaje = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0"))
    ruta_programada_polyline = models.TextField(blank=True, default="")
    ruta_programada_distancia_metros = models.PositiveIntegerField(default=0)
    ruta_programada_duracion_segundos = models.PositiveIntegerField(default=0)
    ruta_programada_fuente = models.CharField(max_length=20, blank=True, default="")
    ruta_programada_actualizada_en = models.DateTimeField(null=True, blank=True)

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
        indexes = [
            models.Index(fields=["estatus", "fecha_ruta"]),
            models.Index(fields=["repartidor", "estatus"]),
            models.Index(fields=["unidad_operativa", "estatus"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["repartidor"],
                condition=Q(estatus="EN_RUTA", repartidor__isnull=False),
                name="rutaentrega_unica_en_ruta_repartidor",
            ),
            models.UniqueConstraint(
                fields=["unidad_operativa"],
                condition=Q(estatus="EN_RUTA", unidad_operativa__isnull=False),
                name="rutaentrega_unica_en_ruta_unidad",
            ),
        ]

    def __str__(self) -> str:
        return self.folio or self.nombre

    def clean(self):
        super().clean()
        if self.estatus not in {self.ESTATUS_EN_RUTA, self.ESTATUS_COMPLETADA}:
            return

        errors = {}
        if not self.repartidor_id:
            errors["repartidor"] = "La ruta debe tener repartidor antes de iniciar seguimiento."
        if not self.unidad_operativa_id:
            errors["unidad_operativa"] = "La ruta debe tener unidad operativa antes de iniciar seguimiento."
        if self.pk and not self.paradas.exists():
            errors["estatus"] = "La ruta debe tener al menos una parada antes de iniciar seguimiento."
        if not self.pk:
            errors["estatus"] = "Crea la ruta como planeada y agrega paradas antes de iniciar seguimiento."
        if errors:
            raise ValidationError(errors)

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

    def recompute_route_control(self):
        paradas = self.paradas.all()
        total = paradas.count()
        if not total:
            self.cumplimiento_porcentaje = Decimal("0")
            return
        visitadas = paradas.filter(estado=ParadaRuta.ESTADO_VISITADA).count()
        self.cumplimiento_porcentaje = (Decimal(visitadas) / Decimal(total) * Decimal("100")).quantize(Decimal("0.01"))


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


class PuntoLogistico(models.Model):
    TIPO_CEDIS = "CEDIS"
    TIPO_SUCURSAL = "SUCURSAL"
    TIPO_PROVEEDOR = "PROVEEDOR"
    TIPO_TALLER = "TALLER"
    TIPO_BANCO = "BANCO"
    TIPO_AUTORIZADO = "AUTORIZADO"
    TIPO_CHOICES = [
        (TIPO_CEDIS, "CEDIS"),
        (TIPO_SUCURSAL, "Sucursal"),
        (TIPO_PROVEEDOR, "Proveedor"),
        (TIPO_TALLER, "Taller"),
        (TIPO_BANCO, "Banco"),
        (TIPO_AUTORIZADO, "Punto autorizado"),
    ]

    sucursal = models.ForeignKey(
        Sucursal,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="puntos_logisticos",
    )
    nombre = models.CharField(max_length=160)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_SUCURSAL)
    latitud = models.DecimalField(max_digits=9, decimal_places=6)
    longitud = models.DecimalField(max_digits=9, decimal_places=6)
    radio_geocerca_metros = models.PositiveIntegerField(default=80)
    activo = models.BooleanField(default=True)
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["tipo", "nombre"]
        verbose_name = "Punto logístico"
        verbose_name_plural = "Puntos logísticos"
        indexes = [
            models.Index(fields=["tipo", "activo"]),
            models.Index(fields=["sucursal", "activo"]),
        ]
        constraints = [
            models.CheckConstraint(
                check=Q(latitud__gte=Decimal("-90")) & Q(latitud__lte=Decimal("90")),
                name="puntologistico_latitud_rango",
            ),
            models.CheckConstraint(
                check=Q(longitud__gte=Decimal("-180")) & Q(longitud__lte=Decimal("180")),
                name="puntologistico_longitud_rango",
            ),
            models.CheckConstraint(
                check=Q(radio_geocerca_metros__gte=20) & Q(radio_geocerca_metros__lte=1000),
                name="puntologistico_radio_rango",
            ),
            models.CheckConstraint(
                check=~(Q(latitud=Decimal("0")) & Q(longitud=Decimal("0"))),
                name="puntologistico_no_cero_cero",
            ),
        ]

    def __str__(self) -> str:
        return self.nombre


class ParadaRuta(models.Model):
    ESTADO_PENDIENTE = "PENDIENTE"
    ESTADO_VISITADA = "VISITADA"
    ESTADO_OMITIDA = "OMITIDA"
    ESTADO_FUERA_RADIO = "FUERA_RADIO"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_VISITADA, "Visitada"),
        (ESTADO_OMITIDA, "Omitida"),
        (ESTADO_FUERA_RADIO, "Fuera de radio"),
    ]
    ENTREGA_PENDIENTE = "PENDIENTE"
    ENTREGA_ENTREGADA = "ENTREGADA"
    ENTREGA_CON_DIFERENCIA = "CON_DIFERENCIA"
    ENTREGA_NO_ENTREGADA = "NO_ENTREGADA"
    ENTREGA_ESTADO_CHOICES = [
        (ENTREGA_PENDIENTE, "Pendiente"),
        (ENTREGA_ENTREGADA, "Entregada"),
        (ENTREGA_CON_DIFERENCIA, "Con diferencia"),
        (ENTREGA_NO_ENTREGADA, "No entregada"),
    ]
    REVISION_NO_REQUERIDA = "NO_REQUERIDA"
    REVISION_PENDIENTE = "PENDIENTE"
    REVISION_AUTORIZADA = "AUTORIZADA"
    REVISION_RECHAZADA = "RECHAZADA"
    REVISION_CORREGIDA = "CORREGIDA"
    REVISION_CHOICES = [
        (REVISION_NO_REQUERIDA, "No requerida"),
        (REVISION_PENDIENTE, "Pendiente"),
        (REVISION_AUTORIZADA, "Autorizada"),
        (REVISION_RECHAZADA, "Rechazada"),
        (REVISION_CORREGIDA, "Corregida"),
    ]

    ruta = models.ForeignKey(RutaEntrega, on_delete=models.CASCADE, related_name="paradas")
    punto = models.ForeignKey(PuntoLogistico, on_delete=models.PROTECT, related_name="paradas_ruta")
    orden = models.PositiveIntegerField(default=1)
    punto_nombre_snapshot = models.CharField(max_length=160, blank=True, default="", editable=False)
    latitud_geocerca = models.DecimalField(max_digits=9, decimal_places=6, editable=False)
    longitud_geocerca = models.DecimalField(max_digits=9, decimal_places=6, editable=False)
    radio_geocerca_metros = models.PositiveIntegerField(editable=False)
    hora_estimada = models.DateTimeField(null=True, blank=True)
    hora_llegada_real = models.DateTimeField(null=True, blank=True)
    hora_salida_real = models.DateTimeField(null=True, blank=True)
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE)
    entrega_estado = models.CharField(max_length=20, choices=ENTREGA_ESTADO_CHOICES, default=ENTREGA_PENDIENTE)
    entrega_confirmada_en = models.DateTimeField(null=True, blank=True)
    entrega_confirmada_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="paradas_logistica_confirmadas",
    )
    entrega_notas = models.TextField(blank=True, default="")
    revision_entrega_estado = models.CharField(
        max_length=20,
        choices=REVISION_CHOICES,
        default=REVISION_NO_REQUERIDA,
    )
    revision_entrega_causa = models.CharField(max_length=40, blank=True)
    revision_entrega_datos = models.JSONField(default=dict, blank=True)
    revision_entrega_revisada_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="entregas_logistica_revisadas",
    )
    revision_entrega_revisada_en = models.DateTimeField(null=True, blank=True)
    revision_entrega_resolucion = models.TextField(blank=True)
    distancia_llegada_metros = models.PositiveIntegerField(null=True, blank=True)
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["ruta", "orden", "id"]
        unique_together = [("ruta", "orden")]
        verbose_name = "Parada de ruta"
        verbose_name_plural = "Paradas de ruta"
        indexes = [
            models.Index(fields=["ruta", "estado"]),
            models.Index(fields=["punto", "estado"]),
        ]

    def __str__(self) -> str:
        return f"{self.ruta.folio} · {self.orden} · {self.punto_nombre_snapshot or self.punto.nombre}"

    def save(self, *args, **kwargs):
        if self.punto_id:
            if not self.punto_nombre_snapshot:
                self.punto_nombre_snapshot = self.punto.nombre
            if self.latitud_geocerca is None:
                self.latitud_geocerca = self.punto.latitud
            if self.longitud_geocerca is None:
                self.longitud_geocerca = self.punto.longitud
            if not self.radio_geocerca_metros:
                self.radio_geocerca_metros = self.punto.radio_geocerca_metros
        super().save(*args, **kwargs)


class RutaCargaChecklist(models.Model):
    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_EN_REVISION = "EN_REVISION"
    ESTATUS_CONFIRMADA = "CONFIRMADA"
    ESTATUS_CON_INCIDENCIA = "CON_INCIDENCIA"
    ESTATUS_BLOQUEADA = "BLOQUEADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_EN_REVISION, "En revisión"),
        (ESTATUS_CONFIRMADA, "Carga confirmada"),
        (ESTATUS_CON_INCIDENCIA, "Con incidencia"),
        (ESTATUS_BLOQUEADA, "Bloqueada"),
    ]

    ruta = models.OneToOneField(RutaEntrega, on_delete=models.CASCADE, related_name="checklist_carga")
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    point_sync_job = models.ForeignKey(
        "pos_bridge.PointSyncJob",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="checklists_carga_ruta",
    )
    sincronizado_en = models.DateTimeField(null=True, blank=True)
    confirmado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="checklists_carga_confirmados",
    )
    confirmado_en = models.DateTimeField(null=True, blank=True)
    motivo_override = models.CharField(max_length=240, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-ruta__fecha_ruta", "-id"]
        verbose_name = "Checklist de carga de ruta"
        verbose_name_plural = "Checklists de carga de ruta"
        indexes = [
            models.Index(fields=["estatus", "sincronizado_en"]),
        ]

    def __str__(self) -> str:
        return f"{self.ruta.folio} · {self.get_estatus_display()}"

    @property
    def esta_completa(self) -> bool:
        if self.estatus == self.ESTATUS_CONFIRMADA:
            return True
        return self.lineas.exists() and not self.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists()


class RutaCargaChecklistLinea(models.Model):
    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_CARGADA = "CARGADA"
    ESTATUS_PARCIAL = "PARCIAL"
    ESTATUS_FALTANTE = "FALTANTE"
    ESTATUS_SOBRANTE = "SOBRANTE"
    ESTATUS_NO_APLICA = "NO_APLICA"
    ESTATUS_ZERO_EXPECTED = "ZERO_EXPECTED"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_CARGADA, "Cargada"),
        (ESTATUS_PARCIAL, "Parcial"),
        (ESTATUS_FALTANTE, "Faltante"),
        (ESTATUS_SOBRANTE, "Sobrante"),
        (ESTATUS_NO_APLICA, "No aplica"),
        (ESTATUS_ZERO_EXPECTED, "Enviado cero"),
    ]

    MOTIVO_STOCK_LIMITADO = "stock_limitado"
    MOTIVO_SIN_STOCK = "sin_stock"
    MOTIVO_PRODUCCION_NO_LISTA = "produccion_no_lista"
    MOTIVO_PRODUCTO_DANADO = "producto_danado"
    MOTIVO_CAMBIO_AUTORIZADO = "cambio_autorizado"
    MOTIVO_OTRO = "otro"
    MOTIVO_CHOICES = [
        (MOTIVO_STOCK_LIMITADO, "Stock limitado"),
        (MOTIVO_SIN_STOCK, "No hay stock"),
        (MOTIVO_PRODUCCION_NO_LISTA, "Producción aún no está lista"),
        (MOTIVO_PRODUCTO_DANADO, "Producto dañado"),
        (MOTIVO_CAMBIO_AUTORIZADO, "Cambio autorizado"),
        (MOTIVO_OTRO, "Otro"),
    ]

    checklist = models.ForeignKey(RutaCargaChecklist, on_delete=models.CASCADE, related_name="lineas")
    parada = models.ForeignKey(ParadaRuta, on_delete=models.PROTECT, related_name="lineas_carga")
    point_transfer_line = models.ForeignKey(
        "pos_bridge.PointTransferLine",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="lineas_checklist_carga",
    )
    transfer_external_id = models.CharField(max_length=40, db_index=True)
    detail_external_id = models.CharField(max_length=40, db_index=True)
    source_hash = models.CharField(max_length=64, db_index=True)
    item_code = models.CharField(max_length=80, blank=True, default="")
    item_name = models.CharField(max_length=250)
    unit = models.CharField(max_length=40, blank=True, default="")
    erp_origin_branch = models.ForeignKey(
        Sucursal,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="lineas_carga_origen",
    )
    erp_destination_branch = models.ForeignKey(
        Sucursal,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="lineas_carga_destino",
    )
    cantidad_solicitada = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0"))
    cantidad_enviada_esperada = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0"))
    cantidad_cargada = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    motivo_diferencia = models.CharField(max_length=40, choices=MOTIVO_CHOICES, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    client_event_id = models.CharField(max_length=80, blank=True, default="")
    validado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="lineas_carga_validadas",
    )
    validado_en = models.DateTimeField(null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["parada__orden", "item_name", "id"]
        verbose_name = "Línea de checklist de carga"
        verbose_name_plural = "Líneas de checklist de carga"
        indexes = [
            models.Index(fields=["checklist", "estatus"]),
            models.Index(fields=["parada", "estatus"]),
            models.Index(fields=["source_hash"]),
        ]
        constraints = [
            models.UniqueConstraint(fields=["checklist", "source_hash"], name="rutacarga_linea_source_unica"),
            models.UniqueConstraint(
                fields=["source_hash"],
                condition=~Q(source_hash=""),
                name="rutacarga_linea_source_global_unica",
            ),
            models.UniqueConstraint(
                fields=["checklist", "client_event_id"],
                condition=~Q(client_event_id=""),
                name="rutacarga_linea_evento_cliente_unico",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.checklist.ruta.folio} · {self.item_name} · {self.get_estatus_display()}"


class ParadaEntregaEvidencia(models.Model):
    TIPO_FOTO_ENTREGA = "FOTO_ENTREGA"
    TIPO_INCIDENCIA = "INCIDENCIA"
    TIPO_ENTREGA_PARCIAL = "ENTREGA_PARCIAL"
    TIPO_CONFIRMACION = "CONFIRMACION"
    TIPO_CHOICES = [
        (TIPO_FOTO_ENTREGA, "Foto de entrega"),
        (TIPO_INCIDENCIA, "Incidencia"),
        (TIPO_ENTREGA_PARCIAL, "Entrega parcial"),
        (TIPO_CONFIRMACION, "Confirmación"),
    ]

    ruta = models.ForeignKey(RutaEntrega, on_delete=models.CASCADE, related_name="evidencias_entrega")
    parada = models.ForeignKey(ParadaRuta, on_delete=models.PROTECT, related_name="evidencias_entrega")
    linea_carga = models.ForeignKey(
        RutaCargaChecklistLinea,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="evidencias_entrega",
    )
    tipo = models.CharField(max_length=30, choices=TIPO_CHOICES, default=TIPO_CONFIRMACION)
    cantidad_entregada = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)
    foto = models.ImageField(upload_to="logistica/evidencias_entrega/", null=True, blank=True)
    comentario = models.TextField(blank=True, default="")
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    precision_metros = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    client_event_id = models.CharField(max_length=80, blank=True, default="")
    capturado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="evidencias_entrega_capturadas",
    )
    capturado_en = models.DateTimeField(default=timezone.now, db_index=True)
    ip_registro = models.GenericIPAddressField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-capturado_en", "-id"]
        verbose_name = "Evidencia de entrega por parada"
        verbose_name_plural = "Evidencias de entrega por parada"
        indexes = [
            models.Index(fields=["ruta", "capturado_en"]),
            models.Index(fields=["parada", "capturado_en"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["ruta", "capturado_por", "client_event_id"],
                condition=~Q(client_event_id=""),
                name="paradaevidencia_evento_cliente_unico",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.ruta.folio} · {self.parada.punto_nombre_snapshot} · {self.get_tipo_display()}"


class UbicacionRuta(models.Model):
    ruta = models.ForeignKey(RutaEntrega, on_delete=models.CASCADE, related_name="ubicaciones")
    repartidor = models.ForeignKey("Repartidor", on_delete=models.PROTECT, related_name="ubicaciones_ruta")
    unidad = models.ForeignKey("Unidad", on_delete=models.PROTECT, related_name="ubicaciones_ruta")
    latitud = models.DecimalField(max_digits=9, decimal_places=6)
    longitud = models.DecimalField(max_digits=9, decimal_places=6)
    precision_metros = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    velocidad_kmh = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    bateria_porcentaje = models.PositiveIntegerField(null=True, blank=True)
    timestamp_dispositivo = models.DateTimeField(null=True, blank=True)
    timestamp_servidor = models.DateTimeField(default=timezone.now, db_index=True)
    ip_registro = models.GenericIPAddressField(null=True, blank=True)
    fuera_de_geocerca = models.BooleanField(default=False)

    class Meta:
        ordering = ["-timestamp_servidor", "-id"]
        verbose_name = "Ubicación de ruta"
        verbose_name_plural = "Ubicaciones de ruta"
        indexes = [
            models.Index(fields=["ruta", "timestamp_servidor"]),
            models.Index(fields=["repartidor", "timestamp_servidor"]),
            models.Index(fields=["fuera_de_geocerca", "timestamp_servidor"]),
        ]
        constraints = [
            models.CheckConstraint(
                check=Q(latitud__gte=Decimal("-90")) & Q(latitud__lte=Decimal("90")),
                name="ubicacionruta_latitud_rango",
            ),
            models.CheckConstraint(
                check=Q(longitud__gte=Decimal("-180")) & Q(longitud__lte=Decimal("180")),
                name="ubicacionruta_longitud_rango",
            ),
            models.CheckConstraint(
                check=~(Q(latitud=Decimal("0")) & Q(longitud=Decimal("0"))),
                name="ubicacionruta_no_cero_cero",
            ),
            models.CheckConstraint(
                check=Q(precision_metros__isnull=True) | Q(precision_metros__gte=Decimal("0")),
                name="ubicacionruta_precision_no_negativa",
            ),
            models.CheckConstraint(
                check=Q(velocidad_kmh__isnull=True) | Q(velocidad_kmh__gte=Decimal("0")),
                name="ubicacionruta_velocidad_no_negativa",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.ruta.folio} · {self.timestamp_servidor:%Y-%m-%d %H:%M}"


class EventoRuta(models.Model):
    TIPO_SALIDA = "SALIDA"
    TIPO_LLEGADA_GEOFENCE = "LLEGADA_GEOFENCE"
    TIPO_DESVIO = "DESVIO"
    TIPO_PARADA_LARGA = "PARADA_LARGA"
    TIPO_GPS_PERDIDO = "GPS_PERDIDO"
    TIPO_GPS_PRECISION_BAJA = "GPS_PRECISION_BAJA"
    TIPO_UBICACION_TARDIA = "UBICACION_TARDIA"
    TIPO_SALTO_IMPOSIBLE = "SALTO_IMPOSIBLE"
    TIPO_INCIDENCIA_MANUAL = "INCIDENCIA_MANUAL"
    TIPO_CIERRE = "CIERRE"
    TIPO_ENTREGA = "ENTREGA"
    TIPO_ENTREGA_EXCEPCIONAL = "ENTREGA_EXCEPCIONAL"
    TIPO_ENTREGA_AUTORIZADA = "ENTREGA_AUTORIZADA"
    TIPO_ENTREGA_RECHAZADA = "ENTREGA_RECHAZADA"
    TIPO_ENTREGA_CORREGIDA = "ENTREGA_CORREGIDA"
    TIPO_INCONSISTENCIA_ENTREGA = "INCONSISTENCIA_ENTREGA"
    TIPO_RECARGA_CEDIS = "RECARGA_CEDIS"
    TIPO_CHOICES = [
        (TIPO_SALIDA, "Salida"),
        (TIPO_LLEGADA_GEOFENCE, "Llegada a geocerca"),
        (TIPO_DESVIO, "Desvío"),
        (TIPO_PARADA_LARGA, "Parada larga"),
        (TIPO_GPS_PERDIDO, "GPS perdido"),
        (TIPO_GPS_PRECISION_BAJA, "GPS con precisión baja"),
        (TIPO_UBICACION_TARDIA, "Ubicación tardía"),
        (TIPO_SALTO_IMPOSIBLE, "Salto GPS imposible"),
        (TIPO_INCIDENCIA_MANUAL, "Incidencia manual"),
        (TIPO_CIERRE, "Cierre"),
        (TIPO_ENTREGA, "Entrega"),
        (TIPO_ENTREGA_EXCEPCIONAL, "Entrega excepcional"),
        (TIPO_ENTREGA_AUTORIZADA, "Entrega excepcional autorizada"),
        (TIPO_ENTREGA_RECHAZADA, "Entrega excepcional rechazada"),
        (TIPO_ENTREGA_CORREGIDA, "Entrega excepcional corregida"),
        (TIPO_INCONSISTENCIA_ENTREGA, "Inconsistencia de entrega"),
        (TIPO_RECARGA_CEDIS, "Recarga CEDIS"),
    ]

    SEVERIDAD_INFO = "info"
    SEVERIDAD_OK = "ok"
    SEVERIDAD_ALERTA = "alerta"
    SEVERIDAD_CRITICA = "critica"
    SEVERIDAD_CHOICES = [
        (SEVERIDAD_INFO, "Informativo"),
        (SEVERIDAD_OK, "Cumplimiento"),
        (SEVERIDAD_ALERTA, "Alerta"),
        (SEVERIDAD_CRITICA, "Crítica"),
    ]

    ruta = models.ForeignKey(RutaEntrega, on_delete=models.CASCADE, related_name="eventos")
    parada = models.ForeignKey(ParadaRuta, on_delete=models.SET_NULL, null=True, blank=True, related_name="eventos")
    ubicacion = models.ForeignKey(UbicacionRuta, on_delete=models.SET_NULL, null=True, blank=True, related_name="eventos")
    tipo = models.CharField(max_length=30, choices=TIPO_CHOICES)
    severidad = models.CharField(max_length=10, choices=SEVERIDAD_CHOICES, default=SEVERIDAD_INFO)
    descripcion = models.TextField()
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    distancia_metros = models.PositiveIntegerField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    clave_auditoria = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        unique=True,
        db_index=True,
        editable=False,
    )
    REVISION_ALERTA_PENDIENTE = "PENDIENTE"
    REVISION_ALERTA_RESUELTA = "RESUELTA"
    REVISION_ALERTA_CHOICES = [
        (REVISION_ALERTA_PENDIENTE, "Pendiente"),
        (REVISION_ALERTA_RESUELTA, "Resuelta"),
    ]
    revision_alerta_estado = models.CharField(
        max_length=15,
        choices=REVISION_ALERTA_CHOICES,
        default=REVISION_ALERTA_PENDIENTE,
    )
    revision_alerta_motivo = models.TextField(blank=True, default="")
    revision_alerta_resuelta_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="alertas_logistica_resueltas",
    )
    revision_alerta_resuelta_en = models.DateTimeField(null=True, blank=True)
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="eventos_ruta_creados",
    )
    creado_en = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        verbose_name = "Evento de ruta"
        verbose_name_plural = "Eventos de ruta"
        indexes = [
            models.Index(fields=["ruta", "tipo", "creado_en"]),
            models.Index(fields=["severidad", "creado_en"]),
        ]
        constraints = [
            models.CheckConstraint(
                check=Q(latitud__isnull=True) | (Q(latitud__gte=Decimal("-90")) & Q(latitud__lte=Decimal("90"))),
                name="eventoruta_latitud_rango",
            ),
            models.CheckConstraint(
                check=Q(longitud__isnull=True) | (Q(longitud__gte=Decimal("-180")) & Q(longitud__lte=Decimal("180"))),
                name="eventoruta_longitud_rango",
            ),
            models.CheckConstraint(
                check=Q(latitud__isnull=True) | Q(longitud__isnull=True) | ~(Q(latitud=Decimal("0")) & Q(longitud=Decimal("0"))),
                name="eventoruta_no_cero_cero",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.ruta.folio} · {self.get_tipo_display()}"


class AuditoriaEntregaCursor(models.Model):
    """Punto durable del ultimo dia operativo auditado completamente."""

    clave = models.CharField(max_length=40, unique=True, default="entregas_ruta")
    ultima_fecha_exitosa = models.DateField(null=True, blank=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"{self.clave}: {self.ultima_fecha_exitosa or 'sin ejecutar'}"


class Unidad(models.Model):
    codigo = models.CharField(max_length=40, unique=True)
    descripcion = models.CharField(max_length=180)
    sucursal = models.ForeignKey(Sucursal, on_delete=models.PROTECT, related_name="unidades_logistica")
    placa = models.CharField(max_length=30, blank=True, default="")
    color = models.CharField(max_length=40, null=True, blank=True)
    modelo = models.CharField(max_length=40, null=True, blank=True)
    marca = models.CharField(max_length=80, null=True, blank=True)
    activa = models.BooleanField(default=True)
    folio_consecutivo = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["codigo"]
        verbose_name = "Unidad logística"
        verbose_name_plural = "Unidades logísticas"

    def __str__(self) -> str:
        return f"{self.codigo} · {self.placa or self.descripcion}"


class Repartidor(models.Model):
    TIPO_EMPLEADO_DOLCE = "empleado_dolce"
    TIPO_EMPLEADO_CONDUCTOR_OCASIONAL = "empleado_conductor_ocasional"
    TIPO_EXTERNO_AUTORIZADO = "externo_autorizado"
    TIPO_CUENTA_TECNICA = "cuenta_tecnica"
    TIPO_IDENTIDAD_CHOICES = [
        (TIPO_EMPLEADO_DOLCE, "Empleado Dolce"),
        (TIPO_EMPLEADO_CONDUCTOR_OCASIONAL, "Empleado conductor ocasional"),
        (TIPO_EXTERNO_AUTORIZADO, "Externo autorizado"),
        (TIPO_CUENTA_TECNICA, "Cuenta técnica"),
    ]

    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="repartidor_logistica")
    unidad_asignada = models.ForeignKey(
        Unidad,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="repartidores_asignados",
    )
    telefono = models.CharField(max_length=30, blank=True, default="")
    sucursal = models.ForeignKey(Sucursal, on_delete=models.PROTECT, related_name="repartidores_logistica")
    numero_licencia = models.CharField(max_length=60, blank=True, default="")
    licencia_expedicion = models.DateField(null=True, blank=True)
    licencia_expiracion = models.DateField(null=True, blank=True)
    archivo_licencia = models.FileField(upload_to="licencias_repartidores/", null=True, blank=True)
    tipo_identidad = models.CharField(
        max_length=30,
        choices=TIPO_IDENTIDAD_CHOICES,
        default=TIPO_EMPLEADO_DOLCE,
    )
    empresa_externa = models.CharField(max_length=160, blank=True, default="")
    motivo_autorizacion = models.CharField(max_length=240, blank=True, default="")
    autorizado_por = models.CharField(max_length=160, blank=True, default="")
    notas_identidad = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["user__username"]
        verbose_name = "Repartidor"
        verbose_name_plural = "Repartidores"

    def __str__(self) -> str:
        return nombre_operativo_usuario(self.user)

    @property
    def es_externo_autorizado(self) -> bool:
        return self.tipo_identidad == self.TIPO_EXTERNO_AUTORIZADO


class ReporteUnidad(models.Model):
    TIPO_FALLA = "falla"
    TIPO_MANTENIMIENTO = "mantenimiento"
    TIPO_ACCIDENTE = "accidente"
    TIPO_LLANTA = "llanta"
    TIPO_DOCUMENTOS = "documentos"
    TIPO_OTRO = "otro"
    TIPO_CHOICES = [
        (TIPO_FALLA, "Falla mecánica"),
        (TIPO_MANTENIMIENTO, "Mantenimiento preventivo"),
        (TIPO_ACCIDENTE, "Accidente"),
        (TIPO_LLANTA, "Llanta"),
        (TIPO_DOCUMENTOS, "Documentos"),
        (TIPO_OTRO, "Otro"),
    ]

    SEVERIDAD_INFORMATIVO = "informativo"
    SEVERIDAD_URGENTE = "urgente"
    SEVERIDAD_CRITICO = "critico"
    SEVERIDAD_CHOICES = [
        (SEVERIDAD_INFORMATIVO, "Informativo"),
        (SEVERIDAD_URGENTE, "Urgente"),
        (SEVERIDAD_CRITICO, "Crítico"),
    ]

    ESTATUS_ABIERTO = "abierto"
    ESTATUS_EN_PROCESO = "en_proceso"
    ESTATUS_PROGRAMADO = "programado"
    ESTATUS_CERRADO = "cerrado"
    ESTATUS_CHOICES = [
        (ESTATUS_ABIERTO, "Abierto"),
        (ESTATUS_EN_PROCESO, "En proceso"),
        (ESTATUS_PROGRAMADO, "Programado"),
        (ESTATUS_CERRADO, "Cerrado"),
    ]

    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT, null=True, blank=True, related_name="reportes_unidad")
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="reportes")
    tipo = models.CharField(max_length=30, choices=TIPO_CHOICES)
    severidad = models.CharField(max_length=20, choices=SEVERIDAD_CHOICES, default=SEVERIDAD_INFORMATIVO)
    descripcion = models.TextField()
    foto = models.ImageField(upload_to="logistica/reportes/", null=True, blank=True)
    kilometraje = models.PositiveIntegerField(null=True, blank=True)
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    ip_reporte = models.GenericIPAddressField(null=True, blank=True)
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_ABIERTO)
    fecha_reporte = models.DateTimeField(auto_now_add=True)
    fecha_cierre = models.DateTimeField(null=True, blank=True)
    asignado_a = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reportes_logistica_asignados",
    )
    proveedor_servicio = models.CharField(max_length=180, blank=True, default="")
    fecha_servicio_programado = models.DateTimeField(null=True, blank=True)
    costo_servicio = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    notas_compras = models.TextField(blank=True, default="")
    notificacion_escalada = models.BooleanField(default=False)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_reporte", "-id"]
        verbose_name = "Reporte de unidad"
        verbose_name_plural = "Reportes de unidad"
        indexes = [
            models.Index(fields=["estatus", "severidad", "fecha_reporte"]),
            models.Index(fields=["unidad", "fecha_reporte"]),
        ]

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.get_tipo_display()} · {self.get_estatus_display()}"

    def save(self, *args, **kwargs):
        previous_status = None
        if self.pk:
            previous_status = type(self).objects.filter(pk=self.pk).values_list("estatus", flat=True).first()
        closing = self.estatus == self.ESTATUS_CERRADO and previous_status != self.ESTATUS_CERRADO
        reopening = previous_status == self.ESTATUS_CERRADO and self.estatus != self.ESTATUS_CERRADO
        if closing and self.fecha_cierre is None:
            self.fecha_cierre = timezone.now()
        elif reopening:
            self.fecha_cierre = None
        if closing or reopening:
            update_fields = kwargs.get("update_fields")
            if update_fields is not None:
                kwargs["update_fields"] = set(update_fields) | {"fecha_cierre"}
        super().save(*args, **kwargs)


class ReporteUnidadReafirmacion(models.Model):
    reporte = models.ForeignKey(ReporteUnidad, on_delete=models.CASCADE, related_name="reafirmaciones")
    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT, related_name="reafirmaciones_reporte")
    comentario = models.TextField(blank=True)
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    ip_registro = models.GenericIPAddressField(null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        verbose_name = "Reafirmación de reporte"
        verbose_name_plural = "Reafirmaciones de reportes"
        indexes = [
            models.Index(fields=["reporte", "creado_en"]),
            models.Index(fields=["repartidor", "creado_en"]),
        ]

    def __str__(self) -> str:
        return f"Reporte {self.reporte_id} reafirmado por {self.repartidor}"


class BitacoraRepartidor(models.Model):
    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT, related_name="bitacoras")
    fecha = models.DateField(default=timezone.localdate)
    km_inicio = models.PositiveIntegerField()
    km_fin = models.PositiveIntegerField(null=True, blank=True)
    novedades = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "-id"]
        unique_together = [("repartidor", "fecha")]
        verbose_name = "Bitácora de repartidor"
        verbose_name_plural = "Bitácoras de repartidores"

    def __str__(self) -> str:
        return f"{self.repartidor} · {self.fecha:%Y-%m-%d}"


class BitacoraSalidaLlegada(models.Model):
    NIVEL_GAS = [
        ("vacio", "Vacío"),
        ("1/4", "1/4"),
        ("1/2", "1/2"),
        ("3/4", "3/4"),
        ("lleno", "Lleno"),
    ]

    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT)
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT)
    fecha = models.DateField(auto_now_add=True)
    folio = models.CharField(max_length=30, editable=False)
    hora_salida = models.DateTimeField(null=True, blank=True)
    km_salida = models.PositiveIntegerField()
    nivel_gas_salida = models.CharField(max_length=10, choices=NIVEL_GAS)
    foto_tablero_salida = models.ImageField(upload_to="bitacora/")
    hora_llegada = models.DateTimeField(null=True, blank=True)
    km_llegada = models.PositiveIntegerField(null=True, blank=True)
    nivel_gas_llegada = models.CharField(max_length=10, choices=NIVEL_GAS, blank=True)
    foto_tablero_llegada = models.ImageField(upload_to="bitacora/", null=True, blank=True)
    litros_cargados = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    costo_combustible = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    foto_ticket_combustible = models.ImageField(upload_to="bitacora/combustible/", null=True, blank=True)
    cerrada = models.BooleanField(default=False)
    ip_registro = models.GenericIPAddressField(null=True)
    latitud_salida = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud_salida = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)

    class Meta:
        ordering = ["-fecha", "-hora_salida"]
        verbose_name = "Bitácora salida/llegada"
        verbose_name_plural = "Bitácoras salida/llegada"

    def __str__(self) -> str:
        return f"{self.repartidor} · {self.unidad.codigo} · {self.fecha:%Y-%m-%d}"

    def save(self, *args, **kwargs):
        if not self.pk:
            with transaction.atomic():
                if not self.hora_salida:
                    self.hora_salida = timezone.now()
                unidad = Unidad.objects.select_for_update().get(pk=self.unidad_id)
                unidad.folio_consecutivo += 1
                unidad.save(update_fields=["folio_consecutivo"])
                self.folio = f"{unidad.codigo}-{unidad.folio_consecutivo:04d}"
                return super().save(*args, **kwargs)
        return super().save(*args, **kwargs)


class CargaCombustibleUnidad(models.Model):
    AUDITORIA_PENDIENTE = "pendiente"
    AUDITORIA_OK = "ok"
    AUDITORIA_REVISION = "revision"
    AUDITORIA_ALTO_RIESGO = "alto_riesgo"
    AUDITORIA_ERROR = "error"
    AUDITORIA_CHOICES = [
        (AUDITORIA_PENDIENTE, "Pendiente"),
        (AUDITORIA_OK, "OK"),
        (AUDITORIA_REVISION, "Revisar"),
        (AUDITORIA_ALTO_RIESGO, "Alto riesgo"),
        (AUDITORIA_ERROR, "Error IA"),
    ]

    bitacora = models.ForeignKey(
        BitacoraSalidaLlegada,
        on_delete=models.PROTECT,
        related_name="cargas_combustible",
    )
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="cargas_combustible")
    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT, related_name="cargas_combustible")
    litros = models.DecimalField(max_digits=7, decimal_places=2)
    importe_total = models.DecimalField(max_digits=10, decimal_places=2)
    nivel_gas_despues = models.CharField(max_length=10, choices=BitacoraSalidaLlegada.NIVEL_GAS, blank=True)
    foto_ticket = models.ImageField(upload_to="bitacora/combustible/")
    fecha_registro = models.DateTimeField(auto_now_add=True)
    ip_registro = models.GenericIPAddressField(null=True)
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    ticket_sha256 = models.CharField(max_length=64, blank=True, db_index=True)
    auditoria_estado = models.CharField(max_length=20, choices=AUDITORIA_CHOICES, default=AUDITORIA_PENDIENTE)
    auditoria_score = models.PositiveSmallIntegerField(default=0)
    auditoria_motivos = models.JSONField(default=list, blank=True)
    auditoria_detalle = models.JSONField(default=dict, blank=True)
    auditoria_analizada_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-fecha_registro"]
        verbose_name = "Carga de combustible"
        verbose_name_plural = "Cargas de combustible"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.litros} L · ${self.importe_total}"


class InspeccionVehiculo(models.Model):
    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT)
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT)
    fecha = models.DateTimeField(auto_now_add=True)
    km_entrada = models.PositiveIntegerField()
    km_salida = models.PositiveIntegerField(null=True, blank=True)
    nivel_gas_entrada = models.CharField(max_length=10, choices=BitacoraSalidaLlegada.NIVEL_GAS)
    nivel_gas_salida = models.CharField(max_length=10, choices=BitacoraSalidaLlegada.NIVEL_GAS, blank=True)
    ext_llanta_refaccion = models.BooleanField(default=False)
    ext_gato = models.BooleanField(default=False)
    ext_llave_rueda = models.BooleanField(default=False)
    ext_rines = models.BooleanField(default=False)
    ext_aire_neumaticos = models.BooleanField(default=False)
    ext_claxon = models.BooleanField(default=False)
    ext_limpiaparabrisas = models.BooleanField(default=False)
    ext_luces = models.BooleanField(default=False)
    ext_direccionales = models.BooleanField(default=False)
    ext_control_llave = models.BooleanField(default=False)
    ext_antena = models.BooleanField(default=False)
    ext_tapon_gas = models.BooleanField(default=False)
    int_espejos = models.BooleanField(default=False)
    int_encendedor = models.BooleanField(default=False)
    int_luz_interior = models.BooleanField(default=False)
    int_ac = models.BooleanField(default=False)
    int_radio = models.BooleanField(default=False)
    int_botones_radio = models.BooleanField(default=False)
    int_rejillas_ac = models.BooleanField(default=False)
    int_tapetes = models.BooleanField(default=False)
    int_cinturones = models.BooleanField(default=False)
    int_tarjeta_circulacion = models.BooleanField(default=False)
    niv_aceite = models.BooleanField(default=False)
    niv_frenos = models.BooleanField(default=False)
    niv_bateria = models.BooleanField(default=False)
    niv_agua_limpiadores = models.BooleanField(default=False)
    niv_agua_radiador = models.BooleanField(default=False)
    est_asientos_delanteros = models.BooleanField(default=False)
    est_asientos_traseros = models.BooleanField(default=False)
    est_cajuela = models.BooleanField(default=False)
    est_tablero = models.BooleanField(default=False)
    est_panel_puertas = models.BooleanField(default=False)
    est_visera = models.BooleanField(default=False)
    tiene_golpes = models.BooleanField(default=False)
    descripcion_golpes = models.TextField(blank=True)
    foto_golpes = models.ImageField(upload_to="inspecciones/", null=True, blank=True)
    observaciones = models.TextField(blank=True)
    ip_registro = models.GenericIPAddressField(null=True)
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)

    class Meta:
        ordering = ["-fecha"]
        verbose_name = "Inspección de vehículo"
        verbose_name_plural = "Inspecciones de vehículo"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.repartidor} · {self.fecha:%Y-%m-%d %H:%M}"


class InspeccionDiaria(models.Model):
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="inspecciones_diarias")
    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT, related_name="inspecciones_diarias")
    fecha = models.DateField(auto_now_add=True)
    hora = models.DateTimeField(auto_now_add=True)

    aceite_ok = models.BooleanField(default=False)
    refrigerante_ok = models.BooleanField(default=False)
    liquido_frenos_ok = models.BooleanField(default=False)
    limpiaparabrisas_ok = models.BooleanField(default=False)
    presion_llantas_ok = models.BooleanField(default=False)
    desgaste_llantas_ok = models.BooleanField(default=False)
    luces_ok = models.BooleanField(default=False)
    escobillas_ok = models.BooleanField(default=False)
    bateria_ok = models.BooleanField(default=False)
    tablero_ok = models.BooleanField(default=False)
    documentos_ok = models.BooleanField(default=False)
    licencia_ok = models.BooleanField(default=False)
    kit_emergencia_ok = models.BooleanField(default=False)

    observaciones = models.TextField(blank=True)
    tiene_fallas = models.BooleanField(default=False)
    reporte_generado = models.ForeignKey(
        "ReporteUnidad",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="inspeccion_origen",
    )
    ip_registro = models.GenericIPAddressField(null=True)
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)

    class Meta:
        unique_together = [("unidad", "fecha")]
        ordering = ["-hora"]
        verbose_name = "Inspección diaria"
        verbose_name_plural = "Inspecciones diarias"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.fecha:%Y-%m-%d}"


class DocumentoUnidad(models.Model):
    TIPO_TARJETA_CIRCULACION = "tarjeta_circulacion"
    TIPO_SEGURO = "seguro"
    TIPO_OTRO = "otro"
    TIPO_CHOICES = [
        (TIPO_TARJETA_CIRCULACION, "Tarjeta de circulación"),
        (TIPO_SEGURO, "Seguro vehicular"),
        (TIPO_OTRO, "Otro"),
    ]

    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="documentos")
    tipo = models.CharField(max_length=30, choices=TIPO_CHOICES)
    descripcion = models.CharField(max_length=100, blank=True)
    aseguradora = models.CharField(max_length=100, blank=True)
    archivo = models.FileField(upload_to="documentos_unidad/")
    fecha_emision = models.DateField(null=True, blank=True)
    fecha_vencimiento = models.DateField()
    vigente = models.BooleanField(default=True)
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True)
    fecha_registro = models.DateTimeField(auto_now_add=True)
    notas = models.TextField(blank=True)

    class Meta:
        ordering = ["-fecha_vencimiento"]
        verbose_name = "Documento de unidad"
        verbose_name_plural = "Documentos de unidades"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.get_tipo_display()} · {self.fecha_vencimiento:%Y-%m-%d}"


class TipoServicioUnidad(models.Model):
    INTERVALO_KM = "km"
    INTERVALO_TIEMPO = "tiempo"
    INTERVALO_AMBOS = "ambos"
    INTERVALO_CHOICES = [
        (INTERVALO_KM, "Por kilómetros"),
        (INTERVALO_TIEMPO, "Por tiempo"),
        (INTERVALO_AMBOS, "Kilómetros o tiempo (lo que ocurra primero)"),
    ]

    nombre = models.CharField(max_length=100)
    tipo_intervalo = models.CharField(max_length=10, choices=INTERVALO_CHOICES)
    intervalo_km = models.PositiveIntegerField(null=True, blank=True)
    intervalo_meses = models.PositiveIntegerField(null=True, blank=True)
    aplica_todas_unidades = models.BooleanField(default=True)
    activo = models.BooleanField(default=True)
    notas = models.TextField(blank=True)

    class Meta:
        ordering = ["nombre"]
        verbose_name = "Tipo de servicio"
        verbose_name_plural = "Tipos de servicio"

    def __str__(self) -> str:
        return self.nombre


class ServicioRealizadoUnidad(models.Model):
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="servicios")
    tipo_servicio = models.ForeignKey(TipoServicioUnidad, on_delete=models.PROTECT)
    fecha_servicio = models.DateField()
    km_al_servicio = models.PositiveIntegerField(null=True, blank=True)
    proveedor = models.CharField(max_length=100, blank=True)
    costo = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    archivo_factura = models.FileField(upload_to="servicios_unidad/", null=True, blank=True)
    notas = models.TextField(blank=True)
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True)
    fecha_registro = models.DateTimeField(auto_now_add=True)
    proxima_fecha = models.DateField(null=True, blank=True)
    proximos_km = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        ordering = ["-fecha_servicio"]
        verbose_name = "Servicio realizado"
        verbose_name_plural = "Servicios realizados"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.tipo_servicio} · {self.fecha_servicio:%Y-%m-%d}"

    def save(self, *args, **kwargs):
        if self.tipo_servicio_id:
            if self.tipo_servicio.intervalo_meses and self.fecha_servicio:
                from dateutil.relativedelta import relativedelta

                self.proxima_fecha = self.fecha_servicio + relativedelta(months=self.tipo_servicio.intervalo_meses)
            if self.tipo_servicio.intervalo_km and self.km_al_servicio:
                self.proximos_km = self.km_al_servicio + self.tipo_servicio.intervalo_km
        super().save(*args, **kwargs)


class LavadoUnidad(models.Model):
    TIPO_EXTERIOR = "exterior"
    TIPO_INTERIOR = "interior"
    TIPO_CAJA_REFRIGERADA = "caja_refrigerada"
    TIPO_COMPLETO = "completo"
    TIPO_LAVADO_CHOICES = [
        (TIPO_EXTERIOR, "Exterior"),
        (TIPO_INTERIOR, "Interior"),
        (TIPO_CAJA_REFRIGERADA, "Caja refrigerada"),
        (TIPO_COMPLETO, "Completo"),
    ]

    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="lavados")
    fecha = models.DateField()
    tipo_lavado = models.CharField(max_length=30, choices=TIPO_LAVADO_CHOICES, default=TIPO_COMPLETO)
    lavado_exterior = models.BooleanField(default=False)
    lavado_interior = models.BooleanField(default=False)
    lavado_caja_refrigerada = models.BooleanField(default=False)
    costo = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    foto_evidencia = models.ImageField(upload_to="lavados_unidad/", null=True, blank=True)
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True)
    fecha_registro = models.DateTimeField(auto_now_add=True)
    ip_registro = models.GenericIPAddressField(null=True, blank=True)
    latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    notas = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ["-fecha"]
        verbose_name = "Lavado de unidad"
        verbose_name_plural = "Lavados de unidades"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.fecha:%Y-%m-%d}"

    @property
    def partes_lavadas(self) -> list[str]:
        partes = []
        if self.lavado_exterior:
            partes.append("Exterior")
        if self.lavado_interior:
            partes.append("Interior")
        if self.lavado_caja_refrigerada:
            partes.append("Caja refrigerada")
        if partes:
            return partes
        legacy = {
            self.TIPO_EXTERIOR: ["Exterior"],
            self.TIPO_INTERIOR: ["Interior"],
            self.TIPO_CAJA_REFRIGERADA: ["Caja refrigerada"],
            self.TIPO_COMPLETO: ["Exterior", "Interior", "Caja refrigerada"],
        }
        return legacy.get(self.tipo_lavado, [])

    @property
    def partes_lavadas_display(self) -> str:
        return ", ".join(self.partes_lavadas) or "-"


class ReparacionUnidad(models.Model):
    unidad = models.ForeignKey(Unidad, on_delete=models.PROTECT, related_name="reparaciones")
    reporte_origen = models.ForeignKey(
        "ReporteUnidad",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reparaciones",
    )
    fecha_ingreso = models.DateField()
    fecha_entrega = models.DateField(null=True, blank=True)
    descripcion_falla = models.TextField()
    descripcion_reparacion = models.TextField(blank=True)
    proveedor = models.CharField(max_length=100, blank=True)
    costo_total = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    archivo_factura = models.FileField(upload_to="reparaciones_unidad/", null=True, blank=True)
    foto_nota = models.ImageField(upload_to="reparaciones_unidad/", null=True, blank=True)
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True)
    fecha_registro = models.DateTimeField(auto_now_add=True)
    notas = models.TextField(blank=True)

    class Meta:
        ordering = ["-fecha_ingreso"]
        verbose_name = "Reparación de unidad"
        verbose_name_plural = "Reparaciones de unidades"

    def __str__(self) -> str:
        return f"{self.unidad.codigo} · {self.fecha_ingreso:%Y-%m-%d}"


class ConfigAlertaFlota(models.Model):
    TIPO_DOCUMENTO_VENCIMIENTO = "documento_vencimiento"
    TIPO_SERVICIO_PROXIMO = "servicio_proximo"
    TIPO_LAVADO_PENDIENTE = "lavado_pendiente"
    TIPO_ALERTA = [
        (TIPO_DOCUMENTO_VENCIMIENTO, "Vencimiento de documento"),
        (TIPO_SERVICIO_PROXIMO, "Servicio próximo"),
        (TIPO_LAVADO_PENDIENTE, "Lavado pendiente"),
    ]

    tipo = models.CharField(max_length=30, choices=TIPO_ALERTA, unique=True)
    destinatarios = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name="alertas_flota")
    activa = models.BooleanField(default=True)
    dias_anticipacion_1 = models.PositiveIntegerField(default=30)
    dias_anticipacion_2 = models.PositiveIntegerField(default=15)
    dias_anticipacion_3 = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["tipo"]
        verbose_name = "Configuración de alerta de flota"
        verbose_name_plural = "Configuraciones de alertas de flota"

    def __str__(self) -> str:
        return self.get_tipo_display()


class EntregaEcommerce(models.Model):
    """Puente hacia una entrega a domicilio de la tienda en línea (pollyanas-ecommerce).

    No reutiliza EntregaRuta porque esa tabla está atada a PedidoCliente (CRM local);
    un pedido de la tienda en línea vive en el otro sistema, referenciado aquí solo por id.
    """

    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_ENTREGADA = "ENTREGADA"
    ESTATUS_CANCELADA = "CANCELADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_ENTREGADA, "Entregada"),
        (ESTATUS_CANCELADA, "Cancelada"),
    ]

    ruta = models.ForeignKey(
        RutaEntrega,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="entregas_ecommerce",
    )
    repartidor = models.ForeignKey(
        "Repartidor",
        on_delete=models.PROTECT,
        related_name="entregas_ecommerce",
    )
    unidad = models.ForeignKey(
        "Unidad",
        on_delete=models.PROTECT,
        related_name="entregas_ecommerce",
    )
    ecommerce_order_id = models.PositiveIntegerField()
    ecommerce_order_number = models.CharField(max_length=60, blank=True, default="")
    ecommerce_task_id = models.PositiveIntegerField()
    cliente_nombre = models.CharField(max_length=160, blank=True, default="")
    direccion = models.CharField(max_length=255, blank=True, default="")
    driver_access_token = models.CharField(max_length=64)
    driver_url = models.URLField(max_length=300)
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Entrega e-commerce"
        verbose_name_plural = "Entregas e-commerce"
        constraints = [
            models.UniqueConstraint(
                fields=["ecommerce_order_id"],
                condition=Q(estatus="PENDIENTE"),
                name="entregaecommerce_unica_pendiente_por_pedido",
            ),
        ]

    def __str__(self) -> str:
        return f"#{self.ecommerce_order_number or self.ecommerce_order_id} · {nombre_operativo_usuario(self.repartidor.user)}"


class SolicitudDomicilio(models.Model):
    """Servicio a domicilio capturado por un canal distinto a la tienda en línea
    (llamada, WhatsApp, redes sociales). Vive aparte de EntregaEcommerce porque no
    hay un pedido de e-commerce detrás; opcionalmente se liga a un PedidoCliente
    del CRM si ya existe ese registro."""

    CANAL_MOSTRADOR = PedidoCliente.CANAL_MOSTRADOR
    CANAL_WHATSAPP = PedidoCliente.CANAL_WHATSAPP
    CANAL_TELEFONO = PedidoCliente.CANAL_TELEFONO
    CANAL_WEB = PedidoCliente.CANAL_WEB
    CANAL_OTRO = PedidoCliente.CANAL_OTRO
    CANAL_CHOICES = PedidoCliente.CANAL_CHOICES

    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_ASIGNADO = "ASIGNADO"
    ESTATUS_EN_RUTA = "EN_RUTA"
    ESTATUS_ENTREGADO = "ENTREGADO"
    ESTATUS_CANCELADO = "CANCELADO"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_ASIGNADO, "Asignado"),
        (ESTATUS_EN_RUTA, "En ruta"),
        (ESTATUS_ENTREGADO, "Entregado"),
        (ESTATUS_CANCELADO, "Cancelado"),
    ]

    pedido_cliente = models.ForeignKey(
        PedidoCliente,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitudes_domicilio",
    )
    cliente_nombre = models.CharField(max_length=160)
    cliente_telefono = models.CharField(max_length=30, blank=True, default="")
    direccion = models.CharField(max_length=255)
    canal_origen = models.CharField(max_length=20, choices=CANAL_CHOICES, default=CANAL_TELEFONO)
    canal_detalle = models.CharField(max_length=60, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    repartidor = models.ForeignKey(
        "Repartidor",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitudes_domicilio",
    )
    unidad = models.ForeignKey(
        "Unidad",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitudes_domicilio",
    )
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitudes_domicilio_creadas",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    asignado_en = models.DateTimeField(null=True, blank=True)
    entregado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Solicitud de domicilio"
        verbose_name_plural = "Solicitudes de domicilio"

    def __str__(self) -> str:
        return f"{self.cliente_nombre} · {self.get_canal_origen_display()}"
