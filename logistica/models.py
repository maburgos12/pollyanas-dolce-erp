from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models, transaction
from django.db.models import Count, Q, Sum
from django.utils import timezone

from core.models import Sucursal
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

    class Meta:
        ordering = ["user__username"]
        verbose_name = "Repartidor"
        verbose_name_plural = "Repartidores"

    def __str__(self) -> str:
        return self.user.get_full_name() or self.user.username


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

    repartidor = models.ForeignKey(Repartidor, on_delete=models.PROTECT, related_name="reportes_unidad")
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
