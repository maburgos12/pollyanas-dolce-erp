from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from decimal import Decimal
from uuid import uuid4


def _folio_servicio_mantenimiento():
    return f"SM-{timezone.localdate():%y%m%d}-{uuid4().hex[:6].upper()}"


class ProveedorServicio(models.Model):
    """Talleres, técnicos y empresas de mantenimiento — separado de los proveedores de insumos."""

    nombre = models.CharField(max_length=200)
    contacto = models.CharField(max_length=120, blank=True, default="", verbose_name="Nombre del contacto")
    telefono = models.CharField(max_length=30, blank=True, default="")
    especialidad = models.CharField(max_length=120, blank=True, default="",
                                    help_text="Ej. Refrigeración, Electricidad, Mecánica general")
    notas = models.TextField(blank=True, default="")
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre"]
        verbose_name = "Proveedor de servicio"
        verbose_name_plural = "Proveedores de servicio"

    def __str__(self):
        return self.nombre


class ServicioMantenimiento(models.Model):
    """Documento económico único que puede cubrir varios equipos o instalaciones."""

    DISTRIBUCION_SIN_DESGLOSE = "SIN_DESGLOSE"
    DISTRIBUCION_REAL = "DESGLOSE_REAL"
    DISTRIBUCION_PRORRATEO = "PRORRATEO"
    DISTRIBUCION_CHOICES = [
        (DISTRIBUCION_SIN_DESGLOSE, "Sin desglose por objetivo"),
        (DISTRIBUCION_REAL, "Desglose indicado por el proveedor"),
        (DISTRIBUCION_PRORRATEO, "Prorrateo administrativo"),
    ]

    folio = models.CharField(max_length=24, unique=True, default=_folio_servicio_mantenimiento, editable=False)
    fecha_servicio = models.DateField(default=timezone.localdate)
    proveedor = models.ForeignKey(
        "maestros.Proveedor", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="servicios_mantenimiento_agrupados",
    )
    proveedor_nombre = models.CharField(max_length=200, blank=True, default="")
    sucursal_cargo = models.ForeignKey(
        "core.Sucursal", on_delete=models.PROTECT, null=True, blank=True,
        related_name="servicios_mantenimiento_cargados",
        help_text="Centro de costo del documento cuando no existe desglose por alcance.",
    )
    responsable = models.CharField(max_length=120, blank=True, default="")
    numero_documento = models.CharField(
        max_length=80, blank=True, default="", verbose_name="Factura / nota / remisión",
    )
    documento = models.FileField(
        upload_to="mantenimiento/servicios/%Y/%m/", null=True, blank=True,
        help_text="PDF o imagen de la factura, nota o remisión.",
    )
    descripcion_general = models.TextField()
    costo_total = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0"))
    metodo_distribucion = models.CharField(
        max_length=16, choices=DISTRIBUCION_CHOICES, default=DISTRIBUCION_SIN_DESGLOSE,
    )
    clave_origen = models.CharField(
        max_length=160, unique=True, null=True, blank=True,
        help_text="Clave idempotente para importaciones históricas.",
    )
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="servicios_mantenimiento_creados",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_servicio", "-id"]
        verbose_name = "Servicio de mantenimiento"
        verbose_name_plural = "Servicios de mantenimiento"
        constraints = [
            models.CheckConstraint(check=models.Q(costo_total__gte=0), name="servicio_mant_costo_no_negativo"),
        ]

    @property
    def costo_asignado(self):
        return self.detalles.aggregate(total=models.Sum("costo_asignado"))["total"] or Decimal("0")

    def __str__(self):
        return f"{self.folio} · {self.descripcion_general[:80]}"


class DetalleServicioMantenimiento(models.Model):
    """Alcance técnico; no replica el gasto del documento padre."""

    OBJETIVO_ACTIVO = "ACTIVO"
    OBJETIVO_UNIDAD = "UNIDAD"
    OBJETIVO_INSTALACION = "INSTALACION"
    OBJETIVO_CHOICES = [
        (OBJETIVO_ACTIVO, "Equipo / activo"),
        (OBJETIVO_UNIDAD, "Unidad logística"),
        (OBJETIVO_INSTALACION, "Trabajo en instalaciones"),
    ]

    servicio = models.ForeignKey(
        ServicioMantenimiento, on_delete=models.CASCADE, related_name="detalles",
    )
    tipo_objetivo = models.CharField(max_length=16, choices=OBJETIVO_CHOICES)
    activo = models.ForeignKey(
        "activos.Activo", on_delete=models.PROTECT, null=True, blank=True,
        related_name="detalles_servicio_mantenimiento",
    )
    unidad = models.ForeignKey(
        "logistica.Unidad", on_delete=models.PROTECT, null=True, blank=True,
        related_name="detalles_servicio_mantenimiento",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal", on_delete=models.PROTECT, null=True, blank=True,
        related_name="trabajos_instalaciones_mantenimiento",
    )
    instalacion_categoria = models.CharField(max_length=120, blank=True, default="")
    ubicacion = models.CharField(max_length=160, blank=True, default="")
    trabajo_realizado = models.TextField()
    costo_asignado = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    costo_estimado = models.BooleanField(default=False)
    proxima_revision = models.DateField(null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]
        verbose_name = "Alcance de servicio de mantenimiento"
        verbose_name_plural = "Alcances de servicio de mantenimiento"
        constraints = [
            models.CheckConstraint(
                check=models.Q(costo_asignado__isnull=True) | models.Q(costo_asignado__gte=0),
                name="detalle_servicio_costo_no_negativo",
            ),
            models.CheckConstraint(
                check=(
                    models.Q(
                        tipo_objetivo="ACTIVO", activo__isnull=False,
                        unidad__isnull=True, sucursal__isnull=True, instalacion_categoria="",
                    )
                    | models.Q(
                        tipo_objetivo="UNIDAD", activo__isnull=True,
                        unidad__isnull=False, sucursal__isnull=True, instalacion_categoria="",
                    )
                    | models.Q(
                        tipo_objetivo="INSTALACION", activo__isnull=True,
                        unidad__isnull=True, sucursal__isnull=False,
                        instalacion_categoria__gt="",
                    )
                ),
                name="detalle_servicio_objetivo_coherente",
            ),
        ]

    def clean(self):
        super().clean()
        errors = {}
        if self.tipo_objetivo == self.OBJETIVO_ACTIVO:
            if not self.activo_id:
                errors["activo"] = "Selecciona el equipo atendido."
            if self.unidad_id or self.sucursal_id or self.instalacion_categoria:
                errors["tipo_objetivo"] = "Un equipo no puede mezclar unidad o instalación en el mismo alcance."
        elif self.tipo_objetivo == self.OBJETIVO_UNIDAD:
            if not self.unidad_id:
                errors["unidad"] = "Selecciona la unidad atendida."
            if self.activo_id or self.sucursal_id or self.instalacion_categoria:
                errors["tipo_objetivo"] = "Una unidad no puede mezclar activo o instalación en el mismo alcance."
        elif self.tipo_objetivo == self.OBJETIVO_INSTALACION:
            if not self.sucursal_id:
                errors["sucursal"] = "Selecciona la sucursal donde se trabajó."
            if not self.instalacion_categoria:
                errors["instalacion_categoria"] = "Indica el tipo de instalación atendida."
            if self.activo_id or self.unidad_id:
                errors["tipo_objetivo"] = "Un trabajo de instalación no debe crear ni vincular un activo artificial."
        else:
            errors["tipo_objetivo"] = "Selecciona un objetivo válido."
        if errors:
            raise ValidationError(errors)

    @property
    def sucursal_efectiva(self):
        if self.tipo_objetivo == self.OBJETIVO_ACTIVO and self.activo_id:
            return self.activo.sucursal
        if self.tipo_objetivo == self.OBJETIVO_UNIDAD and self.unidad_id:
            return self.unidad.sucursal
        return self.sucursal

    @property
    def objetivo_nombre(self):
        if self.tipo_objetivo == self.OBJETIVO_ACTIVO and self.activo_id:
            return self.activo.nombre
        if self.tipo_objetivo == self.OBJETIVO_UNIDAD and self.unidad_id:
            return self.unidad.codigo
        return self.ubicacion or self.instalacion_categoria

    def __str__(self):
        return f"{self.servicio.folio} · {self.objetivo_nombre}"


class SolicitudCancelacion(models.Model):
    TIPO_FALLA = "falla"
    TIPO_UNIDAD = "unidad"
    TIPO_ORDEN = "orden"
    TIPO_CHOICES = [
        (TIPO_FALLA, "Reporte de falla"),
        (TIPO_UNIDAD, "Reporte de unidad logística"),
        (TIPO_ORDEN, "Orden de mantenimiento"),
    ]

    ESTATUS_PENDIENTE = "pendiente"
    ESTATUS_APROBADA = "aprobada"
    ESTATUS_RECHAZADA = "rechazada"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_APROBADA, "Aprobada y eliminada"),
        (ESTATUS_RECHAZADA, "Rechazada"),
    ]

    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    objeto_id = models.PositiveIntegerField()
    referencia = models.CharField(max_length=200)
    motivo = models.TextField()
    estatus = models.CharField(max_length=12, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE)
    solicitado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name="solicitudes_cancelacion",
    )
    resuelto_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cancelaciones_resueltas",
    )
    notas_resolucion = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(default=timezone.now)
    resuelto_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Solicitud de cancelación"
        verbose_name_plural = "Solicitudes de cancelación"

    def __str__(self):
        return f"{self.get_tipo_display()} #{self.objeto_id} · {self.estatus}"
