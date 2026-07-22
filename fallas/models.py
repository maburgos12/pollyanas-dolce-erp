from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone


class CategoriaFalla(models.Model):
    """Catálogo maestro de categorías de falla."""

    TIPO_EQUIPO = "equipo"
    TIPO_INSTALACION = "instalacion"
    TIPO_MOBILIARIO = "mobiliario"
    TIPO_OTRO = "otro"
    TIPOS = [
        (TIPO_EQUIPO, "Equipo y Maquinaria"),
        (TIPO_INSTALACION, "Instalaciones"),
        (TIPO_MOBILIARIO, "Mobiliario y Decoración"),
        (TIPO_OTRO, "Otro"),
    ]

    nombre = models.CharField(max_length=100)
    tipo = models.CharField(max_length=20, choices=TIPOS)
    activo = models.BooleanField(default=True)
    orden = models.PositiveSmallIntegerField(default=0)

    class Meta:
        ordering = ["orden", "nombre"]
        verbose_name = "Categoría de falla"
        verbose_name_plural = "Categorías de fallas"

    def __str__(self):
        return f"{self.get_tipo_display()} > {self.nombre}"


class ReporteFalla(models.Model):
    """Reporte principal de falla creado desde la PWA móvil."""

    PRIORIDAD_BAJA = "baja"
    PRIORIDAD_MEDIA = "media"
    PRIORIDAD_ALTA = "alta"
    PRIORIDAD_CRITICA = "critica"
    PRIORIDAD = [
        (PRIORIDAD_BAJA, "Baja"),
        (PRIORIDAD_MEDIA, "Media"),
        (PRIORIDAD_ALTA, "Alta"),
        (PRIORIDAD_CRITICA, "Crítica - Operación detenida"),
    ]

    ESTATUS_ABIERTO = "abierto"
    ESTATUS_REVISION = "en_revision"
    ESTATUS_PROCESO = "en_proceso"
    ESTATUS_RESUELTO = "resuelto"
    ESTATUS_CERRADO = "cerrado"
    ESTATUS_CANCELADO = "cancelado"
    ESTATUS = [
        (ESTATUS_ABIERTO, "Abierto"),
        (ESTATUS_REVISION, "En revisión"),
        (ESTATUS_PROCESO, "En proceso"),
        (ESTATUS_RESUELTO, "Finalizado"),
        (ESTATUS_CERRADO, "Cerrado / validado"),
        (ESTATUS_CANCELADO, "Cancelado"),
    ]

    AREA_VENTAS = "ventas"
    AREA_PRODUCCION = "produccion"
    AREA_GENERAL = "general"
    AREAS = [
        (AREA_VENTAS, "Ventas"),
        (AREA_PRODUCCION, "Producción"),
        (AREA_GENERAL, "General"),
    ]
    OBJETIVO_EQUIPO = "EQUIPO"
    OBJETIVO_INSTALACION = "INSTALACION"
    OBJETIVO_CHOICES = [
        (OBJETIVO_EQUIPO, "Equipo registrado"),
        (OBJETIVO_INSTALACION, "Instalación de sucursal"),
    ]

    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="fallas")
    activo_relacionado = models.ForeignKey(
        "activos.Activo",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fallas",
        verbose_name="Activo relacionado (opcional)",
    )
    categoria = models.ForeignKey(CategoriaFalla, on_delete=models.PROTECT, related_name="reportes")
    tipo_objetivo = models.CharField(max_length=16, choices=OBJETIVO_CHOICES, default=OBJETIVO_EQUIPO)
    area_instalacion = models.CharField(max_length=120, blank=True, default="")
    area = models.CharField(
        max_length=15,
        choices=AREAS,
        default=AREA_GENERAL,
        verbose_name="Área",
        help_text="Área responsable de dar seguimiento al reporte",
    )
    titulo = models.CharField(max_length=200)
    descripcion = models.TextField()
    prioridad = models.CharField(max_length=10, choices=PRIORIDAD, default=PRIORIDAD_MEDIA)
    foto_evidencia = models.ImageField(
        upload_to="fallas/evidencias/%Y/%m/",
        null=True,
        blank=True,
        help_text="Foto opcional; si falta debe registrarse una justificación",
    )
    justificacion_sin_foto = models.TextField(blank=True, default="")
    latitud = models.DecimalField(max_digits=10, decimal_places=7, null=True, blank=True)
    longitud = models.DecimalField(max_digits=10, decimal_places=7, null=True, blank=True)
    estatus = models.CharField(max_length=15, choices=ESTATUS, default=ESTATUS_ABIERTO)
    reportado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="fallas_reportadas",
    )
    asignado_a = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fallas_asignadas",
    )
    cerrado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fallas_cerradas",
    )
    fecha_reporte = models.DateTimeField(default=timezone.now, editable=False)
    fecha_asignacion = models.DateTimeField(null=True, blank=True)
    fecha_resolucion = models.DateTimeField(null=True, blank=True)
    fecha_cierre = models.DateTimeField(null=True, blank=True)
    costo_estimado = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    costo_real = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    proveedor_servicio = models.CharField(max_length=200, blank=True)
    notas_internas = models.TextField(blank=True)

    class Meta:
        ordering = ["-fecha_reporte"]
        verbose_name = "Reporte de falla"
        verbose_name_plural = "Reportes de fallas"
        indexes = [
            models.Index(fields=["sucursal", "estatus"]),
            models.Index(fields=["estatus", "prioridad"]),
            models.Index(fields=["fecha_reporte"]),
        ]

    def __str__(self):
        return f"[{self.get_estatus_display()}] {self.sucursal} - {self.titulo}"

    def clean(self):
        super().clean()
        errors = {}
        if not self.foto_evidencia and not self.justificacion_sin_foto.strip():
            errors["justificacion_sin_foto"] = "Escribe una justificación cuando no adjuntes fotografía."
        if self.tipo_objetivo == self.OBJETIVO_EQUIPO:
            if not self.activo_relacionado_id:
                errors["activo_relacionado"] = "Selecciona un equipo registrado."
            elif self.sucursal_id and self.activo_relacionado.sucursal_id != self.sucursal_id:
                errors["activo_relacionado"] = "El equipo no pertenece a la sucursal operativa."
            if self.categoria_id and self.categoria.tipo != CategoriaFalla.TIPO_EQUIPO:
                errors["categoria"] = "Selecciona una categoría de equipo."
        elif self.tipo_objetivo == self.OBJETIVO_INSTALACION:
            if self.activo_relacionado_id:
                errors["activo_relacionado"] = "Una falla de instalación no debe vincular un equipo."
            if not self.area_instalacion.strip():
                errors["area_instalacion"] = "Indica el área de la instalación."
            if self.categoria_id and self.categoria.tipo != CategoriaFalla.TIPO_INSTALACION:
                errors["categoria"] = "Selecciona una categoría de instalaciones."
        else:
            errors["tipo_objetivo"] = "Selecciona equipo o instalación."
        if self.latitud is not None and not (-90 <= self.latitud <= 90):
            errors["latitud"] = "La latitud debe estar entre -90 y 90."
        if self.longitud is not None and not (-180 <= self.longitud <= 180):
            errors["longitud"] = "La longitud debe estar entre -180 y 180."
        if errors:
            raise ValidationError(errors)

    @property
    def tiempo_respuesta_horas(self):
        if self.fecha_asignacion:
            delta = self.fecha_asignacion - self.fecha_reporte
            return round(delta.total_seconds() / 3600, 1)
        return None

    @property
    def tiempo_resolucion_horas(self):
        if self.fecha_resolucion:
            delta = self.fecha_resolucion - self.fecha_reporte
            return round(delta.total_seconds() / 3600, 1)
        return None


class BitacoraFalla(models.Model):
    """Historial de cambios de estatus y comentarios."""

    reporte = models.ForeignKey(ReporteFalla, on_delete=models.CASCADE, related_name="bitacora")
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    estatus_anterior = models.CharField(max_length=15, blank=True)
    estatus_nuevo = models.CharField(max_length=15, blank=True)
    comentario = models.TextField(blank=True)
    timestamp = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["timestamp"]
        verbose_name = "Bitácora de falla"
        verbose_name_plural = "Bitácora de fallas"

    def __str__(self):
        return f"[{self.timestamp:%d/%m %H:%M}] {self.usuario} -> {self.estatus_nuevo}"

    def get_estatus_nuevo_display(self):
        return dict(ReporteFalla.ESTATUS).get(self.estatus_nuevo, self.estatus_nuevo)


class EvidenciaSeguimientoFalla(models.Model):
    """Archivo adjunto a un avance de mantenimiento visible en el seguimiento."""

    bitacora = models.ForeignKey(BitacoraFalla, on_delete=models.CASCADE, related_name="evidencias")
    archivo = models.FileField(upload_to="fallas/seguimiento/%Y/%m/")
    nombre = models.CharField(max_length=255, blank=True)
    subido_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["creado_en"]
        verbose_name = "Evidencia de seguimiento de falla"
        verbose_name_plural = "Evidencias de seguimiento de fallas"

    def __str__(self):
        return self.nombre or self.archivo.name
