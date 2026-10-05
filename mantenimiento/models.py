from django.conf import settings
from django.db import models
from django.utils import timezone


class ProveedorServicio(models.Model):
    """Talleres, técnicos y empresas de mantenimiento — separado de los proveedores de insumos."""

    nombre = models.CharField(max_length=200)
    contacto = models.CharField(max_length=120, blank=True, default="", verbose_name="Nombre del contacto")
    telefono = models.CharField(max_length=30, blank=True, default="")
    whatsapp = models.CharField(max_length=30, blank=True, default="",
                                verbose_name="WhatsApp / celular")
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


class VinculoAtencionEquipo(models.Model):
    """Atención documentada entre fuentes; no implica equivalencia financiera."""
    orden = models.ForeignKey('activos.OrdenMantenimiento', on_delete=models.PROTECT, related_name='vinculos_atencion')
    reporte = models.ForeignKey('fallas.ReporteFalla', on_delete=models.PROTECT, related_name='vinculos_atencion')
    creador = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name='vinculos_atencion_creados')
    creado_en = models.DateTimeField(auto_now_add=True)
    motivo = models.TextField(max_length=2000)

    class Meta:
        ordering = ['-creado_en', '-pk']
        constraints = [models.UniqueConstraint(fields=['orden', 'reporte'], name='mantenimiento_vinculo_orden_reporte_uniq')]


class ComprobanteCapturaEquipo(models.Model):
    """Recibo técnico del intento; la orden conserva todos los datos de negocio."""
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    operacion = models.CharField(max_length=32)
    clave = models.UUIDField()
    huella = models.CharField(max_length=64)
    orden = models.ForeignKey('activos.OrdenMantenimiento', on_delete=models.SET_NULL, null=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['usuario', 'operacion', 'clave'], name='mant_captura_usuario_op_clave_uniq')]


class ComprobanteConfiguracionPlan(models.Model):
    """Recibo técnico; el resultado pertenece a Plan, incluso tras su eliminación."""
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    operacion = models.CharField(max_length=32)
    clave = models.UUIDField()
    huella = models.CharField(max_length=64)
    plan = models.ForeignKey('activos.PlanMantenimiento', on_delete=models.SET_NULL, null=True)
    activo_ref = models.ForeignKey('activos.Activo', on_delete=models.SET_NULL, null=True)
    objeto_id = models.PositiveIntegerField(null=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=['usuario', 'operacion', 'clave'], name='mant_plan_usuario_op_clave_uniq')]


class VinculoProveedorDocumental(models.Model):
    """M:N confirmado por IDs, con procedencia conservada tras borrar fuentes."""
    perfil = models.ForeignKey(ProveedorServicio, null=True, on_delete=models.SET_NULL,
                               related_name="vinculos_documentales")
    proveedor = models.ForeignKey("maestros.Proveedor", null=True, on_delete=models.SET_NULL,
                                  related_name="vinculos_tecnicos_documentales")
    perfil_original_id = models.PositiveBigIntegerField(editable=False)
    proveedor_original_id = models.PositiveBigIntegerField(editable=False)
    motivo = models.TextField()
    evidencia = models.TextField()
    autor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL,
                              related_name="vinculos_proveedores_confirmados")
    autor_original_id = models.PositiveBigIntegerField(editable=False)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-pk"]
        constraints = [models.UniqueConstraint(fields=["perfil_original_id", "proveedor_original_id"],
                                               name="mant_vinculo_proveedor_par_unico")]


class DocumentoFinancieroTrabajo(models.Model):
    """Confirmación M:N de soporte existente; nunca equivale a un nuevo gasto."""
    tipo_trabajo = models.CharField(max_length=8, choices=[("orden", "Orden"), ("falla", "Incidencia")])
    trabajo_original_id = models.PositiveBigIntegerField(editable=False)
    tipo_documento = models.CharField(max_length=12, choices=[("obligacion", "Obligación"), ("gasto", "Gasto"), ("cfdi", "CFDI"), ("movimiento", "Movimiento")])
    documento_original_id = models.PositiveBigIntegerField(editable=False)
    orden = models.ForeignKey("activos.OrdenMantenimiento", null=True, on_delete=models.SET_NULL, related_name="documentos_financieros")
    falla = models.ForeignKey("fallas.ReporteFalla", null=True, on_delete=models.SET_NULL, related_name="documentos_financieros")
    obligacion = models.ForeignKey("reportes.ObligacionGasto", null=True, on_delete=models.SET_NULL, related_name="trabajos_documentados")
    gasto = models.ForeignKey("reportes.GastoOperativoMensual", null=True, on_delete=models.SET_NULL, related_name="trabajos_documentados")
    cfdi = models.ForeignKey("sat_client.CfdiDescargado", null=True, on_delete=models.SET_NULL, related_name="trabajos_documentados")
    movimiento = models.ForeignKey("syncfy_client.MovimientoBancario", null=True, on_delete=models.SET_NULL, related_name="trabajos_documentados")
    orden_original_id = models.PositiveBigIntegerField(null=True, editable=False)
    falla_original_id = models.PositiveBigIntegerField(null=True, editable=False)
    obligacion_original_id = models.PositiveBigIntegerField(null=True, editable=False)
    gasto_original_id = models.PositiveBigIntegerField(null=True, editable=False)
    cfdi_original_id = models.PositiveBigIntegerField(null=True, editable=False)
    movimiento_original_id = models.PositiveBigIntegerField(null=True, editable=False)
    motivo = models.TextField(max_length=2000)
    evidencia = models.TextField(max_length=4000)
    autor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL, related_name="documentos_trabajo_confirmados")
    autor_original_id = models.PositiveBigIntegerField(editable=False)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-pk"]
        constraints = [
            models.UniqueConstraint(fields=["tipo_trabajo", "trabajo_original_id", "tipo_documento", "documento_original_id"], name="mant_trabajo_documento_unico"),
            models.UniqueConstraint(fields=["tipo_trabajo", "trabajo_original_id", "gasto_original_id"], condition=models.Q(gasto_original_id__isnull=False), name="mant_trabajo_gasto_unico"),
            models.CheckConstraint(check=(models.Q(tipo_trabajo="orden", orden_original_id=models.F("trabajo_original_id"), orden_original_id__isnull=False, falla_original_id__isnull=True) | models.Q(tipo_trabajo="falla", falla_original_id=models.F("trabajo_original_id"), falla_original_id__isnull=False, orden_original_id__isnull=True)), name="mant_documento_trabajo_tipo"),
            models.CheckConstraint(check=(models.Q(tipo_documento="obligacion", obligacion_original_id=models.F("documento_original_id"), obligacion_original_id__isnull=False, cfdi_original_id__isnull=True, movimiento_original_id__isnull=True) | models.Q(tipo_documento="gasto", gasto_original_id=models.F("documento_original_id"), gasto_original_id__isnull=False, obligacion_original_id__isnull=True, cfdi_original_id__isnull=True, movimiento_original_id__isnull=True) | models.Q(tipo_documento="cfdi", cfdi_original_id=models.F("documento_original_id"), cfdi_original_id__isnull=False, obligacion_original_id__isnull=True, gasto_original_id__isnull=True, movimiento_original_id__isnull=True) | models.Q(tipo_documento="movimiento", movimiento_original_id=models.F("documento_original_id"), movimiento_original_id__isnull=False, obligacion_original_id__isnull=True, gasto_original_id__isnull=True, cfdi_original_id__isnull=True)), name="mant_documento_fuente_tipo"),
            *[models.CheckConstraint(check=models.Q(**{f"{campo}__isnull": True}) | models.Q(**{f"{campo}_id": models.F(f"{campo}_original_id"), f"{campo}_original_id__isnull": False}), name=f"mant_doc_{campo}_original") for campo in ("orden", "falla", "obligacion", "gasto", "cfdi", "movimiento", "autor")],
        ]

    def save(self, *args, **kwargs):
        from django.core.exceptions import ValidationError
        db = kwargs.get("using") or self._state.db or "default"
        if not self._state.adding or (self.pk is not None and type(self).objects.using(db).filter(pk=self.pk).exists()):
            raise ValidationError("La confirmación documental es inmutable.")
        kwargs["force_insert"] = True
        return super().save(*args, **kwargs)
