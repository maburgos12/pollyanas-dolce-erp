from django.conf import settings
from django.db import models


class ConceptoConciliacion(models.Model):
    FAMILIA_VENTA = "venta"
    FAMILIA_TARJETA = "tarjeta"
    FAMILIA_TRANSFERENCIA = "transferencia"
    FAMILIA_GASTO = "gasto"
    FAMILIA_NOMINA = "nomina"
    FAMILIA_FISCAL = "fiscal"
    FAMILIA_BALANCE = "balance"
    FAMILIA_PENDIENTE = "pendiente"
    FAMILIA_CHOICES = [
        (FAMILIA_VENTA, "Venta"),
        (FAMILIA_TARJETA, "Tarjeta"),
        (FAMILIA_TRANSFERENCIA, "Transferencia"),
        (FAMILIA_GASTO, "Gasto"),
        (FAMILIA_NOMINA, "Nomina"),
        (FAMILIA_FISCAL, "Fiscal"),
        (FAMILIA_BALANCE, "Balance"),
        (FAMILIA_PENDIENTE, "Pendiente"),
    ]

    TIPO_ABONO = "abono"
    TIPO_CARGO = "cargo"
    TIPO_AMBOS = "ambos"
    TIPO_MOVIMIENTO_CHOICES = [
        (TIPO_ABONO, "Abono"),
        (TIPO_CARGO, "Cargo"),
        (TIPO_AMBOS, "Ambos"),
    ]

    CFDI_EMITIDO = "emitido"
    CFDI_RECIBIDO = "recibido"
    CFDI_NOMINA = "nomina"
    CFDI_PAGO = "pago"
    CFDI_EGRESO = "egreso"
    CFDI_NINGUNO = "ninguno"
    CFDI_OPCIONAL = "opcional"
    CFDI_ESPERADO_CHOICES = [
        (CFDI_EMITIDO, "CFDI emitido"),
        (CFDI_RECIBIDO, "CFDI recibido"),
        (CFDI_NOMINA, "Nomina timbrada"),
        (CFDI_PAGO, "Complemento de pago"),
        (CFDI_EGRESO, "CFDI de egreso"),
        (CFDI_NINGUNO, "Sin CFDI esperado"),
        (CFDI_OPCIONAL, "CFDI opcional"),
    ]

    codigo = models.CharField(max_length=80, unique=True)
    nombre = models.CharField(max_length=160)
    descripcion = models.TextField(blank=True)
    familia = models.CharField(max_length=30, choices=FAMILIA_CHOICES)
    tipo_movimiento = models.CharField(max_length=10, choices=TIPO_MOVIMIENTO_CHOICES)
    cfdi_esperado = models.CharField(max_length=20, choices=CFDI_ESPERADO_CHOICES)
    forma_pago_esperada = models.CharField(max_length=2, blank=True)
    requiere_rep = models.BooleanField(default=False)
    requiere_cfdi_recibido = models.BooleanField(default=False)
    requiere_evidencia_externa = models.BooleanField(default=False)
    afecta_iva = models.BooleanField(default=False)
    afecta_isr = models.BooleanField(default=False)
    afecta_flujo = models.BooleanField(default=True)
    permite_conciliacion_automatica = models.BooleanField(default=False)
    tolerancia_monto = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    tolerancia_porcentaje = models.DecimalField(max_digits=7, decimal_places=4, default=0)
    cuenta_contable_sugerida = models.CharField(max_length=120, blank=True)
    palabras_clave = models.JSONField(default=list, blank=True)
    evidencia_requerida = models.JSONField(default=list, blank=True)
    prioridad = models.PositiveIntegerField(default=100)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["prioridad", "codigo"]
        verbose_name = "Concepto de conciliacion"
        verbose_name_plural = "Conceptos de conciliacion"
        indexes = [
            models.Index(fields=["familia", "activo"]),
            models.Index(fields=["tipo_movimiento", "activo"]),
            models.Index(fields=["cfdi_esperado"]),
        ]

    def __str__(self) -> str:
        return f"{self.codigo} | {self.nombre}"


class ImportacionBancaria(models.Model):
    FUENTE_MANUAL_CSV = "manual_csv"
    FUENTE_MANUAL_EXCEL = "manual_excel"
    FUENTE_CHOICES = [
        (FUENTE_MANUAL_CSV, "Carga manual CSV"),
        (FUENTE_MANUAL_EXCEL, "Carga manual Excel"),
    ]

    ESTADO_PREVIEW = "preview"
    ESTADO_IMPORTADA = "importada"
    ESTADO_ERROR = "error"
    ESTADO_CHOICES = [
        (ESTADO_PREVIEW, "Preview"),
        (ESTADO_IMPORTADA, "Importada"),
        (ESTADO_ERROR, "Error"),
    ]

    cuenta = models.ForeignKey(
        "syncfy_client.CuentaBancaria",
        on_delete=models.PROTECT,
        related_name="importaciones_bancarias",
    )
    fuente = models.CharField(max_length=20, choices=FUENTE_CHOICES)
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PREVIEW)
    archivo_nombre = models.CharField(max_length=255)
    archivo_hash = models.CharField(max_length=64, db_index=True)
    total_filas = models.IntegerField(default=0)
    movimientos_nuevos = models.IntegerField(default=0)
    movimientos_duplicados = models.IntegerField(default=0)
    filas_con_error = models.IntegerField(default=0)
    preview = models.JSONField(default=list, blank=True)
    errores = models.JSONField(default=list, blank=True)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Importacion bancaria"
        verbose_name_plural = "Importaciones bancarias"
        indexes = [
            models.Index(fields=["cuenta", "creado_en"]),
            models.Index(fields=["estado"]),
        ]

    def __str__(self) -> str:
        return f"{self.cuenta} | {self.archivo_nombre} | {self.estado}"
