from django.db import models


class SolicitudDescarga(models.Model):
    TIPO_CFDI = "CFDI"
    TIPO_METADATA = "Metadata"
    TIPO_CHOICES = [(TIPO_CFDI, "CFDI"), (TIPO_METADATA, "Metadata")]

    ESTADO_ACEPTADA = 1
    ESTADO_EN_PROCESO = 2
    ESTADO_TERMINADA = 3
    ESTADO_ERROR = 4
    ESTADO_RECHAZADA = 5
    ESTADO_VENCIDA = 6
    ESTADO_CHOICES = [
        (ESTADO_ACEPTADA, "Aceptada"),
        (ESTADO_EN_PROCESO, "En Proceso"),
        (ESTADO_TERMINADA, "Terminada"),
        (ESTADO_ERROR, "Error"),
        (ESTADO_RECHAZADA, "Rechazada"),
        (ESTADO_VENCIDA, "Vencida"),
    ]

    DIRECCION_EMITIDOS = "emitidos"
    DIRECCION_RECIBIDOS = "recibidos"
    DIRECCION_CHOICES = [
        (DIRECCION_EMITIDOS, "Emitidos"),
        (DIRECCION_RECIBIDOS, "Recibidos"),
    ]

    id_solicitud = models.CharField(max_length=100, unique=True, null=True, blank=True)
    fecha_inicial = models.DateField()
    fecha_final = models.DateField()
    rfc_solicitante = models.CharField(max_length=13)
    tipo_solicitud = models.CharField(max_length=10, choices=TIPO_CHOICES, default=TIPO_CFDI)
    direccion = models.CharField(max_length=10, choices=DIRECCION_CHOICES)
    estado = models.IntegerField(choices=ESTADO_CHOICES, default=ESTADO_ACEPTADA)
    codigo_estado = models.CharField(max_length=10, null=True, blank=True)
    numero_cfdis = models.IntegerField(null=True, blank=True)
    ids_paquetes = models.JSONField(default=list, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)
    error_detalle = models.TextField(null=True, blank=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Solicitud SAT"
        verbose_name_plural = "Solicitudes SAT"
        indexes = [
            models.Index(fields=["fecha_inicial", "fecha_final", "direccion"]),
            models.Index(fields=["rfc_solicitante", "tipo_solicitud", "estado"]),
        ]

    def __str__(self) -> str:
        return f"{self.rfc_solicitante} {self.direccion} {self.fecha_inicial:%Y-%m}"


class CfdiDescargado(models.Model):
    TIPO_EMITIDO = "emitido"
    TIPO_RECIBIDO = "recibido"
    TIPO_CFDI_CHOICES = [(TIPO_EMITIDO, "Emitido"), (TIPO_RECIBIDO, "Recibido")]

    uuid = models.CharField(max_length=36, unique=True)
    solicitud = models.ForeignKey(
        SolicitudDescarga,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cfdis",
    )
    rfc_emisor = models.CharField(max_length=13)
    nombre_emisor = models.CharField(max_length=255, null=True, blank=True)
    rfc_receptor = models.CharField(max_length=13)
    nombre_receptor = models.CharField(max_length=255, null=True, blank=True)
    subtotal = models.DecimalField(max_digits=14, decimal_places=2)
    total = models.DecimalField(max_digits=14, decimal_places=2)
    descuento = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    moneda = models.CharField(max_length=10, default="MXN")
    tipo_cambio = models.DecimalField(max_digits=10, decimal_places=6, default=1)
    tipo_comprobante = models.CharField(max_length=1)
    tipo_cfdi = models.CharField(max_length=10, choices=TIPO_CFDI_CHOICES)
    uso_cfdi = models.CharField(max_length=10, null=True, blank=True)
    metodo_pago = models.CharField(max_length=5, null=True, blank=True)
    forma_pago = models.CharField(max_length=5, null=True, blank=True)
    fecha_emision = models.DateTimeField()
    fecha_timbrado = models.DateTimeField(null=True, blank=True)
    estatus = models.CharField(max_length=20, default="vigente")
    xml_raw = models.TextField(null=True, blank=True)
    descargado_en = models.DateTimeField(auto_now_add=True)
    conciliado = models.BooleanField(default=False)

    class Meta:
        ordering = ["-fecha_emision"]
        verbose_name = "CFDI Descargado"
        verbose_name_plural = "CFDIs Descargados"
        indexes = [
            models.Index(fields=["rfc_emisor", "fecha_emision"]),
            models.Index(fields=["rfc_receptor", "fecha_emision"]),
            models.Index(fields=["total", "fecha_emision"]),
            models.Index(fields=["conciliado"]),
        ]

    def __str__(self) -> str:
        return f"{self.uuid} ${self.total}"


class CfdiPagoRelacionado(models.Model):
    cfdi_pago = models.ForeignKey(
        CfdiDescargado,
        on_delete=models.CASCADE,
        related_name="pagos_detalle",
    )
    uuid_relacionado = models.CharField(max_length=36, db_index=True)
    fecha_pago = models.DateTimeField(db_index=True)
    monto = models.DecimalField(max_digits=14, decimal_places=2)
    moneda = models.CharField(max_length=10, default="MXN")
    forma_pago = models.CharField(max_length=5, blank=True)
    num_parcialidad = models.CharField(max_length=20, blank=True)
    importe_saldo_anterior = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    importe_saldo_insoluto = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["fecha_pago", "uuid_relacionado"]
        verbose_name = "Pago relacionado CFDI"
        verbose_name_plural = "Pagos relacionados CFDI"
        constraints = [
            models.UniqueConstraint(
                fields=["cfdi_pago", "uuid_relacionado", "num_parcialidad"],
                name="uniq_cfdi_pago_docto_parcialidad",
            )
        ]
        indexes = [
            models.Index(fields=["fecha_pago", "monto"]),
            models.Index(fields=["uuid_relacionado", "fecha_pago"]),
        ]

    def __str__(self) -> str:
        return f"{self.cfdi_pago.uuid} -> {self.uuid_relacionado} ${self.monto}"


class LogDescargaSat(models.Model):
    NIVEL_INFO = "INFO"
    NIVEL_WARN = "WARN"
    NIVEL_ERROR = "ERROR"
    NIVEL_CHOICES = [
        (NIVEL_INFO, "Info"),
        (NIVEL_WARN, "Warning"),
        (NIVEL_ERROR, "Error"),
    ]

    nivel = models.CharField(max_length=10, choices=NIVEL_CHOICES, default=NIVEL_INFO)
    mensaje = models.TextField()
    solicitud = models.ForeignKey(
        SolicitudDescarga,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="logs",
    )
    cfdis_descargados = models.IntegerField(default=0)
    cfdis_nuevos = models.IntegerField(default=0)
    duracion_segundos = models.IntegerField(null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Log descarga SAT"
        verbose_name_plural = "Logs descarga SAT"

    def __str__(self) -> str:
        return f"{self.nivel} {self.creado_en:%Y-%m-%d %H:%M}"


class SolicitudDocumentoSat(models.Model):
    TIPO_CONSTANCIA = "constancia"
    TIPO_OPINION = "opinion"
    TIPO_BUZON = "buzon"
    TIPO_CHOICES = [
        (TIPO_CONSTANCIA, "Constancia de situación fiscal"),
        (TIPO_OPINION, "Opinión de cumplimiento"),
        (TIPO_BUZON, "Buzón Tributario"),
    ]

    ESTADO_PENDIENTE = "pendiente"
    ESTADO_PROCESANDO = "procesando"
    ESTADO_LISTO = "listo"
    ESTADO_ERROR = "error"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_PROCESANDO, "Procesando"),
        (ESTADO_LISTO, "Listo"),
        (ESTADO_ERROR, "Error"),
    ]

    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES)
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE)
    archivo = models.FileField(upload_to="sat/documentos/", null=True, blank=True)
    mensaje = models.TextField(blank=True)
    solicitado_por = models.ForeignKey(
        "auth.User",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitudes_documentos_sat",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Solicitud documento SAT"
        verbose_name_plural = "Solicitudes documentos SAT"
        indexes = [
            models.Index(fields=["tipo", "estado", "-creado_en"]),
        ]

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} {self.estado}"
