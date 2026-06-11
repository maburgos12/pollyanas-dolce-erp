from django.db import models


class CuentaBancaria(models.Model):
    BANCO_BANBAJIO = "banbajio"
    BANCO_BBVA = "bbva"
    BANCO_AMEX = "amex"
    ORIGEN_SYNCFY = "syncfy"
    ORIGEN_MANUAL = "manual"
    BANCO_CHOICES = [
        (BANCO_BANBAJIO, "BanBajio Empresas"),
        (BANCO_BBVA, "BBVA Empresas"),
        (BANCO_AMEX, "American Express"),
    ]
    ORIGEN_CHOICES = [
        (ORIGEN_SYNCFY, "Syncfy"),
        (ORIGEN_MANUAL, "Carga manual"),
    ]

    banco = models.CharField(max_length=20, choices=BANCO_CHOICES, unique=True)
    nombre_display = models.CharField(max_length=100)
    id_site_syncfy = models.CharField(max_length=50)
    origen = models.CharField(max_length=20, choices=ORIGEN_CHOICES, default=ORIGEN_SYNCFY)
    id_credential = models.CharField(max_length=100, null=True, blank=True)
    id_account = models.CharField(max_length=100, null=True, blank=True)
    numero_cuenta = models.CharField(max_length=32, null=True, blank=True)
    activa = models.BooleanField(default=True)
    ultima_sync = models.DateTimeField(null=True, blank=True)
    saldo_actual = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Cuenta Bancaria"
        verbose_name_plural = "Cuentas Bancarias"

    def __str__(self) -> str:
        return f"{self.get_banco_display()} - {self.numero_cuenta or 'sin numero'}"


class MovimientoBancario(models.Model):
    TIPO_CARGO = "cargo"
    TIPO_ABONO = "abono"
    TIPO_CHOICES = [(TIPO_CARGO, "Cargo"), (TIPO_ABONO, "Abono")]

    id_transaction = models.CharField(max_length=100, unique=True)
    cuenta = models.ForeignKey(CuentaBancaria, on_delete=models.CASCADE, related_name="movimientos")
    descripcion = models.CharField(max_length=500)
    monto = models.DecimalField(max_digits=14, decimal_places=2)
    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    moneda = models.CharField(max_length=10, default="MXN")
    fecha_transaccion = models.DateTimeField()
    fecha_refresh = models.DateTimeField()
    extra_raw = models.JSONField(default=dict, blank=True)
    conciliado = models.BooleanField(default=False)
    cfdi_relacionado = models.ForeignKey(
        "sat_client.CfdiDescargado",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="movimientos_bancarios",
    )
    descargado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fecha_transaccion"]
        verbose_name = "Movimiento Bancario"
        verbose_name_plural = "Movimientos Bancarios"
        indexes = [
            models.Index(fields=["cuenta", "fecha_transaccion"]),
            models.Index(fields=["monto", "fecha_transaccion"]),
            models.Index(fields=["conciliado"]),
            models.Index(fields=["tipo", "fecha_transaccion"]),
        ]

    def __str__(self) -> str:
        return f"{self.cuenta.banco} | {self.fecha_transaccion.date()} | {self.tipo} ${self.monto}"


class LogSyncfy(models.Model):
    NIVEL_INFO = "INFO"
    NIVEL_WARN = "WARN"
    NIVEL_ERROR = "ERROR"
    NIVEL_CHOICES = [
        (NIVEL_INFO, "Info"),
        (NIVEL_WARN, "Warning"),
        (NIVEL_ERROR, "Error"),
    ]

    nivel = models.CharField(max_length=10, choices=NIVEL_CHOICES, default=NIVEL_INFO)
    cuenta = models.ForeignKey(CuentaBancaria, on_delete=models.SET_NULL, null=True, blank=True)
    mensaje = models.TextField()
    movimientos_nuevos = models.IntegerField(default=0)
    movimientos_total = models.IntegerField(default=0)
    duracion_segundos = models.IntegerField(null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Log Syncfy"
        verbose_name_plural = "Logs Syncfy"

    def __str__(self) -> str:
        return f"{self.nivel} {self.creado_en:%Y-%m-%d %H:%M}"
