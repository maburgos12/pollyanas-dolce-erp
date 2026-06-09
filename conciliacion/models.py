from django.conf import settings
from django.db import models


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
