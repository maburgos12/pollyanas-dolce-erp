from django.db import models
from django.utils import timezone
from maestros.models import Insumo, UnidadMedida
from django.conf import settings
from unidecode import unidecode

class Receta(models.Model):
    nombre = models.CharField(max_length=250)
    nombre_normalizado = models.CharField(max_length=260, db_index=True)
    sheet_name = models.CharField(max_length=120, blank=True, default="")
    hash_contenido = models.CharField(max_length=64, unique=True)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Receta"
        verbose_name_plural = "Recetas"
        ordering = ["nombre"]

    def save(self, *args, **kwargs):
        self.nombre_normalizado = " ".join(unidecode((self.nombre or "")).lower().strip().split())
        super().save(*args, **kwargs)

    @property
    def costo_total_estimado(self):
        return sum((l.costo_total_estimado or 0) for l in self.lineas.all())

    @property
    def pendientes_matching(self):
        return self.lineas.filter(match_status=LineaReceta.STATUS_NEEDS_REVIEW).count()

    def __str__(self) -> str:
        return self.nombre

class LineaReceta(models.Model):
    STATUS_AUTO = "AUTO_APPROVED"
    STATUS_NEEDS_REVIEW = "NEEDS_REVIEW"
    STATUS_REJECTED = "REJECTED"
    STATUS_CHOICES = [
        (STATUS_AUTO, "Auto"),
        (STATUS_NEEDS_REVIEW, "Needs review"),
        (STATUS_REJECTED, "Rejected"),
    ]

    MATCH_EXACT = "EXACT"
    MATCH_CONTAINS = "CONTAINS"
    MATCH_FUZZY = "FUZZY"
    MATCH_NONE = "NO_MATCH"

    receta = models.ForeignKey(Receta, related_name="lineas", on_delete=models.CASCADE)
    posicion = models.PositiveIntegerField(default=0)

    insumo = models.ForeignKey(Insumo, null=True, blank=True, on_delete=models.SET_NULL)
    insumo_texto = models.CharField(max_length=250)
    cantidad = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    unidad_texto = models.CharField(max_length=40, blank=True, default="")
    unidad = models.ForeignKey(UnidadMedida, null=True, blank=True, on_delete=models.SET_NULL)

    costo_linea_excel = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    costo_unitario_snapshot = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    match_score = models.FloatField(default=0)
    match_method = models.CharField(max_length=20, default=MATCH_NONE)
    match_status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_REJECTED)
    aprobado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    aprobado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "Línea de receta"
        verbose_name_plural = "Líneas de receta"
        ordering = ["receta", "posicion"]

    @property
    def costo_total_estimado(self):
        # Preferencia: costo de Excel si existe, si no: cantidad * costo_unitario_snapshot
        if self.costo_linea_excel is not None:
            return float(self.costo_linea_excel)
        if self.cantidad is not None and self.costo_unitario_snapshot is not None:
            return float(self.cantidad) * float(self.costo_unitario_snapshot)
        return 0.0

    def __str__(self) -> str:
        return f"{self.receta.nombre}: {self.insumo_texto}"
