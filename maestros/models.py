from django.db import models
from django.utils import timezone
from unidecode import unidecode

class UnidadMedida(models.Model):
    TIPO_MASA = "MASS"
    TIPO_VOLUMEN = "VOLUME"
    TIPO_PIEZA = "UNIT"
    TIPO_CHOICES = [
        (TIPO_MASA, "Masa"),
        (TIPO_VOLUMEN, "Volumen"),
        (TIPO_PIEZA, "Pieza"),
    ]

    codigo = models.CharField(max_length=20, unique=True)  # kg, g, lt, ml, pza, etc
    nombre = models.CharField(max_length=60)
    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES, default=TIPO_PIEZA)
    # factor para convertir a unidad base del tipo (g para masa, ml para volumen, pza para unidad)
    factor_to_base = models.DecimalField(max_digits=18, decimal_places=6, default=1)

    class Meta:
        verbose_name = "Unidad de medida"
        verbose_name_plural = "Unidades de medida"

    def __str__(self) -> str:
        return self.codigo

class Proveedor(models.Model):
    nombre = models.CharField(max_length=200, unique=True)
    lead_time_dias = models.PositiveIntegerField(default=0)
    activo = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Proveedor"
        verbose_name_plural = "Proveedores"

    def __str__(self) -> str:
        return self.nombre

class Insumo(models.Model):
    codigo = models.CharField(max_length=60, blank=True, default="")
    nombre = models.CharField(max_length=250)
    nombre_normalizado = models.CharField(max_length=260, db_index=True)
    unidad_base = models.ForeignKey(UnidadMedida, null=True, blank=True, on_delete=models.SET_NULL)
    proveedor_principal = models.ForeignKey(Proveedor, null=True, blank=True, on_delete=models.SET_NULL)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Insumo"
        verbose_name_plural = "Insumos"
        indexes = [models.Index(fields=["nombre_normalizado"])]

    def save(self, *args, **kwargs):
        self.nombre_normalizado = " ".join(unidecode((self.nombre or "")).lower().strip().split())
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.nombre


class InsumoAlias(models.Model):
    nombre = models.CharField(max_length=250)
    nombre_normalizado = models.CharField(max_length=260, unique=True, db_index=True)
    insumo = models.ForeignKey(Insumo, on_delete=models.CASCADE, related_name="aliases")
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Alias de insumo"
        verbose_name_plural = "Aliases de insumos"
        ordering = ["nombre"]
        indexes = [models.Index(fields=["nombre_normalizado"])]

    def save(self, *args, **kwargs):
        self.nombre_normalizado = " ".join(unidecode((self.nombre or "")).lower().strip().split())
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.nombre} -> {self.insumo.nombre}"


class CostoInsumo(models.Model):
    insumo = models.ForeignKey(Insumo, on_delete=models.CASCADE)
    proveedor = models.ForeignKey(Proveedor, null=True, blank=True, on_delete=models.SET_NULL)
    fecha = models.DateField(default=timezone.now)
    moneda = models.CharField(max_length=10, default="MXN")
    costo_unitario = models.DecimalField(max_digits=18, decimal_places=6)
    source_hash = models.CharField(max_length=64, unique=True)  # idempotencia
    raw = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Costo de insumo"
        verbose_name_plural = "Costos de insumo"
        ordering = ["-fecha", "insumo__nombre"]

    def __str__(self) -> str:
        return f"{self.insumo.nombre} - {self.costo_unitario} {self.moneda}"

def seed_unidades_basicas():
    # Crea unidades base típicas (safe to call multiple times)
    units = [
        ("g", "Gramo", UnidadMedida.TIPO_MASA, 1),
        ("kg", "Kilogramo", UnidadMedida.TIPO_MASA, 1000),
        ("ml", "Mililitro", UnidadMedida.TIPO_VOLUMEN, 1),
        ("lt", "Litro", UnidadMedida.TIPO_VOLUMEN, 1000),
        ("pza", "Pieza", UnidadMedida.TIPO_PIEZA, 1),
        ("pz", "Pieza", UnidadMedida.TIPO_PIEZA, 1),
        ("unidad", "Unidad", UnidadMedida.TIPO_PIEZA, 1),
    ]
    for code, name, tipo, factor in units:
        UnidadMedida.objects.get_or_create(
            codigo=code,
            defaults={"nombre": name, "tipo": tipo, "factor_to_base": factor},
        )
