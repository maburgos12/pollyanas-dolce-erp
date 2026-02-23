from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models
from django.db.models import Sum
from django.utils import timezone

from recetas.utils.normalizacion import normalizar_nombre


class Empleado(models.Model):
    CONTRATO_FIJO = "FIJO"
    CONTRATO_TEMPORAL = "TEMPORAL"
    CONTRATO_HONORARIOS = "HONORARIOS"
    CONTRATO_CHOICES = [
        (CONTRATO_FIJO, "Fijo"),
        (CONTRATO_TEMPORAL, "Temporal"),
        (CONTRATO_HONORARIOS, "Honorarios"),
    ]

    codigo = models.CharField(max_length=40, unique=True, blank=True)
    nombre = models.CharField(max_length=180)
    nombre_normalizado = models.CharField(max_length=180, db_index=True, editable=False)
    area = models.CharField(max_length=120, blank=True, default="")
    puesto = models.CharField(max_length=120, blank=True, default="")
    tipo_contrato = models.CharField(max_length=20, choices=CONTRATO_CHOICES, default=CONTRATO_FIJO)
    fecha_ingreso = models.DateField(default=timezone.localdate)
    salario_diario = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    telefono = models.CharField(max_length=40, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    sucursal = models.CharField(max_length=120, blank=True, default="")
    activo = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre", "id"]
        verbose_name = "Empleado"
        verbose_name_plural = "Empleados"

    def __str__(self) -> str:
        return f"{self.codigo} · {self.nombre}" if self.codigo else self.nombre

    def _generate_codigo(self) -> str:
        yymm = timezone.localdate().strftime("%y%m")
        prefix = f"EMP-{yymm}-"
        seq = Empleado.objects.filter(codigo__startswith=prefix).count() + 1
        return f"{prefix}{seq:03d}"

    def save(self, *args, **kwargs):
        self.nombre_normalizado = normalizar_nombre(self.nombre or "")
        if not self.codigo:
            self.codigo = self._generate_codigo()
        super().save(*args, **kwargs)


class NominaPeriodo(models.Model):
    TIPO_SEMANAL = "SEMANAL"
    TIPO_QUINCENAL = "QUINCENAL"
    TIPO_MENSUAL = "MENSUAL"
    TIPO_CHOICES = [
        (TIPO_SEMANAL, "Semanal"),
        (TIPO_QUINCENAL, "Quincenal"),
        (TIPO_MENSUAL, "Mensual"),
    ]

    ESTATUS_BORRADOR = "BORRADOR"
    ESTATUS_CERRADA = "CERRADA"
    ESTATUS_PAGADA = "PAGADA"
    ESTATUS_CHOICES = [
        (ESTATUS_BORRADOR, "Borrador"),
        (ESTATUS_CERRADA, "Cerrada"),
        (ESTATUS_PAGADA, "Pagada"),
    ]

    folio = models.CharField(max_length=40, unique=True, blank=True)
    tipo_periodo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_QUINCENAL)
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_BORRADOR)
    total_bruto = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0"))
    total_descuentos = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0"))
    total_neto = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0"))
    notas = models.TextField(blank=True, default="")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="nominas_creadas",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_fin", "-id"]
        verbose_name = "Nómina periodo"
        verbose_name_plural = "Nóminas periodos"

    def __str__(self) -> str:
        return self.folio or f"Nómina {self.id}"

    def _generate_folio(self) -> str:
        period = self.fecha_inicio.strftime("%Y%m")
        prefix = f"NOM-{period}-"
        seq = NominaPeriodo.objects.filter(folio__startswith=prefix).count() + 1
        return f"{prefix}{seq:03d}"

    def recompute_totals(self) -> None:
        agg = self.lineas.aggregate(
            bruto=Sum("total_percepciones"),
            descuentos=Sum("descuentos"),
            neto=Sum("neto_calculado"),
        )
        self.total_bruto = agg.get("bruto") or Decimal("0")
        self.total_descuentos = agg.get("descuentos") or Decimal("0")
        self.total_neto = agg.get("neto") or Decimal("0")

    def save(self, *args, **kwargs):
        if not self.folio:
            self.folio = self._generate_folio()
        super().save(*args, **kwargs)


class NominaLinea(models.Model):
    periodo = models.ForeignKey(NominaPeriodo, on_delete=models.CASCADE, related_name="lineas")
    empleado = models.ForeignKey(Empleado, on_delete=models.PROTECT, related_name="lineas_nomina")
    dias_trabajados = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0"))
    salario_base = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    bonos = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    descuentos = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    total_percepciones = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    neto_calculado = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    observaciones = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("periodo", "empleado")
        ordering = ["empleado__nombre", "id"]
        verbose_name = "Línea nómina"
        verbose_name_plural = "Líneas nómina"

    def __str__(self) -> str:
        return f"{self.periodo.folio} · {self.empleado.nombre}"

    def save(self, *args, **kwargs):
        if (self.salario_base or Decimal("0")) <= 0 and (self.empleado.salario_diario or Decimal("0")) > 0:
            self.salario_base = (self.empleado.salario_diario or Decimal("0")) * (self.dias_trabajados or Decimal("0"))
        self.total_percepciones = (self.salario_base or Decimal("0")) + (self.bonos or Decimal("0"))
        self.neto_calculado = self.total_percepciones - (self.descuentos or Decimal("0"))
        super().save(*args, **kwargs)
