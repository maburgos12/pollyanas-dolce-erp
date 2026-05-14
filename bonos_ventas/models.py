from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models, transaction

from core.models import Sucursal
from rrhh.models import Empleado, NominaLinea, NominaPeriodo


CATEGORIA_GRANDE = "GRANDE"
CATEGORIA_MEDIANO = "MEDIANO"
CATEGORIA_CHICO = "CHICO"
CATEGORIA_MINI = "MINI"
CATEGORIA_VELAS_ACCESORIOS = "VELAS_ACCESORIOS"
CATEGORIA_VASOS = "VASOS"

CATEGORIAS_PRODUCTO = [
    (CATEGORIA_GRANDE, "Grande"),
    (CATEGORIA_MEDIANO, "Mediano"),
    (CATEGORIA_CHICO, "Chico"),
    (CATEGORIA_MINI, "Mini"),
    (CATEGORIA_VELAS_ACCESORIOS, "Velas/Accesorios"),
    (CATEGORIA_VASOS, "Vasos"),
]

SUCURSALES_EXCLUIDAS_VENTAS = {"Matriz", "CEDIS", "Devoluciones", "Almacén"}


def _money(value) -> Decimal:
    return Decimal(value or 0).quantize(Decimal("0.01"))


class ConfigBonoVentasPeriodo(models.Model):
    mes = models.PositiveSmallIntegerField()
    anio = models.PositiveSmallIntegerField()
    dias_laborables = models.PositiveSmallIntegerField(default=23)
    bono_base = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("300.00"))
    pct_uniforme = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("20.00"))
    pct_asistencia = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("35.00"))
    pct_puntualidad = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("20.00"))
    limite_uniforme = models.PositiveSmallIntegerField(default=1)
    limite_asistencia = models.PositiveSmallIntegerField(default=2)
    limite_puntualidad = models.PositiveSmallIntegerField(default=2)
    bono_ventas_adicional = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("300.00"))
    umbral_crecimiento_pct = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("5.00"))
    peso_grande = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("15.00"))
    peso_mediano = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("35.00"))
    peso_chico = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("20.00"))
    peso_mini = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("15.00"))
    peso_velas_accesorios = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("5.00"))
    peso_vasos = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("10.00"))
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("mes", "anio")
        ordering = ["-anio", "-mes"]
        verbose_name = "Configuración de bono - Ventas"
        verbose_name_plural = "Configuraciones de bonos - Ventas"

    def __str__(self) -> str:
        return f"Config Ventas {self.mes}/{self.anio}"

    def get_peso_categoria(self, categoria: str) -> Decimal:
        return {
            CATEGORIA_GRANDE: self.peso_grande,
            CATEGORIA_MEDIANO: self.peso_mediano,
            CATEGORIA_CHICO: self.peso_chico,
            CATEGORIA_MINI: self.peso_mini,
            CATEGORIA_VELAS_ACCESORIOS: self.peso_velas_accesorios,
            CATEGORIA_VASOS: self.peso_vasos,
        }.get(categoria, Decimal("0.00"))

    @transaction.atomic
    def aplicar_a_nomina(self, nomina: NominaPeriodo) -> int:
        updated = 0
        for bono in self.bonos.select_related("empleado", "sucursal").filter(estatus__in=["BORRADOR", "CERRADO"]):
            bono.recalcular()
            bono.save()
            linea, _ = NominaLinea.objects.get_or_create(periodo=nomina, empleado=bono.empleado)
            linea.dias_trabajados = Decimal(bono.dias_trabajados)
            linea.bonos = bono.total_a_pagar
            linea.save()
            updated += 1
        nomina.recompute_totals()
        nomina.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
        return updated


class VentaCategoriaSucursal(models.Model):
    FUENTE_MANUAL = "MANUAL"
    FUENTE_POS_BRIDGE = "POS_BRIDGE"

    periodo = models.ForeignKey(ConfigBonoVentasPeriodo, on_delete=models.CASCADE, related_name="ventas_categoria")
    sucursal = models.ForeignKey(Sucursal, on_delete=models.PROTECT)
    categoria = models.CharField(max_length=30, choices=CATEGORIAS_PRODUCTO)
    cantidad_actual = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal("0.000"))
    cantidad_anterior = models.DecimalField(max_digits=12, decimal_places=3, default=Decimal("0.000"))
    pct_crecimiento = models.DecimalField(max_digits=8, decimal_places=4, default=Decimal("0.0000"))
    activo_bono = models.BooleanField(default=False)
    monto_bono_categoria = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    fuente = models.CharField(
        max_length=20,
        choices=[(FUENTE_MANUAL, "Manual"), (FUENTE_POS_BRIDGE, "pos_bridge")],
        default=FUENTE_MANUAL,
    )
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("periodo", "sucursal", "categoria")
        ordering = ["sucursal__nombre", "categoria"]

    def __str__(self) -> str:
        return f"{self.sucursal.nombre} / {self.categoria} - {self.periodo}"

    def calcular_crecimiento(self) -> None:
        if self.cantidad_anterior and self.cantidad_anterior > 0:
            self.pct_crecimiento = (
                (self.cantidad_actual - self.cantidad_anterior) / self.cantidad_anterior * Decimal("100")
            ).quantize(Decimal("0.0001"))
        else:
            self.pct_crecimiento = Decimal("0.0000")
        self.activo_bono = self.pct_crecimiento >= self.periodo.umbral_crecimiento_pct
        self.monto_bono_categoria = _money(
            self.periodo.bono_ventas_adicional * self.periodo.get_peso_categoria(self.categoria) / Decimal("100")
            if self.activo_bono
            else 0
        )

    def save(self, *args, **kwargs):
        self.calcular_crecimiento()
        super().save(*args, **kwargs)


class BonoVentasEmpleado(models.Model):
    ESTATUS = [("BORRADOR", "Borrador"), ("CERRADO", "Cerrado"), ("PAGADO", "Pagado")]

    periodo = models.ForeignKey(ConfigBonoVentasPeriodo, on_delete=models.PROTECT, related_name="bonos")
    empleado = models.ForeignKey(Empleado, on_delete=models.PROTECT, related_name="bonos_ventas")
    sucursal = models.ForeignKey(Sucursal, on_delete=models.PROTECT)
    dias_trabajados = models.PositiveSmallIntegerField(default=0)
    dias_asistencia = models.PositiveSmallIntegerField(default=0)
    dias_uniforme = models.PositiveSmallIntegerField(default=0)
    dias_puntualidad = models.PositiveSmallIntegerField(default=0)
    pasa_uniforme = models.BooleanField(default=False)
    pasa_asistencia = models.BooleanField(default=False)
    pasa_puntualidad = models.BooleanField(default=False)
    monto_uniforme = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    monto_asistencia = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    monto_puntualidad = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    sub1 = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    bono_ventas = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    pasa_bono_ventas = models.BooleanField(default=False)
    ajuste_positivo = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    ajuste_negativo = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    bono_extra = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    desc_ajuste_positivo = models.CharField(max_length=200, blank=True, default="")
    desc_ajuste_negativo = models.CharField(max_length=200, blank=True, default="")
    desc_bono_extra = models.CharField(max_length=200, blank=True, default="")
    total_a_pagar = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    estatus = models.CharField(max_length=20, choices=ESTATUS, default="BORRADOR")
    observaciones = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("periodo", "empleado")
        ordering = ["sucursal__nombre", "empleado__nombre"]
        verbose_name = "Bono de Ventas"
        verbose_name_plural = "Bonos de Ventas"

    def __str__(self) -> str:
        return f"{self.empleado.nombre} - {self.periodo}"

    def _dias_base(self) -> int:
        return int(self.dias_trabajados or self.periodo.dias_laborables or 0)

    def _monto_concepto(self, base: Decimal, pct: Decimal, pasa: bool) -> Decimal:
        return _money(base * pct / Decimal("100")) if pasa else Decimal("0.00")

    def recalcular(self) -> None:
        cfg = self.periodo
        base = _money(cfg.bono_base)
        dias_base = self._dias_base()
        dias_asistencia = int(self.dias_asistencia or self.dias_trabajados or 0)
        self.pasa_asistencia = (dias_base - dias_asistencia) <= cfg.limite_asistencia
        self.pasa_uniforme = (dias_base - int(self.dias_uniforme or 0)) <= cfg.limite_uniforme
        self.pasa_puntualidad = (dias_base - int(self.dias_puntualidad or 0)) <= cfg.limite_puntualidad
        self.monto_uniforme = self._monto_concepto(base, cfg.pct_uniforme, self.pasa_uniforme)
        self.monto_asistencia = self._monto_concepto(base, cfg.pct_asistencia, self.pasa_asistencia)
        self.monto_puntualidad = self._monto_concepto(base, cfg.pct_puntualidad, self.pasa_puntualidad)
        self.sub1 = _money(self.monto_uniforme + self.monto_asistencia + self.monto_puntualidad)
        self.bono_ventas = _money(
            sum(
                venta.monto_bono_categoria
                for venta in VentaCategoriaSucursal.objects.filter(
                    periodo=self.periodo,
                    sucursal=self.sucursal,
                    activo_bono=True,
                )
            )
        )
        self.pasa_bono_ventas = self.bono_ventas > 0
        self.total_a_pagar = _money(
            self.sub1 + self.bono_ventas + self.ajuste_positivo + self.bono_extra - self.ajuste_negativo
        )


class RegistroDiarioVentas(models.Model):
    bono = models.ForeignKey(BonoVentasEmpleado, on_delete=models.CASCADE, related_name="registros")
    dia = models.PositiveSmallIntegerField()
    tiene_uniforme = models.BooleanField(default=True)
    tiene_puntualidad = models.BooleanField(default=True)
    tiene_asistencia = models.BooleanField(default=True)
    puntos_de_vista = models.CharField(max_length=500, blank=True, default="")
    capturado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("bono", "dia")
        ordering = ["dia"]

    def __str__(self) -> str:
        return f"{self.bono.empleado.nombre} - día {self.dia}"
