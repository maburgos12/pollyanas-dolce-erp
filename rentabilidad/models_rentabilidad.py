"""
sucursales/models_rentabilidad.py
Añadir este contenido al models.py del app 'sucursales' (o crear app 'rentabilidad').

Relación: cada Sucursal puede tener múltiples registros mensuales
de SucursalRentabilidad. Esto permite historial, tendencias y comparativas.
"""

from django.db import models
from django.utils import timezone
from django.core.validators import MinValueValidator
from decimal import Decimal


class EstadoRentabilidad(models.TextChoices):
    SUBSIDIADA     = "SUBSIDIADA",     "Subsidiada"
    EQUILIBRIO     = "EQUILIBRIO",     "Punto de equilibrio"
    RECUPERACION   = "RECUPERACION",   "En recuperación"
    RENTABLE       = "RENTABLE",       "Rentable"
    ESTRELLA       = "ESTRELLA",       "Estrella"
    SIN_DATOS      = "SIN_DATOS",      "Sin datos suficientes"


class SucursalRentabilidad(models.Model):
    """
    Snapshot mensual de rentabilidad por sucursal.
    Se genera automáticamente por Celery Beat al cierre de cada mes
    y también puede calcularse manualmente desde el dashboard.
    """

    # ------------------------------------------------------------------ #
    # Relación
    # ------------------------------------------------------------------ #
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.CASCADE,
        related_name="rentabilidad_mensual",
    )
    periodo = models.DateField(
        help_text="Primer día del mes que representa este registro. Ej: 2025-01-01"
    )

    # ------------------------------------------------------------------ #
    # Ingresos
    # ------------------------------------------------------------------ #
    ventas_brutas        = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    descuentos           = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    devoluciones         = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    @property
    def ventas_netas(self):
        return self.ventas_brutas - self.descuentos - self.devoluciones

    # ------------------------------------------------------------------ #
    # Costos variables
    # ------------------------------------------------------------------ #
    costo_materia_prima  = models.DecimalField(max_digits=14, decimal_places=2, default=0,
        help_text="Costo total de ingredientes vendidos (CMV)")
    empaque              = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    otros_costos_variables = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    @property
    def costo_variable_total(self):
        return self.costo_materia_prima + self.empaque + self.otros_costos_variables

    @property
    def margen_bruto(self):
        """Ventas netas - costos variables"""
        if self.ventas_netas == 0:
            return Decimal("0")
        return self.ventas_netas - self.costo_variable_total

    @property
    def porcentaje_margen_bruto(self):
        if self.ventas_netas == 0:
            return Decimal("0")
        return (self.margen_bruto / self.ventas_netas * 100).quantize(Decimal("0.01"))

    # ------------------------------------------------------------------ #
    # Gastos fijos de la sucursal
    # ------------------------------------------------------------------ #
    renta                = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    nomina_directa       = models.DecimalField(max_digits=12, decimal_places=2, default=0,
        help_text="Nómina de empleados que trabajan directamente en la sucursal")
    servicios_luz_agua   = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    mantenimiento        = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    gastos_admin_prorrateados = models.DecimalField(max_digits=12, decimal_places=2, default=0,
        help_text="Porción de gastos corporativos (DG, contabilidad, etc.) asignada a esta sucursal")
    otros_gastos_fijos   = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    @property
    def gasto_fijo_total(self):
        return (
            self.renta + self.nomina_directa + self.servicios_luz_agua
            + self.mantenimiento + self.gastos_admin_prorrateados
            + self.otros_gastos_fijos
        )

    # ------------------------------------------------------------------ #
    # Utilidad operativa
    # ------------------------------------------------------------------ #
    @property
    def utilidad_operativa(self):
        """EBITDA simplificado: margen bruto - gastos fijos"""
        return self.margen_bruto - self.gasto_fijo_total

    @property
    def porcentaje_utilidad_operativa(self):
        if self.ventas_netas == 0:
            return Decimal("0")
        return (self.utilidad_operativa / self.ventas_netas * 100).quantize(Decimal("0.01"))

    @property
    def es_operativamente_rentable(self):
        return self.utilidad_operativa > 0

    # ------------------------------------------------------------------ #
    # Punto de equilibrio
    # ------------------------------------------------------------------ #
    @property
    def punto_equilibrio_mensual(self):
        """
        PE = Gastos Fijos / (1 - (Costos Variables / Ventas Netas))
        Cuánto hay que vender para cubrir todos los costos.
        """
        if self.ventas_netas == 0:
            return Decimal("0")
        ratio_cv = self.costo_variable_total / self.ventas_netas
        if ratio_cv >= 1:
            return Decimal("0")
        return (self.gasto_fijo_total / (1 - ratio_cv)).quantize(Decimal("0.01"))

    @property
    def porcentaje_avance_pe(self):
        """Qué % del punto de equilibrio cubrieron las ventas reales"""
        pe = self.punto_equilibrio_mensual
        if pe == 0:
            return Decimal("0")
        return (self.ventas_netas / pe * 100).quantize(Decimal("0.01"))

    @property
    def brecha_pe(self):
        """
        Positivo = cuánto le FALTA para llegar al PE.
        Negativo = cuánto SOBREPASÓ el PE (excedente).
        """
        return self.punto_equilibrio_mensual - self.ventas_netas

    # ------------------------------------------------------------------ #
    # Inversión y ROI
    # ------------------------------------------------------------------ #
    inversion_inicial = models.DecimalField(
        max_digits=14, decimal_places=2, default=0,
        help_text="Inversión total para abrir la sucursal (obra, equipo, inventario inicial, etc.)"
    )
    fecha_apertura = models.DateField(null=True, blank=True)

    @property
    def meses_operando(self):
        if not self.fecha_apertura:
            return None
        from dateutil.relativedelta import relativedelta
        delta = relativedelta(self.periodo, self.fecha_apertura)
        return delta.years * 12 + delta.months + 1

    @property
    def utilidad_acumulada(self):
        """Suma de utilidades operativas desde apertura hasta este periodo"""
        qs = SucursalRentabilidad.objects.filter(
            sucursal=self.sucursal,
            periodo__lte=self.periodo,
        )
        total = Decimal("0")
        for r in qs:
            total += r.utilidad_operativa
        return total

    @property
    def inversion_recuperada(self):
        """Cuánto de la inversión se ha recuperado con utilidades acumuladas"""
        if self.inversion_inicial == 0:
            return Decimal("0")
        rec = self.utilidad_acumulada
        return min(rec, self.inversion_inicial)

    @property
    def porcentaje_recuperacion_inversion(self):
        if self.inversion_inicial == 0:
            return Decimal("100")
        return (self.inversion_recuperada / self.inversion_inicial * 100).quantize(Decimal("0.01"))

    @property
    def inversion_pendiente(self):
        return max(self.inversion_inicial - self.utilidad_acumulada, Decimal("0"))

    @property
    def payback_meses_estimados(self):
        """
        Meses estimados para recuperar inversión completa
        basado en la utilidad promedio de los últimos 3 meses.
        """
        if self.inversion_pendiente == 0:
            return 0
        ultimos = SucursalRentabilidad.objects.filter(
            sucursal=self.sucursal,
            periodo__lte=self.periodo,
        ).order_by("-periodo")[:3]
        if not ultimos:
            return None
        promedio = sum(r.utilidad_operativa for r in ultimos) / len(ultimos)
        if promedio <= 0:
            return None  # nunca se va a recuperar con el ritmo actual
        return int((self.inversion_pendiente / promedio).quantize(Decimal("1")))

    @property
    def roi_anualizado(self):
        """ROI anualizado = (Utilidad anual estimada / Inversión) × 100"""
        if self.inversion_inicial == 0:
            return None
        ultimos = SucursalRentabilidad.objects.filter(
            sucursal=self.sucursal,
            periodo__lte=self.periodo,
        ).order_by("-periodo")[:12]
        if not ultimos:
            return None
        util_12 = sum(r.utilidad_operativa for r in ultimos)
        return (util_12 / self.inversion_inicial * 100).quantize(Decimal("0.01"))

    # ------------------------------------------------------------------ #
    # Subsidio (¿está siendo costeada por otras sucursales o DG?)
    # ------------------------------------------------------------------ #
    subsidio_recibido = models.DecimalField(
        max_digits=12, decimal_places=2, default=0,
        help_text="Monto que la empresa está poniendo de su propio flujo para sostener esta sucursal este mes"
    )

    @property
    def es_subsidiada(self):
        return self.utilidad_operativa < 0

    @property
    def monto_subsidio_implicito(self):
        """
        Si la utilidad es negativa, ese es el monto que el negocio
        está transfiriendo a esta sucursal (aunque no esté registrado explícitamente).
        """
        return abs(min(self.utilidad_operativa, Decimal("0")))

    # ------------------------------------------------------------------ #
    # Clasificación automática de estado
    # ------------------------------------------------------------------ #
    estado = models.CharField(
        max_length=20,
        choices=EstadoRentabilidad.choices,
        default=EstadoRentabilidad.SIN_DATOS,
        db_index=True,
    )
    diagnostico_ia     = models.TextField(blank=True, default="",
        help_text="Texto generado por el agente IA explicando el estado de la sucursal")
    recomendaciones_ia = models.JSONField(default=list,
        help_text="Lista de acciones recomendadas por el agente")
    alerta_nivel       = models.IntegerField(default=0,
        help_text="0=ok, 1=atención, 2=urgente")

    # ------------------------------------------------------------------ #
    # Control
    # ------------------------------------------------------------------ #
    calculado_en = models.DateTimeField(auto_now=True)
    calculado_por_agente = models.BooleanField(default=False)
    notas_manuales = models.TextField(blank=True, default="")

    class Meta:
        unique_together = ("sucursal", "periodo")
        ordering = ["-periodo", "sucursal"]
        verbose_name = "Rentabilidad de sucursal"
        verbose_name_plural = "Rentabilidad de sucursales"

    def __str__(self):
        return f"{self.sucursal} — {self.periodo.strftime('%b %Y')} — {self.get_estado_display()}"

    def calcular_estado(self):
        """
        Clasificación por reglas deterministas.
        El agente IA complementa con contexto cualitativo.
        """
        pct = float(self.porcentaje_avance_pe)
        util = float(self.utilidad_operativa)
        roi  = float(self.roi_anualizado or 0)
        inv  = float(self.porcentaje_recuperacion_inversion)

        if self.ventas_brutas == 0 and self.gasto_fijo_total > 0:
            self.estado = EstadoRentabilidad.SUBSIDIADA
            self.alerta_nivel = 2
        elif self.ventas_brutas == 0:
            self.estado = EstadoRentabilidad.SIN_DATOS
            self.alerta_nivel = 0
        elif util < 0 and pct < 80:
            self.estado = EstadoRentabilidad.SUBSIDIADA
            self.alerta_nivel = 2
        elif util < 0 and pct >= 80:
            self.estado = EstadoRentabilidad.EQUILIBRIO
            self.alerta_nivel = 1
        elif util >= 0 and inv < 100:
            self.estado = EstadoRentabilidad.RECUPERACION
            self.alerta_nivel = 0
        elif util >= 0 and inv >= 100 and roi < 20:
            self.estado = EstadoRentabilidad.RENTABLE
            self.alerta_nivel = 0
        elif util >= 0 and inv >= 100 and roi >= 20:
            self.estado = EstadoRentabilidad.ESTRELLA
            self.alerta_nivel = 0
        return self.estado

    def save(self, *args, **kwargs):
        self.calcular_estado()
        super().save(*args, **kwargs)
