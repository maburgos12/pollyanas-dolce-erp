from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models, transaction

from rrhh.models import Empleado, NominaLinea, NominaPeriodo


AREA_HORNOS = "HORNOS"
AREA_EMBETUNADO = "EMBETUNADO"
AREA_PRODUCCION = "PRODUCCION"
AREA_ARMADO = "ARMADO"
AREA_LOGISTICA = "LOGISTICA"
AREA_CRUCERO = "CRUCERO"

AREAS_PRODUCCION = [
    (AREA_HORNOS, "Hornos"),
    (AREA_PRODUCCION, "Producción"),
    (AREA_ARMADO, "Armado"),
    (AREA_LOGISTICA, "Logística"),
    (AREA_CRUCERO, "Crucero"),
]


def normalizar_area_produccion(value: str) -> str:
    area = (value or "").strip().upper()
    aliases = {
        "HORNO": AREA_HORNOS,
        "HORNOS": AREA_HORNOS,
        "EMBETUNADO": AREA_EMBETUNADO,
        "EMBETUNADOS": AREA_EMBETUNADO,
        "PRODUCCION": AREA_PRODUCCION,
        "PRODUCCIÓN": AREA_PRODUCCION,
        "PROD": AREA_PRODUCCION,
        "ARMADO": AREA_ARMADO,
        "ARMADOS": AREA_ARMADO,
        "LOGISTICA": AREA_LOGISTICA,
        "LOGÍSTICA": AREA_LOGISTICA,
        "CRUCERO": AREA_CRUCERO,
    }
    return aliases.get(area, area)


def area_bono_produccion_empleado(empleado: Empleado) -> str:
    puesto_operativo = (empleado.puesto_operativo or "").strip().upper()
    if puesto_operativo in {AREA_HORNOS, AREA_ARMADO, AREA_CRUCERO}:
        return puesto_operativo
    if puesto_operativo in {"PRODUCCION", "EMBETUNADO"}:
        return AREA_PRODUCCION
    if puesto_operativo == "ENVIO_SUCURSAL":
        return AREA_LOGISTICA
    return normalizar_area_produccion(empleado.area)


def _money(value) -> Decimal:
    return Decimal(value or 0).quantize(Decimal("0.01"))


class ConfigBonoPeriodo(models.Model):
    mes = models.PositiveSmallIntegerField()
    anio = models.PositiveSmallIntegerField()
    dias_laborables = models.PositiveSmallIntegerField(default=23)
    monto_hornos = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("1000.00"))
    monto_embetunado = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("850.00"))
    monto_area_produccion = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("850.00"))
    monto_armado = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("850.00"))
    monto_logistica = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("850.00"))
    monto_crucero = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("950.00"))
    pct_produccion = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("65.00"))
    pct_asistencia = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("15.00"))
    pct_puntualidad = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("15.00"))
    pct_uniforme = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("5.00"))
    premio_embetunado = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("400.00"))
    limite_uniforme = models.PositiveSmallIntegerField(default=1)
    limite_asistencia = models.PositiveSmallIntegerField(default=2)
    limite_puntualidad = models.PositiveSmallIntegerField(default=2)
    limite_produccion = models.PositiveSmallIntegerField(default=2)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("mes", "anio")
        ordering = ["-anio", "-mes"]
        verbose_name = "Configuración de bono - Producción"
        verbose_name_plural = "Configuraciones de bonos - Producción"

    def __str__(self) -> str:
        return f"Config Producción {self.mes}/{self.anio}"

    def get_monto_area(self, area: str) -> Decimal:
        return {
            AREA_HORNOS: self.monto_hornos,
            AREA_EMBETUNADO: self.monto_embetunado,
            AREA_PRODUCCION: self.monto_area_produccion,
            AREA_ARMADO: self.monto_armado,
            AREA_LOGISTICA: self.monto_logistica,
            AREA_CRUCERO: self.monto_crucero,
        }.get(normalizar_area_produccion(area), Decimal("0.00"))

    def get_regla_area(self, area: str) -> "ConfigBonoArea":
        area = normalizar_area_produccion(area)
        regla, _ = self.reglas_area.get_or_create(
            area=area,
            defaults=ConfigBonoArea.defaults_for_area(area),
        )
        return regla

    @transaction.atomic
    def asegurar_reglas_area(self) -> None:
        for area, _label in AREAS_PRODUCCION:
            self.reglas_area.get_or_create(area=area, defaults=ConfigBonoArea.defaults_for_area(area))

    @transaction.atomic
    def recalcular_todos(self) -> int:
        from .empleados import bonos_produccion_elegibles_queryset

        self.asegurar_reglas_area()
        bonos = list(bonos_produccion_elegibles_queryset(self.bonos.all()).select_related("empleado"))
        max_embetunados = max(
            (bono.total_embetunados or 0 for bono in bonos if bono.area == AREA_PRODUCCION),
            default=0,
        )
        for bono in bonos:
            bono.gano_premio_embetunado = (
                bono.area == AREA_PRODUCCION
                and max_embetunados > 0
                and (bono.total_embetunados or 0) == max_embetunados
            )
            bono.recalcular()
            bono.save()
        return len(bonos)

    @transaction.atomic
    def aplicar_a_nomina(self, nomina: NominaPeriodo) -> int:
        from .empleados import bonos_produccion_elegibles_queryset

        self.recalcular_todos()
        updated = 0
        bonos = bonos_produccion_elegibles_queryset(
            self.bonos.select_related("empleado").filter(estatus__in=["BORRADOR", "CERRADO"])
        )
        for bono in bonos:
            linea, _ = NominaLinea.objects.get_or_create(periodo=nomina, empleado=bono.empleado)
            linea.dias_trabajados = Decimal(bono.dias_trabajados)
            linea.bonos = bono.total_a_pagar
            linea.save()
            updated += 1
        nomina.recompute_totals()
        nomina.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
        return updated


class ConfigBonoArea(models.Model):
    periodo = models.ForeignKey(ConfigBonoPeriodo, on_delete=models.CASCADE, related_name="reglas_area")
    area = models.CharField(max_length=20, choices=AREAS_PRODUCCION)
    pct_produccion = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("65.00"))
    pct_asistencia = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("15.00"))
    pct_puntualidad = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("15.00"))
    pct_uniforme = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("5.00"))
    limite_uniforme = models.PositiveSmallIntegerField(default=1)
    limite_asistencia = models.PositiveSmallIntegerField(default=2)
    limite_puntualidad = models.PositiveSmallIntegerField(default=2)
    limite_produccion = models.PositiveSmallIntegerField(default=2)
    usa_produccion = models.BooleanField(default=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("periodo", "area")
        ordering = ["area"]
        verbose_name = "Regla de bono por área"
        verbose_name_plural = "Reglas de bonos por área"

    def __str__(self) -> str:
        return f"{self.periodo} - {self.get_area_display()}"

    @classmethod
    def defaults_for_area(cls, area: str) -> dict:
        area = normalizar_area_produccion(area)
        defaults = {
            "pct_produccion": Decimal("65.00"),
            "pct_asistencia": Decimal("15.00"),
            "pct_puntualidad": Decimal("15.00"),
            "pct_uniforme": Decimal("5.00"),
            "limite_uniforme": 1,
            "limite_asistencia": 2,
            "limite_puntualidad": 2,
            "limite_produccion": 2,
            "usa_produccion": True,
        }
        if area == AREA_LOGISTICA:
            defaults.update(
                {
                    "pct_produccion": Decimal("0.00"),
                    "pct_asistencia": Decimal("50.00"),
                    "pct_puntualidad": Decimal("30.00"),
                    "pct_uniforme": Decimal("20.00"),
                    "limite_produccion": 0,
                    "usa_produccion": False,
                }
            )
        return defaults


class BonoProduccionEmpleado(models.Model):
    ESTATUS_BORRADOR = "BORRADOR"
    ESTATUS_CERRADO = "CERRADO"
    ESTATUS_PAGADO = "PAGADO"
    ESTATUS = [
        (ESTATUS_BORRADOR, "Borrador"),
        (ESTATUS_CERRADO, "Cerrado"),
        (ESTATUS_PAGADO, "Pagado"),
    ]

    periodo = models.ForeignKey(ConfigBonoPeriodo, on_delete=models.PROTECT, related_name="bonos")
    empleado = models.ForeignKey(Empleado, on_delete=models.PROTECT, related_name="bonos_produccion")
    area = models.CharField(max_length=20, choices=AREAS_PRODUCCION)
    dias_trabajados = models.PositiveSmallIntegerField(default=0)
    dias_uniforme = models.PositiveSmallIntegerField(default=0)
    dias_puntualidad = models.PositiveSmallIntegerField(default=0)
    dias_asistencia = models.PositiveSmallIntegerField(default=0)
    dias_produccion = models.PositiveSmallIntegerField(default=0)
    total_embetunados = models.PositiveIntegerField(default=0)
    pasa_uniforme = models.BooleanField(default=False)
    pasa_puntualidad = models.BooleanField(default=False)
    pasa_asistencia = models.BooleanField(default=False)
    pasa_produccion = models.BooleanField(default=False)
    gano_premio_embetunado = models.BooleanField(default=False)
    monto_uniforme = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    monto_puntualidad = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    monto_asistencia = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    monto_produccion = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    monto_premio_embetunado = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    ajuste_positivo = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    ajuste_negativo = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    bono_extra = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    desc_ajuste_positivo = models.CharField(max_length=200, blank=True, default="")
    desc_ajuste_negativo = models.CharField(max_length=200, blank=True, default="")
    desc_bono_extra = models.CharField(max_length=200, blank=True, default="")
    total_a_pagar = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0.00"))
    estatus = models.CharField(max_length=20, choices=ESTATUS, default=ESTATUS_BORRADOR)
    observaciones = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("periodo", "empleado")
        ordering = ["area", "empleado__nombre"]
        verbose_name = "Bono de Producción"
        verbose_name_plural = "Bonos de Producción"

    def __str__(self) -> str:
        return f"{self.empleado.nombre} - {self.periodo}"

    def _base_dias(self) -> int:
        return int(self.dias_trabajados or self.periodo.dias_laborables or 0)

    def _monto_concepto(self, base: Decimal, pct: Decimal, pasa: bool) -> Decimal:
        if not pasa:
            return Decimal("0.00")
        return _money(base * Decimal(pct) / Decimal("100"))

    def recalcular(self) -> None:
        cfg = self.periodo
        regla = cfg.get_regla_area(self.area)
        base = _money(cfg.get_monto_area(self.area))
        dias_base = self._base_dias()
        dias_laborables = int(cfg.dias_laborables or dias_base or 0)

        self.pasa_uniforme = (dias_base - int(self.dias_uniforme or 0)) <= regla.limite_uniforme
        self.pasa_asistencia = (dias_laborables - int(self.dias_asistencia or 0)) <= regla.limite_asistencia
        self.pasa_puntualidad = (dias_base - int(self.dias_puntualidad or 0)) <= regla.limite_puntualidad
        self.pasa_produccion = True
        if regla.usa_produccion:
            self.pasa_produccion = (dias_base - int(self.dias_produccion or 0)) <= regla.limite_produccion

        self.monto_uniforme = self._monto_concepto(base, regla.pct_uniforme, self.pasa_uniforme)
        self.monto_asistencia = self._monto_concepto(base, regla.pct_asistencia, self.pasa_asistencia)
        self.monto_puntualidad = self._monto_concepto(base, regla.pct_puntualidad, self.pasa_puntualidad)
        self.monto_produccion = self._monto_concepto(base, regla.pct_produccion, self.pasa_produccion and regla.usa_produccion)
        self.monto_premio_embetunado = _money(cfg.premio_embetunado if self.gano_premio_embetunado else 0)

        bruto = _money(
            self.monto_uniforme
            + self.monto_asistencia
            + self.monto_puntualidad
            + self.monto_produccion
            + self.monto_premio_embetunado
            + self.ajuste_positivo
            + self.bono_extra
            - self.ajuste_negativo
        )
        self.total_a_pagar = max(bruto, Decimal("0.00"))

    def save(self, *args, **kwargs):
        self.area = normalizar_area_produccion(self.area)
        super().save(*args, **kwargs)


class RegistroDiarioProduccion(models.Model):
    bono = models.ForeignKey(BonoProduccionEmpleado, on_delete=models.CASCADE, related_name="registros")
    dia = models.PositiveSmallIntegerField()
    tiene_uniforme = models.BooleanField(default=True)
    tiene_puntualidad = models.BooleanField(default=True)
    tiene_asistencia = models.BooleanField(default=True)
    tiene_produccion = models.BooleanField(default=True)
    cantidad_embetunados = models.PositiveSmallIntegerField(default=0)
    observacion = models.CharField(max_length=300, blank=True, default="")
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
