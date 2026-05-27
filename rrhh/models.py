from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models
from django.db.models import Sum
from django.utils import timezone

from recetas.utils.normalizacion import normalizar_nombre


class BonoEsquema(models.Model):
    codigo = models.CharField(max_length=60, unique=True, db_index=True)
    nombre = models.CharField(max_length=120)
    departamento = models.CharField(max_length=40, blank=True, default="", db_index=True)
    area = models.CharField(max_length=120, blank=True, default="", db_index=True)
    descripcion = models.TextField(blank=True, default="")
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre"]
        verbose_name = "Esquema de bono"
        verbose_name_plural = "Esquemas de bono"

    def __str__(self) -> str:
        return self.nombre

    def save(self, *args, **kwargs):
        if not self.codigo:
            import re

            base = normalizar_nombre(self.nombre or "").upper()
            self.codigo = re.sub(r"[^A-Z0-9]+", "_", base).strip("_")[:60] or "BONO"
        self.codigo = self.codigo.upper().strip()
        self.nombre = (self.nombre or "").strip()
        self.departamento = (self.departamento or "").strip().upper()
        self.area = (self.area or "").strip().upper()
        super().save(*args, **kwargs)


class Empleado(models.Model):
    CONTRATO_FIJO = "FIJO"
    CONTRATO_TEMPORAL = "TEMPORAL"
    CONTRATO_HONORARIOS = "HONORARIOS"
    CONTRATO_CHOICES = [
        (CONTRATO_FIJO, "Fijo"),
        (CONTRATO_TEMPORAL, "Temporal"),
        (CONTRATO_HONORARIOS, "Honorarios"),
    ]
    DEP_ADMINISTRACION = "ADMINISTRACION"
    DEP_VENTAS = "VENTAS"
    DEP_PRODUCCION = "PRODUCCION"
    DEP_RRHH = "RRHH"
    DEP_COMPRAS = "COMPRAS"
    DEP_MANTENIMIENTO = "MANTENIMIENTO"
    DEP_LOGISTICA = "LOGISTICA"
    DEP_MARKETING = "MARKETING"
    DEP_CHOICES = [
        (DEP_ADMINISTRACION, "Administración"),
        (DEP_VENTAS, "Ventas"),
        (DEP_PRODUCCION, "Producción"),
        (DEP_RRHH, "Recursos Humanos"),
        (DEP_COMPRAS, "Compras"),
        (DEP_MANTENIMIENTO, "Mantenimiento"),
        (DEP_LOGISTICA, "Logística"),
        (DEP_MARKETING, "Marketing"),
    ]
    TIPO_POLLYANA = "POLLYANA"
    TIPO_EXTERNO = "EXTERNO"
    TIPO_PERSONAL_CHOICES = [
        (TIPO_POLLYANA, "Pollyana's Dolce"),
        (TIPO_EXTERNO, "Externo / apoyo"),
    ]

    codigo = models.CharField(max_length=40, unique=True, blank=True)
    nombre = models.CharField(max_length=180)
    nombre_normalizado = models.CharField(max_length=180, db_index=True, editable=False)
    rfc = models.CharField(max_length=20, blank=True, default="", db_index=True)
    curp = models.CharField(max_length=24, blank=True, default="", db_index=True)
    nss = models.CharField(max_length=30, blank=True, default="", db_index=True)
    area = models.CharField(max_length=120, blank=True, default="")
    puesto = models.CharField(max_length=120, blank=True, default="")
    tipo_contrato = models.CharField(max_length=20, choices=CONTRATO_CHOICES, default=CONTRATO_FIJO)
    fecha_ingreso = models.DateField(default=timezone.localdate)
    salario_diario = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    telefono = models.CharField(max_length=40, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    sucursal = models.CharField(max_length=120, blank=True, default="")
    departamento_origen = models.CharField(max_length=40, choices=DEP_CHOICES, blank=True, default="", db_index=True)
    departamento = models.CharField(max_length=40, choices=DEP_CHOICES, blank=True, default="", db_index=True)
    puesto_operativo = models.CharField(max_length=80, blank=True, default="", db_index=True)
    jefe_directo = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="colaboradores_directos",
    )
    usuario_erp = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="empleado_rrhh",
    )
    tipo_personal = models.CharField(
        max_length=20,
        choices=TIPO_PERSONAL_CHOICES,
        default=TIPO_POLLYANA,
        db_index=True,
    )
    participa_bonos_ventas = models.BooleanField(default=False)
    participa_bonos_produccion = models.BooleanField(default=False)
    bonos_esquemas = models.ManyToManyField(BonoEsquema, blank=True, related_name="empleados")
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
    horas_trabajadas = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0"))
    horas_dia = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0"))
    horas_extra = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0"))
    ausencias = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0"))
    incapacidades = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0"))
    sdi = models.DecimalField("Salario diario integrado", max_digits=12, decimal_places=2, default=Decimal("0"))
    sbc = models.DecimalField("Salario base cotización", max_digits=12, decimal_places=2, default=Decimal("0"))
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


class NominaConceptoLinea(models.Model):
    TIPO_PERCEPCION = "PERCEPCION"
    TIPO_DEDUCCION = "DEDUCCION"
    TIPO_OBLIGACION = "OBLIGACION"
    TIPO_CHOICES = [
        (TIPO_PERCEPCION, "Percepción"),
        (TIPO_DEDUCCION, "Deducción"),
        (TIPO_OBLIGACION, "Obligación"),
    ]

    linea = models.ForeignKey(NominaLinea, on_delete=models.CASCADE, related_name="conceptos")
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES)
    codigo_concepto = models.CharField(max_length=20, blank=True, default="")
    nombre = models.CharField(max_length=180)
    valor = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    importe = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["linea__empleado__nombre", "tipo", "codigo_concepto", "id"]
        verbose_name = "Concepto de línea nómina"
        verbose_name_plural = "Conceptos de líneas nómina"

    def __str__(self) -> str:
        return f"{self.linea} · {self.codigo_concepto} {self.nombre}"


class NominaImportacion(models.Model):
    ESTATUS_SIMULADA = "SIMULADA"
    ESTATUS_IMPORTADA = "IMPORTADA"
    ESTATUS_ERROR = "ERROR"
    ESTATUS_CHOICES = [
        (ESTATUS_SIMULADA, "Simulada"),
        (ESTATUS_IMPORTADA, "Importada"),
        (ESTATUS_ERROR, "Error"),
    ]

    archivo_nombre = models.CharField(max_length=255)
    archivo_hash = models.CharField(max_length=64, db_index=True)
    periodo = models.ForeignKey(
        NominaPeriodo,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="importaciones",
    )
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_SIMULADA)
    empleados_detectados = models.PositiveIntegerField(default=0)
    total_percepciones = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0"))
    total_deducciones = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0"))
    total_neto = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0"))
    resumen = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="nomina_importaciones",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        verbose_name = "Importación de nómina"
        verbose_name_plural = "Importaciones de nómina"

    def __str__(self) -> str:
        return f"{self.archivo_nombre} · {self.estatus}"


class EmpleadoBaja(models.Model):
    MOTIVO_ABANDONO = "abandono"
    MOTIVO_MEJOR_OPORTUNIDAD = "mejor_oportunidad"
    MOTIVO_CAMBIO_DOMICILIO = "cambio_domicilio"
    MOTIVO_AMBIENTE = "ambiente"
    MOTIVO_REMUNERACION = "remuneracion"
    MOTIVO_NO_APTO = "no_apto"
    MOTIVO_OTRO = "otro"
    MOTIVO_CHOICES = [
        (MOTIVO_ABANDONO, "Abandono laboral"),
        (MOTIVO_MEJOR_OPORTUNIDAD, "Mejor oportunidad de empleo"),
        (MOTIVO_CAMBIO_DOMICILIO, "Cambio de domicilio"),
        (MOTIVO_AMBIENTE, "Mal ambiente laboral"),
        (MOTIVO_REMUNERACION, "Baja remuneración"),
        (MOTIVO_NO_APTO, "No apto"),
        (MOTIVO_OTRO, "Otro"),
    ]

    empleado = models.ForeignKey(
        "rrhh.Empleado",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="bajas_rrhh",
    )
    nombre = models.CharField(max_length=180)
    area = models.CharField(max_length=120, blank=True, default="")
    puesto = models.CharField(max_length=120, blank=True, default="")
    tipo_contrato = models.CharField(max_length=20, choices=Empleado.CONTRATO_CHOICES, default=Empleado.CONTRATO_FIJO)
    fecha_ingreso = models.DateField()
    fecha_baja = models.DateField()
    motivo = models.CharField(max_length=32, choices=MOTIVO_CHOICES, default=MOTIVO_OTRO)
    observacion = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fecha_baja", "nombre"]
        verbose_name = "Baja de empleado"
        verbose_name_plural = "Bajas de empleados"

    @property
    def antiguedad_meses(self) -> Decimal:
        if not self.fecha_ingreso or not self.fecha_baja:
            return Decimal("0")
        dias = max((self.fecha_baja - self.fecha_ingreso).days, 0)
        return (Decimal(dias) / (Decimal("365") / Decimal("12"))).quantize(Decimal("0.01"))

    @property
    def en_periodo_prueba(self) -> bool:
        return self.antiguedad_meses <= Decimal("3")

    def save(self, *args, **kwargs):
        if self.empleado_id:
            self.nombre = self.nombre or self.empleado.nombre
            self.area = self.area or self.empleado.area
            self.puesto = self.puesto or self.empleado.puesto
            self.tipo_contrato = self.tipo_contrato or self.empleado.tipo_contrato
            self.fecha_ingreso = self.fecha_ingreso or self.empleado.fecha_ingreso
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.nombre} · {self.fecha_baja}"


class PlantillaAutorizada(models.Model):
    anio = models.PositiveSmallIntegerField()
    mes = models.PositiveSmallIntegerField(null=True, blank=True, help_text="Vacío aplica como plantilla base anual")
    area = models.CharField(max_length=120)
    puesto = models.CharField(max_length=120, blank=True, default="")
    cantidad = models.PositiveSmallIntegerField(default=0)
    notas = models.CharField(max_length=200, blank=True, default="")
    actualizado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [("anio", "mes", "area", "puesto")]
        ordering = ["-anio", "-mes", "area", "puesto"]
        verbose_name = "Plantilla autorizada"
        verbose_name_plural = "Plantillas autorizadas"

    def __str__(self) -> str:
        periodo = f"{self.anio}-{self.mes:02d}" if self.mes else str(self.anio)
        puesto = f" · {self.puesto}" if self.puesto else ""
        return f"{periodo} · {self.area}{puesto}: {self.cantidad}"


class VacanteRRHH(models.Model):
    ESTADO_SOLICITADA = "solicitada"
    ESTADO_RECLUTAMIENTO = "reclutamiento"
    ESTADO_CUBIERTA = "cubierta"
    ESTADO_PAUSADA = "pausada"
    ESTADO_CANCELADA = "cancelada"
    ESTADO_CHOICES = [
        (ESTADO_SOLICITADA, "Solicitada"),
        (ESTADO_RECLUTAMIENTO, "En reclutamiento"),
        (ESTADO_CUBIERTA, "Cubierta"),
        (ESTADO_PAUSADA, "Pausada"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    area = models.CharField(max_length=120)
    puesto = models.CharField(max_length=120)
    fecha_solicitada = models.DateField()
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES, default=ESTADO_SOLICITADA)
    fecha_cubierta = models.DateField(null=True, blank=True)
    empleado_cubrio = models.ForeignKey(
        "rrhh.Empleado",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_cubiertas",
    )
    motivo_no_cubierta = models.TextField(blank=True, default="")
    sugerencias = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_solicitada", "area", "puesto"]
        verbose_name = "Vacante RRHH"
        verbose_name_plural = "Vacantes RRHH"

    @property
    def dias_en_cubrir(self) -> int | None:
        if not self.fecha_cubierta:
            return None
        return max((self.fecha_cubierta - self.fecha_solicitada).days, 0)

    def __str__(self) -> str:
        return f"{self.area} · {self.puesto} · {self.get_estado_display()}"


class Turno(models.Model):
    nombre = models.CharField(max_length=60)
    hora_entrada = models.TimeField()
    hora_salida = models.TimeField()
    tolerancia_minutos = models.PositiveSmallIntegerField(default=10)
    activo = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Turno"
        verbose_name_plural = "Turnos"

    def __str__(self) -> str:
        return f"{self.nombre} ({self.hora_entrada}-{self.hora_salida})"


class AsistenciaEmpleado(models.Model):
    FUENTE_HIKCONNECT_API = "hikconnect_api"
    FUENTE_HIKCONNECT_EXCEL = "hikconnect_excel"
    FUENTE_POINT = "point"
    FUENTE_MANUAL = "manual"
    FUENTE_CHOICES = [
        (FUENTE_HIKCONNECT_API, "Hik-Connect API"),
        (FUENTE_HIKCONNECT_EXCEL, "Hik-Connect Excel"),
        (FUENTE_POINT, "PointMeUp"),
        (FUENTE_MANUAL, "Manual"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="asistencias")
    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.SET_NULL, null=True, blank=True)
    fecha = models.DateField()
    entrada = models.DateTimeField(null=True, blank=True)
    salida = models.DateTimeField(null=True, blank=True)
    minutos_trabajados = models.PositiveIntegerField(default=0)
    turno = models.ForeignKey(Turno, null=True, blank=True, on_delete=models.SET_NULL)
    fuente = models.CharField(max_length=20, choices=FUENTE_CHOICES, default=FUENTE_MANUAL)
    observacion = models.TextField(blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("empleado", "fecha")]
        ordering = ["-fecha"]
        verbose_name = "Asistencia"
        verbose_name_plural = "Asistencias"

    def __str__(self) -> str:
        return f"{self.empleado} · {self.fecha}"


class HoraExtra(models.Model):
    ESTADO_PENDIENTE = "pendiente"
    ESTADO_AUTORIZADO = "autorizado"
    ESTADO_RECHAZADO = "rechazado"
    ESTADO_PAGADO = "pagado"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente autorización"),
        (ESTADO_AUTORIZADO, "Autorizado"),
        (ESTADO_RECHAZADO, "Rechazado"),
        (ESTADO_PAGADO, "Pagado en nómina"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="horas_extra")
    asistencia = models.OneToOneField(
        AsistenciaEmpleado,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="hora_extra",
    )
    fecha = models.DateField()
    horas = models.DecimalField(max_digits=4, decimal_places=2)
    tasa_extra = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=Decimal("2.00"),
        help_text="Multiplicador sobre salario/hora",
    )
    monto_calculado = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    estado = models.CharField(max_length=12, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE)
    jefe_directo = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horas_extra_por_autorizar",
        verbose_name="Jefe directo / autorizador",
    )
    autorizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horas_extra_autorizadas",
    )
    fecha_autorizacion_jefe = models.DateTimeField(null=True, blank=True)
    notas = models.TextField(blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fecha"]
        verbose_name = "Hora extra"
        verbose_name_plural = "Horas extra"

    def __str__(self) -> str:
        return f"{self.empleado} · {self.fecha} · {self.horas}h"


class PermisoSalida(models.Model):
    TIPO_PERMISO_HORA = "permiso_hora"
    TIPO_PERMISO_DIA = "permiso_dia"
    TIPO_SALIDA_PERSONAL = "salida_personal"
    TIPO_CITA_MEDICA = "cita_medica"
    TIPO_OTRO = "otro"
    TIPO_CHOICES = [
        (TIPO_PERMISO_HORA, "Permiso por horas"),
        (TIPO_PERMISO_DIA, "Permiso día completo"),
        (TIPO_SALIDA_PERSONAL, "Salida personal"),
        (TIPO_CITA_MEDICA, "Cita médica"),
        (TIPO_OTRO, "Otro"),
    ]
    ESTADO_SOLICITADO = "solicitado"
    ESTADO_APROBADO = "aprobado"
    ESTADO_RECHAZADO = "rechazado"
    ESTADO_CANCELADO = "cancelado"
    ESTADO_CHOICES = [
        (ESTADO_SOLICITADO, "Solicitado"),
        (ESTADO_APROBADO, "Aprobado"),
        (ESTADO_RECHAZADO, "Rechazado"),
        (ESTADO_CANCELADO, "Cancelado"),
    ]
    ESTADO_JEFE_PENDIENTE = "pendiente"
    ESTADO_JEFE_PREAUTORIZADO = "preautorizado"
    ESTADO_JEFE_RECHAZADO = "rechazado"
    ESTADO_JEFE_CHOICES = [
        (ESTADO_JEFE_PENDIENTE, "Pendiente de jefe"),
        (ESTADO_JEFE_PREAUTORIZADO, "Preautorizado por jefe"),
        (ESTADO_JEFE_RECHAZADO, "Rechazado por jefe"),
    ]
    ESTADO_DIRECCION_NO_REQUIERE = "no_requiere"
    ESTADO_DIRECCION_PENDIENTE = "pendiente"
    ESTADO_DIRECCION_AUTORIZADO = "autorizado"
    ESTADO_DIRECCION_RECHAZADO = "rechazado"
    ESTADO_DIRECCION_CHOICES = [
        (ESTADO_DIRECCION_NO_REQUIERE, "No requiere Dirección"),
        (ESTADO_DIRECCION_PENDIENTE, "Pendiente Dirección"),
        (ESTADO_DIRECCION_AUTORIZADO, "Autorizado por Dirección"),
        (ESTADO_DIRECCION_RECHAZADO, "Rechazado por Dirección"),
    ]
    ORIGEN_RRHH = "rrhh"
    ORIGEN_BONOS_VENTAS = "bonos_ventas"
    ORIGEN_BONOS_PRODUCCION = "bonos_produccion"
    ORIGEN_CHOICES = [
        (ORIGEN_RRHH, "Capital Humano"),
        (ORIGEN_BONOS_VENTAS, "Bonos ventas"),
        (ORIGEN_BONOS_PRODUCCION, "Bonos producción"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="permisos")
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES)
    fecha_inicio = models.DateTimeField()
    fecha_fin = models.DateTimeField(null=True, blank=True)
    motivo = models.TextField()
    estado = models.CharField(max_length=12, choices=ESTADO_CHOICES, default=ESTADO_SOLICITADO)
    estado_jefe = models.CharField(max_length=16, choices=ESTADO_JEFE_CHOICES, default=ESTADO_JEFE_PENDIENTE)
    requiere_direccion = models.BooleanField(default=False, db_index=True)
    estado_direccion = models.CharField(
        max_length=16,
        choices=ESTADO_DIRECCION_CHOICES,
        default=ESTADO_DIRECCION_NO_REQUIERE,
    )
    goce_sueldo = models.BooleanField(
        default=True,
        verbose_name="Con goce de sueldo",
        help_text="Desmarca si el permiso es sin goce de sueldo",
    )
    autorizado_jefe_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="permisos_preautorizados_jefe",
    )
    fecha_autorizacion_jefe = models.DateTimeField(null=True, blank=True)
    autorizado_direccion_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="permisos_autorizados_direccion",
    )
    fecha_autorizacion_direccion = models.DateTimeField(null=True, blank=True)
    origen_solicitud = models.CharField(max_length=24, choices=ORIGEN_CHOICES, default=ORIGEN_RRHH)
    autorizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="permisos_autorizados",
    )
    foto_evidencia = models.ImageField(upload_to="permisos/", null=True, blank=True)
    folio = models.CharField(max_length=20, unique=True, editable=False)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Permiso / Salida"
        verbose_name_plural = "Permisos y Salidas"

    def __str__(self) -> str:
        return f"{self.folio} · {self.empleado}"

    def save(self, *args, **kwargs):
        if not self.pk and self.empleado_id:
            from .services_permisos import permiso_requiere_autorizacion_direccion

            self.requiere_direccion = permiso_requiere_autorizacion_direccion(self.empleado)
            self.estado_direccion = (
                self.ESTADO_DIRECCION_PENDIENTE
                if self.requiere_direccion
                else self.ESTADO_DIRECCION_NO_REQUIERE
            )
        if not self.folio:
            import random
            import string
            from datetime import date

            while True:
                sufijo = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
                folio = f"PS-{date.today().strftime('%y%m')}-{sufijo}"
                if not PermisoSalida.objects.filter(folio=folio).exists():
                    self.folio = folio
                    break
        super().save(*args, **kwargs)


class ImportacionChecador(models.Model):
    METODO_API = "api"
    METODO_EXCEL = "excel"
    METODO_CHOICES = [
        (METODO_API, "API directa"),
        (METODO_EXCEL, "Carga Excel"),
    ]

    metodo = models.CharField(max_length=6, choices=METODO_CHOICES)
    archivo = models.FileField(upload_to="checador_imports/", null=True, blank=True)
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    registros_procesados = models.PositiveIntegerField(default=0)
    errores = models.PositiveIntegerField(default=0)
    log = models.TextField(blank=True)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Importación checador"
        verbose_name_plural = "Importaciones checador"

    def __str__(self) -> str:
        return f"{self.get_metodo_display()} · {self.fecha_inicio} a {self.fecha_fin}"


class Prestamo(models.Model):
    METODO_TRANSFERENCIA = "transferencia"
    METODO_CHEQUE = "cheque"
    METODO_EFECTIVO = "efectivo"
    METODO_CHOICES = [
        (METODO_TRANSFERENCIA, "Transferencia electrónica"),
        (METODO_CHEQUE, "Cheque"),
        (METODO_EFECTIVO, "Efectivo"),
    ]

    ESTADO_SOLICITADO = "solicitado"
    ESTADO_AUTORIZADO = "autorizado"
    ESTADO_APROBADO = "aprobado"
    ESTADO_RECHAZADO = "rechazado"
    ESTADO_ACTIVO = "activo"
    ESTADO_LIQUIDADO = "liquidado"
    ESTADO_CANCELADO = "cancelado"
    ESTADO_CHOICES = [
        (ESTADO_SOLICITADO, "Solicitado"),
        (ESTADO_AUTORIZADO, "Autorizado por jefe"),
        (ESTADO_APROBADO, "Aprobado por dirección"),
        (ESTADO_RECHAZADO, "Rechazado"),
        (ESTADO_ACTIVO, "Activo - en descuento"),
        (ESTADO_LIQUIDADO, "Liquidado"),
        (ESTADO_CANCELADO, "Cancelado"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="prestamos")
    folio = models.CharField(max_length=20, unique=True, editable=False)
    concepto = models.TextField(verbose_name="Concepto del préstamo")
    metodo_pago = models.CharField(max_length=15, choices=METODO_CHOICES, default=METODO_TRANSFERENCIA)
    fecha_solicitud = models.DateField()
    fecha_deposito = models.DateField(null=True, blank=True, verbose_name="Fecha pactada para depósito")
    importe = models.DecimalField(max_digits=10, decimal_places=2)
    num_quincenas = models.PositiveSmallIntegerField(verbose_name="Número de quincenas")
    descuento_quincenal = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Descuento por quincena")
    saldo_actual = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        default=Decimal("0.00"),
        verbose_name="Saldo pendiente",
    )
    estado = models.CharField(max_length=12, choices=ESTADO_CHOICES, default=ESTADO_SOLICITADO)
    jefe_directo = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="prestamos_por_autorizar",
        verbose_name="Jefe directo / primer autorizador",
    )
    firma_jefe = models.BooleanField(default=False)
    autorizado_jefe = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="prestamos_autorizados_jefe",
    )
    fecha_auth_jefe = models.DateTimeField(null=True, blank=True)
    firma_direccion = models.BooleanField(default=False)
    autorizado_dg = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="prestamos_autorizados_dg",
    )
    fecha_auth_dg = models.DateTimeField(null=True, blank=True)
    nota_separacion = models.TextField(
        blank=True,
        help_text="En caso de baja, registrar cómo se liquidó el saldo",
    )
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="prestamos_creados",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_solicitud"]
        verbose_name = "Préstamo"
        verbose_name_plural = "Préstamos"

    def save(self, *args, **kwargs):
        if not self.folio:
            import random
            import string
            from datetime import date

            while True:
                sufijo = "".join(random.choices(string.digits, k=4))
                folio = f"PR-{date.today().strftime('%y%m')}-{sufijo}"
                if not Prestamo.objects.filter(folio=folio).exists():
                    self.folio = folio
                    break
        if not self.pk and self.saldo_actual == Decimal("0.00") and self.estado != self.ESTADO_LIQUIDADO:
            self.saldo_actual = self.importe
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.folio} - {self.empleado} - ${self.importe}"

    def recalcular_saldo(self):
        cobrado = (
            self.cuotas.filter(estado=PrestamoCuota.ESTADO_COBRADO).aggregate(Sum("monto_cobrado"))[
                "monto_cobrado__sum"
            ]
            or Decimal("0.00")
        )
        self.saldo_actual = max(self.importe - cobrado, Decimal("0.00"))
        self.save(update_fields=["saldo_actual"])
        if self.saldo_actual == Decimal("0.00") and self.estado == self.ESTADO_ACTIVO:
            self.estado = self.ESTADO_LIQUIDADO
            self.save(update_fields=["estado"])


class PrestamoCuota(models.Model):
    ESTADO_PENDIENTE = "pendiente"
    ESTADO_COBRADO = "cobrado"
    ESTADO_PARCIAL = "parcial"
    ESTADO_OMITIDO = "omitido"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_COBRADO, "Cobrado"),
        (ESTADO_PARCIAL, "Cobro parcial"),
        (ESTADO_OMITIDO, "Omitido / saltado"),
    ]
    FUENTE_MANUAL = "manual"
    FUENTE_CONTPAQ = "contpaq"
    FUENTE_CHOICES = [
        (FUENTE_MANUAL, "Registro manual"),
        (FUENTE_CONTPAQ, "Importación CONTPAQ XLS"),
    ]

    prestamo = models.ForeignKey(Prestamo, on_delete=models.CASCADE, related_name="cuotas")
    numero_quincena = models.PositiveSmallIntegerField(verbose_name="Quincena #")
    fecha_quincena = models.DateField(verbose_name="Fecha de la quincena")
    monto_esperado = models.DecimalField(max_digits=10, decimal_places=2)
    monto_cobrado = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    estado = models.CharField(max_length=10, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE)
    fuente = models.CharField(max_length=10, choices=FUENTE_CHOICES, default=FUENTE_MANUAL)
    fecha_cobro = models.DateField(null=True, blank=True)
    nota = models.CharField(max_length=200, blank=True)
    registrado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["numero_quincena"]
        unique_together = [("prestamo", "numero_quincena")]
        verbose_name = "Cuota de préstamo"
        verbose_name_plural = "Cuotas de préstamo"

    def __str__(self) -> str:
        return f"{self.prestamo.folio} · Q{self.numero_quincena} · {self.estado}"


class ImportacionNominaContpaq(models.Model):
    archivo = models.FileField(upload_to="contpaq_imports/")
    periodo_inicio = models.DateField()
    periodo_fin = models.DateField()
    quincena_num = models.PositiveSmallIntegerField(verbose_name="Número de quincena del año")
    empleados_leidos = models.PositiveIntegerField(default=0)
    prestamos_aplicados = models.PositiveIntegerField(default=0)
    prestamos_sin_match = models.PositiveIntegerField(default=0)
    diferencias_detectadas = models.PositiveIntegerField(default=0)
    log = models.TextField(blank=True)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Importación nómina CONTPAQ"
        verbose_name_plural = "Importaciones nómina CONTPAQ"

    def __str__(self) -> str:
        return f"CONTPAQ {self.periodo_inicio} - {self.periodo_fin}"
