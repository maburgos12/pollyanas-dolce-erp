from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError, models, transaction
from django.db.models import Q, Sum
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
    NIVEL_COLABORADOR = "COLABORADOR"
    NIVEL_ENCARGADA = "ENCARGADA"
    NIVEL_SUPERVISION = "SUPERVISION"
    NIVEL_JEFATURA = "JEFATURA"
    NIVEL_DIRECCION = "DIRECCION"
    NIVEL_ORGANIZACIONAL_CHOICES = [
        (NIVEL_COLABORADOR, "Colaborador"),
        (NIVEL_ENCARGADA, "Encargada / encargado"),
        (NIVEL_SUPERVISION, "Supervisión"),
        (NIVEL_JEFATURA, "Jefatura"),
        (NIVEL_DIRECCION, "Dirección"),
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
    # Fuente única por id (FASE 1). `sucursal` (texto) se conserva como legacy/display
    # mientras los demás consumidores migran a este FK. No emparejar por nombre.
    sucursal_ref = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="empleados",
    )
    departamento_origen = models.CharField(max_length=40, choices=DEP_CHOICES, blank=True, default="", db_index=True)
    departamento = models.CharField(max_length=40, choices=DEP_CHOICES, blank=True, default="", db_index=True)
    puesto_operativo = models.CharField(max_length=80, blank=True, default="", db_index=True)
    nivel_organizacional = models.CharField(
        max_length=20,
        choices=NIVEL_ORGANIZACIONAL_CHOICES,
        blank=True,
        default="",
        db_index=True,
    )
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
    banco = models.CharField(max_length=80, blank=True, default="")
    cuenta_clabe = models.CharField(max_length=18, blank=True, default="")
    numero_cuenta = models.CharField(max_length=20, blank=True, default="")
    activo = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nombre", "id"]
        verbose_name = "Empleado"
        verbose_name_plural = "Empleados"

    def __str__(self) -> str:
        return f"{self.codigo} · {self.nombre}" if self.codigo else self.nombre

    @property
    def sucursal_display(self) -> str:
        """Nombre canónico de la sucursal para mostrar (FK); cae al texto legacy.
        Usar en payloads/UI en vez de `sucursal` (texto) para evitar nombres viejos.
        Prefetch/select_related `sucursal_ref` cuando se itere sobre muchos."""
        if self.sucursal_ref_id:
            return self.sucursal_ref.nombre
        return self.sucursal or ""

    def _generate_codigo(self) -> str:
        yymm = timezone.localdate().strftime("%y%m")
        prefix = f"EMP-{yymm}-"
        seq = Empleado.objects.filter(codigo__startswith=prefix).count() + 1
        return f"{prefix}{seq:03d}"

    def save(self, *args, **kwargs):
        self.codigo = (self.codigo or "").strip()
        self.nombre_normalizado = normalizar_nombre(self.nombre or "")
        if not self.codigo:
            self.codigo = self._generate_codigo()
        super().save(*args, **kwargs)


class CatalogoFuncionOperativa(models.Model):
    codigo = models.CharField(max_length=80, unique=True, db_index=True)
    etiqueta = models.CharField(max_length=120)
    departamento_origen = models.CharField(max_length=40, choices=Empleado.DEP_CHOICES, blank=True, default="", db_index=True)
    departamento_actual = models.CharField(max_length=40, choices=Empleado.DEP_CHOICES, blank=True, default="", db_index=True)
    puesto_operativo = models.CharField(max_length=80, blank=True, default="", db_index=True)
    nivel_organizacional = models.CharField(
        max_length=20,
        choices=Empleado.NIVEL_ORGANIZACIONAL_CHOICES,
        default=Empleado.NIVEL_COLABORADOR,
        db_index=True,
    )
    activo = models.BooleanField(default=True, db_index=True)
    sistema = models.BooleanField(default=False)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["departamento_actual", "etiqueta", "codigo"]
        verbose_name = "Función operativa"
        verbose_name_plural = "Funciones operativas"

    def __str__(self) -> str:
        return f"{self.codigo} · {self.etiqueta}"

    def save(self, *args, **kwargs):
        import re

        if not self.codigo:
            base = normalizar_nombre(self.etiqueta or "").upper()
            self.codigo = base
        self.codigo = re.sub(r"[^A-Z0-9_ ]+", " ", normalizar_nombre(self.codigo or "").upper()).strip()[:80]
        self.codigo = re.sub(r"\s+", " ", self.codigo)
        self.etiqueta = (self.etiqueta or "").strip()
        self.departamento_origen = (self.departamento_origen or "").strip().upper()
        self.departamento_actual = (self.departamento_actual or "").strip().upper()
        self.puesto_operativo = (self.puesto_operativo or "").strip().upper()
        self.nivel_organizacional = (self.nivel_organizacional or Empleado.NIVEL_COLABORADOR).strip().upper()
        super().save(*args, **kwargs)


class EmpleadoIdentidadPendiente(models.Model):
    FUENTE_HIKVISION = "hikvision"
    FUENTE_NOMINA = "nomina"
    FUENTE_CHOICES = [
        (FUENTE_HIKVISION, "Hikvision / checador"),
        (FUENTE_NOMINA, "Nómina / lista de raya"),
    ]

    ESTADO_PENDIENTE = "pendiente"
    ESTADO_VINCULADO = "vinculado"
    ESTADO_DESCARTADO = "descartado"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_VINCULADO, "Vinculado"),
        (ESTADO_DESCARTADO, "Descartado"),
    ]

    fuente = models.CharField(max_length=20, choices=FUENTE_CHOICES, db_index=True)
    codigo_externo = models.CharField(max_length=40, db_index=True)
    nombre_externo = models.CharField(max_length=180)
    nombre_normalizado = models.CharField(max_length=180, db_index=True, editable=False)
    empleado_sugerido = models.ForeignKey(
        Empleado,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="identidades_sugeridas",
    )
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE, db_index=True)
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)
    resuelto_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="identidades_rrhh_resueltas",
    )
    resuelto_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ("fuente", "codigo_externo")
        ordering = ["estado", "-actualizado_en"]
        verbose_name = "Identidad pendiente de empleado"
        verbose_name_plural = "Identidades pendientes de empleados"

    def save(self, *args, **kwargs):
        self.codigo_externo = (self.codigo_externo or "").strip()
        self.nombre_externo = (self.nombre_externo or "").strip()
        self.nombre_normalizado = normalizar_nombre(self.nombre_externo or "")
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.codigo_externo} · {self.nombre_externo}"


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
    ESTADO_REVISION_RRHH = "revision_rrhh"
    ESTADO_PENDIENTE_DIRECCION = "pendiente_direccion"
    ESTADO_AUTORIZADA = "autorizada"
    ESTADO_RECLUTAMIENTO = "reclutamiento"
    ESTADO_CUBIERTA = "cubierta"
    ESTADO_PAUSADA = "pausada"
    ESTADO_DEVUELTA_CORRECCION = "devuelta_correccion"
    ESTADO_RECHAZADA = "rechazada"
    ESTADO_CANCELADA = "cancelada"
    ESTADO_CHOICES = [
        (ESTADO_SOLICITADA, "Solicitada"),
        (ESTADO_REVISION_RRHH, "En revisión RRHH"),
        (ESTADO_PENDIENTE_DIRECCION, "Pendiente autorización"),
        (ESTADO_AUTORIZADA, "Autorizada"),
        (ESTADO_RECLUTAMIENTO, "En reclutamiento"),
        (ESTADO_CUBIERTA, "Cubierta"),
        (ESTADO_PAUSADA, "Pausada"),
        (ESTADO_DEVUELTA_CORRECCION, "Devuelta a corrección"),
        (ESTADO_RECHAZADA, "Rechazada"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    TIPO_REEMPLAZO = "reemplazo"
    TIPO_NUEVA_POSICION = "nueva_posicion"
    TIPO_TEMPORAL = "temporal"
    TIPO_CHOICES = [
        (TIPO_REEMPLAZO, "Reemplazo"),
        (TIPO_NUEVA_POSICION, "Nueva posición"),
        (TIPO_TEMPORAL, "Temporal"),
    ]

    PRIORIDAD_NORMAL = "normal"
    PRIORIDAD_ALTA = "alta"
    PRIORIDAD_URGENTE = "urgente"
    PRIORIDAD_CHOICES = [
        (PRIORIDAD_NORMAL, "Normal"),
        (PRIORIDAD_ALTA, "Alta"),
        (PRIORIDAD_URGENTE, "Urgente"),
    ]

    AUTORIZACION_JEFE_DIRECTO = "jefe_directo"
    AUTORIZACION_DIRECCION = "direccion_general"
    AUTORIZACION_CHOICES = [
        (AUTORIZACION_JEFE_DIRECTO, "Jefe directo"),
        (AUTORIZACION_DIRECCION, "Dirección General"),
    ]

    folio = models.CharField(max_length=20, unique=True, blank=True, editable=False)
    sucursal = models.ForeignKey(
        "core.Sucursal",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_rrhh",
    )
    departamento = models.CharField(max_length=40, choices=Empleado.DEP_CHOICES, blank=True, default="")
    area = models.CharField(max_length=120)
    puesto = models.CharField(max_length=120)
    cantidad_solicitada = models.PositiveSmallIntegerField(default=1)
    tipo_solicitud = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_REEMPLAZO)
    prioridad = models.CharField(max_length=12, choices=PRIORIDAD_CHOICES, default=PRIORIDAD_NORMAL)
    fecha_solicitada = models.DateField()
    fecha_necesaria = models.DateField(null=True, blank=True)
    estado = models.CharField(max_length=24, choices=ESTADO_CHOICES, default=ESTADO_SOLICITADA)
    fecha_cubierta = models.DateField(null=True, blank=True)
    empleado_cubrio = models.ForeignKey(
        "rrhh.Empleado",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_cubiertas",
    )
    motivo_no_cubierta = models.TextField(blank=True, default="")
    motivo_solicitud = models.TextField(blank=True, default="")
    sugerencias = models.TextField(blank=True, default="")
    solicitado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_solicitadas",
    )
    validado_rrhh_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_validadas_rrhh",
    )
    fecha_validacion_rrhh = models.DateTimeField(null=True, blank=True)
    autorizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_autorizadas",
    )
    autorizador_asignado = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_por_autorizar",
    )
    tipo_autorizacion = models.CharField(
        max_length=24,
        choices=AUTORIZACION_CHOICES,
        default=AUTORIZACION_JEFE_DIRECTO,
        db_index=True,
    )
    requiere_direccion = models.BooleanField(default=False, db_index=True)
    fecha_autorizacion = models.DateTimeField(null=True, blank=True)
    rechazado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacantes_rechazadas",
    )
    fecha_rechazo = models.DateTimeField(null=True, blank=True)
    motivo_rechazo = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_solicitada", "area", "puesto"]
        verbose_name = "Vacante RRHH"
        verbose_name_plural = "Vacantes RRHH"

    @property
    def cubiertas_count(self) -> int:
        if not self.pk:
            return 0
        return self.coberturas.count()

    @property
    def pendientes_count(self) -> int:
        return max(int(self.cantidad_solicitada or 1) - self.cubiertas_count, 0)

    @property
    def esta_completa(self) -> bool:
        return self.pendientes_count == 0

    @property
    def dias_en_cubrir(self) -> int | None:
        if not self.fecha_cubierta:
            return None
        return max((self.fecha_cubierta - self.fecha_solicitada).days, 0)

    def save(self, *args, **kwargs):
        if not self.folio:
            import random
            import string

            today = timezone.localdate()
            while True:
                sufijo = "".join(random.choices(string.digits, k=4))
                folio = f"VAC-{today.strftime('%y%m')}-{sufijo}"
                if not VacanteRRHH.objects.filter(folio=folio).exists():
                    self.folio = folio
                    break
        self.area = (self.area or "").strip().upper()
        self.puesto = (self.puesto or "").strip().upper()
        self.departamento = (self.departamento or "").strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.folio} · {self.area} · {self.puesto} · {self.get_estado_display()}"


class VacanteMovimiento(models.Model):
    vacante = models.ForeignKey("rrhh.VacanteRRHH", on_delete=models.CASCADE, related_name="movimientos")
    estado_anterior = models.CharField(max_length=24, blank=True, default="")
    estado_nuevo = models.CharField(max_length=24, choices=VacanteRRHH.ESTADO_CHOICES)
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    comentario = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["creado_en", "id"]
        verbose_name = "Movimiento de vacante"
        verbose_name_plural = "Movimientos de vacante"

    def __str__(self) -> str:
        return f"{self.vacante.folio} · {self.estado_anterior or '-'} -> {self.estado_nuevo}"


class VacanteCobertura(models.Model):
    vacante = models.ForeignKey("rrhh.VacanteRRHH", on_delete=models.CASCADE, related_name="coberturas")
    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="vacantes_cobertura")
    fecha_cobertura = models.DateField(default=timezone.localdate)
    nota = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["fecha_cobertura", "id"]
        unique_together = [("vacante", "empleado")]
        verbose_name = "Cobertura de vacante"
        verbose_name_plural = "Coberturas de vacante"

    def __str__(self) -> str:
        return f"{self.vacante.folio} · {self.empleado.nombre}"


class CandidatoVacante(models.Model):
    FUENTE_REFERENCIA = "referencia"
    FUENTE_BOLSA = "bolsa"
    FUENTE_REDES = "redes"
    FUENTE_OTRO = "otro"
    FUENTE_CHOICES = [
        (FUENTE_REFERENCIA, "Referencia interna"),
        (FUENTE_BOLSA, "Bolsa de trabajo"),
        (FUENTE_REDES, "Redes sociales"),
        (FUENTE_OTRO, "Otro"),
    ]

    ETAPA_ENTREVISTA = "entrevista_pres"
    ETAPA_SELECCIONADO = "seleccionado"
    ETAPA_DOCUMENTOS = "documentos"
    ETAPA_PRUEBA = "prueba"
    ETAPA_CONTRATADO = "contratado"
    ETAPA_DESCARTADO = "descartado"
    ETAPA_CHOICES = [
        (ETAPA_ENTREVISTA, "Entrevista presencial"),
        (ETAPA_SELECCIONADO, "Candidato seleccionado"),
        (ETAPA_DOCUMENTOS, "Solicitud de documentos"),
        (ETAPA_PRUEBA, "Periodo de prueba"),
        (ETAPA_CONTRATADO, "Contratado"),
        (ETAPA_DESCARTADO, "Descartado"),
    ]
    ETAPAS_ACTIVAS = {ETAPA_ENTREVISTA, ETAPA_SELECCIONADO, ETAPA_DOCUMENTOS, ETAPA_PRUEBA}

    vacante = models.ForeignKey("rrhh.VacanteRRHH", on_delete=models.CASCADE, related_name="candidatos")
    nombre = models.CharField(max_length=180)
    telefono = models.CharField(max_length=30, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    fuente = models.CharField(max_length=20, choices=FUENTE_CHOICES, default=FUENTE_REFERENCIA)
    etapa_actual = models.CharField(max_length=20, choices=ETAPA_CHOICES, default=ETAPA_ENTREVISTA)
    empleado = models.ForeignKey(
        Empleado, null=True, blank=True, on_delete=models.SET_NULL, related_name="candidatura"
    )
    notas = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["etapa_actual", "nombre"]
        verbose_name = "Candidato"
        verbose_name_plural = "Candidatos"

    def __str__(self) -> str:
        return f"{self.nombre} — {self.vacante.folio}"

    @property
    def activo(self) -> bool:
        return self.etapa_actual not in {self.ETAPA_CONTRATADO, self.ETAPA_DESCARTADO}


class AltaPendienteEmpleado(models.Model):
    ESTADO_PENDIENTE = "pendiente"
    ESTADO_CONVERTIDA = "convertida"
    ESTADO_CANCELADA = "cancelada"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_CONVERTIDA, "Convertida a empleado"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    vacante = models.ForeignKey("rrhh.VacanteRRHH", on_delete=models.PROTECT, related_name="altas_pendientes")
    candidato = models.ForeignKey(
        CandidatoVacante,
        on_delete=models.PROTECT,
        related_name="altas_pendientes",
    )
    empleado = models.ForeignKey(
        Empleado,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="altas_desde_vacante",
    )
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE, db_index=True)
    nombre = models.CharField(max_length=180)
    telefono = models.CharField(max_length=30, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    sucursal = models.CharField(max_length=120, blank=True, default="")
    departamento = models.CharField(max_length=40, choices=Empleado.DEP_CHOICES, blank=True, default="")
    area = models.CharField(max_length=120, blank=True, default="")
    puesto = models.CharField(max_length=120, blank=True, default="")
    fecha_ingreso_sugerida = models.DateField(null=True, blank=True)
    nota = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="altas_pendientes_creadas",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)
    convertida_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["candidato"],
                condition=Q(estado="pendiente"),
                name="rrhh_alta_pendiente_unica_por_candidato",
            )
        ]
        verbose_name = "Alta pendiente de empleado"
        verbose_name_plural = "Altas pendientes de empleados"

    def __str__(self) -> str:
        return f"{self.nombre} · {self.vacante.folio} · {self.get_estado_display()}"


class VacanteSeguimiento(models.Model):
    # Etapas activas en la UI
    ETAPA_ENTREVISTA_PRES = "entrevista_pres"
    ETAPA_SELECCIONADO = "seleccionado"
    ETAPA_DOCUMENTOS = "documentos"
    ETAPA_PRUEBA = "prueba"
    ETAPA_CONTRATADO = "contratado"
    ETAPA_DESCARTADO = "descartado"
    ETAPA_COMENTARIO = "comentario"
    # Legacy (compatibilidad registros anteriores)
    ETAPA_POSTULANTE = "postulante"
    ETAPA_ENTREVISTA_TEL = "entrevista_tel"
    ETAPA_FILTRO = "filtro"
    ETAPA_BUSQUEDA = "busqueda"
    ETAPA_CONTACTO = "contacto"
    ETAPA_ENTREVISTA = "entrevista"
    ETAPA_OFERTA = "oferta"
    ETAPA_CHOICES = [
        (ETAPA_ENTREVISTA_PRES, "Entrevista presencial"),
        (ETAPA_SELECCIONADO, "Candidato seleccionado"),
        (ETAPA_DOCUMENTOS, "Solicitud de documentos"),
        (ETAPA_PRUEBA, "Periodo de prueba"),
        (ETAPA_CONTRATADO, "Contratado"),
        (ETAPA_DESCARTADO, "Descartado"),
        (ETAPA_COMENTARIO, "Nota"),
        (ETAPA_POSTULANTE, "Candidatos recibidos (legacy)"),
        (ETAPA_ENTREVISTA_TEL, "1ra entrevista tel. (legacy)"),
        (ETAPA_FILTRO, "Filtro preselección (legacy)"),
        (ETAPA_BUSQUEDA, "Búsqueda (legacy)"),
        (ETAPA_CONTACTO, "Contacto (legacy)"),
        (ETAPA_ENTREVISTA, "Entrevista (legacy)"),
        (ETAPA_OFERTA, "Oferta (legacy)"),
    ]

    vacante = models.ForeignKey("rrhh.VacanteRRHH", on_delete=models.CASCADE, related_name="seguimientos")
    candidato_obj = models.ForeignKey(
        CandidatoVacante, null=True, blank=True, on_delete=models.SET_NULL, related_name="seguimientos"
    )
    etapa = models.CharField(max_length=20, choices=ETAPA_CHOICES, default=ETAPA_COMENTARIO)
    candidato = models.CharField(max_length=180, blank=True, default="")
    comentario = models.TextField()
    fecha = models.DateField(default=timezone.localdate)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fecha", "-creado_en", "-id"]
        verbose_name = "Seguimiento de vacante"
        verbose_name_plural = "Seguimientos de vacante"

    def __str__(self) -> str:
        return f"{self.vacante.folio} · {self.get_etapa_display()} · {self.fecha}"


class EmpleadoDocumento(models.Model):
    TIPO_NSS = "nss"
    TIPO_FISCAL = "fiscal"
    TIPO_INE = "ine"
    TIPO_CURP = "curp"
    TIPO_ACTA = "acta"
    TIPO_DOMICILIO = "domicilio"
    TIPO_ESTUDIOS = "estudios"
    TIPO_CARTA_LABORAL = "carta_laboral"
    TIPO_BANCO = "banco"
    TIPO_OTRO = "otro"
    TIPO_CHOICES = [
        (TIPO_NSS, "NSS"),
        (TIPO_FISCAL, "Constancia de situación fiscal"),
        (TIPO_INE, "INE"),
        (TIPO_CURP, "CURP"),
        (TIPO_ACTA, "Acta de nacimiento"),
        (TIPO_DOMICILIO, "Comprobante de domicilio"),
        (TIPO_ESTUDIOS, "Constancia de estudios"),
        (TIPO_CARTA_LABORAL, "Carta laboral"),
        (TIPO_BANCO, "Cuenta bancaria"),
        (TIPO_OTRO, "Otro"),
    ]
    # Tipos que se capturan como texto (sin archivo requerido)
    TIPOS_TEXTO = {TIPO_NSS, TIPO_CURP, TIPO_BANCO}
    # Tipos que sincronizan un campo en Empleado
    CAMPO_EMPLEADO_MAP = {
        TIPO_NSS: "nss",
        TIPO_CURP: "curp",
        TIPO_FISCAL: "rfc",
    }

    empleado = models.ForeignKey(
        Empleado, on_delete=models.CASCADE, related_name="documentos"
    )
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    archivo = models.FileField(
        upload_to="rrhh/documentos/", blank=True, null=True,
        help_text="PDF o imagen del documento"
    )
    valor_texto = models.CharField(
        max_length=200, blank=True, default="",
        help_text="NSS, CURP, RFC u otro dato de texto"
    )
    notas = models.TextField(blank=True, default="")
    subido_por = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name="documentos_subidos"
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["empleado", "tipo", "-creado_en"]
        verbose_name = "Documento de empleado"
        verbose_name_plural = "Documentos de empleados"

    def __str__(self) -> str:
        return f"{self.empleado} · {self.get_tipo_display()}"

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        # Sincronizar campos del empleado si aplica
        campo = self.CAMPO_EMPLEADO_MAP.get(self.tipo)
        if campo and self.valor_texto:
            Empleado.objects.filter(pk=self.empleado_id).update(**{campo: self.valor_texto.strip().upper()})
        # Banco: sincronizar los tres subcampos si vienen en notas estructuradas
        # (la view los guarda directamente en Empleado; aquí solo es fallback)


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
    salida_comida = models.DateTimeField(null=True, blank=True)
    regreso_comida = models.DateTimeField(null=True, blank=True)
    salida = models.DateTimeField(null=True, blank=True)
    minutos_comida = models.PositiveIntegerField(default=0)
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
    ESTADO_CANCELADO = "cancelado"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente autorización"),
        (ESTADO_AUTORIZADO, "Autorizado"),
        (ESTADO_RECHAZADO, "Rechazado"),
        (ESTADO_PAGADO, "Pagado en nómina"),
        (ESTADO_CANCELADO, "Cancelado"),
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


class PermisoSalidaCambio(models.Model):
    ACCION_EDITAR = "editar"
    ACCION_ELIMINAR = "eliminar"
    ACCION_CHOICES = [
        (ACCION_EDITAR, "Correccion"),
        (ACCION_ELIMINAR, "Eliminacion"),
    ]

    permiso = models.ForeignKey(
        PermisoSalida,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="cambios",
    )
    folio = models.CharField(max_length=20, db_index=True)
    empleado_nombre = models.CharField(max_length=180, blank=True, default="")
    accion = models.CharField(max_length=12, choices=ACCION_CHOICES)
    motivo = models.TextField()
    cambios = models.JSONField(default=dict, blank=True)
    realizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="permisos_salida_cambios",
    )
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Cambio de permiso"
        verbose_name_plural = "Cambios de permisos"

    def __str__(self) -> str:
        return f"{self.folio} · {self.get_accion_display()}"


class SuspensionEmpleado(models.Model):
    ESTADO_ACTIVA = "activa"
    ESTADO_CANCELADA = "cancelada"
    ESTADO_CHOICES = [
        (ESTADO_ACTIVA, "Activa"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="suspensiones")
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    motivo = models.TextField()
    con_goce = models.BooleanField(default=False)
    estado = models.CharField(max_length=12, choices=ESTADO_CHOICES, default=ESTADO_ACTIVA)
    aplicada_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        on_delete=models.SET_NULL,
        related_name="suspensiones_aplicadas",
    )
    comentario_cancelacion = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_inicio"]
        verbose_name = "Suspensión"
        verbose_name_plural = "Suspensiones"

    @property
    def dias_naturales(self) -> int:
        return (self.fecha_fin - self.fecha_inicio).days + 1

    def clean(self):
        if self.fecha_inicio and self.fecha_fin and self.fecha_fin < self.fecha_inicio:
            raise ValidationError({"fecha_fin": "La fecha final no puede ser anterior a la fecha inicial."})

    def __str__(self) -> str:
        return f"{self.empleado} · {self.fecha_inicio} a {self.fecha_fin}"


class IncapacidadEmpleado(models.Model):
    TIPO_ENFERMEDAD_GENERAL = "enfermedad_general"
    TIPO_RIESGO_TRABAJO = "riesgo_trabajo"
    TIPO_MATERNIDAD = "maternidad"
    TIPO_OTRO = "otro"
    TIPO_CHOICES = [
        (TIPO_ENFERMEDAD_GENERAL, "Enfermedad general"),
        (TIPO_RIESGO_TRABAJO, "Riesgo de trabajo"),
        (TIPO_MATERNIDAD, "Maternidad"),
        (TIPO_OTRO, "Otro"),
    ]
    ESTADO_ACTIVA = "activa"
    ESTADO_CERRADA = "cerrada"
    ESTADO_CANCELADA = "cancelada"
    ESTADO_CHOICES = [
        (ESTADO_ACTIVA, "Activa"),
        (ESTADO_CERRADA, "Cerrada"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="incapacidades")
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    tipo = models.CharField(max_length=24, choices=TIPO_CHOICES, default=TIPO_ENFERMEDAD_GENERAL, db_index=True)
    folio = models.CharField(max_length=80, blank=True, default="")
    estado = models.CharField(max_length=12, choices=ESTADO_CHOICES, default=ESTADO_ACTIVA, db_index=True)
    notas = models.TextField(blank=True, default="")
    registrada_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="incapacidades_registradas",
    )
    comentario_cancelacion = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_inicio", "-id"]
        constraints = [
            models.CheckConstraint(
                check=Q(fecha_fin__gte=models.F("fecha_inicio")),
                name="rrhh_incapacidad_fecha_fin_gte_inicio",
            ),
            models.UniqueConstraint(
                fields=["empleado", "folio"],
                condition=~Q(folio=""),
                name="rrhh_incapacidad_folio_unico_empleado",
            )
        ]
        verbose_name = "Incapacidad"
        verbose_name_plural = "Incapacidades"

    @property
    def dias_naturales(self) -> int:
        return (self.fecha_fin - self.fecha_inicio).days + 1

    def clean(self):
        if self.fecha_inicio and self.fecha_fin and self.fecha_fin < self.fecha_inicio:
            raise ValidationError({"fecha_fin": "La fecha final no puede ser anterior a la fecha inicial."})
        if self.estado != self.ESTADO_CANCELADA and self.empleado_id and self.fecha_inicio and self.fecha_fin:
            traslape = IncapacidadEmpleado.objects.filter(
                empleado_id=self.empleado_id,
                estado__in=[self.ESTADO_ACTIVA, self.ESTADO_CERRADA],
                fecha_inicio__lte=self.fecha_fin,
                fecha_fin__gte=self.fecha_inicio,
            ).exclude(pk=self.pk)
            if traslape.exists():
                raise ValidationError("Ya existe una incapacidad no cancelada que cruza esas fechas.")

    def save(self, *args, **kwargs):
        self.folio = (self.folio or "").strip()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.empleado} · {self.fecha_inicio} a {self.fecha_fin}"


class ReglamentoLaboral(models.Model):
    ESTADO_BORRADOR = "borrador"
    ESTADO_VIGENTE = "vigente"
    ESTADO_ARCHIVADO = "archivado"
    ESTADO_CHOICES = [
        (ESTADO_BORRADOR, "Borrador"),
        (ESTADO_VIGENTE, "Vigente"),
        (ESTADO_ARCHIVADO, "Archivado"),
    ]

    nombre = models.CharField(max_length=180)
    empresa = models.CharField(max_length=180, default="GRUPO EMPRESARIAL FONSMA S.A. DE C.V.")
    version = models.CharField(max_length=40, default="2026-04-09")
    fecha_documento = models.DateField(null=True, blank=True)
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES, default=ESTADO_BORRADOR, db_index=True)
    fuente_archivo = models.CharField(max_length=240, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_documento", "nombre"]
        verbose_name = "Reglamento laboral"
        verbose_name_plural = "Reglamentos laborales"

    def __str__(self) -> str:
        return f"{self.nombre} · {self.version}"


class ReglaLaboral(models.Model):
    TIPO_VACACIONES = "vacaciones"
    TIPO_DESCANSO = "descanso"
    TIPO_PERMISOS = "permisos"
    TIPO_ASISTENCIA = "asistencia"
    TIPO_DISCIPLINA = "disciplina"
    TIPO_CHOICES = [
        (TIPO_VACACIONES, "Vacaciones"),
        (TIPO_DESCANSO, "Descanso"),
        (TIPO_PERMISOS, "Permisos"),
        (TIPO_ASISTENCIA, "Asistencia"),
        (TIPO_DISCIPLINA, "Disciplina"),
    ]

    reglamento = models.ForeignKey(ReglamentoLaboral, on_delete=models.CASCADE, related_name="reglas")
    clave = models.CharField(max_length=60)
    capitulo = models.CharField(max_length=120, blank=True, default="")
    articulo = models.CharField(max_length=40, blank=True, default="")
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    titulo = models.CharField(max_length=160)
    texto = models.TextField()
    aplica_en_sistema = models.BooleanField(default=True)
    orden = models.PositiveSmallIntegerField(default=0)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["orden", "id"]
        unique_together = [("reglamento", "clave")]
        verbose_name = "Regla laboral"
        verbose_name_plural = "Reglas laborales"

    def __str__(self) -> str:
        return f"{self.articulo or self.clave} · {self.titulo}"


class PoliticaVacaciones(models.Model):
    reglamento = models.ForeignKey(
        ReglamentoLaboral,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="politicas_vacaciones",
    )
    nombre = models.CharField(max_length=140, default="Vacaciones LFT vigente")
    antiguedad_desde = models.PositiveSmallIntegerField()
    antiguedad_hasta = models.PositiveSmallIntegerField(null=True, blank=True)
    dias_laborables = models.DecimalField(max_digits=5, decimal_places=2)
    prima_porcentaje = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("25.00"))
    vigente_desde = models.DateField(default=timezone.localdate)
    vigente_hasta = models.DateField(null=True, blank=True)
    activo = models.BooleanField(default=True, db_index=True)
    notas = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["antiguedad_desde"]
        verbose_name = "Política de vacaciones"
        verbose_name_plural = "Políticas de vacaciones"

    def __str__(self) -> str:
        hasta = self.antiguedad_hasta if self.antiguedad_hasta is not None else "+"
        return f"{self.antiguedad_desde}-{hasta} años · {self.dias_laborables} días"


class SolicitudVacaciones(models.Model):
    ESTADO_SOLICITADA = "solicitada"
    ESTADO_PREAUTORIZADA = "preautorizada"
    ESTADO_APROBADA = "aprobada"
    ESTADO_RECHAZADA = "rechazada"
    ESTADO_CANCELADA = "cancelada"
    ESTADO_CHOICES = [
        (ESTADO_SOLICITADA, "Solicitada"),
        (ESTADO_PREAUTORIZADA, "Preautorizada por jefe"),
        (ESTADO_APROBADA, "Aprobada"),
        (ESTADO_RECHAZADA, "Rechazada"),
        (ESTADO_CANCELADA, "Cancelada"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="solicitudes_vacaciones")
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    dias_laborables = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("0"))
    motivo = models.TextField(blank=True, default="")
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_SOLICITADA, db_index=True)
    jefe_directo = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacaciones_por_preautorizar",
    )
    preautorizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacaciones_preautorizadas",
    )
    fecha_preautorizacion = models.DateTimeField(null=True, blank=True)
    aprobado_rrhh_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacaciones_aprobadas_rrhh",
    )
    fecha_aprobacion_rrhh = models.DateTimeField(null=True, blank=True)
    folio = models.CharField(max_length=20, unique=True, editable=False)
    notas_rrhh = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="vacaciones_solicitadas",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Solicitud de vacaciones"
        verbose_name_plural = "Solicitudes de vacaciones"

    def __str__(self) -> str:
        return f"{self.folio} · {self.empleado}"

    def save(self, *args, **kwargs):
        if not self.folio:
            import random
            import string
            from datetime import date

            while True:
                sufijo = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
                folio = f"VAC-{date.today().strftime('%y%m')}-{sufijo}"
                if not SolicitudVacaciones.objects.filter(folio=folio).exists():
                    self.folio = folio
                    break
        super().save(*args, **kwargs)


class MovimientoVacaciones(models.Model):
    TIPO_GENERADO = "generado"
    TIPO_RESERVADO = "reservado"
    TIPO_CONSUMIDO = "consumido"
    TIPO_LIBERADO = "liberado"
    TIPO_AJUSTE = "ajuste"
    TIPO_CHOICES = [
        (TIPO_GENERADO, "Generado"),
        (TIPO_RESERVADO, "Reservado"),
        (TIPO_CONSUMIDO, "Consumido"),
        (TIPO_LIBERADO, "Liberado"),
        (TIPO_AJUSTE, "Ajuste manual"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="movimientos_vacaciones")
    solicitud = models.ForeignKey(
        SolicitudVacaciones,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="movimientos",
    )
    tipo = models.CharField(max_length=16, choices=TIPO_CHOICES, db_index=True)
    dias = models.DecimalField(max_digits=6, decimal_places=2)
    periodo_anio = models.PositiveSmallIntegerField(db_index=True)
    descripcion = models.CharField(max_length=220, blank=True, default="")
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        verbose_name = "Movimiento de vacaciones"
        verbose_name_plural = "Movimientos de vacaciones"

    def __str__(self) -> str:
        return f"{self.empleado} · {self.tipo} · {self.dias}"


class IncidenciaAsistencia(models.Model):
    TIPO_USO_TOLERANCIA = "uso_tolerancia"
    TIPO_RETARDO = "retardo"
    TIPO_RETARDO_TOLERANCIA = "retardo_tolerancia"
    TIPO_FALTA = "falta"
    TIPO_FALTA_RETARDOS = "falta_retardos"
    TIPO_JORNADA_INCOMPLETA = "jornada_incompleta"
    TIPO_HORA_EXTRA_PENDIENTE = "hora_extra_pendiente"
    TIPO_COMIDA_EXCEDIDA = "comida_excedida"
    TIPO_SUSPENSION = "suspension"
    TIPO_AVISO_BAJA_FALTAS = "aviso_baja_faltas"
    TIPO_BAJA_FALTAS = "baja_faltas"
    TIPO_CHOICES = [
        (TIPO_USO_TOLERANCIA, "Uso de tolerancia"),
        (TIPO_RETARDO, "Retardo"),
        (TIPO_RETARDO_TOLERANCIA, "Retardo por tolerancia recurrente"),
        (TIPO_FALTA, "Falta"),
        (TIPO_FALTA_RETARDOS, "Falta por retardos"),
        (TIPO_JORNADA_INCOMPLETA, "Jornada incompleta"),
        (TIPO_HORA_EXTRA_PENDIENTE, "Hora extra pendiente"),
        (TIPO_COMIDA_EXCEDIDA, "Comida excedida"),
        (TIPO_SUSPENSION, "Suspensión"),
        (TIPO_AVISO_BAJA_FALTAS, "Aviso por faltas"),
        (TIPO_BAJA_FALTAS, "Baja por faltas"),
    ]

    ESTADO_PENDIENTE = "pendiente"
    ESTADO_CONCILIADO = "conciliado"
    ESTADO_RESUELTO = "resuelto"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_CONCILIADO, "Conciliado"),
        (ESTADO_RESUELTO, "Resuelto"),
    ]

    SEVERIDAD_INFO = "info"
    SEVERIDAD_MEDIA = "media"
    SEVERIDAD_ALTA = "alta"
    SEVERIDAD_CRITICA = "critica"
    SEVERIDAD_CHOICES = [
        (SEVERIDAD_INFO, "Informativa"),
        (SEVERIDAD_MEDIA, "Media"),
        (SEVERIDAD_ALTA, "Alta"),
        (SEVERIDAD_CRITICA, "Critica"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="incidencias_asistencia")
    fecha = models.DateField(db_index=True)
    tipo = models.CharField(max_length=32, choices=TIPO_CHOICES, db_index=True)
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE, db_index=True)
    severidad = models.CharField(max_length=12, choices=SEVERIDAD_CHOICES, default=SEVERIDAD_MEDIA)
    asistencia = models.ForeignKey(
        AsistenciaEmpleado,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="incidencias",
    )
    permiso = models.ForeignKey(
        PermisoSalida,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="incidencias_asistencia",
    )
    solicitud_vacaciones = models.ForeignKey(
        SolicitudVacaciones,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="incidencias_asistencia",
    )
    hora_extra = models.ForeignKey(
        HoraExtra,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="incidencias_asistencia",
    )
    minutos = models.IntegerField(default=0)
    goce_sueldo = models.BooleanField(null=True, blank=True)
    ventana_inicio = models.DateField(null=True, blank=True)
    ventana_fin = models.DateField(null=True, blank=True)
    conteo_retardos_15d = models.PositiveSmallIntegerField(default=0)
    conteo_faltas_30d = models.PositiveSmallIntegerField(default=0)
    detalle = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    editado_manual = models.BooleanField(default=False)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [("empleado", "fecha", "tipo")]
        ordering = ["-fecha", "empleado__nombre", "tipo"]
        verbose_name = "Incidencia de asistencia"
        verbose_name_plural = "Incidencias de asistencia"

    def __str__(self) -> str:
        return f"{self.empleado} · {self.fecha} · {self.get_tipo_display()}"


class IncidenciaAsistenciaBitacora(models.Model):
    incidencia = models.ForeignKey(IncidenciaAsistencia, on_delete=models.CASCADE, related_name="bitacora")
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    campo = models.CharField(max_length=80)
    valor_anterior = models.TextField(blank=True)
    valor_nuevo = models.TextField(blank=True)
    comentario = models.TextField()
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Bitácora de incidencia de asistencia"
        verbose_name_plural = "Bitácora de incidencias de asistencia"

    def __str__(self) -> str:
        return f"{self.incidencia} · {self.campo}"


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


class PrenominaCorte(models.Model):
    TIPO_SEMANAL = "SEMANAL"
    TIPO_QUINCENAL = "QUINCENAL"
    TIPO_MENSUAL = "MENSUAL"
    TIPO_RANGO = "RANGO"
    TIPO_CHOICES = [
        (TIPO_SEMANAL, "Semanal"),
        (TIPO_QUINCENAL, "Quincenal"),
        (TIPO_MENSUAL, "Mensual"),
        (TIPO_RANGO, "Rango manual"),
    ]

    ESTADO_BORRADOR = "BORRADOR"
    ESTADO_EN_REVISION = "EN_REVISION"
    ESTADO_LISTO = "LISTO"
    ESTADO_EXPORTADO = "EXPORTADO"
    ESTADO_CERRADO = "CERRADO"
    ESTADO_CHOICES = [
        (ESTADO_BORRADOR, "Borrador"),
        (ESTADO_EN_REVISION, "En revision"),
        (ESTADO_LISTO, "Listo"),
        (ESTADO_EXPORTADO, "Exportado"),
        (ESTADO_CERRADO, "Cerrado"),
    ]

    folio = models.CharField(max_length=40, unique=True, blank=True)
    fecha_inicio = models.DateField(db_index=True)
    fecha_fin = models.DateField(db_index=True)
    fecha_corte = models.DateField(db_index=True)
    tipo_periodo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_QUINCENAL)
    sucursal = models.CharField(max_length=120, blank=True, default="")
    area = models.CharField(max_length=120, blank=True, default="")
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_BORRADOR, db_index=True)
    resumen = models.JSONField(default=dict, blank=True)
    notas = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="prenomina_cortes_creados",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_fin", "-id"]
        verbose_name = "Corte de prenomina"
        verbose_name_plural = "Cortes de prenomina"

    def _generate_folio(self) -> str:
        period = self.fecha_inicio.strftime("%Y%m")
        prefix = f"PRE-{period}-"
        max_seq = 0
        for folio in PrenominaCorte.objects.filter(folio__startswith=prefix).values_list("folio", flat=True):
            try:
                max_seq = max(max_seq, int(folio.removeprefix(prefix)))
            except ValueError:
                continue
        return f"{prefix}{max_seq + 1:03d}"

    def save(self, *args, **kwargs):
        if self.folio:
            super().save(*args, **kwargs)
            return
        for attempt in range(5):
            self.folio = self._generate_folio()
            try:
                with transaction.atomic():
                    super().save(*args, **kwargs)
                return
            except IntegrityError:
                self.folio = ""
                if attempt == 4:
                    raise

    def __str__(self) -> str:
        return self.folio


class PrenominaEmpleadoResumen(models.Model):
    ESTADO_LISTO = "LISTO"
    ESTADO_REVISAR = "REVISAR"
    ESTADO_BLOQUEADO = "BLOQUEADO"
    ESTADO_CHOICES = [
        (ESTADO_LISTO, "Listo"),
        (ESTADO_REVISAR, "Revisar"),
        (ESTADO_BLOQUEADO, "Bloqueado"),
    ]

    corte = models.ForeignKey(PrenominaCorte, on_delete=models.CASCADE, related_name="resumenes")
    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="resumenes_prenomina")
    dias_periodo = models.PositiveSmallIntegerField(default=0)
    dias_laborables = models.PositiveSmallIntegerField(default=0)
    dias_no_laborados_pre_ingreso = models.PositiveSmallIntegerField(default=0)
    dias_asistencia = models.PositiveSmallIntegerField(default=0)
    faltas = models.PositiveSmallIntegerField(default=0)
    retardos = models.PositiveSmallIntegerField(default=0)
    suspensiones = models.PositiveSmallIntegerField(default=0)
    incapacidades = models.PositiveSmallIntegerField(default=0)
    permisos = models.PositiveSmallIntegerField(default=0)
    vacaciones = models.PositiveSmallIntegerField(default=0)
    horas_extra_autorizadas = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0"))
    ajustes_pendientes = models.PositiveSmallIntegerField(default=0)
    alertas_bloqueantes = models.PositiveSmallIntegerField(default=0)
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_LISTO, db_index=True)
    observaciones = models.TextField(blank=True, default="")
    snapshot = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [("corte", "empleado")]
        ordering = ["empleado__nombre", "id"]
        verbose_name = "Resumen de empleado en prenomina"
        verbose_name_plural = "Resumenes de empleados en prenomina"

    def __str__(self) -> str:
        return f"{self.corte.folio} · {self.empleado.nombre}"


class PrenominaEquivalenciaCONTPAQi(models.Model):
    tipo_movimiento_erp = models.CharField(max_length=32, unique=True, db_index=True)
    clave_contpaqi = models.CharField(max_length=20)
    descripcion = models.CharField(max_length=180)
    aplica_valor = models.BooleanField(default=False)
    aplica_horas = models.BooleanField(default=False)
    aplica_importe = models.BooleanField(default=False)
    activo = models.BooleanField(default=True, db_index=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["tipo_movimiento_erp"]
        verbose_name = "Equivalencia CONTPAQi prenomina"
        verbose_name_plural = "Equivalencias CONTPAQi prenomina"

    def __str__(self) -> str:
        return f"{self.tipo_movimiento_erp} -> {self.clave_contpaqi}"


class PrenominaMovimiento(models.Model):
    TIPO_FALTA = "FALTA"
    TIPO_SUSPENSION = "SUSPENSION"
    TIPO_HORA_EXTRA = "HORA_EXTRA"
    TIPO_INCAPACIDAD = "INCAPACIDAD"
    TIPO_CHOICES = [
        (TIPO_FALTA, "Falta"),
        (TIPO_SUSPENSION, "Suspension"),
        (TIPO_HORA_EXTRA, "Hora extra"),
        (TIPO_INCAPACIDAD, "Incapacidad"),
    ]

    ESTADO_PENDIENTE_CONFIGURACION = "PENDIENTE_CONFIGURACION"
    ESTADO_LISTO = "LISTO"
    ESTADO_BLOQUEADO = "BLOQUEADO"
    ESTADO_EXPORTADO = "EXPORTADO"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE_CONFIGURACION, "Pendiente configuracion"),
        (ESTADO_LISTO, "Listo"),
        (ESTADO_BLOQUEADO, "Bloqueado"),
        (ESTADO_EXPORTADO, "Exportado"),
    ]

    corte = models.ForeignKey(PrenominaCorte, on_delete=models.CASCADE, related_name="movimientos")
    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="movimientos_prenomina")
    fecha = models.DateField(db_index=True)
    tipo_movimiento_erp = models.CharField(max_length=32, choices=TIPO_CHOICES, db_index=True)
    clave_contpaqi = models.CharField(max_length=20, blank=True, default="")
    estado = models.CharField(
        max_length=32,
        choices=ESTADO_CHOICES,
        default=ESTADO_PENDIENTE_CONFIGURACION,
        db_index=True,
    )
    valor = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    horas = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    importe = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    fuente = models.CharField(max_length=80, blank=True, default="")
    fuente_modelo = models.CharField(max_length=80, blank=True, default="", db_index=True)
    fuente_id = models.CharField(max_length=80, blank=True, default="", db_index=True)
    referencia = models.CharField(max_length=120, blank=True, default="")
    notas = models.TextField(blank=True, default="")
    metadata = models.JSONField(default=dict, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["empleado__nombre", "fecha", "tipo_movimiento_erp", "id"]
        verbose_name = "Movimiento de prenomina"
        verbose_name_plural = "Movimientos de prenomina"
        constraints = [
            models.UniqueConstraint(
                fields=["corte", "fuente_modelo", "fuente_id", "tipo_movimiento_erp"],
                condition=Q(fuente_modelo__gt="", fuente_id__gt=""),
                name="rrhh_prenomina_movimiento_fuente_unica",
            )
        ]

    def aplicar_equivalencia(self) -> bool:
        equivalencia = PrenominaEquivalenciaCONTPAQi.objects.filter(
            tipo_movimiento_erp=self.tipo_movimiento_erp,
            activo=True,
        ).first()
        if not equivalencia:
            self.clave_contpaqi = ""
            self.estado = self.ESTADO_PENDIENTE_CONFIGURACION
            return False
        self.clave_contpaqi = equivalencia.clave_contpaqi
        self.estado = self.ESTADO_LISTO
        return True

    def __str__(self) -> str:
        return f"{self.corte.folio} · {self.empleado.nombre} · {self.tipo_movimiento_erp}"


class AjusteAsistencia(models.Model):
    TIPO_ENTRADA = "entrada"
    TIPO_SALIDA = "salida"
    TIPO_SALIDA_COMIDA = "salida_comida"
    TIPO_REGRESO_COMIDA = "regreso_comida"
    TIPO_TURNO = "turno"
    TIPO_OBSERVACION = "observacion"
    TIPO_CHOICES = [
        (TIPO_ENTRADA, "Entrada"),
        (TIPO_SALIDA, "Salida"),
        (TIPO_SALIDA_COMIDA, "Salida comida"),
        (TIPO_REGRESO_COMIDA, "Regreso comida"),
        (TIPO_TURNO, "Turno"),
        (TIPO_OBSERVACION, "Observacion"),
    ]

    ESTADO_PENDIENTE = "PENDIENTE"
    ESTADO_APROBADO = "APROBADO"
    ESTADO_RECHAZADO = "RECHAZADO"
    ESTADO_APLICADO = "APLICADO"
    ESTADO_CHOICES = [
        (ESTADO_PENDIENTE, "Pendiente"),
        (ESTADO_APROBADO, "Aprobado"),
        (ESTADO_RECHAZADO, "Rechazado"),
        (ESTADO_APLICADO, "Aplicado"),
    ]

    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="ajustes_asistencia")
    fecha = models.DateField(db_index=True)
    asistencia = models.ForeignKey(
        AsistenciaEmpleado,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ajustes",
    )
    tipo_ajuste = models.CharField(max_length=24, choices=TIPO_CHOICES, db_index=True)
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE, db_index=True)
    valores_anteriores = models.JSONField(default=dict, blank=True)
    valores_propuestos = models.JSONField(default=dict, blank=True)
    valores_aplicados = models.JSONField(default=dict, blank=True)
    motivo = models.TextField()
    comentario_autorizacion = models.TextField(blank=True, default="")
    solicitado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ajustes_asistencia_solicitados",
    )
    autorizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ajustes_asistencia_autorizados",
    )
    aplicado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ajustes_asistencia_aplicados",
    )
    solicitado_en = models.DateTimeField(auto_now_add=True)
    autorizado_en = models.DateTimeField(null=True, blank=True)
    aplicado_en = models.DateTimeField(null=True, blank=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha", "empleado__nombre", "-id"]
        verbose_name = "Ajuste de asistencia"
        verbose_name_plural = "Ajustes de asistencia"

    def __str__(self) -> str:
        return f"{self.empleado.nombre} · {self.fecha} · {self.get_tipo_ajuste_display()}"

    def clean(self):
        super().clean()
        if not self.asistencia_id:
            return
        errors = {}
        if self.empleado_id and self.asistencia.empleado_id != self.empleado_id:
            errors["asistencia"] = "La asistencia debe pertenecer al mismo empleado del ajuste."
        if self.fecha and self.asistencia.fecha != self.fecha:
            errors["fecha"] = "La fecha del ajuste debe coincidir con la fecha de la asistencia."
        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)


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
