# RRHH Prenomina CONTPAQi Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `RRHH -> Prenomina` with saved payroll cutoffs, per-person printable review, audited attendance adjustments, validation gates, and CONTPAQi Nominas movement export.

**Architecture:** Add focused RRHH models for prenomina cuts, per-employee snapshots, exportable movements, CONTPAQi mapping, and attendance adjustments. Keep business logic in new service/exporter modules, reuse existing `Empleado`, `AsistenciaEmpleado`, `IncidenciaAsistencia`, `PermisoSalida`, `SolicitudVacaciones`, `HoraExtra`, and bonus models, then expose the flow through new RRHH views/templates.

**Tech Stack:** Django 5.0, PostgreSQL, Django templates, openpyxl, existing RRHH permissions/helpers, existing ERP CSS/components.

---

## File Structure

- Create `rrhh/services_prenomina.py`: builds/recalculates prenomina cuts, employee summaries, validation status, and movements.
- Create `rrhh/services_ajustes_asistencia.py`: creates, approves, rejects, and applies attendance adjustments with before/after snapshots.
- Create `rrhh/exporters/contpaqi_prenomina.py`: exports internal review XLSX and CONTPAQi movements XLSX/CSV.
- Modify `rrhh/models.py`: add `PrenominaCorte`, `PrenominaEmpleadoResumen`, `PrenominaMovimiento`, `PrenominaEquivalenciaCONTPAQi`, and `AjusteAsistencia`.
- Modify `rrhh/admin.py`: register the new models.
- Modify `rrhh/urls.py`: add prenomina routes.
- Modify `rrhh/views.py`: add module tab for `Prenomina`.
- Create `rrhh/views_prenomina.py`: list/create/detail/person/adjust/export views.
- Create `rrhh/templates/rrhh/prenomina.html`: list and create cutoffs.
- Create `rrhh/templates/rrhh/prenomina_detail.html`: cutoff dashboard, table by employee, validations, export buttons.
- Create `rrhh/templates/rrhh/prenomina_persona.html`: printable per-person report.
- Create `rrhh/templates/rrhh/prenomina_ajuste.html`: approve/reject/apply adjustment view if not handled inline.
- Create `rrhh/tests_prenomina.py`: model, service, view, export, and integration tests.
- Modify `rrhh/tests.py` only if imports or existing navigation assertions need updates.

---

### Task 1: Models, Migration, Admin

**Files:**
- Modify: `rrhh/models.py`
- Modify: `rrhh/admin.py`
- Create: `rrhh/migrations/00XX_prenomina_cortes_ajustes.py`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Write failing model tests**

Append to `rrhh/tests_prenomina.py`:

```python
from datetime import date
from decimal import Decimal

from django.contrib.auth.models import User
from django.test import TestCase

from rrhh.models import (
    AjusteAsistencia,
    AsistenciaEmpleado,
    Empleado,
    PrenominaCorte,
    PrenominaEmpleadoResumen,
    PrenominaEquivalenciaCONTPAQi,
    PrenominaMovimiento,
)


class PrenominaModelTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-prenomina")
        self.empleado = Empleado.objects.create(
            codigo="347",
            nombre="ANAYA BERNAL CARLOS EZEQUIEL",
            fecha_ingreso=date(2026, 6, 10),
            activo=True,
            sucursal="Matriz",
        )

    def test_corte_genera_folio_y_resumen_por_empleado(self):
        corte = PrenominaCorte.objects.create(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            tipo_periodo=PrenominaCorte.TIPO_QUINCENAL,
            creado_por=self.user,
        )
        resumen = PrenominaEmpleadoResumen.objects.create(
            corte=corte,
            empleado=self.empleado,
            dias_periodo=15,
            dias_laborables=6,
            dias_no_laborados_pre_ingreso=9,
            estado=PrenominaEmpleadoResumen.ESTADO_LISTO,
        )

        self.assertTrue(corte.folio.startswith("PRE-202606-"))
        self.assertEqual(str(resumen), f"{corte.folio} · {self.empleado.nombre}")

    def test_movimiento_requiere_equivalencia_para_exportar(self):
        corte = PrenominaCorte.objects.create(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            creado_por=self.user,
        )
        movimiento = PrenominaMovimiento.objects.create(
            corte=corte,
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA,
            valor=Decimal("1"),
        )

        self.assertEqual(movimiento.estado, PrenominaMovimiento.ESTADO_PENDIENTE_CONFIGURACION)

        PrenominaEquivalenciaCONTPAQi.objects.create(
            tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA,
            clave_contpaqi="F",
            descripcion="Falta",
            aplica_valor=True,
            activo=True,
        )
        movimiento.aplicar_equivalencia()
        movimiento.save(update_fields=["clave_contpaqi", "estado"])

        self.assertEqual(movimiento.clave_contpaqi, "F")
        self.assertEqual(movimiento.estado, PrenominaMovimiento.ESTADO_LISTO)

    def test_ajuste_asistencia_guarda_valores_anteriores_y_propuestos(self):
        asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
        )
        ajuste = AjusteAsistencia.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            asistencia=asistencia,
            tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
            estado=AjusteAsistencia.ESTADO_PENDIENTE,
            valores_anteriores={"salida": None},
            valores_propuestos={"salida": "2026-06-11T18:05:00-07:00"},
            motivo="Olvido checar salida.",
            solicitado_por=self.user,
        )

        self.assertEqual(ajuste.estado, AjusteAsistencia.ESTADO_PENDIENTE)
        self.assertEqual(ajuste.valores_anteriores["salida"], None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina.PrenominaModelTests --settings=config.settings_test --noinput
```

Expected: fail with import errors for missing prenomina models.

- [ ] **Step 3: Add models**

Add to `rrhh/models.py` after `ImportacionChecador`:

```python
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
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_fin", "-id"]
        verbose_name = "Corte de prenomina"
        verbose_name_plural = "Cortes de prenomina"

    def _generate_folio(self) -> str:
        period = self.fecha_inicio.strftime("%Y%m")
        prefix = f"PRE-{period}-"
        seq = PrenominaCorte.objects.filter(folio__startswith=prefix).count() + 1
        return f"{prefix}{seq:03d}"

    def save(self, *args, **kwargs):
        if not self.folio:
            self.folio = self._generate_folio()
        super().save(*args, **kwargs)

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

    def __str__(self) -> str:
        return f"{self.corte.folio} · {self.empleado.nombre}"
```

Continue in `rrhh/models.py`:

```python
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
    tipo_movimiento_erp = models.CharField(max_length=40, choices=TIPO_CHOICES, db_index=True)
    clave_contpaqi = models.CharField(max_length=40, blank=True, default="")
    valor = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal("0"))
    horas = models.DecimalField(max_digits=8, decimal_places=2, default=Decimal("0"))
    importe = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0"))
    fuente_modelo = models.CharField(max_length=120, blank=True, default="")
    fuente_id = models.CharField(max_length=40, blank=True, default="")
    estado = models.CharField(max_length=32, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE_CONFIGURACION, db_index=True)
    referencia_erp = models.CharField(max_length=80, blank=True, default="")
    observaciones = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["empleado__nombre", "fecha", "tipo_movimiento_erp", "id"]

    def aplicar_equivalencia(self) -> None:
        eq = PrenominaEquivalenciaCONTPAQi.objects.filter(
            tipo_movimiento_erp=self.tipo_movimiento_erp,
            activo=True,
        ).first()
        if not eq:
            self.clave_contpaqi = ""
            self.estado = self.ESTADO_PENDIENTE_CONFIGURACION
            return
        self.clave_contpaqi = eq.clave_contpaqi
        self.estado = self.ESTADO_LISTO


class PrenominaEquivalenciaCONTPAQi(models.Model):
    tipo_movimiento_erp = models.CharField(max_length=40, unique=True)
    clave_contpaqi = models.CharField(max_length=40)
    descripcion = models.CharField(max_length=160, blank=True, default="")
    aplica_valor = models.BooleanField(default=True)
    aplica_horas = models.BooleanField(default=False)
    aplica_importe = models.BooleanField(default=False)
    activo = models.BooleanField(default=True)

    class Meta:
        ordering = ["tipo_movimiento_erp"]

    def __str__(self) -> str:
        return f"{self.tipo_movimiento_erp} -> {self.clave_contpaqi}"
```

Continue in `rrhh/models.py`:

```python
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
    asistencia = models.ForeignKey(AsistenciaEmpleado, null=True, blank=True, on_delete=models.SET_NULL, related_name="ajustes")
    tipo_ajuste = models.CharField(max_length=32, choices=TIPO_CHOICES)
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES, default=ESTADO_PENDIENTE, db_index=True)
    valores_anteriores = models.JSONField(default=dict, blank=True)
    valores_propuestos = models.JSONField(default=dict, blank=True)
    valores_aplicados = models.JSONField(default=dict, blank=True)
    motivo = models.TextField()
    comentario_autorizacion = models.TextField(blank=True, default="")
    solicitado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="ajustes_asistencia_solicitados")
    autorizado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="ajustes_asistencia_autorizados")
    aplicado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="ajustes_asistencia_aplicados")
    creado_en = models.DateTimeField(auto_now_add=True)
    autorizado_en = models.DateTimeField(null=True, blank=True)
    aplicado_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-fecha", "empleado__nombre", "-id"]

    def __str__(self) -> str:
        return f"{self.empleado} · {self.fecha} · {self.get_tipo_ajuste_display()}"
```

- [ ] **Step 4: Register admin**

In `rrhh/admin.py`, import and register:

```python
@admin.register(PrenominaCorte)
class PrenominaCorteAdmin(admin.ModelAdmin):
    list_display = ("folio", "fecha_inicio", "fecha_fin", "fecha_corte", "estado", "creado_por")
    list_filter = ("estado", "tipo_periodo", "fecha_corte")
    search_fields = ("folio", "notas")


@admin.register(PrenominaEmpleadoResumen)
class PrenominaEmpleadoResumenAdmin(admin.ModelAdmin):
    list_display = ("corte", "empleado", "estado", "faltas", "retardos", "ajustes_pendientes")
    list_filter = ("estado", "corte")
    search_fields = ("empleado__nombre", "empleado__codigo", "corte__folio")


@admin.register(PrenominaMovimiento)
class PrenominaMovimientoAdmin(admin.ModelAdmin):
    list_display = ("corte", "empleado", "fecha", "tipo_movimiento_erp", "clave_contpaqi", "estado")
    list_filter = ("estado", "tipo_movimiento_erp", "fecha")
    search_fields = ("empleado__nombre", "empleado__codigo", "clave_contpaqi", "referencia_erp")


@admin.register(PrenominaEquivalenciaCONTPAQi)
class PrenominaEquivalenciaCONTPAQiAdmin(admin.ModelAdmin):
    list_display = ("tipo_movimiento_erp", "clave_contpaqi", "descripcion", "activo")
    list_filter = ("activo",)
    search_fields = ("tipo_movimiento_erp", "clave_contpaqi", "descripcion")


@admin.register(AjusteAsistencia)
class AjusteAsistenciaAdmin(admin.ModelAdmin):
    list_display = ("empleado", "fecha", "tipo_ajuste", "estado", "solicitado_por", "autorizado_por")
    list_filter = ("estado", "tipo_ajuste", "fecha")
    search_fields = ("empleado__nombre", "empleado__codigo", "motivo")
```

- [ ] **Step 5: Create migration**

Run:

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp ./.venv/bin/python manage.py makemigrations rrhh
```

Expected: one new migration with the five new models.

- [ ] **Step 6: Run model tests**

Run:

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina.PrenominaModelTests --settings=config.settings_test --noinput
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add rrhh/models.py rrhh/admin.py rrhh/migrations/00XX_prenomina_cortes_ajustes.py rrhh/tests_prenomina.py
git commit -m "feat(rrhh): agregar modelos de prenomina"
```

---

### Task 2: Attendance Adjustment Service

**Files:**
- Create: `rrhh/services_ajustes_asistencia.py`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Add failing service tests**

Append to `rrhh/tests_prenomina.py`:

```python
from datetime import time
from django.utils import timezone
from rrhh.services_ajustes_asistencia import aprobar_ajuste_asistencia, crear_ajuste_asistencia, rechazar_ajuste_asistencia


class AjusteAsistenciaServiceTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-ajustes")
        self.empleado = Empleado.objects.create(
            codigo="400",
            nombre="COLABORADOR AJUSTE",
            fecha_ingreso=date(2026, 6, 1),
            activo=True,
        )
        self.asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
        )

    def test_aprobar_ajuste_de_salida_actualiza_asistencia_y_guarda_historial(self):
        salida = timezone.make_aware(datetime(2026, 6, 11, 18, 5))
        ajuste = crear_ajuste_asistencia(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
            valores_propuestos={"salida": salida.isoformat()},
            motivo="Olvido checar salida.",
            solicitado_por=self.user,
        )

        aplicado = aprobar_ajuste_asistencia(ajuste, self.user, "Validado por jefa directa.")
        self.asistencia.refresh_from_db()

        self.assertEqual(aplicado.estado, AjusteAsistencia.ESTADO_APLICADO)
        self.assertEqual(self.asistencia.salida, salida)
        self.assertEqual(aplicado.valores_anteriores["salida"], None)
        self.assertEqual(aplicado.valores_aplicados["salida"], salida.isoformat())

    def test_rechazar_ajuste_no_modifica_asistencia(self):
        salida = timezone.make_aware(datetime(2026, 6, 11, 18, 5))
        ajuste = crear_ajuste_asistencia(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            tipo_ajuste=AjusteAsistencia.TIPO_SALIDA,
            valores_propuestos={"salida": salida.isoformat()},
            motivo="Sin evidencia.",
            solicitado_por=self.user,
        )

        rechazado = rechazar_ajuste_asistencia(ajuste, self.user, "No autorizado.")
        self.asistencia.refresh_from_db()

        self.assertEqual(rechazado.estado, AjusteAsistencia.ESTADO_RECHAZADO)
        self.assertIsNone(self.asistencia.salida)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina.AjusteAsistenciaServiceTests --settings=config.settings_test --noinput
```

Expected: fail with missing module/functions.

- [ ] **Step 3: Implement service**

Create `rrhh/services_ajustes_asistencia.py`:

```python
from __future__ import annotations

from datetime import datetime

from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from rrhh.models import AjusteAsistencia, AsistenciaEmpleado
from rrhh.services_asistencia_reglas import evaluar_dia_empleado


AJUSTE_CAMPO_ASISTENCIA = {
    AjusteAsistencia.TIPO_ENTRADA: "entrada",
    AjusteAsistencia.TIPO_SALIDA: "salida",
    AjusteAsistencia.TIPO_SALIDA_COMIDA: "salida_comida",
    AjusteAsistencia.TIPO_REGRESO_COMIDA: "regreso_comida",
}


def _serializar_dt(value):
    return value.isoformat() if value else None


def _parse_dt(value: str | None):
    if not value:
        return None
    parsed = parse_datetime(value)
    if parsed and timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def crear_ajuste_asistencia(*, empleado, fecha, tipo_ajuste, valores_propuestos, motivo, solicitado_por):
    if not motivo.strip():
        raise ValueError("El motivo del ajuste es obligatorio.")
    asistencia, _ = AsistenciaEmpleado.objects.get_or_create(empleado=empleado, fecha=fecha)
    campo = AJUSTE_CAMPO_ASISTENCIA.get(tipo_ajuste)
    valores_anteriores = {}
    if campo:
        valores_anteriores[campo] = _serializar_dt(getattr(asistencia, campo))
    ajuste = AjusteAsistencia.objects.create(
        empleado=empleado,
        fecha=fecha,
        asistencia=asistencia,
        tipo_ajuste=tipo_ajuste,
        estado=AjusteAsistencia.ESTADO_PENDIENTE,
        valores_anteriores=valores_anteriores,
        valores_propuestos=valores_propuestos,
        motivo=motivo.strip(),
        solicitado_por=solicitado_por,
    )
    return ajuste


@transaction.atomic
def aprobar_ajuste_asistencia(ajuste: AjusteAsistencia, user, comentario: str = "") -> AjusteAsistencia:
    ajuste = AjusteAsistencia.objects.select_for_update().select_related("asistencia", "empleado").get(pk=ajuste.pk)
    if ajuste.estado != AjusteAsistencia.ESTADO_PENDIENTE:
        raise ValueError("Solo se pueden aprobar ajustes pendientes.")
    asistencia = ajuste.asistencia or AsistenciaEmpleado.objects.create(empleado=ajuste.empleado, fecha=ajuste.fecha)
    campo = AJUSTE_CAMPO_ASISTENCIA.get(ajuste.tipo_ajuste)
    valores_aplicados = {}
    update_fields = []
    if campo:
        nuevo_valor = _parse_dt(ajuste.valores_propuestos.get(campo))
        setattr(asistencia, campo, nuevo_valor)
        valores_aplicados[campo] = _serializar_dt(nuevo_valor)
        update_fields.append(campo)
    if update_fields:
        update_fields.append("actualizado_en")
        asistencia.save(update_fields=update_fields)
    ajuste.asistencia = asistencia
    ajuste.estado = AjusteAsistencia.ESTADO_APLICADO
    ajuste.autorizado_por = user
    ajuste.aplicado_por = user
    ajuste.autorizado_en = timezone.now()
    ajuste.aplicado_en = timezone.now()
    ajuste.comentario_autorizacion = comentario.strip()
    ajuste.valores_aplicados = valores_aplicados
    ajuste.save(update_fields=[
        "asistencia",
        "estado",
        "autorizado_por",
        "aplicado_por",
        "autorizado_en",
        "aplicado_en",
        "comentario_autorizacion",
        "valores_aplicados",
    ])
    evaluar_dia_empleado(ajuste.empleado, ajuste.fecha)
    return ajuste


@transaction.atomic
def rechazar_ajuste_asistencia(ajuste: AjusteAsistencia, user, comentario: str = "") -> AjusteAsistencia:
    ajuste = AjusteAsistencia.objects.select_for_update().get(pk=ajuste.pk)
    if ajuste.estado != AjusteAsistencia.ESTADO_PENDIENTE:
        raise ValueError("Solo se pueden rechazar ajustes pendientes.")
    ajuste.estado = AjusteAsistencia.ESTADO_RECHAZADO
    ajuste.autorizado_por = user
    ajuste.autorizado_en = timezone.now()
    ajuste.comentario_autorizacion = comentario.strip()
    ajuste.save(update_fields=["estado", "autorizado_por", "autorizado_en", "comentario_autorizacion"])
    return ajuste
```

- [ ] **Step 4: Run tests**

Run the same command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rrhh/services_ajustes_asistencia.py rrhh/tests_prenomina.py
git commit -m "feat(rrhh): auditar ajustes de asistencia"
```

---

### Task 3: Prenomina Cutoff Service

**Files:**
- Create: `rrhh/services_prenomina.py`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Add failing service tests**

Append tests for:

```python
class PrenominaServiceTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-corte")
        self.empleado = Empleado.objects.create(
            codigo="347",
            nombre="ANAYA BERNAL CARLOS EZEQUIEL",
            fecha_ingreso=date(2026, 6, 10),
            activo=True,
            sucursal="Matriz",
        )

    def test_crear_corte_no_castiga_dias_pre_ingreso(self):
        from rrhh.services_prenomina import crear_corte_prenomina

        corte = crear_corte_prenomina(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            creado_por=self.user,
        )
        resumen = corte.resumenes.get(empleado=self.empleado)

        self.assertEqual(resumen.dias_no_laborados_pre_ingreso, 9)
        self.assertEqual(resumen.faltas, 0)
        self.assertEqual(corte.movimientos.filter(empleado=self.empleado, tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA).count(), 0)

    def test_falta_conciliada_genera_movimiento_si_tiene_equivalencia(self):
        from rrhh.models import IncidenciaAsistencia
        from rrhh.services_prenomina import crear_corte_prenomina

        PrenominaEquivalenciaCONTPAQi.objects.create(
            tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA,
            clave_contpaqi="F",
            descripcion="Falta",
            aplica_valor=True,
        )
        IncidenciaAsistencia.objects.create(
            empleado=self.empleado,
            fecha=date(2026, 6, 11),
            tipo=IncidenciaAsistencia.TIPO_FALTA,
            estado=IncidenciaAsistencia.ESTADO_CONCILIADO,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            detalle="Falta validada.",
        )

        corte = crear_corte_prenomina(
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 15),
            fecha_corte=date(2026, 6, 15),
            creado_por=self.user,
        )

        mov = corte.movimientos.get(empleado=self.empleado, tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA)
        self.assertEqual(mov.clave_contpaqi, "F")
        self.assertEqual(mov.estado, PrenominaMovimiento.ESTADO_LISTO)
```

- [ ] **Step 2: Run tests to verify they fail**

Expected: fail with missing `crear_corte_prenomina`.

- [ ] **Step 3: Implement service**

Create `rrhh/services_prenomina.py` with:

```python
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction

from rrhh.models import (
    AjusteAsistencia,
    Empleado,
    HoraExtra,
    IncidenciaAsistencia,
    PrenominaCorte,
    PrenominaEmpleadoResumen,
    PrenominaMovimiento,
)


def _fechas(inicio: date, fin: date) -> list[date]:
    return [inicio + timedelta(days=offset) for offset in range((fin - inicio).days + 1)]


def _empleados_del_periodo(inicio: date, fin: date, sucursal: str = "", area: str = ""):
    qs = Empleado.objects.filter(activo=True)
    if sucursal:
        qs = qs.filter(sucursal__icontains=sucursal)
    if area:
        qs = qs.filter(area__icontains=area)
    return list(qs.order_by("nombre", "codigo"))


@transaction.atomic
def crear_corte_prenomina(*, fecha_inicio, fecha_fin, fecha_corte, creado_por, tipo_periodo=PrenominaCorte.TIPO_QUINCENAL, sucursal="", area="", notas=""):
    corte = PrenominaCorte.objects.create(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        fecha_corte=fecha_corte,
        tipo_periodo=tipo_periodo,
        sucursal=sucursal,
        area=area,
        notas=notas,
        creado_por=creado_por,
    )
    recalcular_corte_prenomina(corte)
    return corte


@transaction.atomic
def recalcular_corte_prenomina(corte: PrenominaCorte) -> PrenominaCorte:
    corte.resumenes.all().delete()
    corte.movimientos.all().delete()
    empleados = _empleados_del_periodo(corte.fecha_inicio, corte.fecha_fin, corte.sucursal, corte.area)
    fechas = _fechas(corte.fecha_inicio, corte.fecha_fin)
    empleado_ids = [empleado.id for empleado in empleados]
    incidencias = IncidenciaAsistencia.objects.filter(
        empleado_id__in=empleado_ids,
        fecha__range=(corte.fecha_inicio, corte.fecha_fin),
    ).exclude(estado=IncidenciaAsistencia.ESTADO_RESUELTO)
    incidencias_por_empleado = defaultdict(list)
    for incidencia in incidencias:
        incidencias_por_empleado[incidencia.empleado_id].append(incidencia)

    ajustes_pendientes = set(
        AjusteAsistencia.objects.filter(
            empleado_id__in=empleado_ids,
            fecha__range=(corte.fecha_inicio, corte.fecha_fin),
            estado=AjusteAsistencia.ESTADO_PENDIENTE,
        ).values_list("empleado_id", flat=True)
    )

    for empleado in empleados:
        dias_pre_ingreso = sum(1 for fecha in fechas if empleado.fecha_ingreso and fecha < empleado.fecha_ingreso)
        faltas = 0
        retardos = 0
        suspensiones = 0
        alertas = 0
        for incidencia in incidencias_por_empleado.get(empleado.id, []):
            if empleado.fecha_ingreso and incidencia.fecha < empleado.fecha_ingreso:
                continue
            if incidencia.estado == IncidenciaAsistencia.ESTADO_PENDIENTE and incidencia.severidad in {
                IncidenciaAsistencia.SEVERIDAD_ALTA,
                IncidenciaAsistencia.SEVERIDAD_CRITICA,
            }:
                alertas += 1
            if incidencia.tipo == IncidenciaAsistencia.TIPO_FALTA:
                faltas += 1
                if incidencia.estado == IncidenciaAsistencia.ESTADO_CONCILIADO:
                    _crear_movimiento(corte, empleado, incidencia.fecha, PrenominaMovimiento.TIPO_FALTA, Decimal("1"), Decimal("0"), "rrhh.IncidenciaAsistencia", incidencia.id, incidencia.detalle)
            elif incidencia.tipo in {IncidenciaAsistencia.TIPO_RETARDO, IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA}:
                retardos += 1
            elif incidencia.tipo == IncidenciaAsistencia.TIPO_SUSPENSION:
                suspensiones += 1
                if incidencia.estado == IncidenciaAsistencia.ESTADO_CONCILIADO:
                    _crear_movimiento(corte, empleado, incidencia.fecha, PrenominaMovimiento.TIPO_SUSPENSION, Decimal("1"), Decimal("0"), "rrhh.IncidenciaAsistencia", incidencia.id, incidencia.detalle)

        horas_extra = Decimal("0")
        for he in HoraExtra.objects.filter(
            empleado=empleado,
            fecha__range=(corte.fecha_inicio, corte.fecha_fin),
            estado=HoraExtra.ESTADO_AUTORIZADO,
        ):
            horas_extra += Decimal(str(he.horas or "0"))
            _crear_movimiento(corte, empleado, he.fecha, PrenominaMovimiento.TIPO_HORA_EXTRA, Decimal("0"), Decimal(str(he.horas or "0")), "rrhh.HoraExtra", he.id, he.notas)

        pendientes = 1 if empleado.id in ajustes_pendientes else 0
        estado = PrenominaEmpleadoResumen.ESTADO_LISTO
        if alertas or pendientes:
            estado = PrenominaEmpleadoResumen.ESTADO_BLOQUEADO if alertas else PrenominaEmpleadoResumen.ESTADO_REVISAR
        PrenominaEmpleadoResumen.objects.create(
            corte=corte,
            empleado=empleado,
            dias_periodo=len(fechas),
            dias_laborables=max(len(fechas) - dias_pre_ingreso, 0),
            dias_no_laborados_pre_ingreso=dias_pre_ingreso,
            faltas=faltas,
            retardos=retardos,
            suspensiones=suspensiones,
            horas_extra_autorizadas=horas_extra,
            ajustes_pendientes=pendientes,
            alertas_bloqueantes=alertas,
            estado=estado,
            snapshot={"dias_pre_ingreso": dias_pre_ingreso},
        )
    corte.resumen = _resumen_corte(corte)
    corte.estado = PrenominaCorte.ESTADO_LISTO if corte.resumen.get("bloqueados", 0) == 0 and corte.resumen.get("ajustes_pendientes", 0) == 0 else PrenominaCorte.ESTADO_EN_REVISION
    corte.save(update_fields=["resumen", "estado", "actualizado_en"])
    return corte


def _crear_movimiento(corte, empleado, fecha, tipo, valor, horas, fuente_modelo, fuente_id, observaciones):
    mov = PrenominaMovimiento(
        corte=corte,
        empleado=empleado,
        fecha=fecha,
        tipo_movimiento_erp=tipo,
        valor=valor,
        horas=horas,
        fuente_modelo=fuente_modelo,
        fuente_id=str(fuente_id),
        referencia_erp=f"{fuente_modelo}:{fuente_id}",
        observaciones=observaciones or "",
    )
    mov.aplicar_equivalencia()
    mov.save()
    return mov


def _resumen_corte(corte):
    resumenes = corte.resumenes.all()
    return {
        "colaboradores": resumenes.count(),
        "faltas": sum(row.faltas for row in resumenes),
        "retardos": sum(row.retardos for row in resumenes),
        "suspensiones": sum(row.suspensiones for row in resumenes),
        "horas_extra": str(sum((row.horas_extra_autorizadas for row in resumenes), Decimal("0"))),
        "ajustes_pendientes": sum(row.ajustes_pendientes for row in resumenes),
        "bloqueados": resumenes.filter(estado=PrenominaEmpleadoResumen.ESTADO_BLOQUEADO).count(),
        "movimientos_listos": corte.movimientos.filter(estado=PrenominaMovimiento.ESTADO_LISTO).count(),
    }
```

- [ ] **Step 4: Run tests**

Run:

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina.PrenominaServiceTests --settings=config.settings_test --noinput
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rrhh/services_prenomina.py rrhh/tests_prenomina.py
git commit -m "feat(rrhh): calcular cortes de prenomina"
```

---

### Task 4: Exporters

**Files:**
- Create: `rrhh/exporters/contpaqi_prenomina.py`
- Create: `rrhh/exporters/__init__.py` if missing
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Add export tests**

Append:

```python
class PrenominaExportTests(TestCase):
    def test_export_movimientos_contpaqi_incluye_layout_base(self):
        from rrhh.exporters.contpaqi_prenomina import build_movimientos_contpaqi_rows

        user = User.objects.create_user(username="paula-export")
        empleado = Empleado.objects.create(codigo="347", nombre="ANAYA", fecha_ingreso=date(2026, 6, 1), activo=True)
        corte = PrenominaCorte.objects.create(fecha_inicio=date(2026, 6, 1), fecha_fin=date(2026, 6, 15), fecha_corte=date(2026, 6, 15), creado_por=user)
        PrenominaMovimiento.objects.create(
            corte=corte,
            empleado=empleado,
            fecha=date(2026, 6, 11),
            tipo_movimiento_erp=PrenominaMovimiento.TIPO_FALTA,
            clave_contpaqi="F",
            valor=Decimal("1"),
            estado=PrenominaMovimiento.ESTADO_LISTO,
            referencia_erp="rrhh.IncidenciaAsistencia:1",
            observaciones="Falta validada.",
        )

        rows = build_movimientos_contpaqi_rows(corte)

        self.assertEqual(rows[0], ["CodigoEmpleado", "Fecha", "Dia", "TipoMovimiento", "ClaveCONTPAQi", "Valor", "Horas", "Importe", "ReferenciaERP", "Observaciones"])
        self.assertEqual(rows[1][0], "347")
        self.assertEqual(rows[1][4], "F")
```

- [ ] **Step 2: Implement exporter**

Create `rrhh/exporters/contpaqi_prenomina.py`:

```python
from __future__ import annotations

import csv
from io import BytesIO, StringIO

from django.http import HttpResponse
from openpyxl import Workbook

from rrhh.models import PrenominaMovimiento


HEADERS = ["CodigoEmpleado", "Fecha", "Dia", "TipoMovimiento", "ClaveCONTPAQi", "Valor", "Horas", "Importe", "ReferenciaERP", "Observaciones"]


def build_movimientos_contpaqi_rows(corte):
    rows = [HEADERS]
    qs = corte.movimientos.select_related("empleado").filter(estado=PrenominaMovimiento.ESTADO_LISTO).order_by("empleado__codigo", "fecha", "id")
    for mov in qs:
        rows.append([
            mov.empleado.codigo or "",
            mov.fecha.isoformat(),
            mov.fecha.day,
            mov.tipo_movimiento_erp,
            mov.clave_contpaqi,
            str(mov.valor),
            str(mov.horas),
            str(mov.importe),
            mov.referencia_erp,
            mov.observaciones,
        ])
    return rows


def export_movimientos_contpaqi_xlsx(corte):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Movimientos_CONTPAQi"
    for row in build_movimientos_contpaqi_rows(corte):
        sheet.append(row)
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    response = HttpResponse(output.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response["Content-Disposition"] = f'attachment; filename="{corte.folio}_movimientos_contpaqi.xlsx"'
    return response


def export_revision_xlsx(corte):
    workbook = Workbook()
    resumen_sheet = workbook.active
    resumen_sheet.title = "Resumen"
    resumen_sheet.append(["Folio", corte.folio])
    resumen_sheet.append(["Fecha inicio", corte.fecha_inicio.isoformat()])
    resumen_sheet.append(["Fecha fin", corte.fecha_fin.isoformat()])
    empleados_sheet = workbook.create_sheet("Empleados")
    empleados_sheet.append(["Codigo", "Empleado", "Estado", "Faltas", "Retardos", "Suspensiones", "Horas extra", "Ajustes pendientes"])
    for row in corte.resumenes.select_related("empleado").order_by("empleado__nombre"):
        empleados_sheet.append([row.empleado.codigo, row.empleado.nombre, row.estado, row.faltas, row.retardos, row.suspensiones, float(row.horas_extra_autorizadas), row.ajustes_pendientes])
    movimientos_sheet = workbook.create_sheet("Movimientos_CONTPAQi")
    for row in build_movimientos_contpaqi_rows(corte):
        movimientos_sheet.append(row)
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    response = HttpResponse(output.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response["Content-Disposition"] = f'attachment; filename="{corte.folio}_revision_prenomina.xlsx"'
    return response
```

- [ ] **Step 3: Run tests**

Run `PrenominaExportTests`. Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add rrhh/exporters rrhh/tests_prenomina.py
git commit -m "feat(rrhh): exportar prenomina para revision y CONTPAQi"
```

---

### Task 5: Views, URLs, Navigation

**Files:**
- Create: `rrhh/views_prenomina.py`
- Modify: `rrhh/urls.py`
- Modify: `rrhh/views.py`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Add view tests**

Append:

```python
from django.urls import reverse


class PrenominaViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="paula-view", is_superuser=True, is_staff=True)
        self.client.force_login(self.user)
        self.empleado = Empleado.objects.create(codigo="347", nombre="ANAYA", fecha_ingreso=date(2026, 6, 1), activo=True)

    def test_prenomina_list_renderiza_formulario(self):
        response = self.client.get(reverse("rrhh:prenomina"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Generar corte")

    def test_crear_corte_redirige_a_detalle(self):
        response = self.client.post(reverse("rrhh:prenomina"), {
            "fecha_inicio": "2026-06-01",
            "fecha_fin": "2026-06-15",
            "fecha_corte": "2026-06-15",
            "tipo_periodo": PrenominaCorte.TIPO_QUINCENAL,
        })
        corte = PrenominaCorte.objects.get()
        self.assertEqual(response.status_code, 302)
        self.assertIn(reverse("rrhh:prenomina_detail", kwargs={"pk": corte.pk}), response.url)
```

- [ ] **Step 2: Implement URLs**

In `rrhh/urls.py`, import `views_prenomina` and add:

```python
path("prenomina/", views_prenomina.prenomina, name="prenomina"),
path("prenomina/<int:pk>/", views_prenomina.prenomina_detail, name="prenomina_detail"),
path("prenomina/<int:pk>/empleado/<int:empleado_id>/", views_prenomina.prenomina_persona, name="prenomina_persona"),
path("prenomina/<int:pk>/export/revision/", views_prenomina.prenomina_export_revision, name="prenomina_export_revision"),
path("prenomina/<int:pk>/export/contpaqi/", views_prenomina.prenomina_export_contpaqi, name="prenomina_export_contpaqi"),
```

- [ ] **Step 3: Add module tab**

In `rrhh/views.py`, add a tab entry near Nomina/Reporte asistencia:

```python
{"label": "Prenomina", "url_name": "rrhh:prenomina", "key": "prenomina", "submodule": "nomina"},
```

- [ ] **Step 4: Implement views**

Create `rrhh/views_prenomina.py`:

```python
from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date

from core.access import can_manage_rrhh, can_view_rrhh
from rrhh.exporters.contpaqi_prenomina import export_movimientos_contpaqi_xlsx, export_revision_xlsx
from rrhh.models import Empleado, PrenominaCorte, PrenominaEmpleadoResumen, PrenominaMovimiento
from rrhh.services_prenomina import crear_corte_prenomina, recalcular_corte_prenomina
from rrhh.views import _module_tabs


def _parse_fecha(value, default=None):
    return parse_date((value or "").strip()) or default


@login_required
def prenomina(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenomina")
    hoy = timezone.localdate()
    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para generar prenomina")
        inicio = _parse_fecha(request.POST.get("fecha_inicio"))
        fin = _parse_fecha(request.POST.get("fecha_fin"))
        corte_fecha = _parse_fecha(request.POST.get("fecha_corte"), hoy)
        if not inicio or not fin or fin < inicio:
            messages.error(request, "Captura un rango valido.")
            return redirect("rrhh:prenomina")
        corte = crear_corte_prenomina(
            fecha_inicio=inicio,
            fecha_fin=fin,
            fecha_corte=corte_fecha,
            tipo_periodo=(request.POST.get("tipo_periodo") or PrenominaCorte.TIPO_QUINCENAL).strip(),
            sucursal=(request.POST.get("sucursal") or "").strip(),
            area=(request.POST.get("area") or "").strip(),
            creado_por=request.user,
        )
        messages.success(request, f"Corte {corte.folio} generado.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)
    return render(request, "rrhh/prenomina.html", {
        "module_tabs": _module_tabs("prenomina", request.user),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "hoy": hoy.isoformat(),
        "cortes": PrenominaCorte.objects.select_related("creado_por").order_by("-fecha_fin", "-id")[:80],
        "tipo_choices": PrenominaCorte.TIPO_CHOICES,
    })


@login_required
def prenomina_detail(request, pk):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenomina")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    if request.method == "POST" and request.POST.get("action") == "recalcular":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para recalcular prenomina")
        recalcular_corte_prenomina(corte)
        messages.success(request, "Corte recalculado.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)
    return render(request, "rrhh/prenomina_detail.html", {
        "module_tabs": _module_tabs("prenomina", request.user),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "corte": corte,
        "resumenes": corte.resumenes.select_related("empleado").order_by("empleado__nombre"),
        "movimientos": corte.movimientos.select_related("empleado").order_by("empleado__nombre", "fecha")[:300],
    })


@login_required
def prenomina_persona(request, pk, empleado_id):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenomina")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    resumen = get_object_or_404(PrenominaEmpleadoResumen.objects.select_related("empleado"), corte=corte, empleado_id=empleado_id)
    return render(request, "rrhh/prenomina_persona.html", {
        "module_tabs": _module_tabs("prenomina", request.user),
        "corte": corte,
        "resumen": resumen,
        "movimientos": corte.movimientos.filter(empleado_id=empleado_id).order_by("fecha", "tipo_movimiento_erp"),
    })


@login_required
def prenomina_export_revision(request, pk):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para exportar prenomina")
    return export_revision_xlsx(get_object_or_404(PrenominaCorte, pk=pk))


@login_required
def prenomina_export_contpaqi(request, pk):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para exportar movimientos CONTPAQi")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    if corte.resumen.get("bloqueados", 0) or corte.resumen.get("ajustes_pendientes", 0):
        messages.error(request, "No se puede exportar CONTPAQi con bloqueos o ajustes pendientes.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)
    if corte.movimientos.exclude(estado=PrenominaMovimiento.ESTADO_LISTO).exists():
        messages.error(request, "No se puede exportar CONTPAQi con movimientos sin clave configurada.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)
    return export_movimientos_contpaqi_xlsx(corte)
```

- [ ] **Step 5: Run view tests**

Expected: fail until templates exist, then pass after Task 6.

- [ ] **Step 6: Commit after Task 6 passes**

Commit views and URLs with templates in Task 6.

---

### Task 6: Templates and Print UI

**Files:**
- Create: `rrhh/templates/rrhh/prenomina.html`
- Create: `rrhh/templates/rrhh/prenomina_detail.html`
- Create: `rrhh/templates/rrhh/prenomina_persona.html`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Create `prenomina.html`**

Use ERP components:

```django
{% extends "base.html" %}
{% block title %}RRHH · Prenomina{% endblock %}
{% block page_title %}RRHH · Prenomina{% endblock %}
{% block content %}
<section class="module-tabs" aria-label="Submodulos RRHH">
  {% for tab in module_tabs %}
    <a href="{% url tab.url_name %}" class="module-tab {% if tab.active %}active{% endif %}">{{ tab.label }}</a>
  {% endfor %}
</section>

<section class="card">
  <div class="card-header">Crear corte</div>
  <form method="post">
    {% csrf_token %}
    <div class="form-grid-2">
      <div class="form-group"><label>Fecha inicio</label><input class="input-field" type="date" name="fecha_inicio" required></div>
      <div class="form-group"><label>Fecha fin</label><input class="input-field" type="date" name="fecha_fin" required></div>
      <div class="form-group"><label>Fecha corte</label><input class="input-field" type="date" name="fecha_corte" value="{{ hoy }}" required></div>
      <div class="form-group"><label>Tipo</label><select class="input-field" name="tipo_periodo">{% for value,label in tipo_choices %}<option value="{{ value }}">{{ label }}</option>{% endfor %}</select></div>
      <div class="form-group"><label>Sucursal</label><input class="input-field" name="sucursal" placeholder="Todas"></div>
      <div class="form-group"><label>Area</label><input class="input-field" name="area" placeholder="Todas"></div>
    </div>
    <button class="btn btn-primary" type="submit" {% if not can_manage_rrhh %}disabled{% endif %}>Generar corte</button>
  </form>
</section>

<section class="card">
  <div class="card-header">Cortes recientes</div>
  <div class="table-responsive">
    <table class="table table-striped">
      <thead><tr><th>Folio</th><th>Periodo</th><th>Corte</th><th>Estado</th><th>Acciones</th></tr></thead>
      <tbody>
        {% for corte in cortes %}
          <tr>
            <td>{{ corte.folio }}</td>
            <td>{{ corte.fecha_inicio|date:"Y-m-d" }} a {{ corte.fecha_fin|date:"Y-m-d" }}</td>
            <td>{{ corte.fecha_corte|date:"Y-m-d" }}</td>
            <td>{{ corte.get_estado_display }}</td>
            <td><a class="btn btn-secondary btn-sm" href="{% url 'rrhh:prenomina_detail' corte.pk %}">Ver</a></td>
          </tr>
        {% empty %}
          <tr><td colspan="5">Sin cortes registrados.</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</section>
{% endblock %}
```

- [ ] **Step 2: Create `prenomina_detail.html`**

Include compact KPIs, export buttons, table by employee, and movements table. Use:

```django
{% extends "base.html" %}
{% block title %}RRHH · {{ corte.folio }}{% endblock %}
{% block page_title %}RRHH · {{ corte.folio }}{% endblock %}
{% block content %}
<section class="module-tabs" aria-label="Submodulos RRHH">
  {% for tab in module_tabs %}
    <a href="{% url tab.url_name %}" class="module-tab {% if tab.active %}active{% endif %}">{{ tab.label }}</a>
  {% endfor %}
</section>

<section class="card">
  <div class="card-header">Mesa de cierre</div>
  <div class="kpi-grid">
    <article class="kpi-card"><div class="kpi-number">{{ corte.resumen.colaboradores|default:0 }}</div><div class="kpi-label">Colaboradores</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ corte.resumen.faltas|default:0 }}</div><div class="kpi-label">Faltas</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ corte.resumen.retardos|default:0 }}</div><div class="kpi-label">Retardos</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ corte.resumen.ajustes_pendientes|default:0 }}</div><div class="kpi-label">Ajustes pendientes</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ corte.resumen.bloqueados|default:0 }}</div><div class="kpi-label">Bloqueos</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ corte.resumen.movimientos_listos|default:0 }}</div><div class="kpi-label">Movimientos CONTPAQi</div></article>
  </div>
  <div>
    <a class="btn btn-secondary" href="{% url 'rrhh:prenomina_export_revision' corte.pk %}">Exportar revision XLSX</a>
    <a class="btn btn-primary" href="{% url 'rrhh:prenomina_export_contpaqi' corte.pk %}">Exportar movimientos CONTPAQi</a>
    <form method="post" style="display:inline">{% csrf_token %}<input type="hidden" name="action" value="recalcular"><button class="btn btn-secondary" type="submit">Recalcular</button></form>
  </div>
</section>

<section class="card">
  <div class="card-header">Colaboradores</div>
  <div class="table-responsive">
    <table class="table table-striped">
      <thead><tr><th>Codigo</th><th>Empleado</th><th>Ingreso</th><th>Asistencias</th><th>Faltas</th><th>Retardos</th><th>Suspensiones</th><th>Horas extra</th><th>Ajustes</th><th>Estado</th><th>Acciones</th></tr></thead>
      <tbody>
        {% for row in resumenes %}
        <tr>
          <td>{{ row.empleado.codigo }}</td>
          <td>{{ row.empleado.nombre }}</td>
          <td>{{ row.empleado.fecha_ingreso|date:"Y-m-d" }}</td>
          <td>{{ row.dias_asistencia }}</td>
          <td>{{ row.faltas }}</td>
          <td>{{ row.retardos }}</td>
          <td>{{ row.suspensiones }}</td>
          <td>{{ row.horas_extra_autorizadas }}</td>
          <td>{{ row.ajustes_pendientes }}</td>
          <td>{{ row.get_estado_display }}</td>
          <td><a class="btn btn-secondary btn-sm" href="{% url 'rrhh:prenomina_persona' corte.pk row.empleado_id %}">Ver</a></td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</section>
{% endblock %}
```

- [ ] **Step 3: Create `prenomina_persona.html`**

Use a printable layout with:

```django
{% extends "base.html" %}
{% block title %}Prenomina · {{ resumen.empleado.nombre }}{% endblock %}
{% block page_title %}Prenomina · {{ resumen.empleado.nombre }}{% endblock %}
{% block content %}
<section class="card">
  <div class="card-header">
    <div>
      <h2>{{ resumen.empleado.nombre }}{% if resumen.empleado.codigo %} ({{ resumen.empleado.codigo }}){% endif %}</h2>
      <p class="muted">{{ corte.fecha_inicio|date:"Y-m-d" }} a {{ corte.fecha_fin|date:"Y-m-d" }}</p>
    </div>
    <button class="btn btn-secondary" onclick="window.print()">Imprimir / PDF</button>
  </div>
  <div class="kpi-grid">
    <article class="kpi-card"><div class="kpi-number">{{ resumen.faltas }}</div><div class="kpi-label">Faltas</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ resumen.retardos }}</div><div class="kpi-label">Retardos</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ resumen.suspensiones }}</div><div class="kpi-label">Suspensiones</div></article>
    <article class="kpi-card"><div class="kpi-number">{{ resumen.horas_extra_autorizadas }}</div><div class="kpi-label">Horas extra</div></article>
  </div>
</section>
<section class="card">
  <div class="card-header">Movimientos del periodo</div>
  <div class="table-responsive">
    <table class="table table-striped">
      <thead><tr><th>Fecha</th><th>Tipo</th><th>Clave CONTPAQi</th><th>Valor</th><th>Horas</th><th>Observaciones</th></tr></thead>
      <tbody>
        {% for mov in movimientos %}
          <tr><td>{{ mov.fecha|date:"Y-m-d" }}</td><td>{{ mov.get_tipo_movimiento_erp_display }}</td><td>{{ mov.clave_contpaqi|default:"-" }}</td><td>{{ mov.valor }}</td><td>{{ mov.horas }}</td><td>{{ mov.observaciones }}</td></tr>
        {% empty %}
          <tr><td colspan="6">Sin movimientos exportables.</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</section>
{% endblock %}
```

- [ ] **Step 4: Run view tests**

Run:

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina.PrenominaViewTests --settings=config.settings_test --noinput
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rrhh/views_prenomina.py rrhh/urls.py rrhh/views.py rrhh/templates/rrhh/prenomina.html rrhh/templates/rrhh/prenomina_detail.html rrhh/templates/rrhh/prenomina_persona.html rrhh/tests_prenomina.py
git commit -m "feat(rrhh): agregar interfaz de prenomina"
```

---

### Task 7: Adjustment UI Actions

**Files:**
- Modify: `rrhh/views_prenomina.py`
- Modify: `rrhh/templates/rrhh/prenomina_persona.html`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Add tests for creating and approving adjustments from UI**

Write tests that post to:

```python
reverse("rrhh:prenomina_ajuste_crear", kwargs={"pk": corte.pk, "empleado_id": empleado.pk})
reverse("rrhh:prenomina_ajuste_aprobar", kwargs={"pk": corte.pk, "ajuste_id": ajuste.pk})
reverse("rrhh:prenomina_ajuste_rechazar", kwargs={"pk": corte.pk, "ajuste_id": ajuste.pk})
```

Assert adjustment is created, approved/applied, and rejected.

- [ ] **Step 2: Add URLs**

```python
path("prenomina/<int:pk>/empleado/<int:empleado_id>/ajuste/", views_prenomina.prenomina_ajuste_crear, name="prenomina_ajuste_crear"),
path("prenomina/<int:pk>/ajuste/<int:ajuste_id>/aprobar/", views_prenomina.prenomina_ajuste_aprobar, name="prenomina_ajuste_aprobar"),
path("prenomina/<int:pk>/ajuste/<int:ajuste_id>/rechazar/", views_prenomina.prenomina_ajuste_rechazar, name="prenomina_ajuste_rechazar"),
```

- [ ] **Step 3: Implement views using `services_ajustes_asistencia.py`**

Use `crear_ajuste_asistencia`, `aprobar_ajuste_asistencia`, `rechazar_ajuste_asistencia`, then `recalcular_corte_prenomina(corte)`.

- [ ] **Step 4: Add form in person template**

Add a compact form with fields:

- fecha
- tipo_ajuste
- salida or entrada proposed datetime
- motivo

- [ ] **Step 5: Run tests and commit**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina --settings=config.settings_test --noinput
git add rrhh/views_prenomina.py rrhh/urls.py rrhh/templates/rrhh/prenomina_persona.html rrhh/tests_prenomina.py
git commit -m "feat(rrhh): gestionar ajustes desde prenomina"
```

---

### Task 8: Full Verification, PR, Deploy

**Files:**
- No new functional files unless tests reveal fixes.

- [ ] **Step 1: Run targeted tests**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp TEST_DB_NAME=test_pastelerias_erp_prenomina ./.venv/bin/python manage.py test rrhh.tests_prenomina rrhh.tests_reporte_asistencia rrhh.tests_asistencia_reglas --settings=config.settings_test --noinput
```

Expected: PASS.

- [ ] **Step 2: Run checks**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp ./.venv/bin/python manage.py check --settings=config.settings_test
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp ./.venv/bin/python manage.py makemigrations --check --dry-run --settings=config.settings_test
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=pastelerias_erp ./.venv/bin/python manage.py migrate --check --settings=config.settings_test
git diff --check
```

Expected: all clean.

- [ ] **Step 3: Validate with browser**

Start local server through the repo's normal environment if available. Validate:

- `RRHH -> Prenomina` appears in tabs.
- Create a cutoff.
- Open cutoff detail.
- Open per-person report.
- Use browser print preview for person report.
- Attempt CONTPAQi export with missing mapping and confirm it blocks.
- Add mapping, recalculate, export and confirm XLSX downloads.

- [ ] **Step 4: Create PR**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git push -u origin codex/rrhh-prenomina-contpaqi
gh pr create --base main --head codex/rrhh-prenomina-contpaqi --draft --title "feat(rrhh): prenomina para CONTPAQi" --body "Implementa RRHH -> Prenomina con cortes guardados, ajustes auditados, reporte por persona y export CONTPAQi. Incluye pruebas y validaciones."
```

- [ ] **Step 5: Merge and deploy after approval**

After approval:

```bash
gh pr merge <PR_NUMBER> --squash --delete-branch
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 'cd /opt/pastelerias-erp && git pull origin main && docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py migrate --noinput && docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py collectstatic --noinput && docker compose -f /opt/pastelerias-erp/docker-compose.yml restart web worker beat && docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py check'
```

- [ ] **Step 6: Production validation**

Validate in production:

- Login as RRHH/admin.
- Open `RRHH -> Prenomina`.
- Generate cutoff `2026-06-01` to `2026-06-15`.
- Confirm employee 347 shows pre-ingreso days as non-laborable.
- Confirm export review downloads.
- Confirm CONTPAQi export blocks when mappings are missing.
- Configure test mapping in admin only if approved by Mauricio.
- Confirm no service worker issue applies unless static/PWA assets were touched.

- [ ] **Step 7: Cleanup**

After merge, deploy, and validation:

```bash
git switch main
git pull origin main
git branch -D codex/rrhh-prenomina-contpaqi
git fetch --prune origin
```

---

## Self-Review

Spec coverage:

- New `RRHH -> Prenomina` screen: Tasks 5 and 6.
- Saved cutoffs: Tasks 1 and 3.
- Per-person printable report: Task 6.
- Audited attendance adjustments: Tasks 1, 2, and 7.
- CONTPAQi movement export: Task 4.
- Validation gates: Tasks 3, 4, 5, and 8.
- UI standardization with creative composition: Task 6.
- Tests and deploy validation: Task 8.

Placeholder scan:

- No incomplete markers or unspecified "add validation" steps remain.
- Every code-producing task includes exact files and commands.

Type consistency:

- Model names match the spec and service references.
- Exporter uses `PrenominaMovimiento.ESTADO_LISTO`.
- Views use `crear_corte_prenomina` and `recalcular_corte_prenomina` from `rrhh/services_prenomina.py`.
