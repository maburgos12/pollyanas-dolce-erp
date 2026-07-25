# Vacaciones Goce FIFO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Crear bolsas por aniversario y hacer que cada solicitud reserve y consuma primero los días de goce más antiguos.

**Architecture:** `PeriodoVacacional` será la bolsa y `AplicacionGoceVacaciones` conservará el desglose solicitud-periodo. Un servicio transaccional asignará FIFO con bloqueo de filas; `MovimientoVacaciones` seguirá como auditoría mientras un modo sombra compara el cálculo nuevo con el actual.

**Tech Stack:** Django 5.0.1, PostgreSQL 16, Django ORM, DRF 3.14, templates Django y Django TestCase.

---

## Reglas y archivos

Implementar en un worktree nuevo desde `origin/main`. Migraciones, commits, PR, merge y deploy corresponden a Claude según `AGENTS.md`. No modificar migraciones existentes ni activar la nueva fuente antes de aprobar el reporte sombra.

- Modify: `rrhh/models.py`
- Create: `rrhh/migrations/0036_periodo_vacacional_aplicacion_goce.py` (ajustar número al HEAD real)
- Create: `rrhh/services_vacaciones_saldos.py`
- Modify: `rrhh/services_vacaciones.py:96-286`
- Create: `rrhh/management/commands/preparar_periodos_vacacionales.py`
- Modify: `rrhh/views.py:2771-3005`, `rrhh/api_views.py:229-305`, `rrhh/templates/rrhh/vacaciones_list.html`
- Create: `rrhh/tests_vacaciones_saldos.py`; modify `rrhh/tests.py`, `rrhh/tests_asistencia_reglas.py`
- Modify: `docs/ux/action-context-coverage.md`

### Task 1: Worktree y línea base

- [ ] **Step 1: Crear el entorno aislado**

```bash
git fetch origin main
git worktree add /Users/mauricioburgos/Downloads/codex_worktrees/rrhh-vacaciones-goce-fifo \
  -b codex/rrhh-vacaciones-goce-fifo origin/main
cd /Users/mauricioburgos/Downloads/codex_worktrees/rrhh-vacaciones-goce-fifo
bash scripts/git_workspace_preflight.sh --write
```

Expected: limpio, detrás 0 y adelante 0.

- [ ] **Step 2: Aplicar main y verificar Django**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate --check
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py check
```

Expected: cero pendientes y cero errores; detenerse si no cumple.

### Task 2: Modelar bolsas y aplicaciones con TDD

- [ ] **Step 1: Escribir pruebas fallidas en `rrhh/tests_vacaciones_saldos.py`**

```python
def test_empleado_no_duplica_aniversario(self):
    PeriodoVacacional.objects.create(
        empleado=self.empleado, aniversario=date(2025, 3, 7),
        fecha_limite=date(2025, 9, 7), antiguedad_anios=3,
        dias_generados=Decimal("16.00"),
    )
    with self.assertRaises(IntegrityError):
        with transaction.atomic():
            PeriodoVacacional.objects.create(
                empleado=self.empleado, aniversario=date(2025, 3, 7),
                fecha_limite=date(2025, 9, 7), antiguedad_anios=3,
                dias_generados=Decimal("16.00"),
            )

def test_aplicacion_requiere_dias_positivos(self):
    with self.assertRaises(IntegrityError):
        AplicacionGoceVacaciones.objects.create(
            solicitud=self.solicitud, periodo=self.periodo,
            dias=Decimal("0"), estado="reservada",
        )
```

- [ ] **Step 2: Ejecutar y confirmar FAIL por modelos inexistentes**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_saldos.PeriodoVacacionalModelTests --keepdb
```

- [ ] **Step 3: Implementar los modelos mínimos**

```python
class PeriodoVacacional(models.Model):
    empleado = models.ForeignKey(Empleado, on_delete=models.CASCADE, related_name="periodos_vacacionales")
    aniversario = models.DateField()
    fecha_limite = models.DateField()
    antiguedad_anios = models.PositiveSmallIntegerField()
    dias_generados = models.DecimalField(max_digits=6, decimal_places=2)
    origen = models.CharField(max_length=20, default="calculado")
    notas = models.TextField(blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["empleado", "aniversario"], name="uq_periodo_vac_aniversario"),
            models.CheckConstraint(condition=models.Q(dias_generados__gte=0), name="ck_periodo_vac_dias_no_neg"),
        ]

class AplicacionGoceVacaciones(models.Model):
    solicitud = models.ForeignKey(SolicitudVacaciones, on_delete=models.PROTECT, related_name="aplicaciones_goce")
    periodo = models.ForeignKey(PeriodoVacacional, on_delete=models.PROTECT, related_name="aplicaciones_goce")
    dias = models.DecimalField(max_digits=6, decimal_places=2)
    estado = models.CharField(max_length=16, db_index=True)
    excepcion_fifo = models.BooleanField(default=False)
    motivo_excepcion = models.CharField(max_length=220, blank=True, default="")
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["solicitud", "periodo"], name="uq_aplicacion_goce_periodo"),
            models.CheckConstraint(condition=models.Q(dias__gt=0), name="ck_aplicacion_goce_dias_pos"),
        ]
```

- [ ] **Step 4: Claude crea, inspecciona y aplica la migración nueva**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py makemigrations rrhh --name periodo_vacacional_aplicacion_goce
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py sqlmigrate rrhh 0036
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate
```

- [ ] **Step 5: Repetir pruebas; Expected: PASS**

- [ ] **Step 6: Commit quirúrgico por Claude**

```bash
git add rrhh/models.py rrhh/migrations/0036_periodo_vacacional_aplicacion_goce.py rrhh/tests_vacaciones_saldos.py
git commit -m "feat(rrhh): modelar periodos y aplicaciones de goce vacacional"
```

### Task 3: Calcular saldos y reservar FIFO

- [ ] **Step 1: Escribir pruebas de saldo y reparto 7+3**

```python
def test_saldo_periodo_separa_reserva_y_goce(self):
    self.crear_aplicacion(self.periodo_2025, "reservada", "2")
    self.crear_aplicacion(self.periodo_2025, "consumida", "5")
    saldo = saldo_periodo_vacacional(self.periodo_2025)
    self.assertEqual(saldo.reservado, Decimal("2"))
    self.assertEqual(saldo.gozado, Decimal("5"))
    self.assertEqual(saldo.disponible_goce, Decimal("5"))

def test_reserva_fifo_divide_diez_dias(self):
    aplicaciones = reservar_goce_fifo(self.solicitud, Decimal("10"), actor=self.user)
    self.assertEqual(
        [(a.periodo.aniversario.year, a.dias) for a in aplicaciones],
        [(2025, Decimal("7")), (2026, Decimal("3"))],
    )
```

- [ ] **Step 2: Ejecutar; Expected: FAIL por servicio inexistente**

- [ ] **Step 3: Crear `rrhh/services_vacaciones_saldos.py`**

```python
@dataclass(frozen=True)
class SaldoPeriodoVacacional:
    periodo_id: int
    aniversario: date
    dias_generados: Decimal
    reservado: Decimal
    gozado: Decimal
    disponible_goce: Decimal

def reservar_goce_fifo(solicitud, dias, *, actor=None):
    faltantes = Decimal(dias)
    periodos = PeriodoVacacional.objects.select_for_update().filter(
        empleado=solicitud.empleado
    ).order_by("aniversario", "id")
    propuestas = []
    for periodo in periodos:
        disponible = saldo_periodo_vacacional(periodo).disponible_goce
        tomar = min(disponible, faltantes)
        if tomar > 0:
            propuestas.append((periodo, tomar))
            faltantes -= tomar
        if faltantes == 0:
            break
    if faltantes > 0:
        raise ValidationError(f"Saldo insuficiente. Faltan {faltantes} días.")
    return [AplicacionGoceVacaciones.objects.create(
        solicitud=solicitud, periodo=p, dias=d, estado="reservada", actor=actor,
    ) for p, d in propuestas]
```

`saldo_periodo_vacacional` sumará únicamente estados `reservada` y `consumida`; liberadas/revertidas no afectan saldo.

- [ ] **Step 4: Probar rollback sin aplicaciones parciales**

```python
def test_reserva_insuficiente_no_deja_parciales(self):
    with self.assertRaises(ValidationError):
        with transaction.atomic():
            reservar_goce_fifo(self.solicitud, Decimal("100"), actor=self.user)
    self.assertFalse(self.solicitud.aplicaciones_goce.exists())
```

- [ ] **Step 5: Ejecutar el módulo completo; Expected: PASS**

- [ ] **Step 6: Commit por Claude**

```bash
git add rrhh/services_vacaciones_saldos.py rrhh/tests_vacaciones_saldos.py
git commit -m "feat(rrhh): reservar goce vacacional por antiguedad"
```

### Task 4: Integrar solicitudes y demostrar el caso Carolina

- [ ] **Step 1: Escribir la regresión principal**

```python
def test_solicitud_2026_consume_cinco_de_2025(self):
    solicitud = crear_solicitud_vacaciones(
        empleado=self.carolina, fecha_inicio=date(2026, 7, 20),
        fecha_fin=date(2026, 7, 24), motivo="Goce pendiente", actor=self.rrhh,
    )
    aprobar_solicitud_vacaciones_rrhh(solicitud, self.rrhh)
    aplicacion = solicitud.aplicaciones_goce.get()
    self.assertEqual(aplicacion.periodo.aniversario.year, 2025)
    self.assertEqual(aplicacion.dias, Decimal("5"))
    self.assertEqual(saldo_periodo_vacacional(self.periodo_2025).disponible_goce, Decimal("2"))
```

- [ ] **Step 2: Ejecutar; Expected: FAIL porque hoy se usa `fecha_inicio.year`**

- [ ] **Step 3: Integrar dentro de las transacciones actuales**

En creación, conservar validaciones y llamar `reservar_goce_fifo`. En aprobación, cambiar solo reservas a `consumida`; en rechazo, a `liberada`. Crear un movimiento por aplicación con:

```python
periodo_anio = aplicacion.periodo.aniversario.year
dias = aplicacion.dias
solicitud = solicitud
```

- [ ] **Step 4: Ejecutar vacaciones existentes y nuevas**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_saldos rrhh.tests.CapitalHumanoServiceTests --keepdb
```

- [ ] **Step 5: Commit por Claude**

```bash
git add rrhh/services_vacaciones.py rrhh/services_vacaciones_saldos.py rrhh/tests.py rrhh/tests_vacaciones_saldos.py
git commit -m "fix(rrhh): consumir primero vacaciones pendientes antiguas"
```

### Task 5: Migración histórica en modo sombra

- [ ] **Step 1: Probar dry-run e historia inmutable**

```python
def test_preparar_periodos_dry_run_no_escribe(self):
    call_command("preparar_periodos_vacacionales")
    self.assertEqual(PeriodoVacacional.objects.count(), 0)

def test_ejecutar_preserva_movimiento_historico(self):
    call_command("preparar_periodos_vacacionales", "--ejecutar")
    self.assertTrue(MovimientoVacaciones.objects.filter(pk=self.movimiento.pk).exists())
```

- [ ] **Step 2: Crear comando con opciones exactas**

```python
parser.add_argument("--ejecutar", action="store_true")
parser.add_argument("--empleado-id", type=int)
parser.add_argument("--salida-csv")
```

Reconocer solo descripciones con `pendiente de goce`; producir `empleado_id,periodo,saldo_actual,saldo_propuesto,diferencia,clasificacion`. Sin `--ejecutar`, rollback. Si la distribución histórica no es inequívoca o cambia el total, marcar `REQUIERE_REVISION` y no escribir aplicaciones.

- [ ] **Step 3: Ejecutar pruebas y dry-run local**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_saldos.MigracionHistoricaTests --keepdb
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py \
  preparar_periodos_vacacionales --salida-csv /tmp/vacaciones-sombra.csv
```

- [ ] **Step 4: Commit por Claude**

```bash
git add rrhh/management/commands/preparar_periodos_vacacionales.py rrhh/tests_vacaciones_saldos.py
git commit -m "feat(rrhh): preparar vacaciones historicas en modo sombra"
```

### Task 6: API, pantalla, privacidad, asistencia y cierre

- [ ] **Step 1: Probar payload aditivo y sin importes**

```python
def test_saldo_conserva_disponible_y_agrega_periodos(self):
    data = self.client.get(f"/rrhh/api/vacaciones/saldo/?empleado={self.empleado.id}").json()
    self.assertIn("disponible", data)
    self.assertEqual(data["periodos"][0]["anio"], 2025)
    self.assertNotIn("importe", data["periodos"][0])
```

- [ ] **Step 2: Agregar `periodos` sin retirar campos actuales**

Cada fila incluirá `anio`, `fecha_limite`, `generado`, `reservado`, `gozado` y `disponible_goce`. La propuesta FIFO se calcula en backend, nunca se duplica en JavaScript.

- [ ] **Step 3: Renderizar desglose y registrar acciones UX**

Mostrar distribución antes de enviar y en autorizaciones. Mantener filtros/posición, toast y bloqueo exclusivo del botón; actualizar `docs/ux/action-context-coverage.md`. Si existe service worker para esta superficie, subir `CACHE_NAME`.

- [ ] **Step 4: Probar asistencia por fecha, no por bolsa**

```python
def test_vacacion_2026_aplicada_a_2025_justifica_fecha_2026(self):
    resultado = evaluar_asistencia_empleado_fecha(self.empleado, date(2026, 7, 20))
    self.assertEqual(resultado.clasificacion, "vacaciones")
```

- [ ] **Step 5: Ejecutar cierre técnico**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py check
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate --check
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py showmigrations rrhh
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_saldos rrhh.tests.CapitalHumanoServiceTests rrhh.tests_asistencia_reglas --keepdb
git diff --check
git status --short --branch
```

- [ ] **Step 6: Validar navegador, reporte sombra y producción**

Crear localmente una solicitud 7+3 y revisar UI, consola y Network. Claude revisa diff, abre PR borrador y, tras merge, ejecuta `scripts/deploy_web_safe.sh` sin `git pull` previo. Antes de activar bolsas en producción, aprobar el dry-run y confirmar que Carolina termina con dos días pendientes de 2025.

