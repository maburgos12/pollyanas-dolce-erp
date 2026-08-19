# Gastos y servicios correctos en flota — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar para todas las unidades servicios, reparaciones y gasto anual total correctos, impedir servicios realizados con fecha futura y corregir de forma auditable los dos registros inconsistentes de la Cheyenne.

**Architecture:** Extraer el cálculo anual a un servicio de dominio pequeño que sea la única fuente del resumen de flota. Validar fechas en los dos flujos HTML que crean servicios realizados. Corregir los datos de GS-CH1 mediante un comando protegido, idempotente y con simulación previa.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, templates Django, management commands, Service Worker de Logística.

---

## Estructura de archivos

- Crear `logistica/services_flota.py`: reglas de inclusión y cálculo anual por unidad.
- Crear `logistica/tests_flota_gastos.py`: pruebas unitarias, de vistas y del comando de corrección.
- Crear `logistica/management/commands/corregir_historial_cheyenne.py`: corrección productiva exacta, auditable e idempotente.
- Modificar `logistica/views.py`: consumir el resumen común y validar el formulario de detalle de unidad.
- Modificar `mantenimiento/views.py`: aplicar la misma validación en la captura desde Mantenimiento.
- Modificar `logistica/templates/logistica/flota_resumen.html`: mostrar servicios, reparaciones y total.
- Modificar `logistica/static/logistica/pwa/sw.js`: cambiar `CACHE_NAME` por la actualización visible.

No se requieren modelos ni migraciones de esquema.

### Task 1: Preparar PostgreSQL aislado y confirmar la línea base

**Files:**
- No modificar archivos.

- [ ] **Step 1: Levantar PostgreSQL exclusivo del worktree**

```bash
export COMPOSE_PROJECT_NAME=erp_flota_gastos_servicios
export DB_HOST_PORT=55463
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:${DB_HOST_PORT}/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
```

Expected: PostgreSQL reports `accepting connections`.

- [ ] **Step 2: Aplicar la línea base completa**

```bash
python manage.py migrate
python manage.py migrate --check
python manage.py check
```

Expected: no pending migrations and `System check identified no issues` except explicitly documented pre-existing warnings.

### Task 2: Definir con pruebas el resumen anual único

**Files:**
- Create: `logistica/services_flota.py`
- Create: `logistica/tests_flota_gastos.py`

- [ ] **Step 1: Escribir primero las pruebas del resumen**

Create `FlotaGastosResumenTests` with fixtures for one active `Unidad`, an ordinary service type, the inactive type `Registro inicial de kilometraje`, and a user. Add tests equivalent to:

```python
@override_settings(USE_TZ=True)
class FlotaGastosResumenTests(TestCase):
    def test_suma_servicios_y_reparaciones_sin_duplicar(self):
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=self.tipo_servicio,
            fecha_servicio=date(2026, 8, 10),
            costo=Decimal("6898.00"),
        )
        ReparacionUnidad.objects.create(
            unidad=self.unidad,
            fecha_ingreso=date(2026, 8, 11),
            descripcion_falla="Balero",
            costo_total=Decimal("1500.00"),
        )

        resumen = resumen_anual_unidad(self.unidad, year=2026, today=date(2026, 8, 19))

        self.assertEqual(resumen.servicios_cantidad, 1)
        self.assertEqual(resumen.servicios_total, Decimal("6898.00"))
        self.assertEqual(resumen.reparaciones_cantidad, 1)
        self.assertEqual(resumen.reparaciones_total, Decimal("1500.00"))
        self.assertEqual(resumen.gasto_total, Decimal("8398.00"))

    def test_excluye_anulados_futuros_y_registro_inicial(self):
        # Crear un servicio anulado, uno futuro y un registro inicial con costo.
        # Crear además un servicio válido de $400.
        resumen = resumen_anual_unidad(self.unidad, year=2026, today=date(2026, 8, 19))
        self.assertEqual(resumen.servicios_cantidad, 1)
        self.assertEqual(resumen.servicios_total, Decimal("400.00"))
```

The initial-kilometre exclusion must use the stable semantic rule already present in the catalog: `tipo_servicio.nombre == "Registro inicial de kilometraje"`, case-insensitive, rather than a production-specific ID.

- [ ] **Step 2: Ejecutar y verificar RED**

```bash
python manage.py test logistica.tests_flota_gastos.FlotaGastosResumenTests -v 2
```

Expected: FAIL because `logistica.services_flota` or `resumen_anual_unidad` does not exist.

- [ ] **Step 3: Implementar el cálculo mínimo**

Create an immutable result object and bounded queries:

```python
from dataclasses import dataclass
from decimal import Decimal

from django.db.models import Count, Q, Sum

from .models import ReparacionUnidad, ServicioRealizadoUnidad


@dataclass(frozen=True)
class ResumenAnualUnidad:
    servicios_cantidad: int
    servicios_total: Decimal
    reparaciones_cantidad: int
    reparaciones_total: Decimal

    @property
    def gasto_total(self) -> Decimal:
        return self.servicios_total + self.reparaciones_total


def servicios_realizados_validos(unidad, *, today):
    return (
        ServicioRealizadoUnidad.objects.vigentes()
        .filter(unidad=unidad, fecha_servicio__lte=today)
        .exclude(tipo_servicio__nombre__iexact="Registro inicial de kilometraje")
    )


def resumen_anual_unidad(unidad, *, year, today):
    servicios = servicios_realizados_validos(unidad, today=today).filter(fecha_servicio__year=year)
    reparaciones = ReparacionUnidad.objects.filter(unidad=unidad, fecha_ingreso__year=year)
    srv = servicios.aggregate(cantidad=Count("id"), total=Sum("costo"))
    rep = reparaciones.aggregate(cantidad=Count("id"), total=Sum("costo_total"))
    return ResumenAnualUnidad(
        servicios_cantidad=srv["cantidad"],
        servicios_total=srv["total"] or Decimal("0"),
        reparaciones_cantidad=rep["cantidad"],
        reparaciones_total=rep["total"] or Decimal("0"),
    )
```

- [ ] **Step 4: Ejecutar y verificar GREEN**

```bash
python manage.py test logistica.tests_flota_gastos.FlotaGastosResumenTests -v 2
```

Expected: all tests PASS.

- [ ] **Step 5: Dejar punto de revisión para Claude**

Claude reviews and later commits only `logistica/services_flota.py` and `logistica/tests_flota_gastos.py` with message `feat(logistica): calcular gasto anual completo por unidad`.

### Task 3: Corregir el resumen visible para todas las unidades

**Files:**
- Modify: `logistica/views.py` in `flota_resumen`
- Modify: `logistica/templates/logistica/flota_resumen.html:70-100`
- Modify: `logistica/static/logistica/pwa/sw.js:1`
- Test: `logistica/tests_flota_gastos.py`

- [ ] **Step 1: Escribir la prueba de integración de la tarjeta**

Add a logged-in manager and records for one service and one repair. Assert the response context and rendered labels:

```python
def test_resumen_muestra_servicios_reparaciones_y_total(self):
    self.client.force_login(self.usuario_con_acceso)
    response = self.client.get(reverse("logistica:flota_resumen"))
    row = next(item for item in response.context["unidades_resumen"] if item["unidad"] == self.unidad)
    self.assertEqual(row["servicios_anio"], 1)
    self.assertEqual(row["gasto_servicios_anio"], Decimal("6898.00"))
    self.assertEqual(row["reparaciones_anio"], 1)
    self.assertEqual(row["gasto_reparaciones_anio"], Decimal("1500.00"))
    self.assertEqual(row["gasto_total_anio"], Decimal("8398.00"))
    self.assertContains(response, "Servicios: 1 · $6,898.00")
    self.assertContains(response, "Reparaciones: 1 · $1,500.00")
```

Add a second test with a future service newer than a valid service and assert that `ultimo_servicio` is the valid past record.

- [ ] **Step 2: Ejecutar y verificar RED**

```bash
python manage.py test logistica.tests_flota_gastos.FlotaResumenViewTests -v 2
```

Expected: FAIL because the context lacks the new keys and still selects the future service.

- [ ] **Step 3: Usar la fuente única en `flota_resumen`**

Import `resumen_anual_unidad` and `servicios_realizados_validos`. Replace the direct service/reparation calculations with:

```python
servicios_validos = servicios_realizados_validos(unidad, today=today)
ultimo_servicio = servicios_validos.select_related("tipo_servicio").order_by("-fecha_servicio", "-id").first()
resumen_gastos = resumen_anual_unidad(unidad, year=year, today=today)
```

Add explicit context keys:

```python
"servicios_anio": resumen_gastos.servicios_cantidad,
"gasto_servicios_anio": resumen_gastos.servicios_total,
"reparaciones_anio": resumen_gastos.reparaciones_cantidad,
"gasto_reparaciones_anio": resumen_gastos.reparaciones_total,
"gasto_total_anio": resumen_gastos.gasto_total,
```

- [ ] **Step 4: Actualizar la tarjeta sin ocultar componentes**

Replace the current `Gastos año` block with three visible lines:

```django
<strong>Gastos año</strong>
<div class="fleet-muted">Servicios: {{ row.servicios_anio|intcomma }} · ${{ row.gasto_servicios_anio|floatformat:2|intcomma }}</div>
<div class="fleet-muted">Reparaciones: {{ row.reparaciones_anio|intcomma }} · ${{ row.gasto_reparaciones_anio|floatformat:2|intcomma }}</div>
<span class="fleet-badge {% if row.gasto_total_anio %}warn{% else %}ok{% endif %}">Total ${{ row.gasto_total_anio|floatformat:2|intcomma }}</span>
```

Also correct the existing duplicated labels `Lavado Lavado`, `DOC Documentos`, and `Falla Servicios` to `Lavado`, `Documentos`, and `Servicios`; this is confined to the same card and introduces no new UI behavior.

- [ ] **Step 5: Cambiar la versión del Service Worker**

Update only:

```javascript
const CACHE_NAME = "pollyanas-logistica-pwa-v88-flota-gastos-completos";
```

- [ ] **Step 6: Ejecutar y verificar GREEN**

```bash
python manage.py test logistica.tests_flota_gastos.FlotaResumenViewTests -v 2
```

Expected: all tests PASS.

- [ ] **Step 7: Dejar punto de revisión para Claude**

Claude reviews and later commits the view, template, SW bump, and tests with message `fix(logistica): mostrar gastos completos en resumen de flota`.

### Task 4: Rechazar servicios realizados con fecha futura en ambos flujos

**Files:**
- Modify: `logistica/services_flota.py`
- Modify: `logistica/views.py:3374-3392`
- Modify: `mantenimiento/views.py:2227-2291`
- Test: `logistica/tests_flota_gastos.py`

- [ ] **Step 1: Escribir pruebas de rechazo antes del código**

Add one test for `logistica:unidad_servicio_nuevo` and one for `mantenimiento:registrar_servicio_flota`. Each POSTs `today + timedelta(days=1)` in mode `realizado`, then asserts:

```python
self.assertFalse(ServicioRealizadoUnidad.objects.filter(unidad=self.unidad).exists())
messages = [str(message) for message in get_messages(response.wsgi_request)]
self.assertIn("La fecha de un servicio realizado no puede estar en el futuro.", messages)
```

Also add a test proving that `modo_servicio=programado` with `proxima_fecha` in the future remains accepted and does not get rejected by the realized-service rule.

- [ ] **Step 2: Ejecutar y verificar RED**

```bash
python manage.py test logistica.tests_flota_gastos.FechaServicioRealizadoTests -v 2
```

Expected: FAIL because both current write paths accept the future date.

- [ ] **Step 3: Implementar una validación compartida**

Add to `logistica/services_flota.py`:

```python
from django.core.exceptions import ValidationError
from django.utils import timezone


def validar_fecha_servicio_realizado(fecha, *, today=None):
    limite = today or timezone.localdate()
    if fecha > limite:
        raise ValidationError("La fecha de un servicio realizado no puede estar en el futuro.")
```

Call it before `ServicioRealizadoUnidad.objects.create` in `unidad_servicio_nuevo`, and only inside the `realizado` branch of `registrar_servicio_flota`. Catch `ValidationError`, show its message with the existing messages framework, and redirect to the stable `#servicios` fragment for the unit-detail flow.

- [ ] **Step 4: Ejecutar y verificar GREEN**

```bash
python manage.py test logistica.tests_flota_gastos.FechaServicioRealizadoTests -v 2
```

Expected: future realized-service POSTs create zero records; programming remains accepted.

- [ ] **Step 5: Dejar punto de revisión para Claude**

Claude reviews and later commits these changes with message `fix(logistica): impedir servicios realizados con fecha futura`.

### Task 5: Crear corrección auditable e idempotente para GS-CH1

**Files:**
- Create: `logistica/management/commands/corregir_historial_cheyenne.py`
- Test: `logistica/tests_flota_gastos.py`

- [ ] **Step 1: Escribir pruebas del comando protegido**

Create exact fixtures matching the production invariants: unit `GS-CH1`, service #18 equivalent with future date/cost zero, service #20 equivalent with cost `6898.00` and invoice path, canonical type `Reparación correctiva`, and active actor. Test:

```python
call_command("corregir_historial_cheyenne", actor_username=self.actor.username)
self.futuro.refresh_from_db()
self.suspension.refresh_from_db()
self.assertIsNone(self.futuro.anulado_en)  # dry-run
self.assertNotEqual(self.suspension.tipo_servicio, self.tipo_correctivo)

call_command("corregir_historial_cheyenne", actor_username=self.actor.username, apply=True)
self.futuro.refresh_from_db()
self.suspension.refresh_from_db()
self.assertIsNotNone(self.futuro.anulado_en)
self.assertEqual(self.futuro.motivo_anulacion, "Fecha futura y servicio no confirmado")
self.assertEqual(self.suspension.tipo_servicio, self.tipo_correctivo)
self.assertEqual(self.suspension.costo, Decimal("6898.00"))
self.assertTrue(self.suspension.archivo_factura.name)
```

Run the applied command a second time and assert it reports “ya corregido” without changing cost, invoice, actor or timestamps.

- [ ] **Step 2: Ejecutar y verificar RED**

```bash
python manage.py test logistica.tests_flota_gastos.CorregirHistorialCheyenneCommandTests -v 2
```

Expected: FAIL because the command does not exist.

- [ ] **Step 3: Implementar el comando con precondiciones estrictas**

The command must:

```python
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--actor-username", required=True)
        parser.add_argument("--apply", action="store_true")
```

Inside `transaction.atomic()` and `select_for_update()`:

- Resolve active actor.
- Resolve unit by exact code `GS-CH1`.
- Resolve production service IDs 18 and 20 and verify both belong to GS-CH1.
- Verify #18 has date `2026-10-28` and cost zero before mutation.
- Verify #20 has cost `6898.00` and a non-empty invoice before mutation.
- Resolve exact active type `Reparación correctiva`; fail if absent or ambiguous.
- Dry-run by default and roll back.
- With `--apply`, set #18 `anulado_en`, `anulado_por`, and exact reason, leaving `duplicado_de` null because this is not a duplicate.
- Change only `tipo_servicio` on #20.
- If both final states already match, exit successfully with `ya corregido`.

Any failed invariant raises `CommandError` without partial changes.

- [ ] **Step 4: Ejecutar y verificar GREEN**

```bash
python manage.py test logistica.tests_flota_gastos.CorregirHistorialCheyenneCommandTests -v 2
```

Expected: dry-run, apply, idempotence and invariant-failure tests all PASS.

- [ ] **Step 5: Dejar punto de revisión para Claude**

Claude reviews and later commits command and tests with message `fix(logistica): preparar correccion auditable de Cheyenne`.

### Task 6: Validación integral local y entrega a Claude

**Files:**
- Verify all modified files.

- [ ] **Step 1: Ejecutar pruebas enfocadas**

```bash
python manage.py test logistica.tests_flota_gastos -v 2
python manage.py test mantenimiento.tests_v2.MaintenanceUnifiedHistoryV2Tests -v 2
```

Expected: all tests PASS.

- [ ] **Step 2: Ejecutar verificaciones Django**

```bash
python manage.py makemigrations --check
python manage.py migrate --check
python manage.py check
```

Expected: no generated migration, no pending migration, no errors.

- [ ] **Step 3: Revisar alcance y caché**

```bash
git status --short --branch
git diff --check
git diff --stat
git diff -- logistica/services_flota.py logistica/tests_flota_gastos.py logistica/views.py mantenimiento/views.py logistica/templates/logistica/flota_resumen.html logistica/static/logistica/pwa/sw.js logistica/management/commands/corregir_historial_cheyenne.py
```

Expected: only planned files, no whitespace errors, SW version changed once.

- [ ] **Step 4: Entregar la rama a Claude**

Run the repository handoff script with the exact worktree, task ID, validation commands and next owner. Claude performs final code review, commits, PR, merge and deploy; Codex does not perform those actions under the repository contract.

### Task 7: Despliegue, corrección de datos y prueba productiva por Claude

**Files:**
- No further code changes expected.

- [ ] **Step 1: Desplegar exclusivamente después de merge**

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Expected: current `main` deployed, migrations/checks successful, static files collected, web process serving the new cache version.

- [ ] **Step 2: Simular la corrección en producción**

```bash
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web \
  python manage.py corregir_historial_cheyenne \
  --actor-username maburgos12@pollyanasdolce.com
```

Expected: exact preview of #18 annulment and #20 type correction, with no writes.

- [ ] **Step 3: Confirmar invariantes y aplicar**

After reviewing the dry-run output:

```bash
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web \
  python manage.py corregir_historial_cheyenne \
  --actor-username maburgos12@pollyanasdolce.com \
  --apply
```

Expected: #18 annulled with the approved reason; #20 classified as `Reparación correctiva`; cost and invoice unchanged.

- [ ] **Step 4: Validar en navegador real**

Open the production flota summary and verify GS-CH1:

- `Último servicio` is not dated 28/10/2026.
- Services, repairs and total are shown separately.
- The $6,898 suspension expense is included exactly once.
- No fuel amount is included.

Repeat the calculation against the DB for two other active units, including one with repairs and one with services only. Check browser console, network response and active Service Worker cache name.

- [ ] **Step 5: Cerrar la tarea registrada**

After merge, deploy and production validation, use `scripts/task_workspace_close.sh --state merged` and run `scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1`. Record before/after inventory counts and do not remove unrelated worktrees.
