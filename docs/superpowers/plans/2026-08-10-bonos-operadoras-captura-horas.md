# Captura operativa de bonos y horas extra Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Habilitar Horas extras y Captura diaria de bonos para Rosa y Julissa con privilegio mínimo, conservando la autorización de Carolina y sin exponer importes ni ajustes.

**Architecture:** Los viewsets administrativos permanecerán cerrados. Dos contratos separados expondrán fichas y registros diarios mínimos al grupo `bonos_produccion_captura`; Horas extras tendrá una rama explícita para ese grupo y respuestas sanitizadas. La PWA seleccionará esos contratos solo para el perfil limitado.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL 16, React 18 UMD, Service Worker y Django TestCase.

---

## Mapa de archivos

- `bonos_produccion/tests_solicitudes_operadoras.py`: contratos positivos y negativos.
- `bonos_produccion/serializers.py`: serializadores mínimos sin dinero.
- `bonos_produccion/views.py`: querysets, permisos y viewsets acotados.
- `bonos_produccion/urls.py`: rutas de captura operativa.
- `bonos_produccion/templates/bonos_produccion/index.html`: cuatro pestañas y endpoints por perfil.
- `bonos_produccion/static/bonos_produccion/sw.js`: invalidación PWA.
- `bonos_produccion/tests.py`: expectativa de caché.
- `docs/ux/action-context-coverage.md`: evidencia de acciones asíncronas.

### Task 1: Preparar PostgreSQL aislado y línea base

**Files:** No source changes.

- [ ] **Step 1: Start the isolated database**

```bash
export COMPOSE_PROJECT_NAME=erp_bonos_operadoras_captura
export DB_HOST_PORT=55438
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:${DB_HOST_PORT}/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
```

Expected: PostgreSQL reports `accepting connections`.

- [ ] **Step 2: Apply and verify the baseline**

```bash
python3 manage.py migrate
python3 manage.py migrate --check
python3 manage.py check
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras --keepdb -v 2
```

Expected: no pending migrations/check errors and the existing operator suite passes.

### Task 2: Specify and implement limited overtime

**Files:**
- Modify: `bonos_produccion/tests_solicitudes_operadoras.py`
- Modify: `bonos_produccion/views.py`

- [ ] **Step 1: Write failing overtime tests**

Import `HoraExtra`. Add tests that POST for the operator employee and another active Producción employee, then assert `201`, pending state, Carolina as `jefe_directo`, notification, and absence of `monto_calculado`. Add tests that Ventas/inactive targets fail and that edit/delete/authorize/reject all return 403.

```python
@patch("bonos_produccion.views.notificar_hora_extra_solicitada")
def test_operadora_captura_hora_sin_importe(self, notificar):
    response = self.client.post(
        "/api/bonos-produccion/horas-extra/",
        json.dumps({
            "empleado": self.produccion_empleado.id,
            "fecha": "2026-08-12",
            "horas": "2.00",
            "notas": "Pedido especial",
        }),
        content_type="application/json",
    )
    self.assertEqual(response.status_code, 201)
    self.assertNotIn("monto_calculado", response.json())
    hora = HoraExtra.objects.get(pk=response.json()["id"])
    self.assertEqual(hora.estado, HoraExtra.ESTADO_PENDIENTE)
    self.assertEqual(hora.jefe_directo, self.carolina_user)
    notificar.assert_called_once()
```

- [ ] **Step 2: Run RED**

```bash
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCatalogoPermisosTests --keepdb -v 2
```

Expected: the new POST test fails with 403 because the endpoint is still administrative.

- [ ] **Step 3: Implement the minimum overtime branch**

In `HorasExtraProduccionEquipoViewSet`, use `CanAccessBonosProduccion`. For `is_bonos_produccion_capture_only` users, source employees from `empleados_operables_solicitudes_produccion()`, return true from `can_solicitar_empleado` only for that queryset, always return false from `can_gestionar_empleado`, and short-circuit edit/delete/authorize/reject with `_gestion_denegada_operadora()`.

Sanitize list/create responses to these exact keys:

```python
HORAS_EXTRA_OPERADORA_FIELDS = {
    "id", "folio", "empleado", "empleado_nombre", "area", "puesto",
    "sucursal_nombre", "fecha", "horas", "estado", "estado_label",
    "jefe_directo_nombre", "notas", "creado_en", "puede_autorizar",
    "puede_editar", "puede_eliminar",
}
```

- [ ] **Step 4: Run GREEN**

```bash
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCatalogoPermisosTests --keepdb -v 2
```

Expected: all tests pass.

### Task 3: Specify isolated daily bonus capture

**Files:**
- Modify: `bonos_produccion/tests_solicitudes_operadoras.py`

- [ ] **Step 1: Add current-period fixtures and failing API tests**

Create `OperadoraCapturaBonosTests` with `ConfigBonoPeriodo`, `BonoProduccionEmpleado`, a valid Producción employee, an out-of-scope employee, and nonzero `bono_extra`, `ajuste_positivo`, `ajuste_negativo`.

Test `/bonos-captura/` returns only current-period valid records and none of:

```python
FORBIDDEN_MONEY_FIELDS = {
    "total_a_pagar", "bono_extra", "ajuste_positivo", "ajuste_negativo",
    "monto_uniforme", "monto_asistencia", "monto_puntualidad",
    "monto_produccion", "monto_premio_embetunado",
}
```

Test POST then PATCH on `/registros-diarios-captura/`, asserting `capturado_por`, recalculated counters, and exact preservation of the three manual money fields. Test monetary input, previous-period/out-of-scope bonus, DELETE, and writes to `bonos-captura` fail.

- [ ] **Step 2: Run RED**

```bash
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCapturaBonosTests --keepdb -v 2
```

Expected: 404 because both operational routes are absent.

### Task 4: Implement strict serializers and capture-only viewsets

**Files:**
- Modify: `bonos_produccion/serializers.py`
- Modify: `bonos_produccion/views.py`
- Modify: `bonos_produccion/urls.py`

- [ ] **Step 1: Add strict serializers**

`BonoProduccionCapturaSerializer` exposes only id, employee id/name/code, area, daily counters, total embetunados and pass/fail booleans. `RegistroDiarioCapturaSerializer` exposes only id, bono, día, four daily booleans, cantidad embetunados and observación.

```python
def validate(self, attrs):
    unexpected = set(self.initial_data) - set(self.fields)
    if unexpected:
        raise serializers.ValidationError({
            key: "Campo no permitido para captura operativa."
            for key in unexpected
        })
    return attrs
```

Its `validate_bono` checks the `bonos_permitidos` queryset supplied by the view context.

- [ ] **Step 2: Add the current-period queryset**

```python
def bonos_captura_operadora_queryset():
    today = timezone.localdate()
    empleados_ids = empleados_operables_solicitudes_produccion().values_list("id", flat=True)
    return bonos_produccion_elegibles_queryset(
        BonoProduccionEmpleado.objects.select_related("empleado", "periodo").filter(
            periodo__mes=today.month,
            periodo__anio=today.year,
            empleado_id__in=empleados_ids,
        )
    )
```

- [ ] **Step 3: Add the viewsets and routes**

`BonoProduccionCapturaViewSet` is read-only. `RegistroDiarioCapturaViewSet` supports only GET, POST and PATCH; its queryset uses the allowed bonuses, create saves `capturado_por`, and create/update call `_recalcular_desde_registros`.

```python
router.register("bonos-captura", BonoProduccionCapturaViewSet, basename="bonoproduccion-bono-captura")
router.register("registros-diarios-captura", RegistroDiarioCapturaViewSet, basename="bonoproduccion-registro-captura")
```

- [ ] **Step 4: Run GREEN**

```bash
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCapturaBonosTests --keepdb -v 2
```

Expected: all capture tests pass.

### Task 5: Expose only four PWA sections and invalidate cache

**Files:**
- Modify: `bonos_produccion/tests_solicitudes_operadoras.py`
- Modify: `bonos_produccion/templates/bonos_produccion/index.html`
- Modify: `bonos_produccion/static/bonos_produccion/sw.js`
- Modify: `bonos_produccion/tests.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Write failing PWA tests**

Assert the operator source contains the four-tab array, both operational endpoints, no operator mapping for config/resumen/ajustes/revisión, and the new SW version `pollyanas-bonos-produccion-pwa-v23-operadoras-captura`.

- [ ] **Step 2: Run RED**

```bash
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraPwaTests bonos_produccion.tests.BonosProduccionTests.test_pwa_manifest_sw_y_headers --keepdb -v 2
```

Expected: tab, route, and cache assertions fail.

- [ ] **Step 3: Implement the operator PWA branch**

```javascript
const tabsPermitidos=OPERADOR_SOLICITUDES
  ? ['captura','horas_extra','permisos','prestamos']
  : ['config','captura','permisos','horas_extra','resumen','ajustes','revision'];
const initialTab=tabsPermitidos.includes(queryTab)
  ? queryTab
  : (OPERADOR_SOLICITUDES?'captura':(FORCE_CAPTURE?'captura':'config'));
```

Always call `cargarTodo`; use `/bonos-captura/` for limited users. Make `cargarRegistros` and `guardarDia` select `/registros-diarios-captura/` for them. Pass `operadorSolicitudes` into `CapturaTab` and omit `BonusRight`, prizes, discounts and total amounts. Show the fixed current month/year without editable controls. Preserve current administrative behavior.

- [ ] **Step 4: Bump cache and document action coverage**

```javascript
const CACHE_NAME = "pollyanas-bonos-produccion-pwa-v23-operadoras-captura";
```

Extend the existing Bonos Producción/Operadoras row with horas extra and daily capture, button-level blocking, retry behavior, tests and browser evidence.

- [ ] **Step 5: Run GREEN**

```bash
python3 manage.py test bonos_produccion.tests_solicitudes_operadoras bonos_produccion.tests.BonosProduccionTests.test_pwa_manifest_sw_y_headers --keepdb -v 2
```

Expected: all tests pass.

### Task 6: Regression, review and surgical commit

**Files:** Only files listed in the map.

- [ ] **Step 1: Verify schema and Django**

```bash
python3 manage.py makemigrations --check --dry-run
python3 manage.py migrate --check
python3 manage.py check
python3 manage.py showmigrations bonos_produccion bonos_ventas rrhh
```

Expected: no new/pending migrations and no check errors.

- [ ] **Step 2: Run affected suites**

```bash
python3 manage.py test bonos_produccion bonos_ventas rrhh --keepdb --parallel -v 1
```

Expected: zero failures.

- [ ] **Step 3: Review and commit only task files**

```bash
git diff --check
git status --short --branch
git log --oneline --decorate -5
git worktree list
git diff origin/main..HEAD --stat
```

Stage only task files and commit `feat: habilita captura acotada de bonos y horas extra`.

### Task 7: PR, CI, production and closure

**Files:** No new source files unless CI exposes a task defect.

- [ ] **Step 1: Sync, push and create one draft PR**

Fetch `origin/main`; if the branch is behind, merge it and rerun Task 6. Push `codex/bonos-operadoras-captura-horas`. Create one draft PR documenting scope, files, focused/full tests, browser status, absence of migrations and production validation plan.

- [ ] **Step 2: Wait for CI and merge**

Use `gh pr checks --watch`; repair only task-related failures with a new red-green cycle. Mark ready and merge only after required checks pass.

- [ ] **Step 3: Create and verify production backup**

Run `/opt/pastelerias-erp/scripts/backup_db.sh` over SSH, identify the newly created archive and run `gzip -t` on that exact file before deployment.

- [ ] **Step 4: Deploy safely**

On the VPS run only:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Do not run manual `git pull`. Verify production HEAD, containers, `migrate --check` and `check`.

- [ ] **Step 5: Validate production without persistent business writes**

For Rosa and Julissa: app/hours/bonus-capture/daily-capture GET are 200; period/admin bonus/admin daily remain 403; management actions remain 403/405. For Carolina and Dirección, existing administrative paths remain 200. Validate POST behavior inside transaction rollback or avoid creating real requests. Verify served HTML has four tabs/no money controls, served SW has v23, and browser console/network are clean.

- [ ] **Step 6: Close the registered task**

Run `task_workspace_close.sh --state merged` for `bonos-operadoras-captura-horas`, then `task_workspace_audit.sh`; confirm only this exact worktree/branch was removed and unrelated tasks remain untouched.
