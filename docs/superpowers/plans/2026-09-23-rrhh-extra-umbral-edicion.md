# RRHH Extra Threshold and Editable Duration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate automatic overtime proposals only from 50 uncovered minutes, require payable durations in 30-minute blocks, and let the assigned approver edit a pending proposal using readable hours/minutes before authorization.

**Architecture:** Keep detection evidence in minutes and centralize the threshold, duration conversion, and authorization adjustment in RRHH domain services. The existing list POST endpoint will expose the shared adjustment contract with transactional locks, evidence fingerprinting, audit notes, progressive responses, and an inline accessible editor. A dry-run-first command will cancel only legacy automatic pending proposals below the threshold.

**Tech Stack:** Django 5, PostgreSQL 16, Django templates, existing progressive-action/toast contract, unittest/Django TestCase.

---

## File map

- Modify `rrhh/services_extra_conciliacion.py`: constants, minute/decimal conversion, thresholded automatic balance, human duration formatting, authorization-block validation.
- Modify `rrhh/services/__init__.py`: create/update/cancel automatic proposals using the thresholded shared balance.
- Modify `rrhh/services_horas_extra_autorizacion.py`: shared transactional adjustment operation and authorization guard for 30-minute blocks.
- Modify `rrhh/bonos_horas_extra.py`: reuse the shared adjustment evidence contract for pending automatic corrections.
- Modify `rrhh/views.py`: route the `ajustar` action and expose readable duration labels.
- Modify `rrhh/templates/rrhh/horas_extra_list.html`: readable durations and inline adjustment form.
- Modify `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`: compact responsive editor styles.
- Create `rrhh/management/commands/regularizar_horas_extra_umbral.py`: dry-run-first legacy cleanup.
- Modify `rrhh/tests_extra_conciliacion.py`: threshold and formatter tests.
- Modify `rrhh/tests.py`: permissions, adjustment, authorization, command, and web-flow tests.
- Modify `docs/ux/action-context-coverage.md`: register the new progressive action.

### Task 0: Prepare isolated PostgreSQL baseline

**Files:**
- No source changes.

- [ ] **Step 1: Start an isolated PostgreSQL 16 service**

```bash
export COMPOSE_PROJECT_NAME=erp_rrhh_extra_umbral_edicion
export DB_HOST_PORT=55483
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:${DB_HOST_PORT}/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
```

Expected: PostgreSQL reports `accepting connections`.

- [ ] **Step 2: Apply the current main migrations and checks before coding**

```bash
python manage.py migrate
python manage.py migrate --check
python manage.py check
```

Expected: all migrations apply, no pending migration remains, and Django reports zero issues.

### Task 1: Centralize threshold, blocks, and human duration

**Files:**
- Modify: `rrhh/services_extra_conciliacion.py`
- Test: `rrhh/tests_extra_conciliacion.py`

- [ ] **Step 1: Write failing boundary and presentation tests**

Add tests covering 49, 50, 58, 60 and 90 minutes and the public helpers:

```python
def test_saldo_menor_a_50_minutos_no_genera_propuesta(self):
    diagnostico = DiagnosticoHoraExtra(49, "calculado", "")
    self.assertEqual(saldo_automatico_esperado(diagnostico, []), Decimal("0"))

def test_saldo_desde_50_minutos_conserva_evidencia(self):
    diagnostico = DiagnosticoHoraExtra(50, "calculado", "")
    self.assertEqual(saldo_automatico_esperado(diagnostico, []), Decimal("0.83"))

def test_formato_humano_no_muestra_fracciones(self):
    self.assertEqual(formatear_duracion_horas(Decimal("0.50")), "30 min")
    self.assertEqual(formatear_duracion_horas(Decimal("1.00")), "1 h")
    self.assertEqual(formatear_duracion_horas(Decimal("1.50")), "1 h 30 min")

def test_bloque_autorizable_exige_multiplos_de_30_minutos(self):
    self.assertFalse(es_bloque_extra_autorizable(Decimal("0.83")))
    self.assertTrue(es_bloque_extra_autorizable(Decimal("1.00")))
    self.assertTrue(es_bloque_extra_autorizable(Decimal("1.50")))
```

- [ ] **Step 2: Run the focused tests and confirm RED**

Run:

```bash
python manage.py test rrhh.tests_extra_conciliacion.ExtraConciliacionTests --keepdb
```

Expected: failures because the threshold and public formatting/block helpers do not exist.

- [ ] **Step 3: Implement shared minute-domain helpers**

Add domain constants and helpers:

```python
UMBRAL_SOLICITUD_EXTRA_MINUTOS = 50
BLOQUE_AUTORIZACION_EXTRA_MINUTOS = 30

def minutos_a_horas(minutos):
    return (Decimal(minutos) / Decimal("60")).quantize(Decimal("0.01"))

def formatear_duracion_minutos(minutos):
    horas, resto = divmod(int(minutos), 60)
    partes = []
    if horas:
        partes.append(f"{horas} h")
    if resto or not partes:
        partes.append(f"{resto} min")
    return " ".join(partes)

def formatear_duracion_horas(horas):
    return formatear_duracion_minutos(horas_a_minutos(horas))

def es_bloque_extra_autorizable(horas):
    minutos = horas_a_minutos(horas)
    return minutos > 0 and minutos % BLOQUE_AUTORIZACION_EXTRA_MINUTOS == 0
```

Update `saldo_automatico_esperado` to calculate uncovered minutes before applying the 50-minute threshold, then convert back to the existing two-decimal storage format. Coverage must continue excluding the current and cancelled record.

- [ ] **Step 4: Run focused tests and confirm GREEN**

Run the same test command. Expected: all focused reconciliation tests pass.

- [ ] **Step 5: Commit the domain rule**

```bash
git add rrhh/services_extra_conciliacion.py rrhh/tests_extra_conciliacion.py
git commit -m "feat(rrhh): aplicar umbral y formato legible a horas extra"
```

### Task 2: Enforce editable 30-minute authorization blocks

**Files:**
- Modify: `rrhh/services_horas_extra_autorizacion.py`
- Modify: `rrhh/bonos_horas_extra.py`
- Test: `rrhh/tests.py`

- [ ] **Step 1: Write failing service tests**

Cover assigned manager, superuser, unauthorized user, stale manager, non-pending state, missing reason, invalid 70-minute block, accepted 1-hour/90-minute blocks, unchanged evidence, and changed attendance fingerprint.

The central happy-path assertion is:

```python
ajustada, mensaje, error = ajustar_hora_extra_pendiente(
    hora.pk, self.jefe_user, horas=Decimal("1.00"),
    motivo="Se acordó cerrar la hora completa",
)
self.assertEqual(error, "")
self.assertEqual(ajustada.horas, Decimal("1.00"))
self.assertEqual(ajustada.ajuste_autorizacion["saldo"], "0.97")
self.assertEqual(ajustada.ajuste_autorizacion["motivo"], "Se acordó cerrar la hora completa")
```

- [ ] **Step 2: Run the service tests and confirm RED**

Run:

```bash
python manage.py test rrhh.tests.HoraExtraAutorizacionAPIsTests --keepdb
```

Expected: the new adjustment service and block guard are absent.

- [ ] **Step 3: Implement the shared transactional adjustment**

Add `ajustar_hora_extra_pendiente` beside `resolver_hora_extra`. It must call `bloquear_hora_extra`, re-check current manager/superuser permission and pending state, reject non-positive/non-30-minute values, require a reason when changing evidence, set `ajuste_autorizacion` with `evidencia_ajuste_extra`, append an operational correction note, save only the affected fields, and return `(hora_extra, message, error)`.

Before authorization in `resolver_hora_extra`, reject automatic pending proposals whose payable `horas` are not a 30-minute block:

```python
if he.asistencia_id and not es_bloque_extra_autorizable(he.horas):
    return he, "", "Ajusta el tiempo a bloques de 30 minutos antes de autorizar."
```

Extract/reuse the low-level evidence helper from the existing bonus edit flow so both surfaces use the same fingerprint, balance, motive, and concurrency rules without narrowing existing HR management permissions for authorized records.

- [ ] **Step 4: Run service and API regression tests**

```bash
python manage.py test rrhh.tests.HoraExtraAutorizacionAPIsTests bonos_produccion.tests bonos_ventas.tests --keepdb
```

Expected: all tests pass and authorized edits still recalculate `monto_calculado`.

- [ ] **Step 5: Commit the authorization contract**

```bash
git add rrhh/services_horas_extra_autorizacion.py rrhh/bonos_horas_extra.py rrhh/tests.py
git commit -m "feat(rrhh): permitir ajuste auditado antes de autorizar extra"
```

### Task 3: Add readable inline editing to the RRHH list

**Files:**
- Modify: `rrhh/views.py`
- Modify: `rrhh/templates/rrhh/horas_extra_list.html`
- Modify: `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`
- Modify: `docs/ux/action-context-coverage.md`
- Test: `rrhh/tests.py`

- [ ] **Step 1: Write failing view tests**

Assert that an assigned manager sees `Editar horas`, `Tiempo detectado`, hours/minute controls, and human labels, while another user cannot see or submit the editor. POST `action=ajustar` using integer fields:

```python
datos = {
    "hora_extra_id": hora.pk,
    "action": "ajustar",
    "horas_enteras": "1",
    "minutos": "30",
    "motivo_ajuste": "Trabajo acordado con jefatura",
}
response = self.client.post(reverse("rrhh:rrhh_he_list"), datos, HTTP_ACCEPT="application/json")
self.assertEqual(response.status_code, 200)
self.assertEqual(response.json()["redirect"], f"/rrhh/horas-extra/#hora-extra-{hora.pk}")
```

- [ ] **Step 2: Run view tests and confirm RED**

```bash
python manage.py test rrhh.tests.RRHHViewsTests --keepdb
```

Expected: no adjustment action or human duration fields are rendered.

- [ ] **Step 3: Route the progressive adjustment action**

In `horas_extra_list`, parse `horas_enteras` as a non-negative integer and `minutos` from `{0, 30}`, combine them to minutes, convert with the shared helper, and call `ajustar_hora_extra_pendiente`. Preserve the existing JSON/toast/redirect/anchor contract and return field-specific validation messages without discarding submitted values.

Attach presentation-only properties to each record:

```python
he.duracion_label = formatear_duracion_horas(he.horas)
he.saldo_detectado_label = (
    formatear_duracion_horas(Decimal(he.contexto_calculo["saldo_detectado"]))
    if he.contexto_calculo.get("saldo_detectado") is not None else "No disponible"
)
```

- [ ] **Step 4: Build the responsive inline editor**

Replace `{{ he.horas }} h` with `{{ he.duracion_label }}`. For eligible pending rows, render a disclosure labelled `Editar horas` containing labelled number/select fields for hours and minutes, the detected duration, a required reason textarea, and a progressive Guardar button. Keep Autorizar and Rechazar beside it; disabled authorization must reference the explanatory warning.

Update the CSS with a compact bordered editor, stacked controls below 760px, visible focus, and no fractional inputs. Bump the stylesheet query version. Register the action in `docs/ux/action-context-coverage.md`.

- [ ] **Step 5: Run view tests and static checks**

```bash
python manage.py test rrhh.tests.RRHHViewsTests --keepdb
python manage.py check
```

Expected: tests pass and Django reports zero issues.

- [ ] **Step 6: Commit the visible flow**

```bash
git add rrhh/views.py rrhh/templates/rrhh/horas_extra_list.html static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css docs/ux/action-context-coverage.md rrhh/tests.py
git commit -m "feat(rrhh): editar horas extra en formato horas y minutos"
```

### Task 4: Regularize legacy tiny pending proposals safely

**Files:**
- Create: `rrhh/management/commands/regularizar_horas_extra_umbral.py`
- Test: `rrhh/tests.py`

- [ ] **Step 1: Write failing command tests**

Create automatic pending proposals below/equal/above the boundary plus manual and authorized controls. Assert dry-run does not mutate, apply cancels only the automatic pending rows with current uncovered balances below 50 minutes, appends the reason, creates an `AuditLog`, and is idempotent.

- [ ] **Step 2: Run command tests and confirm RED**

```bash
python manage.py test rrhh.tests.HoraExtraUmbralRegularizacionTests --keepdb
```

Expected: command is unknown.

- [ ] **Step 3: Implement dry-run-first command**

The command must enumerate candidate IDs with `select_related`, evaluate them through `contexto_hora_extra`/the shared balance, print exact counts and IDs, and mutate only with `--apply`. Apply each candidate under the established jornada/advisory locks, re-check it after locking, set `estado=cancelado`, append `Cancelada automáticamente: saldo vigente menor al umbral de 50 minutos.`, and write one scoped `AuditLog` summary. No delete or mass `QuerySet.update` is allowed.

- [ ] **Step 4: Run command and regression tests**

```bash
python manage.py test rrhh.tests.HoraExtraUmbralRegularizacionTests rrhh.tests_extra_conciliacion --keepdb
```

Expected: all pass and a second apply changes zero records.

- [ ] **Step 5: Commit the regularization tool**

```bash
git add rrhh/management/commands/regularizar_horas_extra_umbral.py rrhh/tests.py
git commit -m "feat(rrhh): regularizar propuestas menores al umbral"
```

### Task 5: Full verification, release, production cleanup, and browser proof

**Files:**
- Modify only test/docs files if verification exposes a scoped defect.

- [ ] **Step 1: Start isolated PostgreSQL and verify the branch baseline**

Use a unique compose project and host port, export `DATABASE_URL`, wait for `pg_isready`, run all migrations, then run `migrate --check` and `check`.

- [ ] **Step 2: Run the complete affected suite**

```bash
python manage.py test rrhh.tests_extra_conciliacion rrhh.tests_extra_jefatura rrhh.tests.HoraExtraAutorizacionAPIsTests rrhh.tests.HoraExtraAutorizacionConcurrenteTests rrhh.tests.RRHHViewsTests bonos_produccion.tests bonos_ventas.tests --keepdb
python manage.py migrate --check
python manage.py check
git diff --check
```

Expected: zero failures, zero pending migrations, zero system-check issues, and no whitespace errors.

- [ ] **Step 3: Validate the real UI locally at desktop and 390x844**

Confirm readable durations, inline editor, 30-minute controls, permission visibility, progressive success/error toasts, focus, anchor retention, disabled authorization for a non-block value, and no console/network errors.

- [ ] **Step 4: Review diff and open a draft PR**

Verify status, recent commits, worktree registry, branch relation, and `origin/main..HEAD`. Push only this branch and open one draft PR with files, behavior, tests, and browser evidence.

- [ ] **Step 5: Merge only after green CI and deploy through the official script**

Merge the single-scope PR, then run on the VPS:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Do not run a manual `git pull` first. Verify deployed commit, container start/reload state, health, and authenticated `/rrhh/horas-extra/`.

- [ ] **Step 6: Preview and apply production regularization**

Run the management command without `--apply`, capture candidate count/IDs, verify every candidate is automatic/pending/below 50 minutes, then run with `--apply`. Fresh-read the same records and counts; confirm no manual or closed record changed.

- [ ] **Step 7: Verify production behavior and close lifecycle**

At desktop and 390x844, confirm durations use hours/minutes, editor permission and validation work, tiny pending records are gone from Pending but remain Cancelled, and no destructive authorization is performed during visual validation. Close with `task_workspace_close.sh --state merged`, then run the read-only workspace audit and `git worktree prune --dry-run`.
