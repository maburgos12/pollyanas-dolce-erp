# Mi trabajo de colaboradores por tipo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separar la bandeja personal de seguimiento por Minutas, Proyectos y Compromisos, con conteos de estado exclusivos del tipo seleccionado y apertura predeterminada en Minutas activas.

**Architecture:** Conservar `_items_del_usuario()` como única fuente de visibilidad, enriquecer los elementos una sola vez y derivar de esa colección los totales por tipo. La vista canónica seleccionará siempre un tipo explícito antes de calcular sus estados; la plantilla representará esa jerarquía sin cambiar modelos, permisos, API ni acciones existentes.

**Tech Stack:** Django 5, plantillas Django, CSS modular del ERP, PostgreSQL 16, unittest/Django TestCase, service worker global.

---

## Task 1: Preparar la base PostgreSQL aislada y confirmar la línea base

**Files:**
- No source changes.

- [ ] **Step 1: Start an isolated PostgreSQL service**

  Use a unique `COMPOSE_PROJECT_NAME` and `DB_HOST_PORT`, then start only the `db` service.

- [ ] **Step 2: Export the local development database configuration**

  Set `APP_ENV=development`, `ALLOW_INSECURE_LOCAL_SECRET_KEY=1`, and a PostgreSQL `DATABASE_URL` pointing at the isolated port.

- [ ] **Step 3: Bring the database schema current**

  Run:

  ```bash
  python manage.py migrate
  python manage.py migrate --check
  python manage.py check
  ```

  Expected: migrations apply successfully, no pending migrations, and zero Django check errors.

- [ ] **Step 4: Run the current focused tests**

  Run:

  ```bash
  python manage.py test seguimiento.tests.MiSeguimientoViewTests
  ```

  Expected: the current baseline passes before behavior is changed.

## Task 2: Specify the new navigation behavior with failing tests

**Files:**
- Modify: `seguimiento/tests.py`

- [ ] **Step 1: Add a test for the canonical collaborator entry**

  Create a non-DG user, request `/seguimiento/`, and assert a redirect to:

  ```text
  /seguimiento/minutas/?estado=activos
  ```

  Keep the existing DG redirect assertion unchanged.

- [ ] **Step 2: Add a regression test for overdue-heavy collaborators**

  Create one overdue minute and no active minutes for the authenticated collaborator. Request `/seguimiento/minutas/` without a query string and assert:

  ```python
  self.assertEqual(response.context["active_bucket"], "activos")
  self.assertEqual(response.context["bucket_counts"]["activos"], 0)
  self.assertEqual(response.context["bucket_counts"]["vencidos"], 1)
  self.assertNotContains(response, overdue_item.titulo)
  ```

- [ ] **Step 3: Add a test for type-scoped counts and lists**

  Create active and overdue examples across Minuta, Proyecto, and Compromiso. Assert that the Minutas screen reports only minute state counts, that its visible list excludes both other types, and that the type totals still expose all three independent counts.

- [ ] **Step 4: Add a test for type links resetting to active**

  Request a vencidos state and assert each type selector link contains its own route plus `?estado=activos`; assert `aria-current="page"` identifies the selected type and selected state.

- [ ] **Step 5: Make older tests explicit about overdue state**

  Update tests whose purpose is overdue filtering or ordering to request `?estado=vencidos`; update the old generic `/seguimiento/` collaborator assertion to expect the canonical redirect.

- [ ] **Step 6: Run the focused tests and verify RED**

  Run the new tests directly.

  Expected: failures show the current implicit-overdue fallback, missing canonical redirect, or missing type navigation context. Do not edit production code before observing these failures.

- [ ] **Step 7: Commit the behavioral tests**

  ```bash
  git add seguimiento/tests.py
  git commit -m "test(seguimiento): exigir bandeja personal separada por tipo"
  ```

## Task 3: Implement canonical type and active-state selection

**Files:**
- Modify: `seguimiento/views.py`
- Test: `seguimiento/tests.py`

- [ ] **Step 1: Redirect the generic collaborator route**

  Preserve the existing DG redirect first. For collaborators, when `tipo is None`, return:

  ```python
  return redirect(f'{reverse("seguimiento:minutas")}?estado=activos')
  ```

- [ ] **Step 2: Define the three type navigation records**

  Build `type_nav` from the existing type constants. Each record must expose:

  ```python
  {
      "tipo": TIPO_MINUTA,
      "label": "Minutas",
      "helper": "Acuerdos surgidos de reuniones",
      "count": type_counts[TIPO_MINUTA],
      "url": f'{reverse("seguimiento:minutas")}?estado=activos',
      "is_active": tipo == TIPO_MINUTA,
  }
  ```

  Provide equivalent Proyecto and Compromiso records using their existing routes.

- [ ] **Step 3: Calculate state counts only after filtering by type**

  Replace the optional-type expression with an explicit selected-type subset:

  ```python
  items_del_tipo = [item for item in items if item.tipo == tipo]
  ```

  Derive `items_por_estado` and `bucket_counts` only from that subset.

- [ ] **Step 4: Remove the implicit overdue fallback**

  Normalize absent or unsupported query values to:

  ```python
  active_bucket = "activos"
  ```

  Do not inspect overdue counts when selecting the default.

- [ ] **Step 5: Expose selected-type presentation data**

  Add `type_nav`, `active_type_label`, `active_type_helper`, and `active_type_count` to the context while retaining context values required by existing actions and filters.

- [ ] **Step 6: Run the focused view tests and verify GREEN**

  Run:

  ```bash
  python manage.py test seguimiento.tests.MiSeguimientoViewTests
  ```

  Expected: all tests in the focused class pass.

- [ ] **Step 7: Commit the view behavior**

  ```bash
  git add seguimiento/views.py seguimiento/tests.py
  git commit -m "fix(seguimiento): aislar la bandeja personal por tipo"
  ```

## Task 4: Build the approved type-first interface

**Files:**
- Modify: `seguimiento/templates/seguimiento/mi_seguimiento.html`
- Modify: `static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css`
- Test: `seguimiento/tests.py`

- [ ] **Step 1: Replace the simple tabs with type cards**

  Render `type_nav` as three semantic links. Each card shows label, helper, total, and a visible “Seleccionado” marker for the active type. Set `aria-current="page"` only on the active card.

- [ ] **Step 2: Add the selected-type summary**

  Under the type cards, render the selected label and this scoped explanation:

  ```text
  Los estados siguientes corresponden exclusivamente a Minutas.
  ```

  Use the active label dynamically and show its total.

- [ ] **Step 3: Keep the state selector subordinate to the type**

  Keep the four existing states, add an accessible label that names the selected type, and preserve current route links. The current state must use `aria-current="page"`.

- [ ] **Step 4: Make list and empty-state copy type-specific**

  Use the selected type in the list title and explain empty active states without suggesting that overdue items were lost. Preserve all existing detail actions and approval sections.

- [ ] **Step 5: Implement responsive hierarchy and focus states**

  Add CSS for a three-column desktop type grid, a compact selected summary, stacked mobile cards, horizontal state navigation where needed, visible keyboard focus, and `prefers-reduced-motion` support. Reuse the ERP colors and typography.

- [ ] **Step 6: Update the stylesheet cache query**

  Change the template stylesheet query suffix to:

  ```text
  20260928-colaboradores-tipos-v1
  ```

- [ ] **Step 7: Run template and view tests**

  Run the focused tests and assert the response contains the three labels, type-specific helper text, scoped totals, and active type/state markers.

- [ ] **Step 8: Commit the interface**

  ```bash
  git add seguimiento/templates/seguimiento/mi_seguimiento.html static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css seguimiento/tests.py
  git commit -m "feat(seguimiento): mostrar bandeja personal por tipo y estado"
  ```

## Task 5: Invalidate the shared application shell cache

**Files:**
- Modify: `static/erp-sw.js`
- Modify: `templates/base.html`
- Modify: `core/templates/core/login.html`
- Modify: `core/tests.py`
- Modify: `mantenimiento/tests.py`

- [ ] **Step 1: Update the single global cache version**

  Replace every test and runtime reference to the previous cache identifier with:

  ```text
  pollyanas-erp-shell-20260928-seguimiento-colaboradores-tipos-v1
  ```

- [ ] **Step 2: Run cache consistency tests**

  Run the exact core and maintenance tests that assert the shared service worker version.

- [ ] **Step 3: Commit the cache bump**

  ```bash
  git add static/erp-sw.js templates/base.html core/templates/core/login.html core/tests.py mantenimiento/tests.py
  git commit -m "chore(pwa): renovar caché para bandeja de seguimiento"
  ```

## Task 6: Verify behavior locally in tests and browser

**Files:**
- No source changes unless a verified defect is found.

- [ ] **Step 1: Run the affected test suites**

  Run:

  ```bash
  python manage.py test seguimiento core
  python manage.py test mantenimiento.tests --keepdb
  python manage.py migrate --check
  python manage.py check
  ```

  Expected: all selected tests pass, no pending migrations, and zero check errors.

- [ ] **Step 2: Inspect the complete diff**

  Confirm only the approved view, template, CSS, tests, documentation, and global cache-version references changed. Confirm there are no model, migration, permission, assignment, or data changes.

- [ ] **Step 3: Validate desktop behavior in a real browser**

  With representative local records, verify:

  - `/seguimiento/` lands on Minutas + Activos for a collaborator.
  - Minutas, Proyectos, and Compromisos each open their own Activos state.
  - State counts and visible records are scoped to the selected type.
  - Explicit Vencidos remains available.
  - DG preview-as-collaborator follows the same behavior.
  - No relevant console or network errors occur.

- [ ] **Step 4: Validate responsive behavior**

  Repeat the primary navigation at a mobile viewport and confirm cards stack, states remain reachable, focus is visible, and the page has no document-level horizontal overflow.

- [ ] **Step 5: Commit any verification-only corrections**

  If verification reveals a defect, add the smallest regression test first, then fix and commit it separately.

## Task 7: Deliver through PR, deployment, and authenticated production proof

**Files:**
- No planned source changes.

- [ ] **Step 1: Run pre-PR repository checks**

  Review status, recent commits, registered worktrees, and `git diff origin/main..HEAD --stat`. Confirm the worktree is clean and the branch contains only this task.

- [ ] **Step 2: Push and open a draft PR**

  Include the functional summary, main files, tests, local browser validation, and explicit note that there are no migrations or data changes.

- [ ] **Step 3: Wait for CI and review the final diff**

  Require all GitHub checks to pass. Resolve failures with a regression test and scoped commit; do not merge a mixed or failing branch.

- [ ] **Step 4: Merge the PR to `main`**

  Merge only after the required checks pass and the final diff matches the approved scope.

- [ ] **Step 5: Deploy using the official VPS script**

  On `/opt/pastelerias-erp`, run only:

  ```bash
  bash scripts/deploy_web_safe.sh
  ```

  Do not run a manual `git pull` first.

- [ ] **Step 6: Verify the authenticated production screen**

  Confirm the deployed commit and cache version, then validate a direct collaborator and DG preview of a collaborator with a large portfolio. Capture evidence that the screen opens at Minutas + Activos and that counts/lists remain separated across all types.

- [ ] **Step 7: Close the registered task**

  After merge, deployment, and production proof, run the official task audit and close script with `--state merged`; verify the worktree and task branch are removed and remote references are pruned.
