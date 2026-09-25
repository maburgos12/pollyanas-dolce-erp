# Producido vs Vendido Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cargar Producido vs Vendido por mes bajo demanda sin escanear 28.7 millones de snapshots ni costear recetas que no aportan costo de merma.

**Architecture:** La vista conservará el servicio canónico mensual y limitará el trabajo auxiliar al periodo y a las recetas relevantes. El helper compartido de costos reutilizará resoluciones dentro de una ejecución, manteniendo la misma jerarquía de fuentes.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, unittest.mock.

---

### Task 1: Periodos sin escaneo de snapshots

**Files:**
- Modify: `reportes/tests_producido_vs_vendido.py`
- Modify: `reportes/views_produccion.py:822-842`

- [ ] **Step 1: Write the failing test**

Agregar una prueba que cree evidencia de ventas/conversión, invoque `_available_periods` y capture SQL para afirmar que ningún query referencia `pos_bridge_pointinventorysnapshot`, conservando el periodo seleccionado y los meses con evidencia compacta.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 manage.py test reportes.tests_producido_vs_vendido.ProducidoVsVendidoCanonicalBalanceTests.test_available_periods_do_not_scan_raw_inventory_snapshots --keepdb`

Expected: FAIL porque la implementación actual consulta `PointInventorySnapshot`.

- [ ] **Step 3: Write minimal implementation**

Eliminar `PointInventorySnapshot` de las fuentes del selector. Conservar `canonical_sales_evidence_months`, cierres mensuales, producción, conversiones, merma y el periodo seleccionado. El servicio mensual seguirá resolviendo snapshots exactos cuando construya el mes pedido.

- [ ] **Step 4: Run test to verify it passes**

Run: el mismo test; Expected: PASS.

### Task 2: Costear solamente mermas visibles

**Files:**
- Modify: `reportes/tests_producido_vs_vendido.py`
- Modify: `reportes/views_produccion.py:484-505`

- [ ] **Step 1: Write the failing test**

Parchear `get_total_cost_map`, construir una receta con merma y otra sin merma, y afirmar que el helper recibe únicamente el identificador con merma.

- [ ] **Step 2: Run test to verify it fails**

Run: el test individual nuevo; Expected: FAIL porque hoy se envían todas las recetas.

- [ ] **Step 3: Write minimal implementation**

Derivar `cost_recipe_ids` desde `balance.rows[recipe.id].waste != 0` y pedir costos solo para ese conjunto. Mantener `ZERO` para filas sin merma.

- [ ] **Step 4: Run test to verify it passes**

Run: el test individual nuevo; Expected: PASS.

### Task 3: Reutilizar resoluciones de costos

**Files:**
- Modify: `recetas/tests.py`
- Modify: `recetas/utils/derived_product_presentations.py:133-316`

- [ ] **Step 1: Write failing parity and query-budget tests**

Crear varias líneas que compartan una preparación interna y un insumo comprado. Afirmar costos exactos y un límite de consultas que impida resolver de nuevo la misma preparación/identidad por insumo.

- [ ] **Step 2: Run tests to verify they fail on query budget**

Run: los tests individuales nuevos; Expected: FAIL por exceso de consultas, manteniendo correctos los importes.

- [ ] **Step 3: Write minimal implementation**

Mantener mapas por `prep_recipe.id` e insumo durante `get_total_cost_map`; cuando la búsqueda masiva ya determinó que un insumo no tiene preparación, resolver directamente su costo canónico sin repetir `resolve_preparation_recipe_for_insumo`.

- [ ] **Step 4: Run tests to verify parity and budget pass**

Run: los tests individuales nuevos; Expected: PASS.

### Task 4: Regresión, medición y entrega

**Files:**
- Modify only if required by failing assertions: files listed above.

- [ ] **Step 1: Run module suites**

Run: `python3 manage.py test reportes.tests_producido_vs_vendido pos_bridge.tests.test_monthly_product_balance_service recetas.tests --keepdb`.

- [ ] **Step 2: Run project checks**

Run: `python3 manage.py migrate --check` and `python3 manage.py check`; Expected: no pending migrations and 0 errors.

- [ ] **Step 3: Measure production-shaped query behavior locally**

Capture query counts for `_available_periods` and `get_total_cost_map`; confirm the inventory history scan is absent and the cost query count is bounded.

- [ ] **Step 4: Review diff and commit**

Confirm only scoped files, commit with a descriptive message, push and open a draft PR with tests and measurements.

- [ ] **Step 5: Merge, deploy, and validate production**

After CI succeeds, merge to `main`, run `scripts/deploy_web_safe.sh` without a manual pull, verify deployed commit, authenticated page contents, month switching, HTTP status, logs and end-to-end response time.
