# Auditoría de Inventario con Historial Point Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolver diferencias mensuales con el historial transaccional de Point, persistirlo sin duplicados y explicarlo en el agente auditor.

**Architecture:** Un servicio de lectura por excepción usa el cliente Point y las tablas de historial existentes. El balance mensual reutiliza únicamente historias con cobertura completa; el agente muestra la conciliación y conserva como pendiente cualquier relación origen-destino no explícita.

**Tech Stack:** Django 5, PostgreSQL 16, `PointHttpSessionClient`, modelos `PointProductHistoryImport/Row`, pruebas `django.test.TestCase`.

---

### Task 1: Captura idempotente y conciliación del historial

**Files:**
- Create: `pos_bridge/services/audit_stock_history_service.py`
- Create: `pos_bridge/tests/test_audit_stock_history_service.py`

- [ ] **Step 1: Write the failing persistence tests**

Cubrir una captura con `FK_Movimiento`, repetición de la misma respuesta, reutilización cuando el mes ya está cubierto, movimientos cancelados y ventana truncada de 500 filas.

```python
def test_repeated_capture_upserts_point_movement(self):
    service = AuditStockHistoryService(client=self.client)
    first = service.capture(self.branch, self.product, self.month)
    second = service.capture(self.branch, self.product, self.month)
    self.assertEqual(PointProductHistoryImport.objects.count(), 1)
    self.assertEqual(PointProductHistoryRow.objects.count(), 1)
    self.assertEqual(first.movement_ids, second.movement_ids)
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
python3 manage.py test pos_bridge.tests.test_audit_stock_history_service -v 2
```

Expected: failure because `audit_stock_history_service` does not exist.

- [ ] **Step 3: Implement the minimum service**

Implement:

```python
class AuditStockHistoryService:
    def capture(self, branch, product, month):
        import_record = self._canonical_import(branch, product)
        if self._covers_month(import_record, month):
            return self.reconcile(branch, product, month)
        rows = self.client.get_stock_history(product.external_id, branch.external_id)
        self._upsert_rows(import_record, rows)
        return self.reconcile(branch, product, month)

    def reconcile(self, branch, product, month, *, opening, point_closing):
        rows = self._month_rows(branch, product, month)
        return self._build_reconciliation(rows, opening, point_closing)

    def capture_discrepant_cases(self, cases, month):
        self.client.login()
        return {
            case.id: self.capture(case.branch, case.product, month)
            for case in cases
        }
```

The implementation must use a deterministic SHA-256 import identity, `FK_Movimiento` as `row_number`, `update_or_create`, one client login per batch, `America/Mazatlan`, and these output fields: `coverage_status`, `opening`, `closing`, category totals, movement IDs, unknown movements, aggregate-source differences, and unexplained remainder.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the same test command. Expected: all tests pass and no duplicate history rows exist.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/audit_stock_history_service.py pos_bridge/tests/test_audit_stock_history_service.py
git commit -m "feat(pos_bridge): reutiliza historial Point para auditoría"
```

### Task 2: Use cached history in the monthly balance

**Files:**
- Modify: `pos_bridge/services/branch_inventory_traceability_service.py`
- Modify: `pos_bridge/tests/test_branch_inventory_traceability_service.py`

- [ ] **Step 1: Write the failing balance tests**

Add a complete August history for 3 Pecados Chico with opening 23, production 518, conversion input 2, adjustment input 4, return 2, conversion output 10 and transfer output 533. Assert that the resulting line closes at 6 with difference zero, conversion output 10, and history IDs in `source_trace`.

Also assert that incomplete or unknown history leaves the original line unchanged.

- [ ] **Step 2: Run the focused tests and verify RED**

```bash
python3 manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: the standard aggregate balance still reports a difference of -10.

- [ ] **Step 3: Overlay only complete cached histories**

Load the persisted reconciliations in one bounded query and replace movement totals only when `coverage_status == "COMPLETE"` and `unexplained_remainder == 0`. Preserve the original aggregate totals inside `source_trace["aggregate_comparison"]`; never hide mismatches or unknown movement types.

- [ ] **Step 4: Run focused balance and materializer tests**

```bash
python3 manage.py test \
  pos_bridge.tests.test_branch_inventory_traceability_service \
  reportes.tests_inventory_traceability_materializer -v 2
```

Expected: all pass; two identical rebuilds retain the same fingerprint.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/branch_inventory_traceability_service.py pos_bridge/tests/test_branch_inventory_traceability_service.py
git commit -m "fix(reportes): concilia balances con historial Point completo"
```

### Task 3: Teach the auditor to fetch exceptions and explain them

**Files:**
- Modify: `reportes/services_inventory_audit_agent.py`
- Modify: `reportes/management/commands/investigate_inventory_audit_cases.py`
- Modify: `reportes/tests_inventory_audit_agent.py`
- Modify: `reportes/tests_inventory_traceability_views.py` only if the existing facts block cannot display the result.

- [ ] **Step 1: Write failing agent and command tests**

Assert that the command selects only nonzero cases, uses one Point client session, captures one history per branch-product, rebuilds from cached evidence, and writes these facts:

```python
self.assertIn(
    "Point acredita 10 piezas de salida por conversión",
    case.investigation_summary["facts"],
)
self.assertEqual(case.difference, Decimal("0"))
self.assertEqual(case.point_closing, Decimal("6"))
```

Assert separately that the 64 rebanadas remain a probable relation and not a confirmed origin-destination link.

- [ ] **Step 2: Run agent tests and verify RED**

```bash
python3 manage.py test reportes.tests_inventory_audit_agent -v 2
```

- [ ] **Step 3: Add the exception-resolution phase**

The management command must capture only discrepant canonical cases, invoke the existing materializer once after new histories are saved, and let the normal agent investigation persist the explanation. `--dry-run` must perform no Point call and no write. A failure for one product must be recorded and must not abort the remaining products.

- [ ] **Step 4: Reuse the current detail UI**

Populate `facts`, `hypotheses`, `missing`, and `point_history` in `investigation_summary`. Do not add technical badges or another table. Change the template only if a single compact Point-history paragraph is required for comprehension; if changed, bump the applicable service-worker cache in the same commit.

- [ ] **Step 5: Run focused tests and verify GREEN**

```bash
python3 manage.py test \
  reportes.tests_inventory_audit_agent \
  reportes.tests_inventory_traceability_views -v 2
```

- [ ] **Step 6: Commit**

```bash
git add reportes/services_inventory_audit_agent.py \
  reportes/management/commands/investigate_inventory_audit_cases.py \
  reportes/tests_inventory_audit_agent.py
git commit -m "feat(reportes): explica diferencias con historial Point"
```

### Task 4: Full verification, PR, deploy and production proof

**Files:**
- No new files expected.

- [ ] **Step 1: Run project checks**

```bash
python3 manage.py migrate --check
python3 manage.py check
python3 manage.py test \
  pos_bridge.tests.test_audit_stock_history_service \
  pos_bridge.tests.test_branch_inventory_traceability_service \
  reportes.tests_inventory_traceability_materializer \
  reportes.tests_inventory_audit_agent \
  reportes.tests_inventory_traceability_views -v 2
```

Expected: no pending migrations, zero system-check errors, all focused tests pass.

- [ ] **Step 2: Review diff and repository state**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
```

- [ ] **Step 3: Create draft PR, review, merge and deploy**

Use the official PR flow, merge only the scoped commits, then run on the VPS:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

- [ ] **Step 4: Resolve August exceptions and verify 3 Pecados Chico**

Run the production auditor for `2026-08`, then verify read-only that case 79 has conversion output 10, expected closing 6, Point closing 6, difference 0, cached unique movement IDs, and an operational explanation.

- [ ] **Step 5: Validate the authenticated production screen**

Open the August case in the real browser. Confirm the agent facts, the zero remainder, no false confirmed slice origin, no console error, and no repeated network work when reloading.

- [ ] **Step 6: Close the worktree**

Run the workspace audit and `task_workspace_close.sh --state merged`; verify branch/worktree cleanup and prune state.
