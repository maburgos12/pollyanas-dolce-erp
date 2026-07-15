# Inventory Stock Idempotency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Garantizar que recepciones de compras y ajustes de inventario apliquen stock una sola vez ante doble clic, reintento o concurrencia PostgreSQL, mostrando `Procesando…` y una respuesta informativa al segundo intento.

**Architecture:** Las funciones que aplican stock serán la frontera transaccional: volverán a leer y bloquearán primero la entidad operativa y después la existencia canónica. Las vistas traducirán el resultado de dominio a mensajes Django o JSON progresivo, mientras el helper global `erp_actions.js` bloqueará solo el botón presionado y seguirá una redirección segura para refrescar la fila sin duplicar lógica.

**Tech Stack:** Django 5, PostgreSQL 16, `transaction.atomic`, `select_for_update`, `TransactionTestCase`, JavaScript nativo y contrato global `data-async-action`.

---

## File map

- `compras/views.py`: frontera transaccional de recepción y respuesta HTML/JSON del cambio de estado.
- `compras/tests.py`: regresiones secuenciales, contrato de la vista y mensajes idempotentes.
- `compras/tests_idempotencia.py`: carrera PostgreSQL de dos recepciones simultáneas.
- `compras/templates/compras/recepciones.html`: formularios Cerrar con `data-async-action`, ancla y etiqueta pendiente.
- `inventario/views.py`: frontera transaccional del ajuste y respuesta HTML/JSON compartida.
- `inventario/tests.py`: regresiones secuenciales y contrato HTML/JSON.
- `inventario/tests_idempotencia.py`: carrera PostgreSQL de dos aprobaciones simultáneas.
- `inventario/templates/inventario/ajustes.html`: formulario Aprobar y aplicar con el contrato asíncrono.
- `static/js/erp_actions.js`: soporte de redirección progresiva después de una acción correcta.
- `core/tests_actions.py`: contrato del helper global, incluido bloqueo y redirección.
- `templates/base.html`: versión de caché del helper global.
- `docs/ux/action-context-coverage.md`: evidencia de cobertura de Compras e Inventario.

### Task 1: Resultado idempotente para recepciones de compras

**Files:**
- Modify: `compras/tests.py`
- Modify: `compras/views.py`

- [ ] **Step 1: Write the failing sequential retry test**

Extender `ComprasOrdenesRecepcionesFiltersTests.test_cerrar_recepcion_desde_estatus_aplica_entrada_una_sola_vez` para invocar nuevamente la frontera y comprobar el resultado explícito:

```python
result = _apply_recepcion_to_inventario(self.recepcion_pendiente, acted_by=self.user)
existencia.refresh_from_db()
self.assertEqual(result, {"applied": False, "reason": "ya_aplicado"})
self.assertEqual(existencia.stock_actual, Decimal("2"))
self.assertEqual(
    MovimientoInventario.objects.filter(
        source_hash=f"recepcion:{self.recepcion_pendiente.id}:entrada"
    ).count(),
    1,
)
```

- [ ] **Step 2: Run the test and verify the current code does not provide transactional locking**

Run:

```bash
docker run --rm --env-file "$ERP_ROOT/.env" -e DB_HOST=db -e DB_PORT=5432 \
  --network pastelerias_erp_sprint1_default -v "$PWD:/app" -w /app \
  --entrypoint python pastelerias_erp_sprint1-web manage.py test \
  compras.tests.ComprasOrdenesRecepcionesFiltersTests.test_cerrar_recepcion_desde_estatus_aplica_entrada_una_sola_vez --keepdb
```

Expected: the existing assertions may pass sequentially, but the new test documents the result contract before the concurrency test exposes the race.

- [ ] **Step 3: Implement the transactional boundary**

Add `from django.db import transaction` if it is not already imported and update the function:

```python
@transaction.atomic
def _apply_recepcion_to_inventario(recepcion: RecepcionCompra, acted_by=None) -> dict:
    recepcion = RecepcionCompra.objects.select_for_update().get(pk=recepcion.pk)
    orden = OrdenCompra.objects.select_related("solicitud", "solicitud__insumo").get(pk=recepcion.orden_id)
    solicitud = orden.solicitud
    if not solicitud or not solicitud.insumo_id:
        return {"applied": False, "reason": "sin_solicitud_o_insumo"}

    cantidad = _to_decimal(str(solicitud.cantidad or 0), "0")
    if cantidad <= 0:
        return {"applied": False, "reason": "cantidad_no_positiva"}

    source_hash = f"recepcion:{recepcion.id}:entrada"
    if MovimientoInventario.objects.filter(source_hash=source_hash).exists():
        return {"applied": False, "reason": "ya_aplicado"}

    insumo_canonical = canonical_insumo_by_id(solicitud.insumo_id) or solicitud.insumo
    existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=insumo_canonical)
    existencia = ExistenciaInsumo.objects.select_for_update().get(pk=existencia.pk)
    prev_stock = existencia.stock_actual
```

Apply only the decorator, locked re-read, explicit order query and locked existence shown above; from `prev_stock` through the existing return value, keep the current implementation unchanged. Do not catch `IntegrityError`: the lock on the reception serializes retries for the same source, while `atomic()` guarantees rollback completo ante cualquier fallo posterior.

- [ ] **Step 4: Run the focused test**

Run the command from Step 2.

Expected: PASS; stock `2`, one movement, second result `ya_aplicado`.

- [ ] **Step 5: Commit the reception domain change**

```bash
git add compras/views.py compras/tests.py
git commit -m "fix(compras): hacer idempotente la entrada por recepción"
```

### Task 2: Concurrencia PostgreSQL para recepciones

**Files:**
- Create: `compras/tests_idempotencia.py`

- [ ] **Step 1: Write a real two-connection concurrency test**

Create a `TransactionTestCase` that builds a supplier, unit, insumo, request, order and pending reception, then runs two workers:

```python
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Barrier

from django.contrib.auth import get_user_model
from django.db import close_old_connections
from django.test import TransactionTestCase

from compras.models import OrdenCompra, RecepcionCompra, SolicitudCompra
from compras.views import _apply_recepcion_to_inventario
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, Proveedor, UnidadMedida


class RecepcionStockConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-concurrente",
            nombre="Kilogramo concurrente",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        proveedor = Proveedor.objects.create(nombre="Proveedor concurrente", activo=True)
        self.insumo = Insumo.objects.create(
            nombre="Harina concurrente",
            categoria="Masa",
            unidad_base=unidad,
            proveedor_principal=proveedor,
            activo=True,
        )
        self.cantidad = Decimal("2")
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="prueba.concurrente",
            insumo=self.insumo,
            proveedor_sugerido=proveedor,
            cantidad=self.cantidad,
            fecha_requerida=date(2026, 7, 14),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            referencia="OC CONCURRENTE",
            proveedor=proveedor,
            fecha_emision=date(2026, 7, 14),
            monto_estimado=Decimal("100"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )
        self.recepcion = RecepcionCompra.objects.create(
            orden=orden,
            fecha_recepcion=date(2026, 7, 14),
            conformidad_pct=Decimal("100"),
            estatus=RecepcionCompra.STATUS_PENDIENTE,
            observaciones="Prueba concurrente",
        )

    def _apply(self, barrier):
        close_old_connections()
        barrier.wait(timeout=5)
        try:
            recepcion = RecepcionCompra.objects.get(pk=self.recepcion.pk)
            return _apply_recepcion_to_inventario(recepcion, acted_by=None)
        finally:
            close_old_connections()

    def test_dos_transacciones_aplican_recepcion_una_sola_vez(self):
        barrier = Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(self._apply, barrier) for _ in range(2)]
            results = [future.result(timeout=10) for future in futures]

        existencia = ExistenciaInsumo.objects.get(insumo=self.insumo)
        self.assertEqual(sorted(result["applied"] for result in results), [False, True])
        self.assertEqual(existencia.stock_actual, self.cantidad)
        self.assertEqual(
            MovimientoInventario.objects.filter(
                source_hash=f"recepcion:{self.recepcion.id}:entrada"
            ).count(),
            1,
        )
```

Add `from datetime import date` and the model imports shown in the setup. The test uses real PostgreSQL rows and no mocks.

- [ ] **Step 2: Prove the test detects the race**

Temporarily run the test against `origin/main` in a disposable worktree or temporarily revert only the Task 1 function, then run:

```bash
python manage.py test compras.tests_idempotencia.RecepcionStockConcurrencyTests --keepdb
```

Expected RED: duplicate stock, `IntegrityError`, or results other than exactly one applied and one idempotent. Restore Task 1 immediately after observing the expected failure.

- [ ] **Step 3: Run the test with the transactional implementation**

Run the same test in the PostgreSQL container.

Expected: PASS repeatedly for three executions; no timeout, deadlock or unhandled exception.

- [ ] **Step 4: Commit the concurrency proof**

```bash
git add compras/tests_idempotencia.py
git commit -m "test(compras): probar cierre concurrente de recepción"
```

### Task 3: Resultado idempotente para ajustes de inventario

**Files:**
- Modify: `inventario/tests.py`
- Modify: `inventario/views.py`

- [ ] **Step 1: Write the failing sequential retry test**

Extend `InventarioAjustesApprovalTests.test_admin_aprueba_y_aplica_ajuste`:

```python
result = _apply_ajuste(ajuste, self.admin)
self.existencia.refresh_from_db()
self.assertEqual(result, {"applied": False, "reason": "ya_aplicado"})
self.assertEqual(self.existencia.stock_actual, Decimal("8"))
self.assertEqual(MovimientoInventario.objects.filter(referencia=ajuste.folio).count(), 1)
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python manage.py test inventario.tests.InventarioAjustesApprovalTests.test_admin_aprueba_y_aplica_ajuste --keepdb
```

Expected: FAIL because the current `_apply_ajuste` applies the delta twice and returns `None`.

- [ ] **Step 3: Implement adjustment locking and result contract**

Change the signature and first statements while preserving the existing trace and audit body:

```python
@transaction.atomic
def _apply_ajuste(ajuste: AjusteInventario, acted_by, comentario: str = "") -> dict:
    ajuste = (
        AjusteInventario.objects.select_for_update()
        .select_related("insumo")
        .get(pk=ajuste.pk)
    )
    if ajuste.estatus == AjusteInventario.STATUS_APLICADO:
        return {"applied": False, "reason": "ya_aplicado"}

    insumo_canonical = canonical_insumo_by_id(ajuste.insumo_id) or ajuste.insumo
    existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=insumo_canonical)
    existencia = ExistenciaInsumo.objects.select_for_update().get(pk=existencia.pk)
    prev_stock = existencia.stock_actual
```

From `delta = ajuste.cantidad_fisica - ajuste.cantidad_sistema` through the existing final audit call, keep the current body unchanged, then append `return {"applied": True, "reason": "aplicado"}`. The only early return is the locked `ya_aplicado` result above.

- [ ] **Step 4: Run focused adjustment tests**

```bash
python manage.py test inventario.tests.InventarioAjustesApprovalTests --keepdb
```

Expected: all tests PASS, including variants canonicales, rejection and permissions.

- [ ] **Step 5: Commit adjustment idempotency**

```bash
git add inventario/views.py inventario/tests.py
git commit -m "fix(inventario): serializar aplicación de ajustes"
```

### Task 4: Concurrencia PostgreSQL para ajustes

**Files:**
- Create: `inventario/tests_idempotencia.py`

- [ ] **Step 1: Write the concurrent adjustment test**

Use the same two-thread structure with separate connections:

```python
class AjusteStockConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="admin_ajuste_concurrente",
            email="admin.ajuste.concurrente@example.com",
            password="test12345",
        )
        unidad = UnidadMedida.objects.create(
            codigo="kg-ajuste-concurrente",
            nombre="Kilogramo ajuste concurrente",
            tipo=UnidadMedida.TIPO_MASA,
        )
        self.insumo = Insumo.objects.create(
            nombre="Azúcar ajuste concurrente",
            unidad_base=unidad,
            activo=True,
        )
        self.existencia = ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("10"),
        )
        self.ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema=Decimal("10"),
            cantidad_fisica=Decimal("8"),
            motivo="Conteo concurrente",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.admin,
        )

    def _apply(self, barrier, user_id):
        close_old_connections()
        barrier.wait(timeout=5)
        try:
            user = get_user_model().objects.get(pk=user_id)
            ajuste = AjusteInventario.objects.get(pk=self.ajuste.pk)
            return _apply_ajuste(ajuste, user)
        finally:
            close_old_connections()

    def test_dos_transacciones_aplican_ajuste_una_sola_vez(self):
        barrier = Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(self._apply, barrier, self.admin.id) for _ in range(2)]
            results = [future.result(timeout=10) for future in futures]

        self.existencia.refresh_from_db()
        self.ajuste.refresh_from_db()
        self.assertEqual(sorted(result["applied"] for result in results), [False, True])
        self.assertEqual(self.existencia.stock_actual, Decimal("8"))
        self.assertEqual(self.ajuste.estatus, AjusteInventario.STATUS_APLICADO)
        self.assertEqual(MovimientoInventario.objects.filter(referencia=self.ajuste.folio).count(), 1)
```

- [ ] **Step 2: Prove RED against the pre-fix function**

Run the test once with Task 3 temporarily reverted.

Expected: final stock or movement count differs, or one worker raises an unhandled database exception.

- [ ] **Step 3: Verify GREEN and stability**

Run the PostgreSQL test three times with Task 3 restored.

Expected: three clean passes without deadlock or timeout.

- [ ] **Step 4: Commit the concurrency proof**

```bash
git add inventario/tests_idempotencia.py
git commit -m "test(inventario): probar ajustes concurrentes"
```

### Task 5: Progressive action response and `Procesando…`

**Files:**
- Modify: `core/tests_actions.py`
- Modify: `static/js/erp_actions.js`
- Modify: `templates/base.html`
- Modify: `compras/tests.py`
- Modify: `compras/views.py`
- Modify: `compras/templates/compras/recepciones.html`
- Modify: `inventario/tests.py`
- Modify: `inventario/views.py`
- Modify: `inventario/templates/inventario/ajustes.html`

- [ ] **Step 1: Write failing global-helper and template tests**

Add assertions that the helper follows a server redirect and that affected forms opt in:

```python
def test_helper_sigue_redireccion_despues_de_accion_exitosa(self):
    js = (ROOT / "static" / "js" / "erp_actions.js").read_text(encoding="utf-8")
    self.assertIn("payload.redirect", js)
    self.assertIn("window.location.assign(payload.redirect)", js)
```

In module tests assert `data-async-action`, `data-context-anchor`, and `data-pending-label="Procesando…"` for Cerrar and Aprobar.

- [ ] **Step 2: Run tests and verify RED**

```bash
python manage.py test core.tests_actions compras.tests inventario.tests --keepdb
```

Expected: failures for missing redirect support and missing form attributes.

- [ ] **Step 3: Add safe redirect support to the shared helper**

After a successful JSON payload and toast, follow only same-origin relative redirects:

```javascript
showToast(payload.toast || { type: "success", message: "Acción completada." });
if (payload.redirect) {
  var redirectUrl = new URL(payload.redirect, window.location.origin);
  if (redirectUrl.origin === window.location.origin) {
    window.location.assign(redirectUrl.pathname + redirectUrl.search + redirectUrl.hash);
    return;
  }
}
```

Keep the existing `finally` restoration so errors re-enable the submitter. Bump the query version for `erp_actions.js` in `templates/base.html`.

- [ ] **Step 4: Opt in only the stock-mutating forms**

For each Cerrar form:

```html
<form method="post"
      action="{% url 'compras:recepcion_estatus' r.id 'CERRADA' %}"
      class="inline-form"
      data-async-action
      data-context-anchor="recepcion-{{ r.id }}">
  {% csrf_token %}
  <input type="hidden" name="return_query" value="{{ current_query }}">
  <input type="hidden" name="context_anchor" value="recepcion-{{ r.id }}">
  <button class="btn btn-success" type="submit" data-pending-label="Procesando…">Cerrar</button>
</form>
```

Add `id="recepcion-{{ r.id }}"` to the row. Leave Diferencias unchanged because it does not apply stock.

For adjustment approval:

Add `id="ajuste-{{ a.id }}"` to the existing table row and replace the current approval form with:

```html
<form method="post" class="d-flex gap-2 flex-wrap"
      data-async-action data-context-anchor="ajuste-{{ a.id }}">
  {% csrf_token %}
  <input type="hidden" name="action" value="approve">
  <input type="hidden" name="ajuste_id" value="{{ a.id }}">
  <input type="hidden" name="context_anchor" value="ajuste-{{ a.id }}">
  <input class="input-field" name="comentario_revision" placeholder="Comentario (opcional)">
  <button class="btn btn-primary btn-small" type="submit" data-pending-label="Procesando…">Aprobar y aplicar</button>
</form>
```

Leave Rechazar outside this stock-specific migration.

- [ ] **Step 5: Return one shared HTML/JSON result from each view**

Introduce small local helpers that inspect `Accept: application/json` and either return `JsonResponse` or add the same Django message before the existing redirect. JSON shape:

```python
{
    "ok": True,
    "redirect": f"{return_url}#{context_anchor}",
    "toast": {
        "type": "info" if result["reason"] == "ya_aplicado" else "success",
        "message": message,
        "persistent": False,
    },
}
```

Messages:

```python
success = f"Recepción {recepcion.folio} cerrada y stock aplicado."
already = f"La recepción {recepcion.folio} ya estaba aplicada; el stock no cambió nuevamente."
adjustment_success = f"Ajuste {ajuste.folio} aprobado y aplicado."
adjustment_already = f"El ajuste {ajuste.folio} ya estaba aplicado; el stock no cambió nuevamente."
```

The HTML fallback must redirect to the same URL and fragment and use `messages.success` or `messages.info` with the identical text.

- [ ] **Step 6: Test JSON, HTML fallback, context and errors**

For both modules assert:

```python
response = self.client.post(url, data, HTTP_ACCEPT="application/json", HTTP_X_REQUESTED_WITH="XMLHttpRequest")
self.assertEqual(response.status_code, 200)
self.assertTrue(response.json()["ok"])
self.assertEqual(response.json()["toast"]["type"], "success")
self.assertTrue(response.json()["redirect"].endswith(f"#ajuste-{ajuste.id}"))
```

Repeat after the record is applied and expect `toast.type == "info"` plus “el stock no cambió nuevamente”. Keep existing blocker and permission tests to prove errors do not mutate stock.

- [ ] **Step 7: Run the affected suites**

```bash
python manage.py test core.tests_actions compras.tests inventario.tests \
  compras.tests_idempotencia inventario.tests_idempotencia --keepdb
```

Expected: all tests PASS.

- [ ] **Step 8: Commit the action-context migration**

```bash
git add core/tests_actions.py static/js/erp_actions.js templates/base.html \
  compras/views.py compras/tests.py compras/templates/compras/recepciones.html \
  inventario/views.py inventario/tests.py inventario/templates/inventario/ajustes.html
git commit -m "fix(stock): bloquear doble envío y conservar contexto"
```

### Task 6: Coverage record and complete verification

**Files:**
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Record the migrated screens**

Split the pending rows so the table states:

```markdown
| Compras / Recepciones | Cerrar y aplicar entrada de stock | Sí, redirección progresiva | Sí, `#recepcion-<id>` | pruebas de compras + navegador | Cubierto |
| Inventario / Ajustes | Aprobar y aplicar ajuste | Sí, redirección progresiva | Sí, `#ajuste-<id>` | pruebas de inventario + navegador | Cubierto |
```

Keep unrelated Compras and Inventario actions explicitly pending.

- [ ] **Step 2: Run migration and system gates in PostgreSQL**

```bash
python manage.py migrate --check
python manage.py check
python manage.py test compras inventario core.tests_actions --keepdb
```

Expected: zero pending migrations, zero check errors and all tests PASS. The known `reportes.W001` warning may appear during tests but must not become an error.

- [ ] **Step 3: Run concurrency tests repeatedly**

```bash
for run in 1 2 3; do
  python manage.py test compras.tests_idempotencia inventario.tests_idempotencia --keepdb || exit 1
done
```

Expected: three complete passes, no timeout, deadlock or intermittent failure.

- [ ] **Step 4: Validate in a real browser against local PostgreSQL**

For one controlled pending reception and adjustment:

1. Open DevTools Network and Console.
2. Press Cerrar or Aprobar y aplicar.
3. Verify only the pressed button disables and reads `Procesando…`.
4. Attempt a second click and verify one POST only.
5. Verify return to the same row fragment and an accessible success toast.
6. Repeat the endpoint after application and verify an info toast stating stock did not change again.
7. Force a validation error and verify inputs remain and the button is enabled again.
8. Confirm no JavaScript console error and inspect stock/movement counts in PostgreSQL.

- [ ] **Step 5: Commit documentation evidence**

```bash
git add docs/ux/action-context-coverage.md
git commit -m "docs(ux): registrar acciones idempotentes de stock"
```

- [ ] **Step 6: Review final branch hygiene**

```bash
git status --short --branch
git log --oneline --decorate -8
git diff origin/main..HEAD --stat
git worktree list
```

Expected: clean worktree; only specification, plan, stock idempotency, action UI, tests and coverage documentation differ from `origin/main`.

### Task 7: Pull request, deployment and production validation

**Files:**
- Verify only; no file modification in this task.

- [ ] **Step 1: Push and open a draft PR**

The PR summary must list the two stock flows, PostgreSQL concurrency evidence, browser evidence, exact test commands and the fact that no production stock data was altered for testing.

- [ ] **Step 2: Wait for all CI jobs**

Expected: every required check completes successfully; historical CI from PR #831 does not count.

- [ ] **Step 3: Close PR #831 only after the replacement is ready**

Comment that the implementation was reconstructed on current `main` with concurrent PostgreSQL and browser validation, link the replacement PR, then close #831 without merging it.

- [ ] **Step 4: Merge and deploy through the safe script**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

Do not run a manual `git pull` first. Verify production `HEAD`, container start time and `web-ready`.

- [ ] **Step 5: Validate production without inventing stock movements**

Use an already safe pending test record approved by Mauricio, or validate the UI and idempotent response with a temporary non-operational record created for QA. Read stock and movement counts before and after; do not reuse a real completed reception or adjust real payroll/inventory data merely for testing.

- [ ] **Step 6: Clean task branches**

After merge, deploy and production verification, remove the local/remote task branch and worktree, then run `git fetch --prune origin`.
