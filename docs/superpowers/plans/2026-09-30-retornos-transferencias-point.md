# Point Transfer Returns Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproducir en la auditoría el retorno automático que Point genera al origen en transferencias finalizadas parcialmente y mostrar al repartidor real en revisiones.

**Architecture:** Mantener `PointTransferLine` como única fuente persistida. El servicio compartido conservará la salida enviada y registrará como entrada al origen el retorno positivo que Point genera al finalizar una recepción parcial; cada movimiento caerá en su fecha operativa. La plantilla de revisiones reutilizará la relación existente `ruta.repartidor.user`.

**Tech Stack:** Django 5, PostgreSQL 16, plantillas Django y `django.test.TestCase`.

---

### Task 1: Aplicar el retorno Point al saldo del origen

**Files:**
- Modify: `pos_bridge/tests/test_branch_inventory_traceability_service.py:1885-1915`
- Modify: `pos_bridge/services/branch_inventory_traceability_service.py:1150-1265`

- [ ] **Step 1: Cambiar la prueba para exigir la salida neta y la evidencia del retorno**

```python
def test_completed_transfer_quantity_mismatch_is_explicit_on_both_legs(self):
    # ... cierres existentes ...
    transfer = self._transfer(sent_quantity="4", received_quantity="3")

    result = self.service.build(month=date(2026, 8, 1))

    by_branch = {line.branch.external_id: line for line in result.lines}
    self.assertEqual(by_branch["CENTRO"].transfer_out, Decimal("4"))
    self.assertEqual(by_branch["CENTRO"].transfer_in, Decimal("1"))
    self.assertEqual(by_branch["PLAZA"].transfer_in, Decimal("3"))
    for branch_code in ("CENTRO", "PLAZA"):
        issue = next(
            issue
            for issue in by_branch[branch_code].issues
            if issue.code == "TRANSFER_QUANTITY_MISMATCH"
        )
        self.assertEqual(issue.source_ids, (transfer.id,))
        self.assertIn("Point retornó 1", issue.message)
```

- [ ] **Step 2: Ejecutar la prueba y confirmar que falla con salida 4**

Run: `python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service.BranchInventoryTraceabilityServiceTests.test_completed_transfer_quantity_mismatch_is_explicit_on_both_legs --keepdb`

Expected: FAIL porque `transfer_out` todavía es `Decimal("4")`.

- [ ] **Step 3: Implementar el cálculo mínimo en `_apply_transfers`**

```python
sent_quantity = Decimal(row.sent_quantity)
received_quantity = Decimal(row.received_quantity)
returned_quantity = Decimal("0")
if (
    row.is_finalized
    and row.is_received
    and row.received_at is not None
    and received_quantity < sent_quantity
):
    returned_quantity = sent_quantity - received_quantity

if origin_in_month:
    self._add_balance(
        transfer_out,
        (row.origin_branch_id, product_id),
        sent_quantity,
        row.id,
    )

if destination_in_month and returned_quantity > 0:
    self._add_balance(
        transfer_in,
        (row.origin_branch_id, product_id),
        returned_quantity,
        row.id,
    )
```

En el mensaje de `TRANSFER_QUANTITY_MISMATCH`, añadir únicamente cuando `returned_quantity > 0`:

```python
return_detail = (
    f" Point retornó {returned_quantity} al almacén origen."
    if returned_quantity > 0
    else ""
)
```

- [ ] **Step 4: Añadir una regresión intermensual**

Crear una transferencia parcial enviada el 31 de agosto y recibida el 1 de septiembre. Verificar que agosto conserva toda la salida y que septiembre registra el retorno en el origen y la recepción en el destino.

- [ ] **Step 5: Ejecutar las pruebas modificadas y las regresiones de transferencias abiertas**

Run: `python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service.BranchInventoryTraceabilityServiceTests.test_completed_transfer_quantity_mismatch_is_explicit_on_both_legs pos_bridge.tests.test_branch_inventory_traceability_service.BranchInventoryTraceabilityServiceTests.test_partial_transfer_return_uses_received_month pos_bridge.tests.test_branch_inventory_traceability_service.BranchInventoryTraceabilityServiceTests.test_incomplete_transfer_records_only_origin_and_auditable_issue --keepdb`

Expected: 3 tests, OK.

- [ ] **Step 6: Commit del cálculo**

```bash
git add pos_bridge/services/branch_inventory_traceability_service.py pos_bridge/tests/test_branch_inventory_traceability_service.py
git commit -m "fix(reportes): reconoce retornos parciales de Point"
```

### Task 2: Mostrar al repartidor real en revisiones

**Files:**
- Modify: `logistica/tests_discrepancias.py`
- Modify: `logistica/templates/logistica/revisiones_entrega.html:11-14`

- [ ] **Step 1: Crear la prueba de presentación con actor técnico distinto**

```python
def test_bandeja_muestra_repartidor_de_ruta_no_actor_tecnico(self):
    UserModuleAccess.objects.create(
        user=self.jefe,
        module="logistica.rutas",
        access=UserModuleAccess.ACCESS_MANAGE,
        updated_by=self.jefe,
    )
    actor_tecnico = User.objects.create_user(
        username="actor.tecnico",
        first_name="Actor",
        last_name="Técnico",
    )
    DiscrepanciaLogistica.objects.create(
        ruta=self.ruta,
        parada=self.parada,
        linea_carga=self.linea,
        origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
        cantidad_enviada=Decimal("2"),
        cantidad_cargada=Decimal("2"),
        cantidad_recibida=Decimal("1"),
        motivo="diferencia_recepcion_point",
        asignado_a=self.jefe,
        creado_por=actor_tecnico,
    )
    self.client.force_login(self.jefe)

    response = self.client.get(reverse("logistica:revisiones_entrega"))

    self.assertContains(response, self.ruta.repartidor.user.get_full_name())
    self.assertNotContains(response, actor_tecnico.get_full_name())
```

- [ ] **Step 2: Ejecutar la prueba y confirmar que muestra al actor técnico**

Run: `python manage.py test logistica.tests_discrepancias.DiscrepanciasLogisticaTests.test_bandeja_muestra_repartidor_de_ruta_no_actor_tecnico --keepdb`

Expected: FAIL porque la plantilla todavía renderiza `caso.creado_por`.

- [ ] **Step 3: Reutilizar la relación del repartidor existente**

```django
<td>{{ caso.ruta.repartidor.user.get_full_name|default:caso.ruta.repartidor.user.username }}</td>
```

- [ ] **Step 4: Ejecutar la prueba de presentación y el módulo de discrepancias**

Run: `python manage.py test logistica.tests_discrepancias --keepdb`

Expected: OK.

- [ ] **Step 5: Commit de la presentación**

```bash
git add logistica/templates/logistica/revisiones_entrega.html logistica/tests_discrepancias.py
git commit -m "fix(logistica): muestra repartidor real en revisiones"
```

### Task 3: Validar agosto, integrar y desplegar

**Files:**
- No additional source files.

- [ ] **Step 1: Ejecutar pruebas enfocadas y controles Django**

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service logistica.tests_discrepancias reportes.tests_inventory_audit_agent --keepdb
python manage.py migrate --check
python manage.py check
```

Expected: todas las pruebas OK, cero migraciones pendientes y cero errores.

- [ ] **Step 2: Simular la reconstrucción de agosto sin escribir producción**

```bash
python manage.py rebuild_product_inventory_audit --month 2026-08 --dry-run
```

Expected: la ejecución termina sin escrituras. Antes de aplicar producción, verificar que el caso CEDIS/3 Pecados Chico refleje salida 2, retorno 1 y salida neta 1 después de reconstruir la materialización mensual.

- [ ] **Step 3: Revisar diff y crear PR**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD
git push -u origin codex/reportes-retornos-transferencia-point
gh pr create --base main --head codex/reportes-retornos-transferencia-point --title "Corrige retornos parciales de transferencias Point" --body "## Resumen
- audita el retorno automático de Point en recepciones parciales
- muestra el repartidor real de la ruta

## Pruebas
- pruebas enfocadas de trazabilidad, logística y agente auditor
- django check y migrate --check"
```

Expected: PR limitado al diseño, fuente, cálculo, pruebas y plantilla.

- [ ] **Step 4: Mergear y desplegar por el flujo oficial**

Después de CI aprobado:

```bash
gh pr merge "$(gh pr view --json number --jq .number)" --merge
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

Expected: servidor en el commit mergeado, `migrate --check` y `check` exitosos.

- [ ] **Step 5: Reconstruir agosto y verificar idempotencia**

Ejecutar la reconstrucción/materialización mensual y después la investigación:

```bash
docker compose exec -T web python manage.py rebuild_product_inventory_audit --month 2026-08
docker compose exec -T web python manage.py investigate_inventory_audit_cases --month 2026-08
docker compose exec -T web python manage.py investigate_inventory_audit_cases --month 2026-08
```

Expected: la segunda investigación reporta `updated=0`, `notification_groups=0` y `notifications=0`. Confirmar por consulta que la transferencia 37934 conserva enviado 2, recibido 1 y salida neta 1 en CEDIS.

- [ ] **Step 6: Validar las dos pantallas autenticadas**

Abrir:

```text
/reportes/auditoria-inventario/casos/79/
/logistica/rutas/revisiones/
```

Expected: el caso muestra el retorno Point sin faltante ficticio y la revisión `RUT-202608-0029` muestra Carlos Anaya, no Mauricio. Consola sin errores.

- [ ] **Step 7: Cerrar el worktree oficialmente**

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 --task point_transfer_returns_audit --state merged
```

Expected: tarea cerrada, rama retirada y checkout raíz limpio/sincronizado.
