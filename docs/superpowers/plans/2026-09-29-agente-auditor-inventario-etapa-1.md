# Agente auditor de inventario — Etapa 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extender la auditoría mensual existente para investigar, priorizar, asignar y notificar excepciones relevantes sin duplicar datos ni modificar movimientos Point.

**Architecture:** Un servicio idempotente en `reportes` construirá una proyección de investigación desde `ProductInventoryAuditCase.source_trace`, relaciones explícitas de Logística y responsables canónicos de RRHH/accesos. La proyección se persistirá en el mismo expediente y usará `core.Notificacion`; la pantalla solo presentará prioridad, responsable y hallazgos operativos.

**Tech Stack:** Django 5, PostgreSQL 16, ORM transaccional, plantillas Django, CSS existente y pruebas `django.test`.

---

## Mapa de archivos

- `reportes/models.py`: campos persistentes y estados del encargado virtual.
- `reportes/migrations/0061_inventory_audit_investigation.py`: migración aditiva e índices.
- `reportes/services_inventory_audit_agent.py`: investigación, prioridad, asignación y notificación idempotente.
- `reportes/services_inventory_traceability.py`: invoca la investigación después de materializar una corrida completa, sin acoplarla al cálculo de cantidades.
- `reportes/management/commands/investigate_inventory_audit_cases.py`: ejecución acotada por mes y vista previa.
- `reportes/views_inventory_traceability.py`: filtros, orden y contexto legible.
- `reportes/templates/reportes/auditoria_inventario.html`: atención y responsable sin agregar otra tabla.
- `reportes/templates/reportes/auditoria_inventario_caso.html`: bloque de investigación.
- `static/css/inventory_audit_v1.css`: estados compactos y responsive.
- `reportes/tests_inventory_audit_agent.py`: reglas del servicio y notificaciones.
- `reportes/tests_inventory_traceability_views.py`: contenido, filtros y consultas.

### Task 1: Persistir la proyección del encargado

**Files:**
- Modify: `reportes/models.py`
- Create: `reportes/migrations/0061_inventory_audit_investigation.py`
- Test: `reportes/tests_inventory_audit_agent.py`

- [ ] **Step 1: Escribir la prueba fallida del contrato**

```python
def test_case_starts_grouped_without_inventing_assignee(self):
    case = self.make_case()
    self.assertEqual(case.attention_level, "GROUPED")
    self.assertEqual(case.responsible_area, "ADMINISTRATION")
    self.assertIsNone(case.assigned_to_id)
    self.assertEqual(case.investigation_summary, {})
```

- [ ] **Step 2: Confirmar el fallo**

Run: `python3 manage.py test reportes.tests_inventory_audit_agent.InventoryAuditAgentModelTests --keepdb`

Expected: `AttributeError` o campo inexistente.

- [ ] **Step 3: Añadir campos aditivos**

```python
class AttentionLevel(models.TextChoices):
    GROUPED = "GROUPED", "Agrupado para revisión"
    NORMAL = "NORMAL", "Revisión normal"
    HIGH = "HIGH", "Atención inmediata"

class ResponsibleArea(models.TextChoices):
    LOGISTICS = "LOGISTICS", "Logística"
    SALES = "SALES", "Ventas"
    PRODUCTION = "PRODUCTION", "Producción / CEDIS"
    ADMINISTRATION = "ADMINISTRATION", "Administración"

attention_level = models.CharField(max_length=12, choices=AttentionLevel.choices, default=AttentionLevel.GROUPED)
responsible_area = models.CharField(max_length=20, choices=ResponsibleArea.choices, default=ResponsibleArea.ADMINISTRATION)
assigned_to = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="inventory_audit_cases_assigned")
assignment_reason = models.CharField(max_length=240, blank=True, default="")
investigation_summary = models.JSONField(default=dict, blank=True)
investigation_fingerprint = models.CharField(max_length=64, blank=True, default="", db_index=True)
last_notified_fingerprint = models.CharField(max_length=64, blank=True, default="")
investigated_at = models.DateTimeField(null=True, blank=True)
```

- [ ] **Step 4: Generar e inspeccionar migración**

Run: `python3 manage.py makemigrations reportes --name inventory_audit_investigation`

Expected: solo campos e índices aditivos en `ProductInventoryAuditCase`.

- [ ] **Step 5: Probar modelo y migración**

Run: `python3 manage.py migrate && python3 manage.py migrate --check`

Expected: migración aplicada y cero pendientes.

### Task 2: Investigar con relaciones exactas

**Files:**
- Create: `reportes/services_inventory_audit_agent.py`
- Test: `reportes/tests_inventory_audit_agent.py`

- [ ] **Step 1: Escribir pruebas fallidas de Logística, recurrencia y ambigüedad**

```python
def test_exact_transfer_relation_reuses_logistics_owner(self):
    case, discrepancy = self.make_case_with_open_logistics_discrepancy()
    result = InventoryAuditAgent().investigate_case(case)
    self.assertEqual(result.attention_level, "HIGH")
    self.assertEqual(result.assigned_to_id, discrepancy.assigned_to_id)
    self.assertEqual(result.investigation_summary["related_logistics_discrepancy_ids"], [discrepancy.id])

def test_ambiguous_area_candidates_leave_person_unassigned(self):
    case = self.make_case(issue_codes=["MISSING_CONVERSION_ORIGIN"])
    self.make_two_production_heads()
    result = InventoryAuditAgent().investigate_case(case)
    self.assertEqual(result.responsible_area, "PRODUCTION")
    self.assertIsNone(result.assigned_to_id)
```

- [ ] **Step 2: Confirmar fallos**

Run: `python3 manage.py test reportes.tests_inventory_audit_agent.InventoryAuditAgentServiceTests --keepdb`

Expected: servicio inexistente.

- [ ] **Step 3: Implementar un resultado inmutable y clasificación conservadora**

```python
@dataclass(frozen=True)
class InvestigationResult:
    attention_level: str
    responsible_area: str
    assigned_to_id: int | None
    assignment_reason: str
    summary: dict[str, object]
    fingerprint: str
```

El servicio debe obtener las transferencias con `source_trace["transfers"]`, relacionarlas por `RutaCargaChecklistLinea.point_transfer_line_id`, consultar discrepancias abiertas, contar recurrencias del mismo `branch_id/product_id` y separar `facts`, `hypotheses` y `missing`.

- [ ] **Step 4: Implementar resolución de responsable sin heurística nominal**

```python
def _unique_head_for_department(code: str):
    candidates = User.objects.filter(
        is_active=True,
        empleado_rrhh__departamento=code,
        empleado_rrhh__nivel_organizacional="JEFATURA",
    ).distinct()
    return candidates.first() if candidates.count() == 1 else None
```

Logística prioriza `DiscrepanciaLogistica.asignado_a`; Producción, Ventas y Administración usan jefatura única. Cualquier empate devuelve `None`.

- [ ] **Step 5: Ejecutar pruebas focalizadas**

Run: `python3 manage.py test reportes.tests_inventory_audit_agent --keepdb`

Expected: todas pasan.

### Task 3: Persistir y notificar de forma idempotente

**Files:**
- Modify: `reportes/services_inventory_audit_agent.py`
- Create: `reportes/management/commands/investigate_inventory_audit_cases.py`
- Test: `reportes/tests_inventory_audit_agent.py`

- [ ] **Step 1: Escribir pruebas de vista previa y doble ejecución**

```python
def test_second_equal_run_does_not_duplicate_notification(self):
    service = InventoryAuditAgent()
    service.run_month(self.month)
    service.run_month(self.month)
    self.assertEqual(Notificacion.objects.filter(objeto_tipo="reportes.ProductInventoryAuditCase").count(), 1)

def test_dry_run_writes_nothing(self):
    before = self.persisted_state()
    InventoryAuditAgent().run_month(self.month, dry_run=True)
    self.assertEqual(self.persisted_state(), before)
```

- [ ] **Step 2: Implementar persistencia bajo transacción**

Bloquear cada caso con `select_for_update`, guardar solo cuando cambie la huella y programar la notificación con `transaction.on_commit`. Notificar únicamente `HIGH` con `assigned_to_id` y cuando `last_notified_fingerprint != investigation_fingerprint`.

- [ ] **Step 3: Implementar comando acotado**

```bash
python3 manage.py investigate_inventory_audit_cases --month 2026-08 --dry-run
python3 manage.py investigate_inventory_audit_cases --month 2026-08
```

El comando imprime conteos `total`, `high`, `normal`, `grouped`, `assigned`, `unassigned` y `notifications`.

- [ ] **Step 4: Ejecutar pruebas**

Run: `python3 manage.py test reportes.tests_inventory_audit_agent --keepdb`

Expected: escritura y notificación idempotentes.

### Task 4: Integrar sin bloquear el balance

**Files:**
- Modify: `reportes/services_inventory_traceability.py`
- Test: `reportes/tests_inventory_traceability_materializer.py`

- [ ] **Step 1: Escribir prueba de integración**

```python
@patch("reportes.services_inventory_traceability.transaction.on_commit")
def test_complete_rebuild_schedules_agent_after_commit(self, on_commit):
    InventoryAuditMaterializer(traceability_service=self.complete_service).rebuild(self.month)
    on_commit.assert_called_once()
```

- [ ] **Step 2: Programar investigación solo tras corrida completa**

El callback debe ejecutar la investigación del mes ya materializado. Una corrida `SOURCE_INCOMPLETE` global no reemplaza la corrida ni dispara notificaciones masivas; el comando manual puede investigar el estado persistido existente.

- [ ] **Step 3: Ejecutar pruebas del materializador**

Run: `python3 manage.py test reportes.tests_inventory_traceability_materializer --keepdb`

Expected: balance, reaperturas y callback pasan.

### Task 5: Presentar la investigación sin ensuciar la pantalla

**Files:**
- Modify: `reportes/views_inventory_traceability.py`
- Modify: `reportes/templates/reportes/auditoria_inventario.html`
- Modify: `reportes/templates/reportes/auditoria_inventario_caso.html`
- Modify: `static/css/inventory_audit_v1.css`
- Test: `reportes/tests_inventory_traceability_views.py`

- [ ] **Step 1: Escribir pruebas de orden, filtro y contenido**

```python
def test_exceptions_are_ordered_high_first_and_show_owner(self):
    response = self.client.get(self.dashboard_url, {"month": "2026-08", "attention": "HIGH"})
    self.assertContains(response, "Atención inmediata")
    self.assertContains(response, "Responsable")
    self.assertNotContains(response, "reportes_productinventoryauditcase")
```

- [ ] **Step 2: Añadir filtro y orden estable**

Orden: `HIGH`, `NORMAL`, `GROUPED`, luego diferencia absoluta descendente, sucursal y producto. Precargar `assigned_to` para no introducir N+1.

- [ ] **Step 3: Mantener seis columnas**

La columna Estado contiene badge de atención, estado actual y responsable. El detalle muestra listas `Hechos comprobados`, `Posibles explicaciones` y `Falta comprobar`.

- [ ] **Step 4: Ajustar CSS existente**

Reutilizar tokens, encabezado sticky y `colgroup`; no agregar tarjetas anidadas ni movimiento decorativo. Añadir estados de foco y responsive.

- [ ] **Step 5: Ejecutar pruebas de vistas**

Run: `python3 manage.py test reportes.tests_inventory_traceability_views --keepdb`

Expected: filtros, permisos, encabezados y detalle pasan.

### Task 6: Verificar, desplegar e investigar agosto

**Files:**
- Modify if required: `reportes/admin.py`
- Modify if UI static changed and module has SW: corresponding cache version only when applicable

- [ ] **Step 1: Ejecutar validación local completa**

```bash
python3 manage.py check
python3 manage.py migrate --check
python3 manage.py test \
  reportes.tests_inventory_audit_agent \
  reportes.tests_inventory_traceability_materializer \
  reportes.tests_inventory_traceability_views \
  pos_bridge.tests.test_branch_inventory_traceability_service \
  --keepdb
```

- [ ] **Step 2: Revisar diff y migración**

Confirmar que no hay descargas Point, cambios de cantidades ni permisos automáticos.

- [ ] **Step 3: Commit, PR y CI**

Crear commits quirúrgicos, subir rama, abrir un PR y revisar el diff completo antes del merge.

- [ ] **Step 4: Desplegar por el flujo oficial**

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

- [ ] **Step 5: Vista previa productiva**

```bash
docker compose -f docker-compose.yml exec -T web \
  python manage.py investigate_inventory_audit_cases --month 2026-08 --dry-run
```

Verificar que las 506 fuentes incompletas estén agrupadas y que 3 Pecados Chico se relacione con la discrepancia 300.

- [ ] **Step 6: Aplicar y repetir para comprobar idempotencia**

Ejecutar el comando sin `--dry-run` dos veces y comprobar que la segunda corrida informa cero notificaciones nuevas.

- [ ] **Step 7: Validar interfaz autenticada**

Abrir agosto en producción, revisar consola y Network, filtro de atención, fila de 3 Pecados Chico y detalle de investigación.

- [ ] **Step 8: Cerrar worktree**

Ejecutar auditoría, cierre oficial `task_workspace_close.sh --state merged` y limpieza de rama remota según el protocolo.

