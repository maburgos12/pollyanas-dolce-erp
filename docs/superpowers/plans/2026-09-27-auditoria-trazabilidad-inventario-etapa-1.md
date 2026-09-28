# Auditoría de trazabilidad de inventario — Etapa 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar una auditoría mensual rápida y comprensible que reconstruya, por producto y ubicación Point, el inventario inicial, todos los movimientos identificables, el saldo esperado y el cierre Point, y que gestione excepciones con explicación y aprobación separadas.

**Architecture:** Conservar las fuentes Point como evidencia inmutable, normalizarlas en un servicio nuevo por ubicación y materializar una proyección mensual de casos para que la pantalla no recalcule el historial. La pantalla actual “Producido vs vendido” seguirá como resumen empresarial y enlazará a una nueva vista de auditoría por excepciones. La conciliación física se deja detrás de un contrato de lectura para una segunda etapa; no se crea una captura paralela.

**Tech Stack:** Django 5.0.1, PostgreSQL 16, templates Django, JavaScript progresivo existente (`data-async-action`), pytest/Django TestCase, Point snapshot and movement models.

---

## Límites de esta entrega

- Implementar exclusivamente la Etapa 1 del diseño aprobado: conciliación de movimientos Point.
- Auditar un mes por solicitud; agosto de 2026 será la primera validación operativa.
- No alterar cierres protegidos, ventas, producción, mermas, conversiones, transferencias ni catálogos Point.
- No declarar automáticamente una diferencia como merma ni resolver asociaciones ambiguas por semejanza.
- No crear captura de conteos físicos. Exponer `physical_count_status="NOT_AVAILABLE"` hasta integrar el módulo canónico en otra entrega.
- No inventar tolerancias. En esta etapa el balance es exacto a la precisión almacenada por Point.

## Contratos de negocio que deben permanecer visibles

```text
inventario esperado = inventario inicial Point
                    + producción
                    + transferencias recibidas
                    + conversiones de entrada
                    - ventas
                    - mermas
                    - transferencias enviadas
                    - conversiones de salida
                    +/- ajustes Point identificados

diferencia de movimientos = cierre Point - inventario esperado
```

- Una ubicación puede ser una sucursal, CEDIS o Devoluciones.
- Devoluciones conserva inventario; recibir producto ahí no constituye merma.
- Una transferencia cambia dos ubicaciones y no cambia el total empresa.
- El cierre Point protegido, la conciliación de movimientos y la futura conciliación física son tres estados distintos.
- Una fuente faltante produce `SOURCE_INCOMPLETE`, nunca una cantidad cero.
- Quien explica un caso no puede aprobar su propia explicación.

### Task 1: Crear el motor de balance por producto y ubicación

**Files:**
- Create: `pos_bridge/services/branch_inventory_traceability_service.py`
- Create: `pos_bridge/tests/test_branch_inventory_traceability_service.py`
- Reference: `pos_bridge/services/monthly_product_balance_service.py`
- Reference: `pos_bridge/models/historical_inventory.py`

- [ ] **Step 1: Escribir pruebas fallidas del cierre inicial, cierre final y aislamiento por ubicación**

Crear pruebas con dos ubicaciones y el mismo producto. Una debe tener faltante y la otra sobrante por la misma cantidad. Verificar que ambas líneas permanezcan como excepciones aunque el total empresa sea cero.

```python
def test_equal_company_total_does_not_hide_branch_differences(self):
    result = self.service.build(month=date(2026, 8, 1))

    by_branch = {(line.branch.external_id, line.product.external_id): line for line in result.lines}
    self.assertEqual(by_branch[("CENTRO", "PASTEL-001")].difference, Decimal("-1"))
    self.assertEqual(by_branch[("PLAZA", "PASTEL-001")].difference, Decimal("1"))
    self.assertEqual(result.company_difference, Decimal("0"))
    self.assertEqual(result.exception_count, 2)
```

- [ ] **Step 2: Ejecutar la prueba y confirmar que falla por ausencia del servicio**

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: `ModuleNotFoundError` para `branch_inventory_traceability_service`.

- [ ] **Step 3: Crear los contratos de salida y validar las dos fechas exactas de cierre**

El archivo nuevo debe definir estructuras inmutables y no devolver diccionarios sin contrato:

```python
@dataclass(frozen=True)
class TraceSourceIssue:
    code: str
    message: str
    branch_id: int | None = None
    product_id: int | None = None
    source_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class BranchProductBalance:
    branch: PointBranch
    product: PointProduct
    opening: Decimal
    production: Decimal
    sales: Decimal
    waste: Decimal
    transfer_in: Decimal
    transfer_out: Decimal
    conversion_in: Decimal
    conversion_out: Decimal
    identified_adjustment: Decimal
    expected_closing: Decimal
    point_closing: Decimal
    difference: Decimal
    source_trace: dict[str, tuple[int, ...]]
    issues: tuple[TraceSourceIssue, ...]


@dataclass(frozen=True)
class BranchInventoryTraceability:
    month: date
    lines: tuple[BranchProductBalance, ...]
    global_issues: tuple[TraceSourceIssue, ...]
    company_difference: Decimal
    exception_count: int
    source_complete: bool
```

`BranchInventoryTraceabilityService.build(month)` debe:

1. normalizar `month` al primer día;
2. obtener el último día del mes anterior y el último día del mes solicitado;
3. exigir un `PointHistoricalInventoryClosing` verificado para cada fecha;
4. agrupar `PointHistoricalInventoryClosingLine.stock` por `(branch_id, product_id)`;
5. formar el universo de claves con la unión de apertura, cierre y movimientos;
6. devolver `source_complete=False` y un problema global si falta cualquiera de los dos cierres.

No ejecutar el resto del cálculo si falta un cierre requerido. Esto evita convertir una ausencia en cero.

- [ ] **Step 4: Ejecutar las pruebas del servicio**

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: pruebas de fechas, aislamiento por ubicación y fuente incompleta en verde.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/branch_inventory_traceability_service.py pos_bridge/tests/test_branch_inventory_traceability_service.py
git commit -m "feat(point): calcula cierres por producto y ubicación"
```

### Task 2: Normalizar ventas, producción y mermas sin perder evidencia

**Files:**
- Modify: `pos_bridge/services/branch_inventory_traceability_service.py`
- Modify: `pos_bridge/tests/test_branch_inventory_traceability_service.py`
- Reference: `pos_bridge/models.py`

- [ ] **Step 1: Escribir pruebas fallidas para movimientos directos y productos no resueltos**

Cubrir estos casos:

- venta disminuye únicamente la sucursal donde ocurrió;
- producción aumenta únicamente la ubicación registrada;
- merma disminuye la ubicación registrada aunque la haya capturado personal de Ventas;
- producto sin código exacto o nombre normalizado único genera `UNRESOLVED_PRODUCT`;
- una coincidencia ambigua no se suma a ningún producto.

- [ ] **Step 2: Confirmar el fallo de las pruebas nuevas**

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: fallos de cantidades de producción, venta y merma aún no incorporadas.

- [ ] **Step 3: Implementar un resolvedor conservador de producto**

Usar esta precedencia:

1. relación directa `product_id` si la fuente la contiene;
2. coincidencia exacta de `external_id`;
3. coincidencia exacta de `sku` solamente si es única;
4. coincidencia de `normalized_name` solamente si es única;
5. problema explícito si no existe o hay más de una coincidencia.

El servicio debe conservar en `source_trace` los IDs de filas de cada fuente:

```python
source_trace = {
    "opening": tuple(opening_line_ids),
    "closing": tuple(closing_line_ids),
    "sales": tuple(sale_ids),
    "production": tuple(production_ids),
    "waste": tuple(waste_ids),
    "transfers": (),
    "conversions": (),
    "adjustments": (),
}
```

Las consultas deben cargar relaciones con `select_related` y agregar en memoria una sola vez por fuente. Queda prohibido consultar costos o catálogos dentro del ciclo por producto.

- [ ] **Step 4: Verificar movimientos directos y número acotado de consultas**

Agregar `assertNumQueries` con un volumen duplicado de productos. El número de consultas no debe crecer con el número de líneas.

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: suite en verde y prueba de consultas estable.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/branch_inventory_traceability_service.py pos_bridge/tests/test_branch_inventory_traceability_service.py
git commit -m "feat(point): incorpora movimientos directos al balance por ubicación"
```

### Task 3: Relacionar transferencias, Devoluciones y conversiones

**Files:**
- Modify: `pos_bridge/services/branch_inventory_traceability_service.py`
- Modify: `pos_bridge/tests/test_branch_inventory_traceability_service.py`
- Reference: `control/services_mermas_devoluciones.py`

- [ ] **Step 1: Escribir pruebas fallidas de transferencia y conversión**

Incluir al menos:

1. CEDIS envía 4 unidades y sucursal recibe 4: `transfer_out=4`, `transfer_in=4`, total empresa sin cambio.
2. Sucursal envía 2 unidades a Devoluciones: baja sucursal, sube Devoluciones, no crea merma.
3. Transferencia enviada pero no recibida: origen registra salida y ambos lados conservan `INCOMPLETE_TRANSFER` con la referencia Point.
4. Un pastel entero convertido en 12 rebanadas: origen `conversion_out=1`, destino `conversion_in=12` en la misma ubicación.
5. Conversión sin producto de origen resoluble: no inventa salida y crea `MISSING_CONVERSION_ORIGIN`.
6. Conversión con equivalencia distinta a la relación canónica vigente: `CONVERSION_EQUIVALENCE_MISMATCH`.

- [ ] **Step 2: Ejecutar y confirmar los fallos específicos**

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: fallos en `transfer_in`, `transfer_out`, `conversion_in` y `conversion_out`.

- [ ] **Step 3: Implementar reglas temporales de transferencias**

- Usar `sent_at` para la salida. Si una transferencia finalizada no tiene `sent_at`, usar `registered_at` y añadir `TRANSFER_DATE_FALLBACK`.
- Registrar entrada solamente con `is_received=True` y `received_at` dentro del mes operativo.
- Excluir transferencias canceladas.
- Mantener la misma referencia en el rastro de origen y destino.
- Detectar Devoluciones por la ubicación Point normalizada existente; no crear una sucursal ERP artificial.

- [ ] **Step 4: Implementar las dos piernas de una conversión**

La fila `PointConversionLine` representa el producto resultante. Resolver el origen con `source_item_code` y después con nombre normalizado único. Validar la equivalencia contra la relación canónica ya disponible entre el producto Point y la receta/presentación; si no existe evidencia canónica, conservar el movimiento como candidato y marcarlo incompleto.

No sumar una merma implícita por rendimiento. Solo una fila real de `PointWasteLine` puede disminuir inventario por merma.

- [ ] **Step 5: Ejecutar la suite completa del servicio**

Run:

```bash
python manage.py test pos_bridge.tests.test_branch_inventory_traceability_service -v 2
```

Expected: todos los escenarios de CEDIS, sucursal, Devoluciones, transferencias y conversiones en verde.

- [ ] **Step 6: Commit**

```bash
git add pos_bridge/services/branch_inventory_traceability_service.py pos_bridge/tests/test_branch_inventory_traceability_service.py
git commit -m "feat(point): traza transferencias y conversiones de inventario"
```

### Task 4: Persistir casos mensuales e historial de auditoría

**Files:**
- Modify: `reportes/models.py`
- Create: `reportes/migrations/00XX_product_inventory_audit.py`
- Create: `reportes/tests_inventory_audit_models.py`

- [ ] **Step 1: Escribir pruebas fallidas de integridad y permisos**

Verificar:

- unicidad de `(month, branch, product)`;
- una sola ejecución materializada por mes y conservación del último error de fuente;
- estados separados de cierre, movimientos y conciliación física;
- evento append-only con actor, fecha y evidencia;
- permiso personalizado `approve_product_inventory_audit`.

- [ ] **Step 2: Confirmar que los modelos aún no existen**

Run:

```bash
python manage.py test reportes.tests_inventory_audit_models -v 2
```

Expected: error de importación para `ProductInventoryAuditCase`.

- [ ] **Step 3: Crear `ProductInventoryAuditCase`**

Crear primero `ProductInventoryAuditRun` como cabecera mensual. Debe tener `month` único, estado `READY` o `SOURCE_INCOMPLETE`, `source_issues` JSON, conteos de resumen, `calculation_fingerprint`, `started_at`, `rebuilt_at` y `last_successful_rebuild_at`. Una reconstrucción fallida actualiza esta cabecera, pero no borra ni reemplaza los casos de la última reconstrucción exitosa.

Después crear `ProductInventoryAuditCase` y relacionarlo con la cabecera mediante `run = ForeignKey(ProductInventoryAuditRun, on_delete=PROTECT, related_name="cases")`.

Campos obligatorios:

```python
class ProductInventoryAuditCase(models.Model):
    class MovementStatus(models.TextChoices):
        BALANCED = "BALANCED", "Conciliado"
        NEEDS_EXPLANATION = "NEEDS_EXPLANATION", "Pendiente de explicación"
        PENDING_APPROVAL = "PENDING_APPROVAL", "Explicación pendiente de aprobación"
        RESOLVED = "RESOLVED", "Resuelto y aprobado"
        SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE", "Fuente incompleta"

    class PointClosingStatus(models.TextChoices):
        AVAILABLE = "AVAILABLE", "Disponible"
        PROTECTED = "PROTECTED", "Protegido"

    class PhysicalStatus(models.TextChoices):
        NOT_AVAILABLE = "NOT_AVAILABLE", "Sin conteo manual"

    month = models.DateField(db_index=True)
    run = models.ForeignKey(ProductInventoryAuditRun, on_delete=models.PROTECT, related_name="cases")
    branch = models.ForeignKey("pos_bridge.PointBranch", on_delete=models.PROTECT)
    product = models.ForeignKey("pos_bridge.PointProduct", on_delete=models.PROTECT)
    opening_point = models.DecimalField(max_digits=18, decimal_places=4)
    production = models.DecimalField(max_digits=18, decimal_places=4)
    sales = models.DecimalField(max_digits=18, decimal_places=4)
    waste = models.DecimalField(max_digits=18, decimal_places=4)
    transfer_in = models.DecimalField(max_digits=18, decimal_places=4)
    transfer_out = models.DecimalField(max_digits=18, decimal_places=4)
    conversion_in = models.DecimalField(max_digits=18, decimal_places=4)
    conversion_out = models.DecimalField(max_digits=18, decimal_places=4)
    identified_adjustment = models.DecimalField(max_digits=18, decimal_places=4)
    expected_closing = models.DecimalField(max_digits=18, decimal_places=4)
    point_closing = models.DecimalField(max_digits=18, decimal_places=4)
    difference = models.DecimalField(max_digits=18, decimal_places=4)
    point_closing_status = models.CharField(max_length=16, choices=PointClosingStatus.choices)
    movement_status = models.CharField(max_length=24, choices=MovementStatus.choices)
    physical_status = models.CharField(max_length=20, choices=PhysicalStatus.choices)
    issue_codes = models.JSONField(default=list)
    source_trace = models.JSONField(default=dict)
    calculation_fingerprint = models.CharField(max_length=64, db_index=True)
    rebuilt_at = models.DateTimeField()
```

Agregar restricción única, índices por mes/estado/ubicación y el permiso personalizado. La migración debe obtener su número real con `python manage.py makemigrations reportes`; no renombrar migraciones ya existentes.

- [ ] **Step 4: Crear `ProductInventoryAuditEvent`**

El evento debe conservar `EXPLAIN`, `APPROVE`, `REJECT` y `REOPEN`, `reason_code`, `notes`, `evidence`, `actor` y `created_at`. No exponer edición ni eliminación desde vistas. Una aprobación referencia la explicación vigente mediante `related_event` para que el historial no dependa del texto más reciente.

- [ ] **Step 5: Generar, inspeccionar y probar la migración**

Run:

```bash
python manage.py makemigrations reportes
python manage.py migrate --check
python manage.py test reportes.tests_inventory_audit_models -v 2
```

Expected: una migración nueva de `reportes`, ninguna migración adicional inesperada y pruebas en verde.

- [ ] **Step 6: Commit**

```bash
git add reportes/models.py reportes/migrations reportes/tests_inventory_audit_models.py
git commit -m "feat(reportes): persiste casos de auditoría de inventario"
```

### Task 5: Materializar el mes de forma idempotente

**Files:**
- Create: `reportes/services_inventory_traceability.py`
- Create: `reportes/management/commands/rebuild_product_inventory_audit.py`
- Create: `reportes/tests_inventory_traceability_materializer.py`

- [ ] **Step 1: Escribir pruebas fallidas de reconstrucción**

Probar:

- dos reconstrucciones idénticas no duplican casos ni eventos;
- un fingerprint idéntico conserva una resolución aprobada;
- un cambio posterior en fuentes reabre un caso aprobado y agrega evento de sistema `REOPEN`;
- fuente incompleta materializa el estado general del mes sin fabricar líneas cero;
- `--dry-run` no escribe casos ni eventos.

- [ ] **Step 2: Confirmar los fallos**

Run:

```bash
python manage.py test reportes.tests_inventory_traceability_materializer -v 2
```

Expected: error de importación del materializador.

- [ ] **Step 3: Implementar el materializador transaccional**

`InventoryAuditMaterializer.rebuild(month, dry_run=False)` debe:

1. llamar al servicio por ubicación;
2. construir un fingerprint SHA-256 con cantidades, problemas y IDs de fuentes ordenados;
3. usar `transaction.atomic()` y `update_or_create` sobre la cabecera y la clave mensual;
4. clasificar diferencia cero sin problemas como `BALANCED`;
5. clasificar cualquier fuente requerida faltante como `SOURCE_INCOMPLETE`;
6. clasificar una diferencia no cero como `NEEDS_EXPLANATION`;
7. conservar `RESOLVED` solo si el fingerprint no cambió;
8. reabrir cambios con un evento de sistema que indique fingerprint anterior y nuevo;
9. no borrar casos históricos desaparecidos: marcarlos `SOURCE_INCOMPLETE` y registrar la razón;
10. si falta una fuente requerida, actualizar únicamente la cabecera a `SOURCE_INCOMPLETE`, conservar los casos de la última ejecución exitosa y terminar sin recalcular cantidades.

El método debe devolver conteos `created`, `updated`, `unchanged`, `reopened`, `balanced`, `exceptions` y `source_incomplete`.

- [ ] **Step 4: Implementar el comando de reconstrucción**

Interfaz exacta:

```bash
python manage.py rebuild_product_inventory_audit --month 2026-08 --dry-run
python manage.py rebuild_product_inventory_audit --month 2026-08
```

El comando debe rechazar formatos distintos de `YYYY-MM`, imprimir los conteos finales y regresar con error si los cierres requeridos no están disponibles. En ese caso solo registra la falla en `ProductInventoryAuditRun`; no modifica casos ni cierres protegidos. Con `--dry-run` no escribe siquiera la cabecera.

- [ ] **Step 5: Ejecutar pruebas de idempotencia**

Run:

```bash
python manage.py test reportes.tests_inventory_traceability_materializer -v 2
```

Expected: suite en verde; la segunda reconstrucción reporta cero creaciones y cero reaperturas.

- [ ] **Step 6: Commit**

```bash
git add reportes/services_inventory_traceability.py reportes/management/commands/rebuild_product_inventory_audit.py reportes/tests_inventory_traceability_materializer.py
git commit -m "feat(reportes): materializa auditoría mensual de inventario"
```

### Task 6: Crear acciones seguras de explicación y aprobación

**Files:**
- Create: `reportes/views_inventory_traceability.py`
- Modify: `reportes/urls.py`
- Create: `reportes/tests_inventory_traceability_views.py`

- [ ] **Step 1: Escribir pruebas fallidas de acceso y transición de estados**

Cubrir:

- usuario de reportes puede consultar resumen y detalle;
- usuario con permiso de cambio puede explicar;
- explicación vacía o causa ausente conserva inputs y responde 400;
- aprobador requiere `reportes.approve_product_inventory_audit`;
- la misma persona no puede explicar y aprobar;
- rechazo vuelve a `NEEDS_EXPLANATION` sin borrar eventos;
- POST tradicional redirige al fragmento estable del caso;
- POST asíncrono responde JSON para el toast global y devuelve el fragmento actualizado.

- [ ] **Step 2: Confirmar 404 en las rutas nuevas**

Run:

```bash
python manage.py test reportes.tests_inventory_traceability_views -v 2
```

Expected: fallos por rutas inexistentes.

- [ ] **Step 3: Añadir rutas y vistas**

Rutas:

```python
path("auditoria-inventario/", views_inventory_traceability.dashboard, name="inventory_audit"),
path("auditoria-inventario/casos/<int:pk>/", views_inventory_traceability.case_detail, name="inventory_audit_case"),
path("auditoria-inventario/casos/<int:pk>/explicar/", views_inventory_traceability.explain_case, name="inventory_audit_explain"),
path("auditoria-inventario/casos/<int:pk>/aprobar/", views_inventory_traceability.approve_case, name="inventory_audit_approve"),
path("auditoria-inventario/casos/<int:pk>/rechazar/", views_inventory_traceability.reject_case, name="inventory_audit_reject"),
```

El dashboard solo lee casos materializados del mes solicitado. No debe invocar la sincronización Point ni reconstruir el mes durante un GET.

- [ ] **Step 4: Implementar transiciones bajo bloqueo de fila**

Las acciones usan `transaction.atomic()` y `select_for_update()`. La explicación cambia `NEEDS_EXPLANATION` a `PENDING_APPROVAL`. Aprobar exige un evento `EXPLAIN` vigente y un usuario distinto. Rechazar agrega evento y regresa a `NEEDS_EXPLANATION`.

La asignación operativa se deriva de `branch`/custodia. El cargo del actor original del movimiento se muestra como evidencia, pero no decide quién atiende el caso.

- [ ] **Step 5: Ejecutar pruebas de vistas**

Run:

```bash
python manage.py test reportes.tests_inventory_traceability_views -v 2
```

Expected: permisos, autoaprobación, rechazo y ambos formatos de respuesta en verde.

- [ ] **Step 6: Commit**

```bash
git add reportes/views_inventory_traceability.py reportes/urls.py reportes/tests_inventory_traceability_views.py
git commit -m "feat(reportes): agrega flujo de explicación y aprobación"
```

### Task 7: Construir la pantalla de excepciones y la trazabilidad legible

**Files:**
- Create: `reportes/templates/reportes/auditoria_inventario.html`
- Create: `reportes/templates/reportes/auditoria_inventario_caso.html`
- Modify: `reportes/templates/reportes/producido_vs_vendido.html`
- Modify: `static/css/styles.css`
- Modify: `reportes/tests_inventory_traceability_views.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Escribir pruebas de contenido y orden fallidas**

Verificar que la pantalla incluya:

- indicadores `Cuadran con Point`, `Pendientes` y `Pendientes de aprobación`;
- filtros de mes, ubicación y estado;
- excepciones antes que conciliados;
- columnas `Producto`, `Ubicación`, `Diferencia`, `Posible causa`, `Estado`, `Revisar`;
- estados separados de `Cierre Point`, `Movimientos` y `Conteo físico`;
- enlace `Auditar por sucursal` desde Producido vs Vendido;
- textos sin nombres internos de tablas, jobs o modelos.

- [ ] **Step 2: Ejecutar y confirmar fallos de contenido**

Run:

```bash
python manage.py test reportes.tests_inventory_traceability_views -v 2
```

Expected: plantillas inexistentes o textos ausentes.

- [ ] **Step 3: Implementar dashboard de carga rápida**

El GET inicial debe consultar solo agregados y filas del mes filtrado. La pestaña primaria contiene excepciones; `Conciliados` es secundaria. No representar el rastro completo en la tabla principal.

Mantener encabezado alineado y visible con una sola tabla semántica:

```css
.page-inventory-audit .audit-table-wrap {
  max-height: calc(100vh - 22rem);
  overflow: auto;
  overscroll-behavior: contain;
}

.page-inventory-audit .audit-table {
  width: 100%;
  min-width: 960px;
  table-layout: fixed;
  border-collapse: separate;
  border-spacing: 0;
}

.page-inventory-audit .audit-table thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: var(--rosa);
}
```

Definir anchos con `colgroup`; no construir encabezado y cuerpo como tablas separadas.

- [ ] **Step 4: Implementar detalle como secuencia de evidencia**

Mostrar en orden:

1. inventario inicial;
2. producción;
3. transferencias recibidas;
4. conversiones de entrada;
5. ventas;
6. mermas;
7. transferencias enviadas;
8. conversiones de salida;
9. ajustes identificados;
10. inventario esperado;
11. cierre Point;
12. diferencia.

Cada grupo carga únicamente los IDs guardados en `source_trace` y muestra fecha operativa, ubicación, cantidad, actor y referencia Point. Un ID faltante después de una recarga se presenta como “Evidencia ya no disponible en la fuente” y no se sustituye silenciosamente.

- [ ] **Step 5: Conectar formularios al contrato asíncrono existente**

Marcar explicar, aprobar y rechazar con `data-async-action`, bloquear solamente el botón presionado y mostrar `Procesando…`. Mantener `reason_code`, comentario y archivo si el servidor rechaza la solicitud. Añadir las acciones a `docs/ux/action-context-coverage.md`.

- [ ] **Step 6: Ejecutar pruebas de vistas y accesibilidad básica**

Run:

```bash
python manage.py test reportes.tests_inventory_traceability_views -v 2
```

Expected: contenido, orden, permisos y respuestas asíncronas en verde.

- [ ] **Step 7: Commit**

```bash
git add reportes/templates/reportes/auditoria_inventario.html reportes/templates/reportes/auditoria_inventario_caso.html reportes/templates/reportes/producido_vs_vendido.html static/css/styles.css reportes/tests_inventory_traceability_views.py docs/ux/action-context-coverage.md
git commit -m "feat(reportes): muestra auditoría de inventario por excepciones"
```

### Task 8: Validar el contrato completo y preparar agosto sin alterar Point

**Files:**
- Modify only if a defect is found: files introduced in Tasks 1–7

- [ ] **Step 1: Ejecutar checks y pruebas focalizadas**

Run:

```bash
python manage.py migrate --check
python manage.py check
python manage.py test \
  pos_bridge.tests.test_branch_inventory_traceability_service \
  reportes.tests_inventory_audit_models \
  reportes.tests_inventory_traceability_materializer \
  reportes.tests_inventory_traceability_views \
  -v 2
```

Expected: cero migraciones sin crear, `System check identified no issues`, todas las pruebas en verde.

- [ ] **Step 2: Revisar el diff y confirmar que no se mezclaron alcances**

Run:

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff --check origin/main..HEAD
git log --oneline --decorate origin/main..HEAD
```

Expected: solo servicio Point, modelos/migración/vistas/templates/CSS/tests/documentación de esta auditoría; árbol limpio.

- [ ] **Step 3: Crear PR borrador y esperar CI**

El PR debe incluir resumen funcional, migración, pruebas ejecutadas, que no hubo escrituras Point y que la conciliación física permanece fuera de alcance. No mergear con CI rojo.

- [ ] **Step 4: Hacer revisión operativa local de agosto en modo lectura**

Run:

```bash
python manage.py rebuild_product_inventory_audit --month 2026-08 --dry-run
```

Expected: conteos de líneas, excepciones y fuentes incompletas sin crear registros. Si las fuentes requeridas fallan, detener el cierre y conservar el diagnóstico exacto; no reconstruir con ceros.

- [ ] **Step 5: Validar la interfaz local en navegador**

Comprobar:

- carga inicial sin recorrer otros meses;
- encabezado fijo y alineado al desplazarse vertical y horizontalmente;
- filtros de agosto y ubicación;
- CEDIS y Devoluciones visibles cuando tengan inventario o movimientos;
- detalle de una cadena producción → transferencia → conversión → venta/merma;
- consola sin errores y solicitudes de red sin 500;
- formularios conservan contexto y muestran toast.

- [ ] **Step 6: Incorporar correcciones encontradas y repetir Steps 1–5**

No aceptar el PR por la sola existencia de datos o HTTP 200. La lectura debe ser operativamente comprensible y la evidencia debe abrir desde el caso.

### Task 9: Desplegar, materializar agosto y verificar producción

**Files:**
- No source changes expected after merge.

- [ ] **Step 1: Mergear el PR aprobado a `main`**

Confirmar que la rama solo contiene esta tarea y que CI terminó en verde.

- [ ] **Step 2: Desplegar por el flujo oficial**

En el VPS, sin ejecutar `git pull` manual antes:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Expected: migración aplicada, estáticos recolectados y procesos recargados/reiniciados según el script.

- [ ] **Step 3: Validar primero agosto en seco contra las fuentes productivas**

```bash
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web \
  python manage.py rebuild_product_inventory_audit --month 2026-08 --dry-run
```

Expected: ambos cierres requeridos disponibles y resumen explícito. Si falta una fuente, no ejecutar el paso de escritura.

- [ ] **Step 4: Materializar exclusivamente agosto de 2026**

```bash
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web \
  python manage.py rebuild_product_inventory_audit --month 2026-08
```

Expected: creación/actualización de la proyección de auditoría sin modificar tablas fuente Point ni el cierre protegido.

- [ ] **Step 5: Repetir la reconstrucción para probar idempotencia productiva**

Ejecutar el mismo comando otra vez.

Expected: `created=0`, `reopened=0`; cualquier diferencia exige investigar antes de presentar el mes como estable.

- [ ] **Step 6: Verificar el flujo autenticado real**

Abrir la auditoría de agosto en `https://erp.pollyanasdolce.com/reportes/auditoria-inventario/?month=2026-08` y comprobar:

- resumen, excepciones y conciliados;
- una sucursal, CEDIS y Devoluciones;
- una transferencia completa y una conversión con sus dos piernas;
- estados separados de cierre Point, movimientos y conteo físico;
- una explicación y aprobación con dos usuarios distintos en un caso de prueba autorizado;
- encabezado fijo/alineado, consola y red sin errores.

- [ ] **Step 7: Cerrar el ciclo de vida de la tarea**

Ejecutar la auditoría final del workspace, limpiar únicamente la rama/worktree corroborados mediante `scripts/task_workspace_close.sh --state merged`, podar referencias y documentar cualquier excepción todavía abierta en agosto.

## Criterio de terminación

La Etapa 1 termina solamente cuando agosto de 2026 se abre con rapidez, cada diferencia permanece separada por producto y ubicación, CEDIS y Devoluciones se comportan como inventarios, las transferencias/conversiones muestran ambas piernas o una excepción explícita, y una persona puede reconstruir la evidencia de un caso sin consultar la base de datos. La ausencia de conteos físicos se muestra como `Sin conteo manual`; no impide ni simula la conciliación de movimientos.
