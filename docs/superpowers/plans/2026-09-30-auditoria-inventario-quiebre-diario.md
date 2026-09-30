# Auditoría diaria de inventario Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Identificar para todos los expedientes de un mes el primer intervalo comprobado donde el saldo deja de coincidir con un snapshot Point, sin descargar ni duplicar información.

**Architecture:** Crear un proyector mensual sin persistencia propia que cargue en lote los snapshots y las filas Point ya referidas por `source_trace`. El agente auditor incorporará el resultado en `investigation_summary`; el detalle del expediente presentará una ventana compacta. Las fuentes sin hora producirán un rango conservador, no un orden inventado.

**Tech Stack:** Django 5, PostgreSQL 16, `Decimal`, `zoneinfo`, plantillas Django y `TestCase`.

---

## Mapa de archivos

- Crear `pos_bridge/services/daily_inventory_break_service.py`: tipos inmutables, carga mensual en lote y cálculo conservador de checkpoints.
- Crear `pos_bridge/tests/test_daily_inventory_break_service.py`: contrato del proyector, zona horaria, rangos intradía, retornos y consultas acotadas.
- Modificar `reportes/services_inventory_audit_agent.py`: preparar una sola proyección mensual, incorporarla a investigación y huella.
- Modificar `reportes/tests_inventory_audit_agent.py`: persistencia, idempotencia y ausencia de consultas por expediente.
- Modificar `reportes/templates/reportes/auditoria_inventario_caso.html`: bloque compacto del primer quiebre.
- Modificar `reportes/tests_inventory_traceability_views.py`: lenguaje operativo y estados de evidencia.
- No modificar modelos, migraciones, jobs de sincronización ni clientes Point.

### Task 1: Proyector puro de checkpoints conservadores

**Files:**
- Create: `pos_bridge/services/daily_inventory_break_service.py`
- Create: `pos_bridge/tests/test_daily_inventory_break_service.py`

- [ ] **Step 1: Escribir pruebas fallidas del cálculo puro**

Definir fixtures mínimos sin ORM para probar: corte exacto, primer snapshot ya diferente, rango compatible por movimientos sin hora, movimiento posterior excluido y falta de snapshots.

```python
class DailyInventoryBreakProjectionTests(SimpleTestCase):
    @staticmethod
    def aware(year, month, day, hour):
        return datetime(year, month, day, hour, tzinfo=ZoneInfo("America/Mazatlan"))

    def test_finds_first_mismatch_after_exact_checkpoint(self):
        result = project_checkpoints(
            opening=Decimal("10"),
            movements=(
                DailyMovement("production", date(2026, 8, 1), Decimal("2"), (1,)),
                DailyMovement("sales", date(2026, 8, 2), Decimal("-3"), (2,)),
            ),
            checkpoints=(
                StockCheckpoint(self.aware(2026, 8, 1, 23), Decimal("12")),
                StockCheckpoint(self.aware(2026, 8, 2, 23), Decimal("8")),
            ),
        )
        self.assertEqual(result.status, DailyBreakStatus.FOUND)
        self.assertEqual(result.last_matching_checkpoint.observed, Decimal("12"))
        self.assertEqual(result.first_mismatch_checkpoint.difference, Decimal("-1"))

    def test_same_day_date_only_rows_form_compatible_range(self):
        result = project_checkpoints(
            opening=Decimal("10"),
            movements=(
                DailyMovement("production", date(2026, 8, 1), Decimal("5"), (1,)),
                DailyMovement("sales", date(2026, 8, 1), Decimal("-4"), (2,)),
            ),
            checkpoints=(StockCheckpoint(self.aware(2026, 8, 1, 18), Decimal("8")),),
        )
        self.assertEqual(result.status, DailyBreakStatus.INCONCLUSIVE)
        self.assertEqual((result.minimum, result.maximum), (Decimal("6"), Decimal("15")))

    def test_without_snapshots_is_insufficient_evidence(self):
        result = project_checkpoints(opening=Decimal("10"), movements=(), checkpoints=())
        self.assertEqual(result.status, DailyBreakStatus.INSUFFICIENT_EVIDENCE)
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar que fallan por importación**

Run:

```bash
python manage.py test pos_bridge.tests.test_daily_inventory_break_service.DailyInventoryBreakProjectionTests
```

Expected: `ImportError` para `daily_inventory_break_service`.

- [ ] **Step 3: Implementar los tipos y la función pura mínima**

Crear tipos congelados y aplicar movimientos con timestamp exacto antes del corte; para filas con solo fecha del mismo día calcular el rango `saldo - salidas` a `saldo + entradas`.

```python
class DailyBreakStatus(StrEnum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    INCONCLUSIVE = "INCONCLUSIVE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"

@dataclass(frozen=True)
class DailyMovement:
    source: str
    occurred_at: date | datetime
    impact: Decimal
    source_ids: tuple[int, ...]

@dataclass(frozen=True)
class StockCheckpoint:
    captured_at: datetime
    observed: Decimal

@dataclass(frozen=True)
class EvaluatedCheckpoint:
    captured_at: datetime
    observed: Decimal
    minimum: Decimal
    maximum: Decimal
    difference: Decimal | None

@dataclass(frozen=True)
class DailyBreakProjection:
    status: DailyBreakStatus
    last_matching_checkpoint: EvaluatedCheckpoint | None
    first_mismatch_checkpoint: EvaluatedCheckpoint | None
    minimum: Decimal | None
    maximum: Decimal | None
    movement_ids_by_source: dict[str, tuple[int, ...]]
    warnings: tuple[str, ...]

    def as_dict(self) -> dict:
        return {
            "status": self.status.value,
            "last_matching_checkpoint": serialize_checkpoint(self.last_matching_checkpoint),
            "first_mismatch_checkpoint": serialize_checkpoint(self.first_mismatch_checkpoint),
            "minimum": decimal_text(self.minimum),
            "maximum": decimal_text(self.maximum),
            "movement_ids_by_source": {
                key: list(value) for key, value in sorted(self.movement_ids_by_source.items())
            },
            "warnings": list(self.warnings),
        }

def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")

def serialize_checkpoint(checkpoint: EvaluatedCheckpoint | None) -> dict | None:
    if checkpoint is None:
        return None
    return {
        "captured_at": checkpoint.captured_at.isoformat(),
        "observed": decimal_text(checkpoint.observed),
        "minimum": decimal_text(checkpoint.minimum),
        "maximum": decimal_text(checkpoint.maximum),
        "difference": decimal_text(checkpoint.difference),
    }
```

`project_checkpoints()` debe ordenar copias locales de entradas, mantener un cursor de movimientos ya completos y devolver el primer observado fuera del rango posible. No mutar argumentos ni consultar ORM.

- [ ] **Step 4: Ejecutar pruebas puras**

Run: `python manage.py test pos_bridge.tests.test_daily_inventory_break_service.DailyInventoryBreakProjectionTests`

Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/daily_inventory_break_service.py pos_bridge/tests/test_daily_inventory_break_service.py
git commit -m "feat(pos_bridge): calcula quiebre diario conservador"
```

### Task 2: Carga mensual en lote desde fuentes existentes

**Files:**
- Modify: `pos_bridge/services/daily_inventory_break_service.py`
- Modify: `pos_bridge/tests/test_daily_inventory_break_service.py`

- [ ] **Step 1: Escribir pruebas fallidas de integración ORM**

Crear un caso con alias Point de la misma sucursal ERP y filas referidas en `source_trace`. Probar que:

```python
service = DailyInventoryBreakService()
results = service.build_month(date(2026, 8, 1), [case])
self.assertIn(case.pk, results)
self.assertEqual(results[case.pk].status, DailyBreakStatus.FOUND)
self.assertEqual(
    results[case.pk].movement_ids_by_source,
    {"production": (production.pk,), "transfer_in": (transfer.pk,)},
)
```

Añadir casos específicos:

```python
def test_partial_transfer_return_is_an_origin_entry(self):
    # enviado 2, recibido 1, finalizada: impacto de retorno +1 en origen
    self.assertEqual(result.first_mismatch_checkpoint.minimum, Decimal("11"))

def test_conversion_out_uses_persisted_trace_impact(self):
    case.source_trace["conversion_out_impacts"] = {str(conversion.pk): "0.8333"}
    self.assertEqual(result.first_mismatch_checkpoint.minimum, Decimal("9.1667"))

def test_month_loader_does_not_query_per_case(self):
    with self.assertNumQueries(7):
        DailyInventoryBreakService().build_month(self.month, cases)
```

Las siete consultas corresponden a sucursales Point relacionadas por `erp_branch_id`, ventas, producción, mermas, transferencias, conversiones y snapshots; el conteo debe permanecer igual al duplicar casos.

- [ ] **Step 2: Ejecutar pruebas ORM y confirmar fallos funcionales**

Run: `python manage.py test pos_bridge.tests.test_daily_inventory_break_service.DailyInventoryBreakServiceTests`

Expected: fallos porque `DailyInventoryBreakService` y sus adaptadores aún no existen.

- [ ] **Step 3: Implementar `build_month()` con consultas por fuente**

El servicio debe:

```python
def build_month(self, month: date, cases: Sequence[ProductInventoryAuditCase]):
    month = month.replace(day=1)
    ids = collect_trace_ids(cases)
    rows = {
        "sales": PointDailySale.objects.filter(id__in=ids["sales"]),
        "production": PointProductionLine.objects.filter(id__in=ids["production"]),
        "waste": PointWasteLine.objects.filter(id__in=ids["waste"]),
        "transfers": PointTransferLine.objects.filter(id__in=ids["transfers"]),
        "conversions": PointConversionLine.objects.filter(id__in=ids["conversions"]),
    }
    snapshots = load_last_local_checkpoint_per_day(month, cases)
    return {case.id: self._project_case(case, rows, snapshots) for case in cases}
```

Reglas obligatorias:

- Resolver sucursales por `erp_branch_id`; solo usar `branch_id` cuando no exista vínculo ERP.
- Usar `timezone.localtime(..., ZoneInfo("America/Mazatlan"))` para timestamps.
- Ventas y producción conservan precisión de fecha; mermas y conversiones usan timestamp.
- Transferencia de salida usa `sent_at`; recepción usa `received_at`; retorno parcial finalizado usa `sent - received` en el origen y `received_at`.
- Conversión de salida usa `source_trace.conversion_out_impacts`; no derivar origen faltante.
- Reducir snapshots al último `captured_at` por fecha local, clave canónica y producto.
- Si un ID conservado ya no existe, agregar advertencia y mantener el caso auditable.

- [ ] **Step 4: Ejecutar pruebas del servicio y revisar consultas**

Run: `python manage.py test pos_bridge.tests.test_daily_inventory_break_service`

Expected: `OK`; el límite de consultas no crece al duplicar expedientes.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/daily_inventory_break_service.py pos_bridge/tests/test_daily_inventory_break_service.py
git commit -m "feat(pos_bridge): proyecta cortes diarios desde fuentes Point"
```

### Task 3: Incorporar el quiebre en el agente auditor

**Files:**
- Modify: `reportes/services_inventory_audit_agent.py`
- Modify: `reportes/tests_inventory_audit_agent.py`

- [ ] **Step 1: Escribir pruebas fallidas del agente**

```python
@patch("reportes.services_inventory_audit_agent.DailyInventoryBreakService")
def test_run_month_builds_daily_projection_once(self, service_cls):
    service_cls.return_value.build_month.return_value = {
        case.id: DailyBreakProjection(
            status=DailyBreakStatus.FOUND,
            last_matching_checkpoint=None,
            first_mismatch_checkpoint=None,
            minimum=Decimal("8"),
            maximum=Decimal("8"),
            movement_ids_by_source={},
            warnings=(),
        )
    }
    InventoryAuditAgent().run_month(self.month)
    service_cls.return_value.build_month.assert_called_once()
    case.refresh_from_db()
    self.assertEqual(case.investigation_summary["daily_break"]["status"], "FOUND")

def test_same_daily_projection_keeps_second_run_idempotent(self):
    first = InventoryAuditAgent().run_month(self.month)
    second = InventoryAuditAgent().run_month(self.month)
    self.assertGreater(first["updated"], 0)
    self.assertEqual(second["updated"], 0)
    self.assertEqual(second["notifications"], 0)
```

Añadir una prueba donde el proyector falla: la investigación mensual se conserva y `daily_break.status` queda `INSUFFICIENT_EVIDENCE` con advertencia, sin abortar todos los casos.

- [ ] **Step 2: Ejecutar pruebas y confirmar fallos**

Run: `python manage.py test reportes.tests_inventory_audit_agent.InventoryAuditAgentServiceTests`

Expected: falta la integración `DailyInventoryBreakService`.

- [ ] **Step 3: Preparar y serializar una sola proyección mensual**

En `__init__` agregar `_daily_break_cache`. En `_prepare_month_context` cargar los casos completos una vez y ejecutar `build_month`. `investigate_case` debe añadir:

```python
daily_break = self._daily_break_cache.get(
    case.id,
    DailyBreakProjection(
        status=DailyBreakStatus.INSUFFICIENT_EVIDENCE,
        last_matching_checkpoint=None,
        first_mismatch_checkpoint=None,
        minimum=None,
        maximum=None,
        movement_ids_by_source={},
        warnings=("No se encontró un corte intermedio para este producto y ubicación.",),
    ),
)
summary = {
    "facts": facts,
    "hypotheses": hypotheses,
    "missing": missing,
    "daily_break": self._daily_break_summary(case, daily_break),
    "related_logistics_discrepancy_ids": [...],
    "recurrence_count": recurrence_count,
    "grouping_key": self._grouping_key(case, issue_codes),
}
```

Agregar un único serializador de presentación en el agente para que la plantilla no conozca claves Point:

```python
SOURCE_LABELS = {
    "sales": "ventas",
    "production": "producciones",
    "waste": "mermas",
    "transfer_in": "entradas por transferencia",
    "transfer_out": "salidas por transferencia",
    "transfer_return": "retornos al origen",
    "conversion_in": "entradas por conversión",
    "conversion_out": "salidas por conversión",
}

def _daily_break_summary(self, case, projection):
    data = projection.as_dict()
    data.update({
        "last_matching_label": self._checkpoint_label(projection.last_matching_checkpoint),
        "first_mismatch_label": self._checkpoint_label(projection.first_mismatch_checkpoint),
        "unlocated_quantity": format(abs(case.difference), "f"),
        "movement_labels": [
            f"{len(ids)} movimiento(s) de {SOURCE_LABELS[source]}"
            for source, ids in sorted(projection.movement_ids_by_source.items())
            if ids
        ],
    })
    return data
```

`_checkpoint_label()` debe convertir `captured_at` a `America/Mazatlan` y formatear `dd/mm/AAAA HH:MM`; si no hay corte devuelve `"Sin corte anterior comprobado"`.

Si `FOUND`, añadir a hechos el primer corte y su diferencia; si `INCONCLUSIVE` o `INSUFFICIENT_EVIDENCE`, añadir a `missing` una frase operativa. Incluir `daily_break` en la huella existente mediante `summary`; no crear otra huella ni otra notificación.

- [ ] **Step 4: Ejecutar pruebas del agente**

Run: `python manage.py test reportes.tests_inventory_audit_agent`

Expected: `OK`, segunda corrida `updated=0` y `notifications=0`.

- [ ] **Step 5: Commit**

```bash
git add reportes/services_inventory_audit_agent.py reportes/tests_inventory_audit_agent.py
git commit -m "feat(reportes): investiga primer quiebre diario"
```

### Task 4: Mostrar la ventana en el expediente existente

**Files:**
- Modify: `reportes/templates/reportes/auditoria_inventario_caso.html`
- Modify: `reportes/tests_inventory_traceability_views.py`

- [ ] **Step 1: Escribir pruebas fallidas de presentación**

```python
def test_case_detail_shows_first_observed_break_without_internal_tokens(self):
    self.case.investigation_summary = {
        "facts": [], "hypotheses": [], "missing": [],
        "daily_break": {
            "status": "FOUND",
            "last_matching_checkpoint": "2026-08-10T22:31:00-07:00",
            "first_mismatch_checkpoint": "2026-08-11T22:31:00-07:00",
            "observed_stock": "8",
            "minimum": "10",
            "maximum": "10",
            "difference": "-2",
            "movement_counts": {"production": 2, "transfer_out": 3},
            "unlocated_quantity": "2",
            "warnings": [],
        },
    }
    self.case.save(update_fields=["investigation_summary"])
    response = self.client.get(self.detail_url, HTTP_ACCEPT="text/html")
    self.assertContains(response, "Primer corte con diferencia")
    self.assertContains(response, "11/08/2026")
    self.assertContains(response, "2 unidades no localizadas")
    self.assertNotContains(response, "FOUND")
    self.assertNotContains(response, "transfer_out")
```

Agregar pruebas para `INCONCLUSIVE` y `INSUFFICIENT_EVIDENCE` con lenguaje claro.

- [ ] **Step 2: Ejecutar pruebas de vista y confirmar fallos**

Run: `python manage.py test reportes.tests_inventory_traceability_views.InventoryTraceabilityViewsTests.test_case_detail_shows_first_observed_break_without_internal_tokens`

Expected: el bloque aún no existe.

- [ ] **Step 3: Añadir bloque semántico compacto**

Insertar después de `Investigación del agente auditor` y antes de `Secuencia del saldo`:

```django
{% with daily=case.investigation_summary.daily_break %}
{% if daily %}
<section class="inventory-audit-daily-break" aria-labelledby="daily-break-title">
  <h2 id="daily-break-title">Primer intervalo a revisar</h2>
  {% if daily.status == "FOUND" %}
    <p>El último corte compatible fue {{ daily.last_matching_label }}. El primer corte con diferencia fue {{ daily.first_mismatch_label }}.</p>
    <p><strong>{{ daily.unlocated_quantity }}</strong> unidades continúan sin localizar.</p>
    <ul>{% for item in daily.movement_labels %}<li>{{ item }}</li>{% endfor %}</ul>
  {% elif daily.status == "INCONCLUSIVE" %}
    <p>Los cortes observados son compatibles, pero Point no informa el orden de todos los movimientos del día.</p>
  {% else %}
    <p>No existe un corte intermedio suficiente para localizar el inicio de la diferencia.</p>
  {% endif %}
</section>
{% endif %}
{% endwith %}
```

Preparar etiquetas humanas en el servicio/agente; la plantilla no traduce claves técnicas. Reutilizar estilos existentes de tarjetas y listas; no añadir CSS si la composición ya queda legible.

- [ ] **Step 4: Ejecutar pruebas de vistas y accesibilidad básica**

Run: `python manage.py test reportes.tests_inventory_traceability_views`

Expected: `OK`; un solo `<main>`, encabezados asociados y ningún token interno visible.

- [ ] **Step 5: Commit**

```bash
git add reportes/templates/reportes/auditoria_inventario_caso.html reportes/tests_inventory_traceability_views.py
git commit -m "feat(reportes): muestra intervalo diario de inventario"
```

### Task 5: Verificación integral, producción y cierre

**Files:**
- Modify only if a failing check identifies a defect directly related to this feature.

- [ ] **Step 1: Levantar PostgreSQL aislado y aplicar migraciones existentes**

```bash
export COMPOSE_PROJECT_NAME=erp_auditoria_quiebre_diario
export DB_HOST_PORT=55493
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:${DB_HOST_PORT}/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
python manage.py migrate
python manage.py migrate --check
```

Expected: PostgreSQL acepta conexiones y no quedan migraciones pendientes.

- [ ] **Step 2: Ejecutar pruebas enfocadas y checks**

```bash
python manage.py test \
  pos_bridge.tests.test_daily_inventory_break_service \
  reportes.tests_inventory_audit_agent \
  reportes.tests_inventory_traceability_views
python manage.py check
python manage.py migrate --check
```

Expected: todas las pruebas pasan, `check` reporta 0 errores y no hay migraciones pendientes.

- [ ] **Step 3: Revisar diff, confirmar que no hay migración ni segunda fuente y hacer commit de correcciones finales si existen**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff --check
git log --oneline --decorate -8
```

Expected: solo documentos, servicio, pruebas, agente y plantilla del alcance.

- [ ] **Step 4: PR, CI, merge y despliegue oficial**

Crear PR en borrador, revisar el diff completo, esperar CI verde, convertir/mergear y ejecutar únicamente:

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

No ejecutar `git pull` manual en el VPS.

- [ ] **Step 5: Reconstruir e investigar agosto de forma controlada**

Ejecutar primero `--dry-run`, luego la corrida real y repetirla para comprobar idempotencia:

```bash
docker compose exec -T web python manage.py investigate_inventory_audit_cases --month 2026-08 --dry-run
docker compose exec -T web python manage.py investigate_inventory_audit_cases --month 2026-08
docker compose exec -T web python manage.py investigate_inventory_audit_cases --month 2026-08
```

Expected: la segunda corrida real devuelve `updated=0`, `notifications=0`; no aumenta el número de expedientes.

- [ ] **Step 6: Validar producción autenticada**

Abrir el caso 79 y comprobar:

- saldo mensual esperado 16, cierre Point 6 y diferencia -10;
- bloque del primer intervalo con estado comprobable o evidencia insuficiente real;
- movimientos agrupados sin nombres técnicos;
- consola sin errores;
- ningún cambio en Point, ventas, mermas o inventario operativo.

- [ ] **Step 7: Cerrar el worktree por el flujo oficial**

```bash
bash scripts/task_workspace_close.sh \
  --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --task auditoria_inventario_quiebre_diario \
  --state merged
```

Expected: rama local/remota eliminada, worktree retirado y raíz limpia/sincronizada.
