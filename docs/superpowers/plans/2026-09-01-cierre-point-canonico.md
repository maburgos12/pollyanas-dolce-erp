# Cierre Point Canónico Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar el balance mensual real de Point por producto, usando conversiones registradas y sin presentar el saldo Point como inventario físico.

**Architecture:** Un nuevo servicio de dominio en `pos_bridge` construirá una proyección mensual inmutable por receta Point. La vista operativa y el constructor del cierre consumirán esa misma proyección; la vista no volverá a inferir conversiones desde ventas o mermas. Los datos adicionales se persistirán en `ProductoMonthClosureLine.metadata`, sin migración.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, templates Django, CSV/XLSX/PDF existentes.

---

## Mapa de archivos

- Crear `pos_bridge/services/monthly_product_balance_service.py`: única fórmula y procedencia de movimientos Point.
- Crear `pos_bridge/tests/test_monthly_product_balance_service.py`: pruebas unitarias/integración del balance y conversiones.
- Modificar `pos_bridge/services/product_month_closure_service.py`: consumir la proyección y persistir metadatos de conversión.
- Modificar `pos_bridge/tests/test_product_month_closure_service.py`: probar paridad cierre/proyección y compatibilidad histórica.
- Modificar `reportes/views_produccion.py`: consumir el balance canónico y retirar la conversión estimada.
- Modificar `reportes/tests_producido_vs_vendido.py`: probar datos, signo, estados y ausencia de estimaciones.
- Modificar `reportes/templates/reportes/producido_vs_vendido.html`: lenguaje Point y explicación de fórmula.
- Modificar exportadores relacionados en `reportes/views_produccion.py` y `reportes/views.py`: encabezados y estados sin “físico”.

### Task 1: Preparar y comprobar PostgreSQL aislado

**Files:** ninguno.

- [ ] **Step 1: Levantar la base aislada**

```bash
export COMPOSE_PROJECT_NAME=erp_reportes_cierre_point_canonico
export DB_HOST_PORT=55481
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:55481/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
```

Expected: `accepting connections`.

- [ ] **Step 2: Aplicar la base de main y verificarla**

```bash
python manage.py migrate
python manage.py migrate --check
python manage.py check
```

Expected: cero migraciones pendientes y `System check identified no issues`.

- [ ] **Step 3: Ejecutar la línea base del módulo**

```bash
python manage.py test \
  pos_bridge.tests.test_product_month_closure_service \
  reportes.tests_producido_vs_vendido --keepdb
```

Expected: PASS antes de editar código.

### Task 2: Construir conversiones reales de Point

**Files:**
- Create: `pos_bridge/services/monthly_product_balance_service.py`
- Create: `pos_bridge/tests/test_monthly_product_balance_service.py`

- [ ] **Step 1: Escribir pruebas fallidas de conversión**

Crear una clase `MonthlyProductBalanceConversionTests` con tres escenarios:

```python
def test_point_conversion_creates_real_destination_entry_and_parent_exit(self):
    self.create_conversion(destination=self.slice, quantity="16")
    result = MonthlyPointProductBalanceService().build(month="2026-08")
    self.assertEqual(result.rows[self.slice.id].conversion_in, Decimal("16"))
    self.assertEqual(result.rows[self.parent.id].conversion_out, Decimal("2"))
    self.assertEqual(result.rows[self.slice.id].conversion_origin, "EQUIVALENCIA_CONFIGURADA")

def test_slice_sales_do_not_invent_conversion(self):
    self.create_sale(recipe=self.slice, quantity="16")
    result = MonthlyPointProductBalanceService().build(month="2026-08")
    self.assertEqual(result.rows[self.slice.id].conversion_in, Decimal("0"))
    self.assertEqual(result.rows[self.parent.id].conversion_out, Decimal("0"))

def test_conversion_without_origin_or_equivalence_is_visible_issue(self):
    self.equivalence.delete()
    self.create_conversion(destination=self.slice, quantity="16")
    result = MonthlyPointProductBalanceService().build(month="2026-08")
    row = result.rows[self.slice.id]
    self.assertEqual(row.conversion_in, Decimal("16"))
    self.assertIn("CONVERSION_ORIGIN_UNRESOLVED", row.issues)
```

El fixture debe crear `Sucursal`, `PointBranch`, recetas padre/porción, `RecetaEquivalencia` con factor 8, `PointSyncJob` y `PointConversionLine` fechado en agosto.

- [ ] **Step 2: Ejecutar y confirmar el fallo**

```bash
python manage.py test pos_bridge.tests.test_monthly_product_balance_service.MonthlyProductBalanceConversionTests --keepdb
```

Expected: FAIL porque `monthly_product_balance_service` todavía no existe.

- [ ] **Step 3: Implementar tipos y carga de conversiones**

Definir contratos inmutables:

```python
@dataclass(frozen=True)
class MonthlyPointBalanceRow:
    receta_id: int
    opening_point: Decimal | None = None
    production: Decimal = ZERO
    sales: Decimal = ZERO
    waste: Decimal = ZERO
    conversion_in: Decimal = ZERO
    conversion_out: Decimal = ZERO
    calculated_closing: Decimal | None = None
    closing_point: Decimal | None = None
    difference_point: Decimal | None = None
    status: str = "REVISAR_FUENTE"
    conversion_origin: str = ""
    issues: tuple[str, ...] = ()
    source_counts: dict[str, int] = field(default_factory=dict)

@dataclass(frozen=True)
class MonthlyPointBalance:
    month_start: date
    month_end: date
    rows: dict[int, MonthlyPointBalanceRow]
    sources: dict[str, str]
    warnings: tuple[str, ...]
```

Agregar `_load_conversions()` que consulte `PointConversionLine` dentro del mes. La entrada usa `quantity`. El origen usa primero `source_item_code`/`source_item_name` homologado y, si están vacíos, `RecetaEquivalencia` activa; la salida será `quantity / factor_conversion`. Una relación ausente añade `CONVERSION_ORIGIN_UNRESOLVED` y no crea salida.

- [ ] **Step 4: Ejecutar las pruebas de conversión**

```bash
python manage.py test pos_bridge.tests.test_monthly_product_balance_service.MonthlyProductBalanceConversionTests --keepdb
```

Expected: PASS.

- [ ] **Step 5: Commit quirúrgico**

```bash
git add pos_bridge/services/monthly_product_balance_service.py pos_bridge/tests/test_monthly_product_balance_service.py
git commit -m "feat(point): proyecta conversiones mensuales reales"
```

### Task 3: Completar snapshots, movimientos y fórmula canónica

**Files:**
- Modify: `pos_bridge/services/monthly_product_balance_service.py`
- Modify: `pos_bridge/tests/test_monthly_product_balance_service.py`

- [ ] **Step 1: Escribir pruebas fallidas del balance**

```python
def test_balance_uses_point_skus_without_rolling_slice_into_parent(self):
    self.create_snapshot(self.parent_product, "10", "2026-07-31T23:00:00")
    self.create_snapshot(self.slice_product, "4", "2026-07-31T23:00:00")
    self.create_snapshot(self.parent_product, "7", "2026-08-31T23:00:00")
    self.create_snapshot(self.slice_product, "12", "2026-08-31T23:00:00")
    result = MonthlyPointProductBalanceService().build(month="2026-08")
    self.assertEqual(result.rows[self.parent.id].opening_point, Decimal("10"))
    self.assertEqual(result.rows[self.slice.id].opening_point, Decimal("4"))

def test_balance_formula_and_difference_sign(self):
    self.seed_complete_balance(opening="10", production="5", conversion_in="0", sales="3", waste="1", conversion_out="2", closing="11")
    row = MonthlyPointProductBalanceService().build(month="2026-08").rows[self.parent.id]
    self.assertEqual(row.calculated_closing, Decimal("9"))
    self.assertEqual(row.difference_point, Decimal("2"))
    self.assertEqual(row.status, "POINT_MAYOR")
```

- [ ] **Step 2: Confirmar que fallan**

```bash
python manage.py test pos_bridge.tests.test_monthly_product_balance_service --keepdb
```

Expected: FAIL en snapshots/fórmula aún no implementados.

- [ ] **Step 3: Implementar loaders y cálculo**

Mover al servicio, conservando sus prioridades y metadatos, los loaders actualmente privados de `ProductMonthClosureService`: ventas oficiales/fallback, producción, merma y selección de snapshots. El cierre dejará delegadores temporales solo donde una prueba histórica los invoque directamente. Agregar:

- `_latest_snapshot_rows(snapshot_date)` con la misma tolerancia y selección por `branch_id, product_id` del cierre actual;
- `_load_sales()` con la misma prioridad de reporte mensual oficial, `PointDailySale` oficial, facts y bridge que usa hoy el cierre;
- `_load_production()` con prioridad `FactProduccionDiaria`, respaldo `PointProductionLine`;
- `_load_waste()` con prioridad `FactProduccionDiaria`, respaldo Point/merma mensual existente;
- homologación por `PointRecipeIdentityService` sin colapsar recetas derivadas;
- `_status(difference, issues)` con `COINCIDE`, `POINT_MAYOR`, `POINT_MENOR`, `REVISAR_FUENTE`.

Aplicar exactamente:

```python
calculated = opening + production + conversion_in - sales - waste - conversion_out
difference = closing_point - calculated
```

Si falta cualquiera de los snapshots, conservar `None`, emitir advertencia y no publicar un estado de cuadre.

- [ ] **Step 4: Ejecutar pruebas**

```bash
python manage.py test pos_bridge.tests.test_monthly_product_balance_service --keepdb
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/monthly_product_balance_service.py pos_bridge/tests/test_monthly_product_balance_service.py
git commit -m "feat(point): calcula balance mensual por producto"
```

### Task 4: Hacer que el cierre consuma la proyección

**Files:**
- Modify: `pos_bridge/services/product_month_closure_service.py`
- Modify: `pos_bridge/tests/test_product_month_closure_service.py`

- [ ] **Step 1: Escribir prueba fallida de paridad**

```python
def test_preview_uses_monthly_point_balance_and_persists_conversion_metadata(self):
    balance = self.seed_point_balance_with_conversion()
    closure = self.service.build(month="2026-08")
    line = closure.lines.get(receta_padre=self.parent)
    self.assertEqual(line.inventario_final_teorico, balance.rows[self.parent.id].calculated_closing)
    self.assertEqual(line.metadata["point_conversion_out"], "2")
    self.assertEqual(line.metadata["balance_contract"], "POINT_PRODUCT_BALANCE_V1")
```

- [ ] **Step 2: Confirmar el fallo**

```bash
python manage.py test pos_bridge.tests.test_product_month_closure_service.ProductMonthClosureServiceTests.test_preview_uses_monthly_point_balance_and_persists_conversion_metadata --keepdb
```

Expected: FAIL por metadatos ausentes.

- [ ] **Step 3: Integrar sin migración**

Inyectar `MonthlyPointProductBalanceService` en `ProductMonthClosureService`. `preview()` obtendrá su conjunto de movimientos de esa proyección y creará la proyección histórica por receta padre con `resolve_closure_recipe_quantity`: apertura, producción, venta, merma y saldos se convierten a unidades equivalentes del padre; conversión de entrada y salida se agregan con el mismo factor y se cancelan dentro del grupo cuando corresponda. Así los campos existentes conservan su significado y el reporte operativo sigue teniendo renglones Point sin consolidar. Persistir en `metadata`:

```python
{
    "balance_contract": "POINT_PRODUCT_BALANCE_V1",
    "point_conversion_in": str(row.conversion_in),
    "point_conversion_out": str(row.conversion_out),
    "conversion_origin": row.conversion_origin,
    "source_counts": row.source_counts,
    "issues": list(row.issues),
    "point_difference": str(row.difference_point or ""),
    "point_status": row.status,
}
```

Conservar `diferencia_teorico_vs_point` con su signo histórico (`calculado - Point`) para no romper consumidores; el signo nuevo (`Point - calculado`) vive en `metadata.point_difference` y en la proyección operativa. Conservar lectura de cierres históricos cuyo `metadata` no tenga estas claves. No ejecutar rebuild.

- [ ] **Step 4: Ejecutar regresiones del cierre**

```bash
python manage.py test pos_bridge.tests.test_product_month_closure_service --keepdb
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pos_bridge/services/product_month_closure_service.py pos_bridge/tests/test_product_month_closure_service.py
git commit -m "refactor(point): unifica proyeccion y cierre mensual"
```

### Task 5: Corregir el reporte y su lenguaje

**Files:**
- Modify: `reportes/views_produccion.py`
- Modify: `reportes/templates/reportes/producido_vs_vendido.html`
- Modify: `reportes/tests_producido_vs_vendido.py`

- [ ] **Step 1: Sustituir las pruebas de conversión estimada por pruebas Point**

```python
def test_report_does_not_infer_conversion_from_slice_sales(self):
    self.seed_slice_sale(quantity="16")
    response = self.client.get(self.url, {"periodo": "2026-08"})
    row = next(item for item in response.context["json_rows"] if item["receta_id"] == self.slice.id)
    self.assertEqual(row["conversion_entrada"], "0")

def test_report_uses_point_balance_status_and_sign(self):
    self.seed_balance(calculated="9", closing_point="11")
    response = self.client.get(self.url, {"periodo": "2026-08"})
    row = next(item for item in response.context["json_rows"] if item["receta_id"] == self.parent.id)
    self.assertEqual(row["diferencia_inventario"], "2")
    self.assertEqual(row["estado_inventario"], "Point mayor")

def test_report_does_not_describe_point_as_physical_inventory(self):
    response = self.client.get(self.url, {"periodo": "2026-08"})
    self.assertContains(response, "Fin. Point")
    self.assertContains(response, "Saldo calc.")
    self.assertNotContains(response, "Sobrante físico")
    self.assertNotContains(response, ">Físico<")
```

- [ ] **Step 2: Ejecutar y confirmar fallos**

```bash
python manage.py test reportes.tests_producido_vs_vendido --keepdb
```

Expected: FAIL porque la vista aún estima conversiones y usa lenguaje físico.

- [ ] **Step 3: Consumir la proyección canónica**

En `_build_context`, sustituir `_conversion_map()` y `_closure_map()` por una sola llamada:

```python
balance = MonthlyPointProductBalanceService().build(month=period.value)
```

Copiar a cada renglón los campos ya calculados. Eliminar `_conversion_map`, `_theoretical_inventory` y `_inventory_status` cuando el grafo confirme que no tienen otros consumidores.

- [ ] **Step 4: Actualizar la tabla**

Usar encabezados `Ini. Point`, `Saldo calc.`, `Fin. Point`, `Dif. Point`. Agregar el texto:

```text
Saldo calculado = inicial Point + producción + conversión de entrada − venta − merma − conversión de salida. Dif. Point = final Point − saldo calculado.
```

Los badges serán `Coincide`, `Point mayor`, `Point menor`, `Revisar fuente` y `Referencia`. Mantener iconografía, espaciado y colores existentes, sin depender solo del color.

- [ ] **Step 5: Ejecutar pruebas**

```bash
python manage.py test reportes.tests_producido_vs_vendido --keepdb
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add reportes/views_produccion.py reportes/templates/reportes/producido_vs_vendido.html reportes/tests_producido_vs_vendido.py
git commit -m "fix(reportes): muestra balance real de Point"
```

### Task 6: Alinear PDF, CSV y XLSX

**Files:**
- Modify: `reportes/views_produccion.py`
- Modify: `reportes/views.py`
- Modify: `reportes/tests_producido_vs_vendido.py`
- Modify: `reportes/tests.py`

- [ ] **Step 1: Escribir pruebas fallidas de encabezados**

Agregar pruebas concretas:

```python
def test_pdf_uses_point_inventory_labels(self):
    response = self.client.get(self.url, {"periodo": "2026-08", "export": "pdf"})
    self.assertEqual(response.status_code, 200)
    self.assertNotIn(b"F\xc3\xadsico", response.content)

def test_closure_csv_uses_neutral_point_labels(self):
    response = self.client.get(reverse("reportes:cierre_producto"), {"month": "2026-08", "format": "csv"})
    body = response.content.decode("utf-8-sig")
    self.assertIn("Saldo calculado", body)
    self.assertIn("Fin. Point", body)
    self.assertIn("Dif. Point", body)
    self.assertNotIn("Sobrante físico", body)

def test_closure_xlsx_uses_neutral_point_labels(self):
    response = self.client.get(reverse("reportes:cierre_producto"), {"month": "2026-08", "format": "xlsx"})
    workbook = load_workbook(BytesIO(response.content), read_only=True)
    headers = [cell.value for cell in next(workbook["Detalle"].iter_rows())]
    self.assertIn("Saldo calculado", headers)
    self.assertIn("Fin. Point", headers)
    self.assertIn("Dif. Point", headers)
```

- [ ] **Step 2: Confirmar los fallos**

```bash
python manage.py test reportes.tests_producido_vs_vendido reportes.tests.ReportesCanonicosTests --keepdb
```

Expected: FAIL en encabezados heredados.

- [ ] **Step 3: Actualizar exportadores**

Cambiar `PDF_EXPORT_COLUMNS` y los encabezados CSV/XLSX. Para cierres históricos, transformar solo la etiqueta visible según el signo almacenado; no reescribir el valor ni el estado en base.

- [ ] **Step 4: Ejecutar pruebas**

```bash
python manage.py test reportes.tests_producido_vs_vendido reportes.tests.ReportesCanonicosTests --keepdb
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reportes/views_produccion.py reportes/views.py reportes/tests_producido_vs_vendido.py reportes/tests.py
git commit -m "fix(reportes): alinea exportaciones del cierre Point"
```

### Task 7: Verificación integral y entrega

**Files:** solo correcciones derivadas de hallazgos directamente relacionados.

- [ ] **Step 1: Ejecutar checks y pruebas completas del alcance**

```bash
python manage.py migrate --check
python manage.py check
python manage.py test \
  pos_bridge.tests.test_monthly_product_balance_service \
  pos_bridge.tests.test_product_month_closure_service \
  reportes.tests_producido_vs_vendido \
  reportes.tests.ReportesCanonicosTests --keepdb
```

Expected: cero migraciones pendientes, cero errores y todas las pruebas PASS.

- [ ] **Step 2: Revisar consumidores y diff**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff --check origin/main..HEAD
python scripts/check_pointdailysale_usage.py
```

Expected: solo archivos del alcance, sin errores de whitespace ni violaciones de fuente.

- [ ] **Step 3: Validar en navegador local**

Abrir `/reportes/produccion/?periodo=2026-08`, comprobar tabla, fórmula, estados, consola y solicitudes de red. Verificar también PDF/CSV/XLSX.

- [ ] **Step 4: Preparar PR**

Confirmar que no hay cambios sin commit, crear PR en borrador con resumen, pruebas y nota explícita de que agosto todavía no fue reconstruido.

- [ ] **Step 5: Desplegar después del merge**

Ejecutar en VPS:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

No ejecutar `git pull` manual.

- [ ] **Step 6: Validar producción sin modificar agosto**

Comprobar la pantalla autenticada, exportaciones, logs y evidencia de fuentes. Presentar comparación anterior/nueva.

- [ ] **Step 7: Solicitar autorización operacional para agosto**

Solo después de la validación, solicitar autorización para ejecutar un rebuild auditable del cierre `2026-08`. Si se autoriza, hacer lectura fresca de base y pantalla después de la escritura.
