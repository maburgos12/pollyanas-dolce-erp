# Inventario con fuente única Point — Entrega 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Crear el contrato canónico de lectura Point por ubicación, normalizar unidades y frescura, y dejar Compras automáticas desactivadas de forma segura.

**Architecture:** `CanonicalPointInventoryService` será el único límite de lectura de existencias Point para insumos. Consultará la última evidencia `PointInventorySnapshot` por código y una ubicación obligatoria, devolverá un resultado tipado con frescura y unidades, y bloqueará decisiones cuando falte una captura vigente. Esta entrega no migra todavía todos los consumidores ni modifica saldos históricos.

**Tech Stack:** Django 5, PostgreSQL 16, dataclasses/enum de Python, Django TestCase.

---

## Estructura de archivos

- Crear `inventario/canonical_point_inventory.py`: tipos, errores y servicio único de lectura.
- Crear `inventario/tests_canonical_point_inventory.py`: contrato, separación de ubicaciones, unidades y frescura.
- Modificar `config/settings.py`: caducidad configurable y Compras automáticas apagadas por defecto.
- Modificar `reportes/models.py`: nuevo valor predeterminado seguro del control operativo.
- Crear `reportes/migrations/0046_autocontrolsettings_disable_auto_purchase_default.py`: alinear el estado de migraciones con el modelo sin cambiar registros existentes.
- Modificar `reportes/tests_operations_automation.py`: demostrar que la generación falla cerrada por defecto.

### Task 1: Tipos canónicos y ubicación obligatoria

**Files:**
- Create: `inventario/canonical_point_inventory.py`
- Test: `inventario/tests_canonical_point_inventory.py`

- [ ] **Step 1: Escribir las pruebas fallidas del contrato**

```python
from decimal import Decimal

from django.test import SimpleTestCase

from inventario.canonical_point_inventory import (
    CanonicalInventoryReading,
    InventoryFreshness,
    InventoryLocation,
    display_quantity,
    require_inventory_location,
)


class CanonicalPointInventoryContractTests(SimpleTestCase):
    def test_location_is_required(self):
        with self.assertRaisesMessage(ValueError, "ubicación de inventario es obligatoria"):
            require_inventory_location(None)

    def test_only_business_locations_are_accepted(self):
        self.assertEqual(require_inventory_location("ALMACEN"), InventoryLocation.ALMACEN)
        self.assertEqual(require_inventory_location("CEDIS"), InventoryLocation.CEDIS)
        with self.assertRaises(ValueError):
            require_inventory_location("CFP")

    def test_base_units_are_presented_as_kg_liters_or_pieces(self):
        self.assertEqual(display_quantity(Decimal("169669.245"), "g"), (Decimal("169.669245"), "kg"))
        self.assertEqual(display_quantity(Decimal("11217.150"), "ml"), (Decimal("11.21715"), "L"))
        self.assertEqual(display_quantity(Decimal("7"), "pza"), (Decimal("7"), "pza"))
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py test inventario.tests_canonical_point_inventory -v 2
```

Expected: `ImportError` porque `inventario.canonical_point_inventory` todavía no existe.

- [ ] **Step 3: Implementar los tipos mínimos**

```python
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class InventoryLocation(StrEnum):
    ALMACEN = "ALMACEN"
    CEDIS = "CEDIS"


class InventoryFreshness(StrEnum):
    FRESH = "FRESH"
    STALE = "STALE"
    MISSING = "MISSING"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class CanonicalInventoryReading:
    insumo_id: int
    codigo_point: str
    location: InventoryLocation
    quantity_base: Decimal | None
    base_unit: str
    display_quantity: Decimal | None
    display_unit: str
    point_quantity: Decimal | None
    point_unit: str
    captured_at: datetime | None
    freshness: InventoryFreshness
    source: str = "POINT"
    sync_job_id: int | None = None
    error: str = ""


def require_inventory_location(value: InventoryLocation | str | None) -> InventoryLocation:
    if value is None:
        raise ValueError("La ubicación de inventario es obligatoria.")
    try:
        return value if isinstance(value, InventoryLocation) else InventoryLocation(str(value).upper())
    except ValueError as exc:
        raise ValueError("La ubicación debe ser ALMACEN o CEDIS.") from exc


def display_quantity(quantity: Decimal, base_unit: str) -> tuple[Decimal, str]:
    normalized = (base_unit or "").strip().lower()
    if normalized == "g":
        return quantity / Decimal("1000"), "kg"
    if normalized == "ml":
        return quantity / Decimal("1000"), "L"
    return quantity, base_unit
```

- [ ] **Step 4: Ejecutar las pruebas y comprobar GREEN**

Run the command from Step 2. Expected: `OK`.

- [ ] **Step 5: Confirmar el cambio**

```bash
git add inventario/canonical_point_inventory.py inventario/tests_canonical_point_inventory.py
git commit -m "feat(inventario): define contrato canónico Point"
```

### Task 2: Lectura de snapshots Point separada por ubicación

**Files:**
- Modify: `inventario/canonical_point_inventory.py`
- Modify: `inventario/tests_canonical_point_inventory.py`
- Modify: `config/settings.py`

- [ ] **Step 1: Escribir fixtures y pruebas fallidas**

Crear `CanonicalPointInventoryServiceTests(TestCase)` con unidad base `g`, insumo `AZM` y snapshots Point para `Almacen` y `CEDIS`. Las pruebas deben demostrar:

```python
def test_read_many_never_compensates_almacen_with_cedis(self):
    almacen = self.service.read_many([self.insumo], location="ALMACEN", now=self.now)
    cedis = self.service.read_many([self.insumo], location="CEDIS", now=self.now)
    self.assertEqual(almacen[self.insumo.id].quantity_base, Decimal("20000.000"))
    self.assertEqual(cedis[self.insumo.id].quantity_base, Decimal("-14140.000"))

def test_missing_point_code_is_not_reported_as_zero(self):
    reading = self.service.read_many([self.insumo_without_code], location="ALMACEN", now=self.now)[
        self.insumo_without_code.id
    ]
    self.assertIsNone(reading.quantity_base)
    self.assertEqual(reading.freshness, InventoryFreshness.MISSING)

def test_stale_snapshot_is_visible_but_not_usable_for_decisions(self):
    reading = self.service.read_many([self.insumo], location="ALMACEN", now=self.now + timedelta(hours=13))[
        self.insumo.id
    ]
    self.assertEqual(reading.display_quantity, Decimal("20"))
    self.assertEqual(reading.freshness, InventoryFreshness.STALE)
    with self.assertRaises(CanonicalInventoryUnavailable):
        self.service.require_fresh([self.insumo], location="ALMACEN", now=self.now + timedelta(hours=13))
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py test inventario.tests_canonical_point_inventory.CanonicalPointInventoryServiceTests -v 2
```

Expected: FAIL porque `CanonicalPointInventoryService` y `CanonicalInventoryUnavailable` no existen.

- [ ] **Step 3: Implementar la lectura mínima**

Añadir a `config/settings.py`:

```python
POINT_INVENTORY_CANONICAL_MAX_AGE_MINUTES = env_int(
    "POINT_INVENTORY_CANONICAL_MAX_AGE_MINUTES",
    720,
)
```

Implementar el servicio con estas reglas exactas:

```python
class CanonicalInventoryUnavailable(RuntimeError):
    def __init__(self, readings: list[CanonicalInventoryReading]):
        self.readings = readings
        super().__init__("Point no tiene inventario vigente para completar esta decisión.")


class CanonicalPointInventoryService:
    BRANCH_BY_LOCATION = {
        InventoryLocation.ALMACEN: "almacen",
        InventoryLocation.CEDIS: "cedis",
    }

    def read_many(self, insumos, *, location, now=None):
        location = require_inventory_location(location)
        now = now or timezone.now()
        insumos = list(insumos)
        codes = {(item.codigo_point or "").strip(): item for item in insumos if (item.codigo_point or "").strip()}
        rows = (
            PointInventorySnapshot.objects.filter(
                branch__normalized_name=self.BRANCH_BY_LOCATION[location],
                product__sku__in=list(codes),
            )
            .select_related("product", "sync_job")
            .order_by("product__sku", "-captured_at", "-id")
            .distinct("product__sku")
        )
        latest = {row.product.sku: row for row in rows}
        return {
            item.id: self._reading(item=item, location=location, snapshot=latest.get((item.codigo_point or "").strip()), now=now)
            for item in insumos
        }

    def require_fresh(self, insumos, *, location, now=None):
        readings = self.read_many(insumos, location=location, now=now)
        invalid = [row for row in readings.values() if row.freshness is not InventoryFreshness.FRESH]
        if invalid:
            raise CanonicalInventoryUnavailable(invalid)
        return readings
```

`_reading` extraerá `Unidad` de `raw_payload`, validará `POINT_UNIT_ALIASES`, convertirá mediante `cantidad_en_unidad_erp`, presentará con `display_quantity` y comparará `captured_at` contra el límite de `settings.POINT_INVENTORY_CANONICAL_MAX_AGE_MINUTES`. Un código, unidad o snapshot faltante debe devolver `MISSING` o `ERROR` con cantidad `None`, nunca cero.

- [ ] **Step 4: Ejecutar las pruebas y comprobar GREEN**

Run the command from Step 2. Expected: `OK`.

- [ ] **Step 5: Ejecutar el módulo completo de reconciliación**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py test inventario.tests_canonical_point_inventory inventario.tests_point_reconciliation -v 2
```

Expected: `OK` with 0 failures.

- [ ] **Step 6: Confirmar el cambio**

```bash
git add config/settings.py inventario/canonical_point_inventory.py inventario/tests_canonical_point_inventory.py
git commit -m "feat(inventario): lee existencia canónica desde Point"
```

### Task 3: Compras automáticas apagadas por defecto

**Files:**
- Modify: `config/settings.py:491`
- Modify: `reportes/models.py:1937`
- Create: `reportes/migrations/0046_autocontrolsettings_disable_auto_purchase_default.py`
- Modify: `reportes/tests_operations_automation.py`

- [ ] **Step 1: Escribir la prueba fallida del valor seguro**

```python
@override_settings(ERP_AUTO_PURCHASE_ENABLED=False)
def test_auto_purchase_is_fail_closed_when_global_flag_is_off(self):
    controls = AutoControlSettings.get_solo()
    controls.enable_auto_purchase = True
    controls.save(update_fields=["enable_auto_purchase"])
    result = generate_purchase_requests_from_production(self.target_date, actor=self.user)
    self.assertFalse(result["enabled"])
    self.assertEqual(result["generated"], 0)
    self.assertFalse(SolicitudCompra.objects.filter(area__startswith="AUTO_PRODUCCION:").exists())
```

- [ ] **Step 2: Ejecutar la prueba y comprobar RED del default de configuración**

Añadir además una prueba `SimpleTestCase` que recargue la configuración sin variable de entorno y espere `ERP_AUTO_PURCHASE_ENABLED is False`. Ejecutar:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py test reportes.tests_operations_automation -v 2
```

Expected: FAIL porque el default actual es `True`.

- [ ] **Step 3: Cambiar los valores predeterminados y crear migración**

En `config/settings.py`:

```python
ERP_AUTO_PURCHASE_ENABLED = env_bool("ERP_AUTO_PURCHASE_ENABLED", default=False)
```

En `reportes/models.py`:

```python
enable_auto_purchase = models.BooleanField(default=False)
```

Crear una migración `AlterField` que cambie únicamente el valor predeterminado del modelo. No incluir `RunPython` y no modificar el valor `False` ya aplicado en producción.

- [ ] **Step 4: Validar migración y prueba**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py migrate
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py migrate --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py test reportes.tests_operations_automation -v 2
```

Expected: migración aplicada, 0 pendientes y tests `OK`.

- [ ] **Step 5: Confirmar el cambio**

```bash
git add config/settings.py reportes/models.py reportes/migrations/0046_autocontrolsettings_disable_auto_purchase_default.py reportes/tests_operations_automation.py
git commit -m "fix(compras): desactiva automatización por defecto"
```

### Task 4: Verificación integrada de la entrega 1

**Files:**
- Verify only.

- [ ] **Step 1: Buscar placeholders y agregaciones introducidas**

```bash
rg -n "NotImplemented|pass$|Sum\(\"stock_actual\"\)" inventario/canonical_point_inventory.py inventario/tests_canonical_point_inventory.py
```

Expected: no matches.

- [ ] **Step 2: Ejecutar checks y pruebas enfocadas**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py migrate --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55480/pastelerias_erp python3 manage.py test inventario.tests_canonical_point_inventory inventario.tests_point_reconciliation reportes.tests_operations_automation -v 2
```

Expected: 0 migraciones pendientes, 0 errores de sistema y tests `OK`.

- [ ] **Step 3: Revisar el diff y estado**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
```

Expected: solo archivos de la entrega 1 y ninguna advertencia de whitespace.

- [ ] **Step 4: Preparar la siguiente entrega**

Registrar como alcance de la Entrega 2 la migración de Compras, MRP, reabasto, Dashboard, BI, alertas y faltantes hacia `CanonicalPointInventoryService`. No abrir PR ni desplegar la Entrega 1 sin revisar primero el diff completo y la evidencia de pruebas.
