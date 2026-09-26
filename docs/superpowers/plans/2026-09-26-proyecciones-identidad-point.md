# Point Product Identity in Forecasts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Pronósticos and Proyecciones select the exact Point product by `PointProduct.id`, preserve Point category/name, keep operational grouping separate, and export every resulting category without changing historical snapshots or production.

**Architecture:** Reuse `PointProduct` as the canonical identity and taxonomy, `PointSalesDailyProductFact` as the existing product-to-recipe relationship, and `_forecastable_queryset` as the single forecastability rule. The view will send validated product IDs; both engines will filter exact IDs, while the existing grouping helper remains presentation-only. No models, migrations, duplicate catalogs, or production writes are introduced.

**Tech Stack:** Django 5, PostgreSQL 16, Django templates, openpyxl, existing Point bridge models, Django TestCase.

---

## Confirmed reuse and file structure

- `pos_bridge/models/product.py` remains unchanged: `PointProduct.id`, `name`, and `category` are already canonical.
- `pos_bridge/models/sales_pipeline.py` remains unchanged: `PointSalesDailyProductFact.point_product` and `.receta` already provide the required relationship.
- `ventas/services/pronostico_engine.py` owns forecastable-product discovery and exact Point filtering.
- `ventas/services/proyecciones_engine.py` owns exact product-to-recipe resolution for operational projections.
- `ventas/views.py` owns request validation, selector composition, and category-complete export ordering.
- `ventas/templates/ventas/pronostico.html` renders product IDs and distinguishes Point category from operational group.
- `ventas/tests.py` holds all regression coverage for this bounded change.

The failed Point recipe synchronization is an independent subsystem. This plan includes a read-only diagnostic checkpoint, but no speculative synchronization fix and no production retry.

### Task 1: Establish the isolated PostgreSQL baseline

**Files:**
- No file changes.

- [ ] **Step 1: Start an isolated PostgreSQL service**

Run:

```bash
export COMPOSE_PROJECT_NAME=erp_ventas_identidad_point
export DB_HOST_PORT=55439
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:55439/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
```

Expected: PostgreSQL reports `accepting connections`.

- [ ] **Step 2: Apply the current main migrations and prove the baseline**

Run:

```bash
python manage.py migrate
python manage.py migrate --check
python manage.py check
python manage.py test ventas --parallel
```

Expected: no pending migrations, Django reports no issues, and the existing Ventas suite passes before implementation.

### Task 2: Reuse forecastability and build the selector from exact Point IDs

**Files:**
- Modify: `ventas/services/pronostico_engine.py:867-892`
- Modify: `ventas/views.py:223-292`
- Test: `ventas/tests.py`

- [ ] **Step 1: Write a failing regression test for SKU collisions in the selector**

Add imports for `get_user_model`, `QueryDict`, `timezone`, `SimpleNamespace`, `PointBranch`, `PointProduct`, and `PointSalesDailyProductFact`, then add a `VentasPointIdentityTests(TestCase)` class. Its setup creates one authorized superuser, one active ERP branch, one Point branch, a product-final recipe, and two active Point products sharing SKU `0160`: sold Bollo Lotus and unsold Glow 2. The superuser follows the existing `_can_view_pronostico` path; it does not introduce a permission shortcut.

```python
class VentasPointIdentityTests(TestCase):
    def setUp(self):
        self.allowed_user = get_user_model().objects.create_superuser(
            username="ventas_point_identity",
            email="ventas-point-identity@example.com",
            password="test12345",
        )
        self.branch = Sucursal.objects.create(codigo="POINT-ID", nombre="Point identidad", activa=True)
        self.point_branch = PointBranch.objects.create(
            external_id="POINT-ID",
            name="Point identidad",
            erp_branch=self.branch,
        )
        self.recipe = Receta.objects.create(
            nombre="Bollo Lotus",
            codigo_point="0160",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Bollo",
            categoria="Bollo",
            hash_contenido="ventas-point-id-bollo-lotus",
        )
        self.bollo = PointProduct.objects.create(
            external_id="point-bollo-lotus",
            sku="0160",
            name="Bollo Lotus",
            category="Bollo",
        )
        self.glow = PointProduct.objects.create(
            external_id="point-glow-2",
            sku="0160",
            name="Glow 2",
            category="Glow",
        )
        PointSalesDailyProductFact.objects.create(
            branch=self.point_branch,
            sale_date=timezone.localdate() - timedelta(days=1),
            sucursal_nombre=self.branch.nombre,
            categoria="Bollo",
            producto_nombre_historico="Bollo Lotus",
            point_product=self.bollo,
            receta=self.recipe,
            match_catalogo_status="EXACT_CODE",
            total_cantidad=Decimal("5"),
            total_venta=Decimal("500"),
            total_venta_neta=Decimal("500"),
        )

    def test_selector_uses_the_sold_point_product_id_not_every_product_with_the_sku(self):
        catalog = ventas_views._catalogo_productos_por_categoria()
        products = [product for rows in catalog.values() for product in rows]

        self.assertEqual([product["id"] for product in products], [self.bollo.id])
        self.assertEqual(products[0]["categoria_point"], "Bollo")
        self.assertEqual(products[0]["grupo_operativo"], "Bollo")
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python manage.py test ventas.tests.VentasPointIdentityTests.test_selector_uses_the_sold_point_product_id_not_every_product_with_the_sku
```

Expected: FAIL because the current catalog expands `0160` to both Point products and does not expose separate category/group fields.

- [ ] **Step 3: Expose forecastable Point IDs from the existing engine rule**

Keep `_forecastable_queryset` as the single rule and add this public helper beside it:

```python
def forecastable_point_product_ids(branch_ids: set[int] | None = None) -> set[int]:
    selected_branch_ids = branch_ids or set(
        Sucursal.objects.filter(activa=True).values_list("id", flat=True)
    )
    if not selected_branch_ids:
        return set()
    return set(
        _forecastable_queryset(selected_branch_ids)
        .values_list("point_product_id", flat=True)
        .distinct()
    )
```

Import it in `ventas/views.py` with `calcular_pronostico`.

- [ ] **Step 4: Replace SKU expansion with exact recent product IDs**

In `_catalogo_productos_por_categoria`:

```python
recent_product_ids = set(
    PointSalesDailyProductFact.objects.filter(
        sale_date__gte=hace_30,
        point_product__active=True,
        point_product_id__isnull=False,
    ).values_list("point_product_id", flat=True).distinct()
)
eligible_product_ids = recent_product_ids & forecastable_point_product_ids()
products = PointProduct.objects.filter(
    active=True,
    id__in=eligible_product_ids,
).only("id", "sku", "name", "category").order_by("category", "name", "id")
```

Preserve the existing exclusions, but calculate the rare Pay de Durazno rule by `point_product_id`, not SKU. Build each item without new persisted data:

```python
group = _category_for_catalog_product(product)
categorias_raw[group].append(
    {
        "id": product.id,
        "nombre": product.name,
        "sku": product.sku,
        "categoria_point": _clean_category_label(product.category),
        "grupo_operativo": group,
    }
)
```

If recent sales are empty, fall back to `forecastable_point_product_ids()` rather than every active catalog record.

- [ ] **Step 5: Run the focused test and the existing category tests**

Run:

```bash
python manage.py test \
  ventas.tests.VentasPointIdentityTests.test_selector_uses_the_sold_point_product_id_not_every_product_with_the_sku \
  ventas.tests.VentasModuleTests.test_forecast_category_prefers_the_sold_point_product_over_duplicate_recipe_skus
```

Expected: both tests pass.

- [ ] **Step 6: Commit the selector-source correction**

```bash
git add ventas/services/pronostico_engine.py ventas/views.py ventas/tests.py
git commit -m "fix(ventas): construye selector con identidad exacta de Point"
```

### Task 3: Validate submitted product IDs in preview and save flows

**Files:**
- Modify: `ventas/views.py:199-207,828-842,876-1003`
- Test: `ventas/tests.py`

- [ ] **Step 1: Write failing tests for parsed, invalid, and inactive IDs**

Add a parser test and a view test using Django's `QueryDict` and authenticated client:

```python
def test_selected_point_product_ids_accepts_positive_ids_once(self):
    request = SimpleNamespace(POST=QueryDict("productos_incluidos=12&productos_incluidos=12&productos_incluidos=x"))
    self.assertEqual(ventas_views._selected_point_product_ids(request), [12])

def test_preview_rejects_point_product_outside_the_visible_catalog(self):
    self.client.force_login(self.allowed_user)
    response = self.client.post(
        reverse("ventas:pronostico"),
        {
            "tab": "pronosticos",
            "fecha_inicio": "2026-10-01",
            "fecha_fin": "2026-10-02",
            "sucursales": [self.branch.id],
            "productos_incluidos": [self.glow.id],
        },
    )
    self.assertContains(response, "La selección contiene productos que no están disponibles para pronóstico")
```

`allowed_user` comes from the explicit `create_superuser` setup in Task 2 and is accepted by the existing `_can_view_pronostico` authorization rule.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
python manage.py test \
  ventas.tests.VentasPointIdentityTests.test_selected_point_product_ids_accepts_positive_ids_once \
  ventas.tests.VentasPointIdentityTests.test_preview_rejects_point_product_outside_the_visible_catalog
```

Expected: FAIL because `_selected_product_skus` still returns strings and the view silently drops values outside the SKU catalog.

- [ ] **Step 3: Implement one ID parser and one catalog-ID helper**

Replace `_selected_product_skus` with:

```python
def _selected_point_product_ids(request) -> list[int]:
    selected = []
    seen = set()
    for value in request.POST.getlist("productos_incluidos"):
        raw = str(value).strip()
        if not raw.isdigit():
            continue
        product_id = int(raw)
        if product_id > 0 and product_id not in seen:
            seen.add(product_id)
            selected.append(product_id)
    return selected


def _catalog_point_product_ids(catalog: OrderedDict[str, list[dict]]) -> set[int]:
    return {int(product["id"]) for products in catalog.values() for product in products}
```

- [ ] **Step 4: Use the same validation in preview and save**

In both `PronosticoVentasView` and `PronosticoGuardarView`:

```python
available_product_ids = _catalog_point_product_ids(categorias_productos)
selected_product_ids = (
    _selected_point_product_ids(request)
    if request.method == "POST"
    else sorted(available_product_ids)
)
invalid_product_ids = set(selected_product_ids) - available_product_ids
if invalid_product_ids:
    form_errors.append(
        "La selección contiene productos que no están disponibles para pronóstico. Actualiza la pantalla e inténtalo de nuevo."
    )
```

Do not silently discard invalid IDs. Pass `point_product_ids=selected_product_ids` to the engines and `_calcular_y_guardar_sync`. Rename the context variable to `selected_product_ids`.

- [ ] **Step 5: Run the focused tests and verify GREEN**

Run:

```bash
python manage.py test ventas.tests.VentasPointIdentityTests
```

Expected: all identity and validation tests pass.

- [ ] **Step 6: Commit request validation**

```bash
git add ventas/views.py ventas/tests.py
git commit -m "fix(ventas): valida productos Point en pronosticos"
```

### Task 4: Filter both engines by exact Point product identity

**Files:**
- Modify: `ventas/services/pronostico_engine.py:867-920`
- Modify: `ventas/services/proyecciones_engine.py:127-141,351-370`
- Modify: `ventas/views.py:828-842`
- Test: `ventas/tests.py`

- [ ] **Step 1: Write a failing engine regression test for Bollo Lotus versus Glow 2**

```python
def test_projection_recipe_resolution_does_not_expand_a_shared_sku(self):
    self.assertEqual(
        _selected_recipe_ids([self.bollo.id]),
        {self.recipe.id},
    )
    self.assertEqual(_selected_recipe_ids([self.glow.id]), set())

def test_forecast_queryset_filters_the_exact_point_product(self):
    queryset = _forecastable_queryset({self.branch.id}, {self.bollo.id})
    self.assertEqual(set(queryset.values_list("point_product_id", flat=True)), {self.bollo.id})
```

Import `_forecastable_queryset` and `_selected_recipe_ids` in the test module.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
python manage.py test \
  ventas.tests.VentasPointIdentityTests.test_projection_recipe_resolution_does_not_expand_a_shared_sku \
  ventas.tests.VentasPointIdentityTests.test_forecast_queryset_filters_the_exact_point_product
```

Expected: FAIL because both helpers interpret the integers as SKU values or still query by `point_product__sku`.

- [ ] **Step 3: Change Pronósticos to exact product IDs**

Use the new contract throughout `pronostico_engine.py`:

```python
def _forecastable_queryset(
    branch_ids: set[int],
    point_product_ids: set[int] | None = None,
):
    # existing forecastable_filter and exclusions stay unchanged
    if point_product_ids is not None:
        queryset = queryset.filter(point_product_id__in=point_product_ids)
    return queryset


def calcular_pronostico(
    fecha_inicio: date,
    fecha_fin: date,
    sucursal_ids: set[int] | list[int] | None = None,
    point_product_ids: set[int] | list[int] | None = None,
) -> dict:
    selected_product_ids = {
        int(value) for value in (point_product_ids or []) if str(value).isdigit()
    }
    product_filter = selected_product_ids if point_product_ids is not None else None
    # pass product_filter to _forecastable_queryset
```

- [ ] **Step 4: Change Proyecciones to the existing exact relationship**

Replace SKU and recipe-code expansion with:

```python
def _selected_recipe_ids(point_product_ids: set[int] | list[int] | None) -> set[int] | None:
    if point_product_ids is None:
        return None
    selected_ids = {int(value) for value in point_product_ids if str(value).isdigit()}
    if not selected_ids:
        return set()
    return set(
        PointSalesDailyProductFact.objects.filter(
            point_product_id__in=selected_ids,
            receta_id__isnull=False,
        ).values_list("receta_id", flat=True).distinct()
    )
```

Rename `skus_incluidos` to `point_product_ids` in `calcular_proyeccion_operativa` and its callers. Do not query `Receta.codigo_point`; the selector already requires an observed forecastable Point relation.

- [ ] **Step 5: Verify GREEN and run all Ventas engine tests**

Run:

```bash
python manage.py test ventas.tests.VentasPointIdentityTests ventas.tests.VentasProjectionEngineTests
```

Expected: all tests pass; the selected Glow product resolves no Bollo recipe.

- [ ] **Step 6: Commit exact engine filtering**

```bash
git add ventas/services/pronostico_engine.py ventas/services/proyecciones_engine.py ventas/views.py ventas/tests.py
git commit -m "fix(ventas): filtra motores por producto Point exacto"
```

### Task 5: Render Point category separately from operational group

**Files:**
- Modify: `ventas/templates/ventas/pronostico.html:113-143,302-314`
- Test: `ventas/tests.py:69-94`

- [ ] **Step 1: Write a failing template contract test**

Add assertions to the existing template test:

```python
self.assertIn('value="{{ prod.id }}"', template)
self.assertIn('prod.id in selected_product_ids', template)
self.assertIn("Categoría Point: {{ prod.categoria_point }}", template)
self.assertNotIn('value="{{ prod.sku }}"', template)
self.assertNotIn("selected_product_skus", template)
```

- [ ] **Step 2: Run the template test and verify RED**

Run:

```bash
python manage.py test ventas.tests.VentasModuleTests.test_pronostico_includes_projection_tab_and_range_presets
```

Expected: FAIL because the template still posts SKU values and has no Point-category label.

- [ ] **Step 3: Update the visible selector and hidden save fields**

Change each checkbox to:

```django
<input
  type="checkbox"
  name="productos_incluidos"
  value="{{ prod.id }}"
  {% if prod.id in selected_product_ids %}checked{% endif %}
>
<span title="{{ prod.nombre }} — Categoría Point: {{ prod.categoria_point }}">
  {{ prod.nombre }}
  <small>Categoría Point: {{ prod.categoria_point }}</small>
</span>
```

Keep the existing group heading as the operational grouping. Replace the hidden SKU loop with:

```django
{% for product_id in selected_product_ids %}
<input type="hidden" name="productos_incluidos" value="{{ product_id }}">
{% endfor %}
```

No new frontend dependency, model field, or catalog is introduced.

- [ ] **Step 4: Run template and view tests**

Run:

```bash
python manage.py test ventas.tests.VentasModuleTests ventas.tests.VentasPointIdentityTests
```

Expected: all tests pass.

- [ ] **Step 5: Commit the UI contract**

```bash
git add ventas/templates/ventas/pronostico.html ventas/tests.py
git commit -m "fix(ventas): muestra categoria Point sin mezclar el grupo"
```

### Task 6: Guarantee category-complete Excel exports

**Files:**
- Modify: `ventas/views.py:450-555`
- Test: `ventas/tests.py`

- [ ] **Step 1: Write a failing order-coverage test**

```python
def test_result_category_order_keeps_unknown_point_categories(self):
    categories = [
        {"categoria": "Cake Topper", "productos": []},
        {"categoria": "Bollo", "productos": []},
    ]

    ordered = ventas_views._ordered_result_categories(categories)

    self.assertEqual([row["categoria"] for row in ordered], ["Bollo", "Cake Topper"])
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python manage.py test ventas.tests.VentasPointIdentityTests.test_result_category_order_keeps_unknown_point_categories
```

Expected: FAIL because the helper does not exist and the Excel writers only iterate `ORDEN_CATEGORIAS`.

- [ ] **Step 3: Add one reusable ordering helper**

```python
def _ordered_result_categories(categories: list[dict]) -> list[dict]:
    return sorted(
        categories,
        key=lambda row: _catalog_category_sort_key(row.get("categoria") or "Sin categoría"),
    )
```

In `_write_pronostico_sheet` and `_write_escenarios_sheet`, replace the `category_map` plus `for category_name in ORDEN_CATEGORIAS` loops with:

```python
for category in _ordered_result_categories(categorias):
    category_name = category.get("categoria") or "Sin categoría"
    # existing row-writing body remains unchanged
```

This reuses `ORDEN_CATEGORIAS` through `_catalog_category_sort_key` while retaining every category actually present.

- [ ] **Step 4: Run the export coverage test and Ventas suite**

Run:

```bash
python manage.py test ventas.tests.VentasPointIdentityTests ventas
```

Expected: `Cake Topper` remains after preferred categories and the complete Ventas suite passes.

- [ ] **Step 5: Commit export coverage**

```bash
git add ventas/views.py ventas/tests.py
git commit -m "fix(ventas): incluye todas las categorias Point en Excel"
```

### Task 7: Diagnose the recipe-sync failure without production writes

**Files:**
- Create: `docs/audits/2026-09-26-point-recipes-sync-failure.md`
- Do not modify synchronization code in this task unless a deterministic local failing test proves a code defect.

- [ ] **Step 1: Record the observed production evidence read-only**

Document jobs `70603` and `70607`, their timestamps, the atomic-transaction error, and the last successful recipes job `62393`. Include only operational metadata and sanitized error text; do not copy recipe payloads or credentials.

- [ ] **Step 2: Trace the recipes job transaction boundary**

Use code graph tracing from the job entry point to the `transaction.atomic` block and every caught `IntegrityError` or `DatabaseError`. Record the exact function and exception path that can continue querying after a database error.

- [ ] **Step 3: Decide the boundary from evidence**

If the failure is reproducible locally, add a separate failing unit test in `pos_bridge/tests/test_product_recipe_sync_service.py` and stop for a dedicated implementation plan because it changes the Point synchronization contract. If the evidence is insufficient, document the missing exception detail and the smallest safe instrumentation needed. Do not retry the production job.

- [ ] **Step 4: Commit the diagnostic record**

```bash
git add docs/audits/2026-09-26-point-recipes-sync-failure.md
git commit -m "docs(point): registra diagnostico de sincronizacion de recetas"
```

### Task 8: Full local verification and review package

**Files:**
- Modify only files already listed if verification exposes a regression covered by this design.

- [ ] **Step 1: Run the full required backend verification**

Run:

```bash
python manage.py migrate --check
python manage.py check
python manage.py test ventas --parallel
git diff --check
```

Expected: no pending migrations, no Django issues, all Ventas tests pass, and no whitespace errors.

- [ ] **Step 2: Run the local server against the isolated PostgreSQL database**

```bash
python manage.py runserver 127.0.0.1:8019
```

Expected: the Ventas forecast route loads without server errors.

- [ ] **Step 3: Validate the real browser flow locally**

Check both Pronósticos and Proyecciones:

- Bollo Lotus and Glow 2 are not coupled by SKU.
- Vaso Fresas con Crema Mediano/Grande do not select Viva Party.
- Each product displays its exact Point category separately from its group.
- Submitting an invalid or stale ID keeps the form context and shows the warning.
- Preview and save forms preserve the same selected IDs.
- Browser console has no new errors and relevant POST requests contain numeric Point product IDs.
- A locally generated Excel includes an out-of-order category fixture such as Cake Topper.

- [ ] **Step 4: Review scope and repository hygiene**

Run:

```bash
git status --short --branch
git log --oneline --decorate -8
git diff origin/main..HEAD --stat
git diff origin/main..HEAD -- ventas/ docs/superpowers docs/audits
git worktree list
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
git worktree prune --dry-run
```

Expected: only the approved Ventas files and documentation appear; no migration, screenshot, log, output, or unrelated module is included.

- [ ] **Step 5: Present the local result before any publication**

Provide Mauricio with the visible local behavior, affected files, exact tests, and remaining recipe-sync diagnostic status. Stop before push, PR, merge, deploy, Point writes, or historical-data changes. Continue only after explicit production-path authorization.
