# Plantilla de productos vendibles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Precargar la plantilla XLSX de cálculo de insumos con productos finales activos de Point que tienen receta fabricada, excluyendo familias no productivas.

**Architecture:** Añadir un selector privado y enfocado en `recetas/views/plan.py` que consulte `RecetaCodigoPointAlias` activos, filtre recetas de producto final fabricado y descarte familias de accesorios mediante una regla normalizada central. La vista XLSX consumirá ese selector; la ruta CSV conservará su contrato actual.

**Tech Stack:** Django 5, ORM de Django, openpyxl, `unidecode`, Django TestCase.

---

### Task 1: Selector de productos vendibles

**Files:**
- Modify: `recetas/tests.py`
- Modify: `recetas/views/plan.py`

- [ ] **Step 1: Write the failing selector test**

Crear recetas de tipo `PRODUCTO_FINAL` y modo `FABRICADO` para familias `Pasteles`, `Velas` y `Letreros`, con aliases Point activos. Agregar también un alias inactivo y una receta `SERVICIO_ACCESORIO`. Afirmar que `_calculo_insumos_template_products()` devuelve solo la receta de `Pasteles`, con `familia`, `codigo_point` y `producto`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python3 manage.py test recetas.tests.SolicitudVentasForecastTests.test_calculo_insumos_plantilla_selecciona_productos_vendibles --keepdb`

Expected: FAIL porque `_calculo_insumos_template_products` todavía no existe.

- [ ] **Step 3: Implement the minimal selector**

Agregar en `recetas/views/plan.py`:

```python
CALCULO_INSUMOS_EXCLUDED_TEMPLATE_FAMILIES = {
    "accesorios",
    "desechables",
    "letreros",
    "velas",
}


def _calculo_insumos_template_products() -> list[dict[str, str]]:
    aliases = (
        RecetaCodigoPointAlias.objects.filter(
            activo=True,
            receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
            receta__modo_costeo=Receta.MODO_COSTEO_FABRICADO,
        )
        .select_related("receta")
        .order_by("receta__familia", "receta__nombre", "codigo_point")
    )
    products = []
    seen_recetas = set()
    for alias in aliases:
        family_key = " ".join(unidecode(alias.receta.familia or "").lower().split())
        if family_key in CALCULO_INSUMOS_EXCLUDED_TEMPLATE_FAMILIES:
            continue
        if alias.receta_id in seen_recetas:
            continue
        seen_recetas.add(alias.receta_id)
        products.append(
            {
                "familia": alias.receta.familia or "Sin familia",
                "codigo_point": alias.codigo_point,
                "producto": alias.receta.nombre,
            }
        )
    return products
```

Ajustar imports existentes para usar `RecetaCodigoPointAlias` y `unidecode` sin duplicarlos.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: el mismo comando de Step 2.

Expected: PASS.

### Task 2: Workbook precargado y compatibilidad

**Files:**
- Modify: `recetas/tests.py`
- Modify: `recetas/views/plan.py`

- [ ] **Step 1: Write the failing XLSX contract test**

Descargar `recetas:calculo_insumos_plantilla` sin `format=csv`, abrir el workbook y afirmar:

```python
self.assertEqual(
    list(ws.values),
    [
        ("familia", "codigo_point", "producto", "presentacion", "cantidad", "notas"),
        ("Pasteles", "PASTEL-01", "Pastel Vendible", "", None, None),
    ],
)
```

Verificar además que Velas, Letreros, aliases inactivos y servicios/accesorios no estén presentes.

- [ ] **Step 2: Run the XLSX test and verify RED**

Run: `python3 manage.py test recetas.tests.SolicitudVentasForecastTests.test_calculo_insumos_plantilla_xlsx_precarga_catalogo_vendible --keepdb`

Expected: FAIL porque la hoja aún contiene los dos ejemplos estáticos y no incluye `familia`.

- [ ] **Step 3: Update only the XLSX branch**

Conservar `headers` y `sample_rows` actuales para CSV. En la rama XLSX usar:

```python
ws.append(["familia", "codigo_point", "producto", "presentacion", "cantidad", "notas"])
for product in _calculo_insumos_template_products():
    ws.append(
        [
            product["familia"],
            product["codigo_point"],
            product["producto"],
            "",
            None,
            None,
        ]
    )
```

- [ ] **Step 4: Prove old imports remain compatible**

Ejecutar el test existente de importación CSV/XLSX sin `familia` identificado mediante `rg -n "calculo_insumos.*import" recetas/tests.py`, y agregar una aserción específica solo si el contrato anterior no está cubierto.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run: ambos tests nuevos y los tests existentes de plantilla/importación de cálculo de insumos.

Expected: PASS, 0 failures.

- [ ] **Step 6: Commit implementation**

```bash
git add recetas/views/plan.py recetas/tests.py docs/superpowers/plans/2026-07-11-recetas-plantilla-productos-vendibles.md
git commit -m "feat(recetas): precargar plantilla con productos vendibles"
```

### Task 3: Verification, PR, deploy and production proof

**Files:**
- Verify: `recetas/views/plan.py`
- Verify: `recetas/tests.py`

- [ ] **Step 1: Run Django verification**

Run with the existing local PostgreSQL connection:

```bash
python3 manage.py migrate --check
python3 manage.py check
python3 manage.py test recetas.tests.SolicitudVentasForecastTests --keepdb
```

Expected: 0 pending migrations, 0 system-check errors, 0 test failures.

- [ ] **Step 2: Review scope and branch hygiene**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git log --oneline --decorate -5
git worktree list
```

Expected: only the spec, plan, `recetas/views/plan.py` and `recetas/tests.py` differ from `origin/main`; worktree is clean.

- [ ] **Step 3: Push and create a draft PR**

Push `codex/recetas-plantilla-productos-vendibles` and open one draft PR containing functional summary, touched files, tests and browser-validation status.

- [ ] **Step 4: Merge and deploy**

After checks pass, merge the PR. On `/opt/pastelerias-erp`, run `git pull origin main`, `python manage.py migrate --check` in the web container and `bash scripts/deploy_web_safe.sh`.

- [ ] **Step 5: Validate production data and browser flow**

Before declaring success, inspect the generated production workbook and verify it has sellable product families, excludes accessory families, contains blank quantities and imports after entering a positive quantity. Confirm the download request is HTTP 200 and the browser console has no related errors.

- [ ] **Step 6: Clean merged branch**

Delete local and remote task branches and run `git fetch --prune origin` only after merge, deploy and production validation are complete.
