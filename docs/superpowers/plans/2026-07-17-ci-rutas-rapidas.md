# CI Routes by Change Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evitar CI completo duplicado y usar una validación corta para cambios exclusivamente documentales o del preflight.

**Architecture:** El workflow principal conservará toda la suite pero solo reaccionará a pull requests con cambios de aplicación y pushes a `main`. Un segundo workflow sin PostgreSQL ni instalación de dependencias cubrirá Markdown, `.gitignore` y el preflight; los cambios mixtos activarán ambos y, por tanto, nunca perderán el CI completo.

**Tech Stack:** GitHub Actions YAML, Bash, Git.

---

### Task 1: Restringir el CI completo a cambios de aplicación

**Files:**
- Modify: `.github/workflows/ci.yml:3-7`

- [ ] **Step 1: Registrar la matriz de eventos esperada**

Verificar antes del cambio que `push.branches` acepta todas las ramas y que no
existen filtros de rutas:

```bash
sed -n '1,18p' .github/workflows/ci.yml
```

Expected: `branches: ["**"]` y ningún `paths-ignore`.

- [ ] **Step 2: Cambiar los eventos del workflow principal**

Sustituir el bloque `on` por:

```yaml
on:
  push:
    branches: [main]
    paths-ignore:
      - "**/*.md"
      - ".gitignore"
      - "scripts/git_workspace_preflight.sh"
  pull_request:
    paths-ignore:
      - "**/*.md"
      - ".gitignore"
      - "scripts/git_workspace_preflight.sh"
```

- [ ] **Step 3: Validar que el CI completo conserva sus jobs**

Run:

```bash
rg -n "postgres:15|Django checks|Verify migrations|focused cierre|legacy full" .github/workflows/ci.yml
```

Expected: las cinco superficies continúan presentes; solo cambia el bloque de eventos.

- [ ] **Step 4: Commit del filtro principal**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: evitar suite duplicada en pushes de ramas"
```

### Task 2: Crear la ruta corta de higiene

**Files:**
- Create: `.github/workflows/repo-hygiene.yml`

- [ ] **Step 1: Crear el workflow corto**

Crear el archivo con este contenido:

```yaml
name: Repo hygiene

on:
  push:
    branches: [main]
    paths:
      - "**/*.md"
      - ".gitignore"
      - "scripts/git_workspace_preflight.sh"
  pull_request:
    paths:
      - "**/*.md"
      - ".gitignore"
      - "scripts/git_workspace_preflight.sh"

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Check whitespace errors
        run: git diff --check HEAD^

      - name: Validate workspace preflight syntax
        run: bash -n scripts/git_workspace_preflight.sh
```

- [ ] **Step 2: Validar sintaxis y ausencia de infraestructura pesada**

Run:

```bash
bash -n scripts/git_workspace_preflight.sh
! rg -n "postgres|setup-python|pip install|manage.py" .github/workflows/repo-hygiene.yml
```

Expected: exit 0 y ninguna coincidencia de infraestructura Django.

- [ ] **Step 3: Commit del workflow corto**

```bash
git add .github/workflows/repo-hygiene.yml
git commit -m "ci: validar cambios documentales por ruta corta"
```

### Task 3: Verificar la matriz completa antes del PR

**Files:**
- Verify: `.github/workflows/ci.yml`
- Verify: `.github/workflows/repo-hygiene.yml`

- [ ] **Step 1: Validar estructura YAML y filtros complementarios**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
import yaml

ci = yaml.load(Path('.github/workflows/ci.yml').read_text(), Loader=yaml.BaseLoader)
hygiene = yaml.load(Path('.github/workflows/repo-hygiene.yml').read_text(), Loader=yaml.BaseLoader)
expected = ['**/*.md', '.gitignore', 'scripts/git_workspace_preflight.sh']
assert ci['on']['push']['branches'] == ['main']
assert ci['on']['push']['paths-ignore'] == expected
assert ci['on']['pull_request']['paths-ignore'] == expected
assert hygiene['on']['push']['branches'] == ['main']
assert hygiene['on']['push']['paths'] == expected
assert hygiene['on']['pull_request']['paths'] == expected
print('CI routing matrix OK')
PY
```

Expected: `CI routing matrix OK`.

- [ ] **Step 2: Ejecutar validaciones del repositorio**

Run:

```bash
git diff --check origin/main...HEAD
bash scripts/git_workspace_preflight.sh --write
```

Expected: sin errores de whitespace y preflight limpio.

- [ ] **Step 3: Revisar que el diff esté limitado al alcance**

Run:

```bash
git diff origin/main...HEAD --stat
git status --short --branch
```

Expected: diseño/plan y los dos workflows; ningún archivo de aplicación modificado.
