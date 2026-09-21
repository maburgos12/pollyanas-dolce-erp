# Resumen compacto de estados RRHH en móvil Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar los cinco estados de horas extra en tres filas compactas en móvil para acercar el primer registro al inicio de la pantalla.

**Architecture:** Conservar el HTML y la consulta actuales. Sobrescribir la cuadrícula y el tamaño interno de las tarjetas solo dentro del breakpoint móvil existente; actualizar la versión del CSS en el template para que el navegador obtenga el recurso nuevo.

**Tech Stack:** Django 5 templates, CSS plano, PostgreSQL 16 local, navegador real para verificación responsiva.

---

## Estructura de archivos

- `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`: única fuente de estilos de esta pantalla; contendrá la regla responsiva.
- `rrhh/templates/rrhh/horas_extra_list.html`: solo cambia el parámetro de versión del CSS; conserva el marcado y los conteos.
- `docs/superpowers/specs/2026-09-21-rrhh-estados-movil-compactos-design.md`: contrato de diseño aprobado, sin cambios funcionales.

### Task 1: Aplicar cuadrícula móvil compacta

**Files:**
- Modify: `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css:38-43`
- Modify: `rrhh/templates/rrhh/horas_extra_list.html:7`

- [ ] **Step 1: Confirmar el estado previo**

Run: `git status --short --branch && git diff --stat && bash scripts/git_workspace_preflight.sh --write`
Expected: rama `codex/rrhh-estados-movil-compactos`, sin cambios ajenos y preflight OK.

- [ ] **Step 2: Cambiar las reglas móviles de CSS**

Sustituir la regla `.ch-status-strip { grid-template-columns: 1fr; }` dentro de `@media (max-width: 760px)` por:

```css
.ch-status-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; padding: 10px; }
.ch-status-card { min-height: 0; padding: 9px 10px; }
.ch-status-card strong { font-size: 14px; line-height: 1.2; }
.ch-status-card span { font-size: 10px; line-height: 1.2; margin-top: 3px; }
.ch-status-card:last-child { grid-column: 1 / -1; }
```

Conservar las demás reglas móviles existentes. `minmax(0, 1fr)` evita que el contenido fuerce el ancho de columna; el último resumen ocupa toda la tercera fila.

- [ ] **Step 3: Actualizar la versión del recurso**

En `rrhh/templates/rrhh/horas_extra_list.html`, usar exactamente:

```html
<link rel="stylesheet" href="/static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css?v=20260921-estados-movil-v1">
```

- [ ] **Step 4: Verificar integridad local**

Run: `git diff --check`
Expected: salida vacía y código 0.

Run: `APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55587/pastelerias_erp python3 manage.py migrate --check`
Expected: código 0, sin migraciones pendientes.

Run: `APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55587/pastelerias_erp python3 manage.py check`
Expected: `System check identified no issues`.

Run: `APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55587/pastelerias_erp python3 manage.py test rrhh.tests_extra_conciliacion --noinput`
Expected: todos los tests pasan, sin escritura en producción.

- [ ] **Step 5: Validar en navegador**

Abrir la ruta local de horas extra con una sesión RRHH autorizada y revisar 320 px, 390 px y escritorio. Confirmar cinco tarjetas en orden, tres filas móviles, primer registro más arriba, conteos íntegros, sin scroll horizontal, errores JS ni fallos del CSS en Network. Si faltan usuarios/datos locales para la pantalla, usar una vista HTML de prueba de los mismos selectores y reportar explícitamente esa limitación; no simularla como prueba del flujo autenticado.

- [ ] **Step 6: Revisar y confirmar el cambio**

Run: `git diff -- static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css rrhh/templates/rrhh/horas_extra_list.html`
Expected: solo CSS móvil y versión del recurso, sin lógica de negocio.

Run: `git status --short --branch && git log --oneline --decorate -5 && git worktree list`
Expected: solo los dos archivos previstos modificados, worktree dedicado.

Run: `git add static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css rrhh/templates/rrhh/horas_extra_list.html && git commit -m "ui(rrhh): compactar estados de horas extra en móvil"`
Expected: commit quirúrgico.

### Task 2: Entrega y verificación real

**Files:** Ningún archivo nuevo.

- [ ] **Step 1: Verificar que la rama contiene una sola tarea**

Run: `git diff origin/main..HEAD --stat && git status --short --branch`
Expected: especificación, plan, CSS y template de este objetivo; árbol limpio.

- [ ] **Step 2: Crear PR y revisar CI**

Crear PR borrador con resumen funcional, pruebas y validación de navegador. Revisar el diff completo y la CI; no mezclar otras tareas ni mergear con checks fallidos.

- [ ] **Step 3: Mergear y desplegar**

Tras CI y revisión satisfactorias, mergear a `main`; ejecutar `bash scripts/deploy_web_safe.sh` en `/opt/pastelerias-erp` sin `git pull` manual previo.

- [ ] **Step 4: Validar producción y cerrar el worktree**

Comprobar en la ruta real de RRHH a 390 px que el CSS versionado llega por Network, que aparecen cinco estados en tres filas, y que los conteos y registros siguen iguales. Documentar cualquier limitación de acceso. Ejecutar auditoría y cierre `scripts/task_workspace_close.sh --state merged` solo después de validación satisfactoria.
