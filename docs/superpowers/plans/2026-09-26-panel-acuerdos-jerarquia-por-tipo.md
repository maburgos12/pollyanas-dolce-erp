# Panel de acuerdos con jerarquía por tipo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hacer que Minutas, Compromisos y Proyectos sean el primer nivel del Panel de acuerdos y que cada tipo tenga sus propios estados, conteos y responsables sin mezclar categorías.

**Architecture:** La vista Django resolverá el tipo activo antes de calcular estados y agrupaciones. La plantilla renderizada por servidor mostrará un selector primario de tipo y una navegación secundaria de estados; se conservarán ruta, permisos, acciones y modelos existentes. El cambio visible actualizará la versión del Service Worker global para invalidar la interfaz anterior instalada.

**Tech Stack:** Django 5, plantillas Django, CSS del ERP, JavaScript progresivo existente, PostgreSQL 16, `django.test.TestCase`.

---

## Estructura de archivos

- `seguimiento/views.py`: resolver `tab`, construir navegación de tipos, limitar los conteos de estado al tipo activo y agrupar solo el resultado final.
- `seguimiento/templates/seguimiento/panel_dg.html`: selector primario de tipo, estados contextuales y textos específicos del tipo.
- `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css`: tarjetas de tipo, estados activos, responsive y foco visible.
- `seguimiento/tests.py`: regresiones de separación, conteos, URLs y renderizado.
- `static/erp-sw.js`: nueva identidad de caché del shell global.
- `templates/base.html`: registrar la nueva versión del Service Worker.
- `core/templates/core/login.html`: registrar la misma versión antes de autenticar.
- `core/tests.py`: fijar el contrato de versión compartida.

### Task 1: Especificar la separación por tipo con pruebas fallidas

**Files:**
- Modify: `seguimiento/tests.py:581-650`
- Test: `seguimiento/tests.py`

- [ ] **Step 1: Reemplazar la expectativa del tablero global por el selector primario**

Actualizar `test_panel_dg_renderiza_resumen_compacto_y_estados_ejecutivos` para exigir la nueva jerarquía:

```python
def test_panel_dg_renderiza_tipos_antes_de_estados(self):
    dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
    dg_user = get_user_model().objects.create_user(
        username="mauricio.resumen",
        password="test12345",
    )
    dg_user.groups.add(dg_group)
    self.client.force_login(dg_user)

    response = self.client.get("/seguimiento/panel/")

    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.context["active_tab"], SeguimientoItem.TIPO_MINUTA)
    self.assertContains(response, 'class="panel-dg-type-nav"')
    self.assertContains(response, 'data-panel-type="MINUTA"')
    self.assertContains(response, 'data-panel-type="COMPROMISO"')
    self.assertContains(response, 'data-panel-type="PROYECTO"')
    self.assertContains(response, 'aria-label="Estados de Minutas"')
    self.assertNotContains(response, 'class="panel-dg-mini-dashboard"')
    self.assertNotContains(response, "¿Qué quieres revisar?")
```

- [ ] **Step 2: Añadir una prueba de conteos de estado aislados por tipo**

Crear tres elementos con estados que revelarían una suma cruzada:

```python
def test_panel_dg_calcula_estados_solo_para_el_tipo_activo(self):
    dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
    dg_user = get_user_model().objects.create_user(
        username="mauricio.tipos",
        password="test12345",
    )
    dg_user.groups.add(dg_group)
    now = timezone.now()
    minuta_vencida = SeguimientoItem.objects.create(
        tipo=SeguimientoItem.TIPO_MINUTA,
        titulo="Minuta vencida separada",
        fecha_limite=now - timedelta(days=2),
        estatus=SeguimientoItem.ESTATUS_PENDIENTE,
    )
    compromiso_vencido = SeguimientoItem.objects.create(
        tipo=SeguimientoItem.TIPO_COMPROMISO,
        titulo="Compromiso vencido separado",
        fecha_limite=now - timedelta(days=3),
        estatus=SeguimientoItem.ESTATUS_PENDIENTE,
    )
    SeguimientoItem.objects.create(
        tipo=SeguimientoItem.TIPO_PROYECTO,
        titulo="Proyecto activo separado",
        fecha_limite=now + timedelta(days=5),
        estatus=SeguimientoItem.ESTATUS_EN_PROCESO,
    )
    self.client.force_login(dg_user)

    minutas = self.client.get("/seguimiento/panel/?tab=MINUTA&estado=vencidos")
    compromisos = self.client.get("/seguimiento/panel/?tab=COMPROMISO&estado=vencidos")

    self.assertEqual(minutas.context["dashboard_counts"]["vencidos"], 1)
    self.assertEqual([item.pk for item in minutas.context["items_estado"]], [minuta_vencida.pk])
    self.assertContains(minutas, "Minuta vencida separada")
    self.assertNotContains(minutas, "Compromiso vencido separado")
    self.assertEqual(compromisos.context["dashboard_counts"]["vencidos"], 1)
    self.assertEqual(
        [item.pk for item in compromisos.context["items_estado"]],
        [compromiso_vencido.pk],
    )
```

- [ ] **Step 3: Añadir una prueba de URLs que conservan tipo y estado**

```python
def test_panel_dg_navegacion_tipo_y_estado_conserva_contexto(self):
    dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
    dg_user = get_user_model().objects.create_user(
        username="mauricio.urls.tipos",
        password="test12345",
    )
    dg_user.groups.add(dg_group)
    self.client.force_login(dg_user)

    response = self.client.get("/seguimiento/panel/?tab=PROYECTO&estado=activos")

    self.assertEqual(response.context["active_tab"], SeguimientoItem.TIPO_PROYECTO)
    self.assertEqual(response.context["active_estado"], "activos")
    state_urls = {item["key"]: item["url"] for item in response.context["state_nav"]}
    type_urls = {item["key"]: item["url"] for item in response.context["type_nav"]}
    self.assertIn("tab=PROYECTO", state_urls["vencidos"])
    self.assertIn("estado=vencidos", state_urls["vencidos"])
    self.assertIn("tab=COMPROMISO", type_urls[SeguimientoItem.TIPO_COMPROMISO])
    self.assertIn("estado=activos", type_urls[SeguimientoItem.TIPO_COMPROMISO])
```

- [ ] **Step 4: Ejecutar las pruebas nuevas y confirmar que fallan por la jerarquía actual**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py test \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_renderiza_tipos_antes_de_estados \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_calcula_estados_solo_para_el_tipo_activo \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_navegacion_tipo_y_estado_conserva_contexto
```

Expected: FAIL porque no existe `type_nav`, el tipo predeterminado todavía es vacío y los estados se calculan antes de filtrar por tipo.

- [ ] **Step 5: Confirmar solo las pruebas**

```bash
git add seguimiento/tests.py
git commit -m "test(seguimiento): exigir estados separados por tipo"
```

### Task 2: Resolver tipo antes de calcular estados y responsables

**Files:**
- Modify: `seguimiento/views.py:1116-1313`
- Test: `seguimiento/tests.py`

- [ ] **Step 1: Definir el catálogo visible de tipos**

Junto a `PANEL_ESTADOS`, añadir:

```python
PANEL_TIPOS = {
    SeguimientoItem.TIPO_MINUTA: "Minutas",
    SeguimientoItem.TIPO_COMPROMISO: "Compromisos",
    SeguimientoItem.TIPO_PROYECTO: "Proyectos",
}
```

- [ ] **Step 2: Resolver el tipo activo con compatibilidad para `tipo`**

Al inicio de `panel_dg`, reemplazar la resolución separada de `filtro_tipo` por:

```python
active_tab = (
    request.GET.get("tab")
    or request.GET.get("tipo")
    or SeguimientoItem.TIPO_MINUTA
).strip().upper()
if active_tab not in PANEL_TIPOS:
    active_tab = SeguimientoItem.TIPO_MINUTA
filtro_tipo = active_tab
```

No aplicar `qs.filter(tipo=filtro_tipo)` antes de obtener `items_base`; el conjunto base debe conservar los tres tipos para construir el selector primario.

- [ ] **Step 3: Construir navegación de tipos antes del filtro de estado**

Después de calcular los atributos visuales de `items_base`, añadir:

```python
type_counts = {
    tipo: sum(1 for item in items_base if item.tipo == tipo)
    for tipo in PANEL_TIPOS
}
type_nav = []
for tipo, label in PANEL_TIPOS.items():
    params = request.GET.copy()
    params.pop("tipo", None)
    params["tab"] = tipo
    params["estado"] = active_estado if active_estado in PANEL_ESTADOS else "vencidos"
    type_nav.append({
        "key": tipo,
        "label": label,
        "count": type_counts[tipo],
        "url": f"?{params.urlencode()}",
    })
```

Para evitar usar `active_estado` antes de definirlo, resolver `active_estado` justo antes de crear `type_nav`, conservando la normalización existente de buckets.

- [ ] **Step 4: Calcular buckets y estados sobre el tipo activo**

Reemplazar el orden actual por:

```python
items_tipo = [item for item in items_base if item.tipo == active_tab]
bucket_counts = {
    bucket: sum(1 for item in items_tipo if item.visual_bucket == bucket)
    for bucket in PANEL_BUCKETS
}
bucket_nav = [
    {"key": bucket, "label": label, "count": bucket_counts[bucket]}
    for bucket, label in PANEL_BUCKETS.items()
]
items_scope = (
    [item for item in items_tipo if item.visual_bucket == active_bucket]
    if active_bucket
    else items_tipo
)
dashboard_counts = {
    estado: sum(1 for item in items_scope if _item_en_estado_panel(item, estado))
    for estado in PANEL_ESTADOS
}
```

Al construir `state_nav`, fijar `params["tab"] = active_tab` y `params["estado"] = estado`. Después obtener `items` exclusivamente desde `items_scope`:

```python
items = [item for item in items_scope if _item_en_estado_panel(item, active_estado)]
```

Eliminar el filtro tardío que deja `active_tab = ""` y filtra `items` después de calcular `dashboard_counts`.

- [ ] **Step 5: Exponer contexto específico del tipo**

Añadir al `render`:

```python
"type_nav": type_nav,
"type_counts": type_counts,
"active_type_label": PANEL_TIPOS[active_tab],
```

Mantener `active_tab`, `state_nav`, `dashboard_counts`, `items_estado` y `colaboradores_estado` para no romper consumidores existentes.

- [ ] **Step 6: Ejecutar las pruebas específicas**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py test \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_renderiza_tipos_antes_de_estados \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_calcula_estados_solo_para_el_tipo_activo \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_navegacion_tipo_y_estado_conserva_contexto
```

Expected: las pruebas de contexto y conteos pasan; la prueba de renderizado aún puede fallar hasta modificar la plantilla.

- [ ] **Step 7: Confirmar la lógica de vista**

```bash
git add seguimiento/views.py
git commit -m "feat(seguimiento): calcular estados por tipo de acuerdo"
```

### Task 3: Renderizar la jerarquía tipo → estado → persona

**Files:**
- Modify: `seguimiento/templates/seguimiento/panel_dg.html:8-125`
- Modify: `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css:1-285`
- Test: `seguimiento/tests.py`

- [ ] **Step 1: Cambiar encabezado y versión del CSS**

En `panel_dg.html`, usar:

```html
<link rel="stylesheet" href="{% static 'css/template_modules/seguimiento-templates-seguimiento-panel-dg.css' %}?v=20260926-panel-tipos-v1">
```

Y cambiar el subtítulo a:

```html
<p>Minutas, compromisos y proyectos, cada uno con su propio seguimiento.</p>
```

- [ ] **Step 2: Sustituir el tablero global por el selector primario**

Eliminar `panel-dg-mini-dashboard` y renderizar:

```html
<nav class="panel-dg-type-nav" aria-label="Tipo de acuerdo">
  {% for type in type_nav %}
  <a href="{{ type.url }}"
     class="panel-dg-type-link {% if active_tab == type.key %}is-active{% endif %}"
     data-panel-type="{{ type.key }}"
     {% if active_tab == type.key %}aria-current="page"{% endif %}>
    <span class="panel-dg-type-initial" aria-hidden="true">{{ type.label|slice:":1" }}</span>
    <span class="panel-dg-type-copy">
      <strong>{{ type.label }}</strong>
      <small>{% if type.key == "MINUTA" %}Acuerdos de reuniones{% elif type.key == "COMPROMISO" %}Tareas y entregables{% else %}Seguimiento por proyecto{% endif %}</small>
    </span>
    <span class="panel-dg-type-count">{{ type.count }}</span>
  </a>
  {% endfor %}
</nav>
```

- [ ] **Step 3: Hacer explícito que los estados pertenecen al tipo**

Antes de `panel-dg-state-nav`, añadir:

```html
<section class="panel-dg-type-summary" aria-labelledby="panel-dg-type-title">
  <div>
    <h2 id="panel-dg-type-title">{{ active_type_label }}</h2>
    <p>Estados y conteos exclusivos de {{ active_type_label|lower }}.</p>
  </div>
  <strong>{{ type_counts|get_item:active_tab }}</strong>
</section>
```

No introducir un filtro de plantilla inexistente. En lugar de `get_item`, la vista debe incluir `active_type_count = type_counts[active_tab]` y la plantilla debe usar `{{ active_type_count }}`.

Actualizar el `aria-label` del nav:

```html
<nav class="panel-dg-state-nav" aria-label="Estados de {{ active_type_label }}">
```

- [ ] **Step 4: Contextualizar la lista por persona**

Usar:

```html
<div class="bi-kicker">{{ active_type_label }} · {{ active_estado_label }}</div>
<h2 id="panel-dg-people-title">Por persona</h2>
```

Cambiar los totales genéricos por:

```html
<span>{{ items_estado|length }} en este estado</span>
```

Y en cada persona:

```html
<strong>{{ colab.nombre }} · <em>{{ colab.count }} {{ active_type_label|lower }}</em></strong>
```

En el vacío mostrar:

```html
<strong>No hay {{ active_type_label }} en {{ active_estado_label|lower }}</strong>
<span>Selecciona otro estado o tipo para continuar.</span>
```

- [ ] **Step 5: Sincronizar filtros y enlaces de auditoría con `tab`**

Retirar el `<select name="tipo">` de `Filtros y auditoría`. Añadir:

```html
<input type="hidden" name="tab" value="{{ active_tab }}">
```

El enlace `Limpiar` debe ser:

```html
<a href="{% url 'seguimiento:panel_dg' %}?tab={{ active_tab }}&estado={{ active_estado }}"
   class="btn btn-secondary btn-sm">Limpiar</a>
```

Los enlaces de auditoría deben incluir `tab={{ active_tab }}`.

- [ ] **Step 6: Añadir estilos de los tipos y retirar los del tablero eliminado**

Eliminar `.panel-dg-mini-dashboard`, `.panel-dg-mini-cell` y `.panel-dg-mini-icon`. Añadir:

```css
.panel-dg-type-nav {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 18px;
}

.panel-dg-type-link {
  display: grid;
  grid-template-columns: 42px minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  min-height: 78px;
  padding: 14px 16px;
  border: 1px solid color-mix(in srgb, var(--vino) 16%, transparent);
  border-radius: 14px;
  background: var(--blanco);
  color: var(--texto);
  text-decoration: none;
}

.panel-dg-type-link.is-active {
  border-color: var(--vino);
  background: var(--vino);
  color: white;
  box-shadow: 0 12px 28px rgba(139, 34, 82, .18);
}

.panel-dg-type-initial {
  display: grid;
  width: 42px;
  height: 42px;
  place-items: center;
  border-radius: 11px;
  background: color-mix(in srgb, var(--rosa) 70%, white);
  color: var(--vino);
  font-family: "Playfair Display", serif;
  font-weight: 900;
}

.panel-dg-type-copy { display: grid; gap: 3px; min-width: 0; }
.panel-dg-type-copy strong { font-size: .95rem; }
.panel-dg-type-copy small { color: var(--texto-light); font-size: .75rem; }
.panel-dg-type-link.is-active .panel-dg-type-copy small { color: rgba(255,255,255,.78); }
.panel-dg-type-count { padding: 3px 8px; border-radius: 999px; background: var(--rosa); color: var(--vino); font-size: .78rem; font-weight: 900; }
.panel-dg-type-link.is-active .panel-dg-type-count { background: white; }
.panel-dg-type-link:focus-visible { outline: 3px solid var(--dorado); outline-offset: 2px; }

.panel-dg-type-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 16px 18px;
  border: 1px solid color-mix(in srgb, var(--vino) 14%, transparent);
  border-bottom: 0;
  border-radius: 14px 14px 0 0;
  background: color-mix(in srgb, var(--rosa) 24%, var(--blanco));
}
.panel-dg-type-summary h2 { margin: 0; font: 700 1.2rem "Playfair Display", serif; }
.panel-dg-type-summary p { margin: 3px 0 0; color: var(--texto-light); font-size: .78rem; }
.panel-dg-type-summary > strong { color: var(--vino); font-size: 1.75rem; }
```

En móvil usar:

```css
@media (max-width: 700px) {
  .panel-dg-type-nav { grid-template-columns: 1fr; }
  .panel-dg-type-link { min-height: 68px; }
}
```

- [ ] **Step 7: Ejecutar las pruebas del panel**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py test seguimiento.tests.SeguimientoColaboradorTests
```

Expected: PASS.

- [ ] **Step 8: Confirmar plantilla y CSS**

```bash
git add seguimiento/templates/seguimiento/panel_dg.html \
  static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css \
  seguimiento/tests.py
git commit -m "feat(seguimiento): priorizar tipo antes del estado"
```

### Task 4: Invalidar el shell PWA global

**Files:**
- Modify: `static/erp-sw.js:1`
- Modify: `templates/base.html:1239-1242`
- Modify: `core/templates/core/login.html:59-61`
- Modify: `core/tests.py:153-185`
- Test: `core/tests.py`

- [ ] **Step 1: Cambiar las pruebas al nuevo identificador compartido**

Reemplazar cada aparición de `20260923-jornadas-semanales-v4` en `core/tests.py` por `20260926-seguimiento-panel-tipos-v5`.

- [ ] **Step 2: Ejecutar las pruebas PWA y confirmar que fallan**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py test \
  core.tests.HallmarkGuardrailsStaticTests.test_base_template_makes_erp_installable_as_pwa \
  core.tests.HallmarkGuardrailsStaticTests.test_login_template_also_exposes_pwa_install_metadata \
  core.tests.HallmarkGuardrailsStaticTests.test_erp_pwa_manifest_and_service_worker_are_minimal
```

Expected: FAIL porque los archivos servidos todavía contienen la versión anterior.

- [ ] **Step 3: Actualizar worker y registros**

Usar exactamente el mismo identificador en los tres archivos:

```javascript
const CACHE_NAME = "pollyanas-erp-shell-20260926-seguimiento-panel-tipos-v5";
```

```javascript
navigator.serviceWorker.register('/erp-sw.js?v=20260926-seguimiento-panel-tipos-v5').catch(function () {});
```

- [ ] **Step 4: Repetir las pruebas PWA**

Run: el mismo comando del Step 2.

Expected: PASS.

- [ ] **Step 5: Confirmar el bump coordinado**

```bash
git add static/erp-sw.js templates/base.html core/templates/core/login.html core/tests.py
git commit -m "chore(pwa): invalidar shell para panel por tipo"
```

### Task 5: Validación integrada y navegador

**Files:**
- Verify: `seguimiento/views.py`
- Verify: `seguimiento/templates/seguimiento/panel_dg.html`
- Verify: `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css`
- Verify: `static/erp-sw.js`

- [ ] **Step 1: Ejecutar validaciones Django y migraciones**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py migrate --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py check
```

Expected: cero migraciones pendientes y cero errores.

- [ ] **Step 2: Ejecutar suites afectadas**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py test seguimiento core
```

Expected: PASS.

- [ ] **Step 3: Levantar la aplicación local y preparar un DG de prueba**

Crear exclusivamente datos locales en la base aislada: usuario DG, elementos de los tres tipos y estados diferentes. Iniciar Django en un puerto libre:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55450/pastelerias_erp \
python3 manage.py runserver 127.0.0.1:8017
```

- [ ] **Step 4: Validar escritorio y móvil en navegador real**

Comprobar en `/seguimiento/panel/`:

- Minutas abre por defecto;
- no aparece `¿Qué quieres revisar?`;
- los conteos de estados cambian al seleccionar Compromisos y Proyectos;
- la lista y cada acordeón solo contienen el tipo activo;
- cambiar de estado conserva `tab` en la URL;
- los filtros y auditoría conservan el tipo;
- viewport de escritorio y `390x844` sin recortes;
- foco visible y navegación por teclado;
- consola sin errores y solicitudes sin respuestas fallidas;
- worker servido y registrado con `20260926-seguimiento-panel-tipos-v5`.

- [ ] **Step 5: Revisar diff y confirmar estado limpio**

```bash
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git status --short --branch
git log --oneline --decorate -8
git worktree list
```

Expected: solo archivos del alcance, sin cambios sin confirmar ni artefactos temporales.

### Task 6: PR, CI, despliegue y validación de producción

**Files:**
- Verify: all files changed by Tasks 1-5

- [ ] **Step 1: Subir la rama y abrir PR en borrador**

El PR debe incluir resumen funcional, archivos principales, pruebas, validación de navegador y confirmación de que no hay migraciones.

- [ ] **Step 2: Esperar CI obligatorio y revisar el diff completo**

Expected: checks requeridos en verde y ningún archivo ajeno al Panel de acuerdos o al bump coordinado del Service Worker.

- [ ] **Step 3: Mergear a `main`**

Solo después de CI verde y revisión final.

- [ ] **Step 4: Desplegar por el flujo oficial**

En el VPS, sin ejecutar `git pull` manual antes:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Expected: migraciones sin pendientes, checks correctos, estáticos recolectados y servicio web disponible.

- [ ] **Step 5: Validar producción autenticada**

Comprobar como Dirección General:

- selector de Minutas, Compromisos y Proyectos como primer nivel;
- ausencia de totales de estados mezclados;
- estados y responsables exclusivos del tipo activo;
- persistencia de `tab` y `estado` en URL;
- responsive `390x844` y escritorio;
- consola, red, CSS servido y Service Worker actualizado.

- [ ] **Step 6: Cerrar rama y worktree**

Después del merge, deploy y validación:

```bash
bash scripts/task_workspace_close.sh --state merged
```

Expected: tarea cerrada, worktree y ramas exactas retiradas, y `origin` podado.
