# Seguimiento de empleados orientado a la acción — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convertir `Mi trabajo / Minutas` en una bandeja móvil que prioriza acuerdos activos, despliega sus puntos y separa cierre, revisión e historial sin cambiar backend ni datos.

**Architecture:** Django continuará entregando exactamente el contexto actual. La plantilla renderizará cada acuerdo una sola vez con atributos semánticos; un módulo JavaScript pequeño calculará el segmento visual y controlará pestañas/acordeones. CSS será mobile-first. Las acciones POST, rutas, CSRF y permisos existentes permanecen intactos.

**Tech Stack:** Django 5 templates, vanilla JavaScript, CSS del ERP, Django `TestCase`, PostgreSQL 16, Service Worker ERP network-only.

---

## Estructura de archivos

- `seguimiento/templates/seguimiento/mi_seguimiento.html`: jerarquía, segmentos y tarjeta accesible; no lógica persistente.
- `static/js/seguimiento_mi_trabajo.js`: clasificación visual, conteos, cambio de segmento y acordeón único.
- `static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css`: layout mobile-first y estados.
- `seguimiento/tests.py`: contrato de render y preservación de acciones/permisos.
- `static/erp-sw.js`, `templates/base.html`, `core/templates/core/login.html`, `core/tests.py`: bump coordinado del SW porque cambian template y estáticos visibles.

No se modificarán `seguimiento/views.py`, modelos, migraciones, URLs ni endpoints.

### Task 1: Fijar el contrato visible con pruebas fallidas

**Files:**
- Modify: `seguimiento/tests.py`

- [ ] **Step 1: Agregar pruebas de estructura en `SeguimientoColaboradorTests`**

```python
def test_mi_trabajo_renderiza_bandeja_operativa_y_segmentos(self):
    ventas, _ = Group.objects.get_or_create(name="VENTAS")
    user = get_user_model().objects.create_user(username="empleada.bandeja", password="test12345")
    user.groups.add(ventas)
    self.client.force_login(user)

    response = self.client.get("/seguimiento/minutas/")

    self.assertEqual(response.status_code, 200)
    self.assertContains(response, "Lo que debes atender")
    self.assertContains(response, 'data-work-bucket="attention"')
    self.assertContains(response, 'data-work-bucket="ready"')
    self.assertContains(response, 'data-work-bucket="review"')
    self.assertContains(response, 'data-work-bucket="history"')
    self.assertContains(response, 'src="/static/js/seguimiento_mi_trabajo.js?')

def test_mi_trabajo_conserva_acciones_existentes_en_tarjeta(self):
    ventas, _ = Group.objects.get_or_create(name="VENTAS")
    user = get_user_model().objects.create_user(username="empleada.acciones", password="test12345")
    user.groups.add(ventas)
    item = SeguimientoItem.objects.create(
        titulo="Actualizar proveedores",
        tipo=SeguimientoItem.TIPO_MINUTA,
        estatus=SeguimientoItem.ESTATUS_PENDIENTE,
        responsable_user=user,
    )
    check = SeguimientoChecklistItem.objects.create(seguimiento=item, titulo="Adjuntar listado")
    self.client.force_login(user)

    response = self.client.get("/seguimiento/minutas/")

    self.assertContains(response, reverse("seguimiento:toggle_checklist", args=[item.pk, check.pk]))
    self.assertContains(response, reverse("seguimiento:registrar_feedback", args=[item.pk]))
    self.assertContains(response, reverse("seguimiento:subir_evidencia", args=[item.pk]))
    self.assertContains(response, reverse("seguimiento:solicitar_prorroga", args=[item.pk]))
    self.assertContains(response, f'data-follow-up-id="{item.pk}"')

def test_mi_trabajo_expone_estado_sin_cambiar_asignacion(self):
    ventas, _ = Group.objects.get_or_create(name="VENTAS")
    owner = get_user_model().objects.create_user(username="empleada.owner", password="test12345")
    outsider = get_user_model().objects.create_user(username="empleada.otra", password="test12345")
    owner.groups.add(ventas)
    outsider.groups.add(ventas)
    visible = SeguimientoItem.objects.create(
        titulo="Acuerdo propio visible",
        tipo=SeguimientoItem.TIPO_MINUTA,
        estatus=SeguimientoItem.ESTATUS_COMPLETADO,
        responsable_user=owner,
    )
    SeguimientoItem.objects.create(
        titulo="Acuerdo ajeno oculto",
        tipo=SeguimientoItem.TIPO_MINUTA,
        estatus=SeguimientoItem.ESTATUS_PENDIENTE,
        responsable_user=outsider,
    )
    self.client.force_login(owner)

    response = self.client.get("/seguimiento/minutas/")

    self.assertContains(response, visible.titulo)
    self.assertContains(response, 'data-is-closed="true"')
    self.assertNotContains(response, "Acuerdo ajeno oculto")
```

- [ ] **Step 2: Ejecutar las pruebas y confirmar RED**

Run:

```bash
python manage.py test \
  seguimiento.tests.SeguimientoColaboradorTests.test_mi_trabajo_renderiza_bandeja_operativa_y_segmentos \
  seguimiento.tests.SeguimientoColaboradorTests.test_mi_trabajo_conserva_acciones_existentes_en_tarjeta \
  seguimiento.tests.SeguimientoColaboradorTests.test_mi_trabajo_expone_estado_sin_cambiar_asignacion -v 2
```

Expected: FAIL porque todavía no existen los segmentos, atributos ni el archivo JavaScript.

- [ ] **Step 3: Commit de pruebas RED mediante Claude**

```bash
git add seguimiento/tests.py
git commit -m "test(seguimiento): definir contrato de bandeja operativa personal"
```

### Task 2: Reorganizar la plantilla sin cambiar acciones

**Files:**
- Modify: `seguimiento/templates/seguimiento/mi_seguimiento.html`

- [ ] **Step 1: Reemplazar hero y KPIs por encabezado y segmentos**

Mantener el `<nav class="module-tabs seguimiento-tabs">` existente. Debajo, sustituir el hero, tarjetas de usuario y KPI grid por:

```django
<section class="seg-work-head" aria-labelledby="seg-work-title">
  <div>
    <div class="bi-kicker">Mi trabajo</div>
    <h1 id="seg-work-title">{% if active_tipo %}Lo que debes atender{% else %}Mi trabajo activo{% endif %}</h1>
    <p>{% if active_tipo %}{{ sections.0.title }} asignadas a tu usuario · ordenadas por prioridad{% else %}Acuerdos activos asignados a tu usuario{% endif %}</p>
  </div>
</section>

<nav class="seg-work-buckets" aria-label="Estado del trabajo">
  <button type="button" class="seg-work-bucket is-active" data-work-bucket="attention" aria-pressed="true">
    <span>Por atender</span><span class="seg-work-count" data-work-count="attention">0</span>
  </button>
  <button type="button" class="seg-work-bucket" data-work-bucket="ready" aria-pressed="false">
    <span>Para cerrar</span><span class="seg-work-count" data-work-count="ready">0</span>
  </button>
  <button type="button" class="seg-work-bucket" data-work-bucket="review" aria-pressed="false">
    <span>En revisión</span><span class="seg-work-count" data-work-count="review">0</span>
  </button>
  <button type="button" class="seg-work-bucket" data-work-bucket="history" aria-pressed="false">
    <span>Historial</span><span class="seg-work-count" data-work-count="history">0</span>
  </button>
</nav>

<div class="seg-work-priority" data-priority-callout hidden>
  <div><strong>Empieza por este acuerdo</strong><span data-priority-copy></span></div>
  <span aria-hidden="true">→</span>
</div>
```

- [ ] **Step 2: Convertir cada `seguimiento-item` en tarjeta clasificable**

Conservar un solo loop por `section.items`. En el `<article>` incluir:

```django
<article class="seguimiento-item seg-work-card"
         data-follow-up-id="{{ item.pk }}"
         data-is-closed="{{ item.esta_cerrado|yesno:'true,false' }}"
         data-status="{{ item.estatus }}"
         data-overdue="{{ item.esta_vencido|yesno:'true,false' }}"
         data-due="{% if item.fecha_limite %}{{ item.fecha_limite|date:'c' }}{% endif %}"
         data-checklist-total="{{ item.checklist_total }}"
         data-checklist-done="{{ item.checklist_done }}"
         data-new-dg-response="{{ item.respuesta_dg_nueva|yesno:'true,false' }}">
```

El encabezado de tarjeta debe ser un botón accesible:

```django
<button class="seg-work-card-toggle" type="button"
        aria-expanded="false" aria-controls="seg-work-detail-{{ item.pk }}">
  <span>
    <span class="seg-work-deadline">{{ item.prioridad_label }}{% if item.fecha_limite %} · {{ item.fecha_limite|date:"d/m/Y H:i" }}{% endif %}</span>
    <strong>{{ item.titulo }}</strong>
    <small>{{ item.checklist_done }} de {{ item.checklist_total }} puntos completados</small>
  </span>
  <span class="seg-work-chevron" aria-hidden="true">⌄</span>
</button>
<div class="seg-work-card-detail" id="seg-work-detail-{{ item.pk }}" hidden>
  {% if item.descripcion %}<p class="seguimiento-item-copy">{{ item.descripcion }}</p>{% endif %}
  {% if item.entregable_esperado %}<p class="seguimiento-item-copy seguimiento-deliverable">Entregable: {{ item.entregable_esperado }}</p>{% endif %}
  <div class="bi-progress-track" aria-label="Avance de checklist">
    <span class="bi-progress-fill pd-js-width" data-pd-width="{{ item.progreso_pct }}%"></span>
  </div>
  <div class="seguimiento-checks">
    {% for check in item.checklist.all %}
      {% if check.origen_step_id %}
        <a class="seguimiento-action-button {% if check.completado %}is-done{% endif %}" href="{% url 'seguimiento:detalle' item.pk %}">{{ check.titulo }}</a>
      {% else %}
        <form method="post" action="{% url 'seguimiento:toggle_checklist' item.pk check.pk %}">
          {% csrf_token %}<button class="seguimiento-action-button {% if check.completado %}is-done{% endif %}" type="submit">{{ check.titulo }}</button>
        </form>
      {% endif %}
    {% empty %}<span class="bi-subtle">Sin checklist</span>{% endfor %}
  </div>
</div>
```

La acción primaria se etiqueta `Continuar y adjuntar evidencia`; el formulario de evidencia existente conserva su `action`, `method`, `enctype`, file input y CSRF. Feedback y prórroga se agrupan en:

```django
<details class="seg-work-more-actions">
  <summary>Más acciones</summary>
  <div class="seg-work-more-panel">
    <form class="seguimiento-action-form" method="post" action="{% url 'seguimiento:registrar_feedback' item.pk %}">
      {% csrf_token %}<textarea name="comentario" required></textarea><button type="submit">Enviar avance</button>
    </form>
    <form class="seguimiento-action-form" method="post" action="{% url 'seguimiento:solicitar_prorroga' item.pk %}">
      {% csrf_token %}<input type="date" name="fecha_solicitada" required><textarea name="motivo" required></textarea><button type="submit">Solicitar más tiempo</button>
    </form>
  </div>
</details>
```

- [ ] **Step 3: Agregar el JavaScript versionado**

```django
{% block extra_js %}
<script src="{% static 'js/seguimiento_mi_trabajo.js' %}?v=20260721-action-inbox-v1" defer></script>
{% endblock %}
```

- [ ] **Step 4: Ejecutar pruebas focales**

Run: el comando de Task 1.

Expected: las pruebas de markup y preservación de acciones pasan; la prueba del archivo JS puede seguir fallando hasta Task 3 si Django no encuentra el static solicitado en validaciones adicionales.

- [ ] **Step 5: Commit de plantilla mediante Claude**

```bash
git add seguimiento/templates/seguimiento/mi_seguimiento.html seguimiento/tests.py
git commit -m "feat(seguimiento): reorganizar Mi trabajo como bandeja operativa"
```

### Task 3: Implementar segmentos y acordeón en JavaScript

**Files:**
- Create: `static/js/seguimiento_mi_trabajo.js`

- [ ] **Step 1: Crear el módulo completo**

```javascript
(function () {
  "use strict";

  const root = document.querySelector(".seguimiento-shell");
  if (!root) return;

  const cards = Array.from(root.querySelectorAll(".seg-work-card"));
  const bucketButtons = Array.from(root.querySelectorAll("[data-work-bucket]"));
  const callout = root.querySelector("[data-priority-callout]");
  const calloutCopy = root.querySelector("[data-priority-copy]");

  function asNumber(card, name) {
    return Number(card.dataset[name] || 0);
  }

  function bucketFor(card) {
    if (card.dataset.isClosed === "true") return "history";
    if (card.dataset.status === "EN_REVISION") return "review";
    const total = asNumber(card, "checklistTotal");
    const done = asNumber(card, "checklistDone");
    if (total > 0 && done >= total) return "ready";
    return "attention";
  }

  function priorityFor(card) {
    if (card.dataset.overdue === "true") return 0;
    if (card.dataset.newDgResponse === "true") return 1;
    if (!card.dataset.due) return 4;
    const hours = (new Date(card.dataset.due).getTime() - Date.now()) / 3600000;
    if (hours <= 24) return 1;
    return 2;
  }

  function closeCard(card) {
    const button = card.querySelector(".seg-work-card-toggle");
    const detail = card.querySelector(".seg-work-card-detail");
    if (!button || !detail) return;
    button.setAttribute("aria-expanded", "false");
    detail.hidden = true;
    card.classList.remove("is-open");
  }

  function openCard(card) {
    cards.forEach((candidate) => {
      if (candidate !== card) closeCard(candidate);
    });
    const button = card.querySelector(".seg-work-card-toggle");
    const detail = card.querySelector(".seg-work-card-detail");
    if (!button || !detail) return;
    button.setAttribute("aria-expanded", "true");
    detail.hidden = false;
    card.classList.add("is-open");
  }

  cards.forEach((card) => {
    card.dataset.bucket = bucketFor(card);
    card.dataset.priority = String(priorityFor(card));
    const button = card.querySelector(".seg-work-card-toggle");
    if (button) {
      button.addEventListener("click", () => {
        if (card.classList.contains("is-open")) closeCard(card);
        else openCard(card);
      });
    }
  });

  function selectBucket(bucket) {
    bucketButtons.forEach((button) => {
      const active = button.dataset.workBucket === bucket;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    cards.forEach((card) => {
      const visible = card.dataset.bucket === bucket;
      card.hidden = !visible;
      if (!visible) closeCard(card);
    });
    const visibleCards = cards
      .filter((card) => card.dataset.bucket === bucket)
      .sort((a, b) => Number(a.dataset.priority) - Number(b.dataset.priority));
    visibleCards.forEach((card) => card.parentElement.appendChild(card));
    if (callout) {
      const first = visibleCards[0];
      const urgent = bucket === "attention" && first && Number(first.dataset.priority) <= 1;
      callout.hidden = !urgent;
      if (urgent && calloutCopy) {
        calloutCopy.textContent = first.dataset.overdue === "true"
          ? "Está vencido y requiere tu atención"
          : "Vence pronto o tiene una respuesta nueva";
      }
    }
    if (visibleCards[0]) openCard(visibleCards[0]);
  }

  ["attention", "ready", "review", "history"].forEach((bucket) => {
    const count = cards.filter((card) => card.dataset.bucket === bucket).length;
    const target = root.querySelector(`[data-work-count="${bucket}"]`);
    if (target) target.textContent = String(count);
  });

  bucketButtons.forEach((button) => {
    button.addEventListener("click", () => selectBucket(button.dataset.workBucket));
  });

  selectBucket("attention");
})();
```

- [ ] **Step 2: Añadir una prueba estática del módulo**

En `seguimiento/tests.py`:

```python
def test_script_mi_trabajo_define_segmentos_y_acordeon_unico(self):
    script = (Path(settings.BASE_DIR) / "static" / "js" / "seguimiento_mi_trabajo.js").read_text()
    self.assertIn('return "history"', script)
    self.assertIn('return "review"', script)
    self.assertIn('return "ready"', script)
    self.assertIn('return "attention"', script)
    self.assertIn("if (candidate !== card) closeCard(candidate)", script)
    self.assertIn('selectBucket("attention")', script)
```

Agregar imports si no existen:

```python
from pathlib import Path
from django.conf import settings
```

- [ ] **Step 3: Ejecutar pruebas del módulo**

Run:

```bash
python manage.py test seguimiento --keepdb -v 1
```

Expected: PASS.

- [ ] **Step 4: Commit del comportamiento mediante Claude**

```bash
git add static/js/seguimiento_mi_trabajo.js seguimiento/tests.py
git commit -m "feat(seguimiento): segmentar acuerdos personales por siguiente acción"
```

### Task 4: Aplicar el diseño mobile-first aprobado

**Files:**
- Modify: `static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css`

- [ ] **Step 1: Sustituir el grid de dashboard por bandeja de una columna**

Eliminar reglas del hero, KPI grid, tres columnas y altura fija de listas. Conservar estilos de formularios que sigan en uso. Añadir:

```css
.seguimiento-shell { width: min(100%, 980px); }
.seg-work-head { display:flex; justify-content:space-between; gap:16px; margin:18px 0 12px; }
.seg-work-head h1 { margin:4px 0; font-family:"Playfair Display",serif; font-size:clamp(1.8rem,5vw,2.6rem); color:var(--texto); }
.seg-work-head p { margin:0; color:var(--texto-light); }
.seg-work-buckets { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:4px; padding:4px; border-radius:14px; background:rgba(139,34,82,.09); }
.seg-work-bucket { min-height:48px; border:0; border-radius:11px; background:transparent; color:#705761; font:800 .78rem/1.15 Nunito,sans-serif; cursor:pointer; }
.seg-work-bucket.is-active { background:var(--blanco); color:var(--vino); box-shadow:0 4px 14px rgba(74,37,50,.09); }
.seg-work-count { display:inline-grid; place-items:center; min-width:22px; height:22px; margin-left:4px; padding:0 5px; border-radius:7px; background:rgba(139,34,82,.10); font-variant-numeric:tabular-nums; }
.seg-work-priority { margin:12px 0 9px; padding:11px 13px; border-left:4px solid #d36b30; border-radius:12px; background:#fff0e6; display:flex; justify-content:space-between; align-items:center; }
.seg-work-priority div { display:grid; gap:2px; }
.seg-work-priority span { color:#805e4b; font-size:.82rem; }
.seguimiento-board,.seguimiento-board.is-detail { display:block; }
.seguimiento-column { padding:0; border:0; background:transparent; box-shadow:none; overflow:visible; }
.seguimiento-column>.bi-panel-head,.seguimiento-section-metrics { display:none; }
.seguimiento-list { display:grid; gap:9px; max-height:none; overflow:visible; padding:0; }
.seg-work-card { padding:0; border:1px solid rgba(139,34,82,.16); border-radius:16px; background:rgba(255,255,255,.94); overflow:hidden; box-shadow:0 8px 20px rgba(65,35,45,.06); }
.seg-work-card[hidden] { display:none; }
.seg-work-card.is-open { border-color:rgba(139,34,82,.42); }
.seg-work-card-toggle { width:100%; min-height:64px; padding:12px 14px; border:0; background:transparent; color:var(--texto); display:grid; grid-template-columns:1fr auto; gap:12px; text-align:left; cursor:pointer; }
.seg-work-card-toggle>span:first-child { display:grid; gap:3px; }
.seg-work-card-toggle strong { font-size:1rem; line-height:1.25; }
.seg-work-card-toggle small { color:var(--texto-light); }
.seg-work-deadline { color:var(--vino); font-size:.72rem; font-weight:900; letter-spacing:.06em; text-transform:uppercase; }
.seg-work-chevron { color:var(--vino); font-size:1.1rem; transition:transform 160ms ease-out; }
.seg-work-card.is-open .seg-work-chevron { transform:rotate(180deg); }
.seg-work-card-detail { padding:0 14px 14px; }
.seg-work-card-detail[hidden] { display:none; }
.seg-work-more-actions { margin-top:10px; }
.seg-work-more-actions>summary { min-height:44px; display:grid; place-items:center; color:var(--vino); font-weight:800; cursor:pointer; }
.seg-work-more-panel { display:grid; gap:10px; padding-top:8px; }
.seguimiento-empty { padding:28px 16px; border-radius:15px; background:rgba(255,255,255,.72); }
@media (max-width:760px) {
  .seguimiento-shell { width:100%; }
  .seg-work-head { margin-top:12px; }
  .seg-work-buckets { display:flex; overflow-x:auto; scroll-snap-type:x proximity; }
  .seg-work-bucket { flex:0 0 104px; scroll-snap-align:start; }
  .seg-work-card-toggle,.seg-work-card-detail { padding-left:12px; padding-right:12px; }
}
@media (prefers-reduced-motion:reduce) { .seg-work-chevron { transition:none; } }
```

- [ ] **Step 2: Ejecutar `git diff --check` y pruebas**

Run:

```bash
git diff --check
python manage.py test seguimiento --keepdb -v 1
```

Expected: sin whitespace errors; pruebas PASS.

- [ ] **Step 3: Commit visual mediante Claude**

```bash
git add static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css
git commit -m "style(seguimiento): priorizar bandeja móvil de acuerdos"
```

### Task 5: Versionar PWA y validar regresiones

**Files:**
- Modify: `static/erp-sw.js`
- Modify: `templates/base.html`
- Modify: `core/templates/core/login.html`
- Modify: `core/tests.py`

- [ ] **Step 1: Actualizar versión coordinada**

En `static/erp-sw.js`:

```javascript
const CACHE_NAME = "pollyanas-erp-shell-v19-seguimiento-action-inbox";
```

En ambos registros de SW (`templates/base.html` y `core/templates/core/login.html`):

```javascript
navigator.serviceWorker.register('/erp-sw.js?v=20260721-seguimiento-action-inbox-v19')
```

Actualizar las aserciones exactas de `core.tests.HallmarkGuardrailsStaticTests` para esperar esos dos valores.

- [ ] **Step 2: Ejecutar suite de regresión**

Run:

```bash
python manage.py migrate --check
python manage.py check
python manage.py test core.tests.NavigationActiveStateTests core.tests.HallmarkGuardrailsStaticTests seguimiento --keepdb -v 1
git diff --check
```

Expected: migraciones pendientes 0; check 0 errores; tests PASS. El warning preexistente `logistica.W911` puede aparecer y debe reportarse, no resolverse en esta tarea.

- [ ] **Step 3: Commit del bump mediante Claude**

```bash
git add static/erp-sw.js templates/base.html core/templates/core/login.html core/tests.py
git commit -m "chore(pwa): renovar shell por bandeja de seguimiento"
```

### Task 6: Validación real y cierre

**Files:**
- No new files.

- [ ] **Step 1: Levantar PostgreSQL aislado y servidor local**

Usar un `COMPOSE_PROJECT_NAME` y puerto libres, aplicar todas las migraciones de `main`, ejecutar `migrate --check` y `check`, y arrancar Django con PostgreSQL. No usar SQLite.

- [ ] **Step 2: Preparar datos locales representativos**

Crear solo en la base local aislada un empleado con:

- un acuerdo vencido con checklist parcial;
- un acuerdo con checklist completo para `Para cerrar`;
- uno `EN_REVISION`;
- uno `COMPLETADO`;
- uno `CANCELADO`;
- un acuerdo asignado a otro usuario para verificar aislamiento.

- [ ] **Step 3: Validar en navegador móvil y escritorio**

En 390×844 y 1440×900 comprobar:

- `Por atender` es la vista inicial;
- completados/cancelados solo aparecen en Historial;
- un solo acordeón queda abierto;
- los puntos son legibles y accionables;
- cada segmento actualiza su conteo;
- las acciones preservan inputs, posición y toast;
- el usuario ajeno no ve el acuerdo;
- consola sin errores;
- documentos/XHR/fetch sin 4xx/5xx inesperados;
- el SW activo usa `v19-seguimiento-action-inbox`.

- [ ] **Step 4: Revisar diff e higiene**

Run:

```bash
git status --short --branch
git log --oneline --decorate -8
git diff origin/main..HEAD --stat
git worktree list
git worktree prune --dry-run
```

Expected: solo los archivos declarados, rama limpia, sin artefactos visuales o temporales.

- [ ] **Step 5: Entregar a Claude para revisión, PR y deploy**

Claude debe revisar el diff, correr checks frescos, hacer push, crear PR draft y seguir el protocolo completo: merge a `main`, `bash scripts/deploy_web_safe.sh` en VPS sin `git pull` manual previo, verificación de producción con un empleado real y limpieza de rama/worktree. No declarar terminado antes de la validación visible en producción.
