# Acciones sin perder contexto Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mantener posición, captura y contexto al ejecutar acciones mutantes, empezando por Revisión administrativa de entregas.

**Architecture:** Mejora progresiva sobre formularios POST existentes. Un helper global opt-in negocia JSON con el mismo endpoint; el fallback HTML conserva redirect y añade fragmento de contexto.

**Tech Stack:** Django 5, templates, JavaScript sin framework, CSS global, Django TestCase y navegador Chromium.

---

### Task 1: Contrato backend y fallback del piloto

**Files:** `logistica/tests.py`, `logistica/views.py`, `logistica/templates/logistica/ruta_detail.html`

- [ ] Escribir pruebas que exijan JSON en éxito/error y redirect con fragmento para HTML.
- [ ] Ejecutarlas y comprobar que fallan por el contrato ausente.
- [ ] Añadir negociación de respuesta sin duplicar `revisar_entrega_excepcional`.
- [ ] Ejecutar las pruebas focalizadas hasta verde.

### Task 2: Toast y helper asíncrono global

**Files:** `templates/base.html`, `static/css/styles.css`, `static/js/erp_actions.js`, `core/tests.py`

- [ ] Escribir pruebas de integración de markup, carga del helper y mensajes como toast.
- [ ] Verificar fallo previo.
- [ ] Implementar región accesible, ciclo de vida, bloqueo del submitter y prevención de doble envío.
- [ ] Verificar reducción de movimiento, safe area móvil y error persistente.

### Task 3: Actualización local del piloto

**Files:** `logistica/tests.py`, `logistica/templates/logistica/ruta_detail.html`, `static/js/erp_actions.js`

- [ ] Probar identificador estable, conservación del textarea y contrato de reemplazo de fila.
- [ ] Implementar formulario opt-in y fragmento HTML de la fila devuelto por JSON.
- [ ] Comprobar que solo cambia la revisión afectada.

### Task 4: Confirmación accesible y documentación permanente

**Files:** `templates/base.html`, `static/js/erp_actions.js`, `AGENTS.md`, `claude.md`, `docs/ux/action-context-coverage.md`

- [ ] Añadir pruebas del contrato modal y documentación sincronizada.
- [ ] Implementar modal solo para formularios destructivos opt-in.
- [ ] Registrar inventario inicial, piloto cubierto y módulos pendientes.

### Task 5: Verificación y entrega

- [ ] Ejecutar pruebas focalizadas, `manage.py check` y `migrate --check` en `erp_actions_context`.
- [ ] Validar el piloto en Chromium: éxito, error, doble clic, consola, Network y fallback sin JS.
- [ ] Revisar diff, commit quirúrgico y PR draft de la primera etapa.
- [ ] Mergear, desplegar con `scripts/deploy_web_safe.sh` y validar producción antes de abrir la etapa siguiente.

