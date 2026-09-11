# Mensaje visible al fallar el inicio de turno Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar de forma inmediata y persistente cualquier error al abrir turno, sin perder la captura y permitiendo reintentar.

**Architecture:** Mantener el contrato actual de la API y normalizar el mensaje en `guardarSalida`. Guardar solo estado efímero de envío/error en el borrador de la PWA, renderizarlo junto al botón y versionar el service worker para entregar el cambio a los teléfonos.

**Tech Stack:** Django templates, JavaScript sin framework, Django TestCase y service worker de Logística.

---

### Task 1: Cubrir la regresión del formulario

**Files:**
- Modify: `logistica/tests.py`

- [ ] **Step 1: Escribir una prueba que exija estado de envío, aviso accesible y recuperación ante error.**
- [ ] **Step 2: Ejecutar la prueba específica y confirmar que falla porque la plantilla actual no contiene esas garantías.**
- [ ] **Step 3: Conservar la prueba como contrato del flujo visible.**

### Task 2: Implementar feedback y reintento

**Files:**
- Modify: `logistica/templates/logistica/pwa.html`

- [ ] **Step 1: Añadir `salida_enviando` y `salida_error` al borrador existente.**
- [ ] **Step 2: Renderizar `salida_error` con `role="alert"` inmediatamente antes del botón.**
- [ ] **Step 3: Bloquear el botón únicamente durante el POST y mostrar “Iniciando…”.**
- [ ] **Step 4: En respuestas HTTP fallidas y excepciones de red, conservar el mensaje, limpiar el estado de envío y renderizar sin perder los campos.**
- [ ] **Step 5: Ejecutar la prueba específica y confirmar que pasa.**

### Task 3: Entregar la PWA nueva y verificar

**Files:**
- Modify: `logistica/static/logistica/pwa/sw.js`
- Modify: `logistica/templates/logistica/pwa.html`
- Modify: `logistica/tests.py`

- [ ] **Step 1: Cambiar `CACHE_NAME` y el parámetro de registro a `v90-mensaje-error-turno`.**
- [ ] **Step 2: Actualizar las aserciones de versión existentes.**
- [ ] **Step 3: Ejecutar pruebas enfocadas de Logística.**
- [ ] **Step 4: Ejecutar `python manage.py migrate --check` y `python manage.py check` con PostgreSQL aislado.**
- [ ] **Step 5: Revisar diff, commit, PR, CI, merge y despliegue oficial.**
- [ ] **Step 6: Validar en producción la plantilla, el service worker y la respuesta visual del flujo.**
