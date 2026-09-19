# Captura compacta de conteos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capturar productos e insumos en renglones móviles compactos y agregar excepciones desde el catálogo durante una ronda abierta.

**Architecture:** Extender el servicio transaccional de acciones con `agregar`, exponer una consulta GET acotada al conteo y reutilizar la sustitución asíncrona de `#conteo-detail`. El formulario enviará las lecturas visibles junto con el artículo nuevo para conservar el borrador.

**Tech Stack:** Django 5, plantillas Django, JavaScript sin framework, CSS responsivo, PostgreSQL 16.

---

### Task 1: Contrato transaccional

**Files:**
- Modify: `inventario/services_conteos.py`
- Test: `inventario/tests_conteos_domain.py`

- [ ] Probar que `agregar` guarda lecturas, resuelve la unidad oficial, crea línea y lectura de la ronda, registra evento y es idempotente.
- [ ] Probar rechazo de duplicados, artículos sin unidad, permisos y estados cerrados.
- [ ] Implementar un resolvedor compartido de artículo y la acción atómica.

### Task 2: Endpoint de búsqueda

**Files:**
- Modify: `inventario/views_conteos.py`
- Modify: `inventario/urls_conteos.py`
- Test: `inventario/tests_conteos_views.py`

- [ ] Probar búsqueda por tipo, nombre y código, exclusión de líneas existentes y permisos.
- [ ] Exponer JSON limitado con clave, nombre, código, unidad y disponibilidad.
- [ ] Aceptar `agregar` dentro del contrato asíncrono existente.

### Task 3: Captura móvil

**Files:**
- Modify: `templates/inventario/conteos/_detalle.html`
- Modify: `static/inventario/conteos.js`
- Modify: `static/inventario/conteos.css`
- Modify: `templates/inventario/conteos/detalle.html`
- Modify: `static/operacion/sw.js`
- Test: `inventario/tests_conteos_views.py`

- [ ] Renderizar selectores Productos/Insumos con conteos y una sola sección visible.
- [ ] Convertir cada artículo en un renglón compacto con cantidad alineada.
- [ ] Añadir búsqueda y agregado sin HTML no confiable.
- [ ] Actualizar versiones estáticas y caché PWA.
- [ ] Verificar a 390 px, accesibilidad, consola y solicitudes de red.

### Task 4: Cierre

- [ ] Ejecutar pruebas del módulo, `manage.py check` y `migrate --check` con PostgreSQL.
- [ ] Revisar diff, commit, PR, CI, merge, despliegue oficial y producción.
- [ ] Validar en producción sin crear conteos ni cantidades de prueba.
