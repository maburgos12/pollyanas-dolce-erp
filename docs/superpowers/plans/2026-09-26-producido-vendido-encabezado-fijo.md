# Encabezado fijo de Producido vs Vendido Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Alinear cada encabezado con los datos de su columna y mantenerlo visible durante la lectura vertical de la tabla.

**Architecture:** La tabla seguirá siendo una sola tabla HTML con `colgroup`. El contenedor existente se convertirá en el área de desplazamiento de ambos ejes y las celdas del `thead` serán fijas dentro de él; reglas específicas del reporte restablecerán la alineación numérica y centrada que hoy sobrescribe el estilo global.

**Tech Stack:** Django templates, CSS, Django TestCase, navegador Chromium.

---

### Task 1: Cubrir el contrato visual con una prueba de regresión

**Files:**
- Modify: `reportes/tests_producido_vs_vendido.py`

- [ ] Añadir una prueba que lea el template y `static/css/styles.css` y exija `scope="col"`, encabezados de estado centrados, `position: sticky`, desplazamiento vertical y reglas específicas para `.text-end` y `.text-center`.
- [ ] Ejecutar solo esa prueba y confirmar que falla porque el contrato todavía no existe.

### Task 2: Implementar encabezado fijo y alineación coherente

**Files:**
- Modify: `reportes/templates/reportes/producido_vs_vendido.html`
- Modify: `static/css/styles.css`
- Modify: `templates/base.html`

- [ ] Añadir semántica de columna y centrar el encabezado de estado.
- [ ] Limitar la altura útil del contenedor, habilitar desplazamiento vertical y fijar las celdas del encabezado.
- [ ] Dar prioridad específica a las alineaciones derecha y centrada del encabezado.
- [ ] Actualizar la versión de `styles.css` en el template base.
- [ ] Ejecutar la prueba nueva y confirmar que pasa.

### Task 3: Verificar regresiones y comportamiento real

**Files:**
- Test: `reportes/tests_producido_vs_vendido.py`

- [ ] Ejecutar las pruebas del reporte, `manage.py check` y `migrate --check` con PostgreSQL.
- [ ] Abrir la pantalla en navegador, recorrer la tabla verticalmente y confirmar que el encabezado sigue visible.
- [ ] Desplazar horizontalmente y confirmar que encabezado y datos avanzan juntos.
- [ ] Revisar consola y validar un viewport móvil.
